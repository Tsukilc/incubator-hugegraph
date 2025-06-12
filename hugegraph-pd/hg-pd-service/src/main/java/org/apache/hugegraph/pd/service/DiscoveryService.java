/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.pd.service;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.pd.RegistryService;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.common.PDRuntimeException;
import org.apache.hugegraph.pd.common.Useless;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.grpc.discovery.DiscoveryServiceGrpc;
import org.apache.hugegraph.pd.grpc.discovery.NodeInfo;
import org.apache.hugegraph.pd.grpc.discovery.NodeInfos;
import org.apache.hugegraph.pd.grpc.discovery.Query;
import org.apache.hugegraph.pd.grpc.discovery.RegisterInfo;
import org.apache.hugegraph.pd.raft.RaftEngine;
import org.apache.hugegraph.pd.raft.RaftStateListener;
import org.lognet.springboot.grpc.GRpcService;
import org.springframework.beans.factory.annotation.Autowired;

import io.grpc.ManagedChannel;
import lombok.extern.slf4j.Slf4j;

/**
 * DiscoveryService 提供了节点发现和注册的 gRPC 服务接口实现。
 * HugeGraph Server 实例或其他需要加入集群的组件可以通过此服务向 PD 注册自身信息，
 * 并查询已注册的其他节点信息。这对于集群成员的自动发现和维护至关重要。
 *
 * 主要功能：
 * - `register(NodeInfo request, ...)`: 允许一个节点（如 HugeGraph Server）向 PD 注册其信息，
 *   包括应用名称、版本、地址、标签（如核数）等。节点信息会带有 TTL (Time-To-Live)，
 *   注册后需要定期续约（通常通过心跳机制间接实现，或 PD 内部有清理过期节点的逻辑）。
 * - `getNodes(Query request, ...)`: 允许客户端根据应用名称等条件查询已注册的节点列表。
 *
 * 节点信息的存储和一致性由底层的 `RegistryService` (通常是 `DiscoveryMetaStore` 的封装)
 * 和 Raft 协议保证。当一个节点注册或其信息更新时，这些操作会通过 Raft 共识流程
 * (RaftEngine -> RaftStateMachine -> DiscoveryMetaStore) 持久化到 PD 集群的 KV 存储中。
 *
 * 对于 HugeGraph Server 的分布式任务调度，DiscoveryService 使得：
 * 1. PD 能够维护一个当前活跃的 HugeGraph Server 节点列表。
 * 2. HugeGraph Server 实例可以通过查询 PD 来发现集群中的其他 Server 节点，
 *    这对于 Server 之间的通信或协调（如果需要的话）是有帮助的。
 *    `ServerInfoManager` 在 Server 端可能会用到类似的服务来感知集群拓扑。
 */
@Useless("discovery related")
@Slf4j
@GRpcService
public class DiscoveryService extends DiscoveryServiceGrpc.DiscoveryServiceImplBase implements
                                                                                    ServiceGrpc,
                                                                                    RaftStateListener {

    static final AtomicLong id = new AtomicLong();
    private static final String CORES = "cores";
    RegistryService register = null;
    //LicenseVerifierService licenseVerifierService;
    @Autowired
    private PDConfig pdConfig;
    private ManagedChannel channel;

    @PostConstruct
    public void init() throws PDException {
        log.info("PDService init………… {}", pdConfig);
        RaftEngine.getInstance().init(pdConfig.getRaft());
        RaftEngine.getInstance().addStateListener(this);
        register = new RegistryService(pdConfig);
        //licenseVerifierService = new LicenseVerifierService(pdConfig);
    }

    private Pdpb.ResponseHeader newErrorHeader(PDException e) {
        Pdpb.ResponseHeader header = Pdpb.ResponseHeader.newBuilder().setError(
                                                 Pdpb.Error.newBuilder().setTypeValue(e.getErrorCode()).setMessage(e.getMessage()))
                                                        .build();
        return header;
    }

    /**
     * 处理节点注册请求。
     * 当一个 HugeGraph Server 实例启动时，它会调用此方法向 PD 注册自己的信息。
     * 注册信息包括节点地址、应用名、版本以及其他标签（如核数，用于许可证校验等）。
     *
     * @param request 包含待注册节点信息的 NodeInfo 对象。
     * @param observer 用于异步返回注册结果。
     */
    @Override
    public void register(NodeInfo request, io.grpc.stub.StreamObserver<RegisterInfo> observer) {
        // 检查当前 PD 节点是否为 Raft Leader。注册是写操作，必须由 Leader 处理。
        if (!isLeader()) {
            redirectToLeader(null, DiscoveryServiceGrpc.getRegisterMethod(), request, observer);
            return;
        }
        int outTimes = pdConfig.getDiscovery().getHeartbeatOutTimes(); // 获取心跳超时次数配置
        RegisterInfo registerInfo;
        try {
            // (注释掉的) 许可证校验逻辑：如果注册的是 HugeGraph Server ("hg")，
            // 会检查节点核数和已注册节点数是否符合许可证限制。
            if (request.getAppName().equals("hg")) {
                Query queryRequest = Query.newBuilder().setAppName(request.getAppName())
                                          .setVersion(request.getVersion()).build();
                NodeInfos nodes = register.getNodes(queryRequest); // 获取已注册的同类节点
                String address = request.getAddress();
                int nodeCount = nodes.getInfoCount() + 1; // 假设是新节点
                for (NodeInfo node : nodes.getInfoList()) {
                    if (node.getAddress().equals(address)) { // 如果是已有节点更新注册
                        nodeCount = nodes.getInfoCount();
                        break;
                    }
                }
                Map<String, String> labelsMap = request.getLabelsMap();
                String coreCount = labelsMap.get(CORES);
                if (StringUtils.isEmpty(coreCount)) {
                    throw new PDException(-1, "core count can not be null");
                }
                int core = Integer.parseInt(coreCount);
                //licenseVerifierService.verify(core, nodeCount); // 实际的许可证校验调用
            }

            // 调用底层的 RegistryService (通常是 DiscoveryMetaStore) 来执行注册。
            // DiscoveryMetaStore 会将节点信息和 TTL 存入 RocksDB，这个写操作会通过 Raft 复制。
            register.register(request, outTimes);

            String valueId = request.getId();
            // 返回注册信息，如果请求中 ID 为 "0" 或空，则分配一个新 ID (基于原子计数器，但实际持久化时可能用其他机制)。
            registerInfo = RegisterInfo.newBuilder().setNodeInfo(NodeInfo.newBuilder().setId(
                                               "0".equals(valueId) ?
                                               String.valueOf(id.incrementAndGet()) : valueId).build())
                                       .build();

        } catch (PDException e) { // PD 自定义异常处理
            registerInfo = RegisterInfo.newBuilder().setHeader(newErrorHeader(e)).build();
            log.debug("registerStore exception: ", e);
        } catch (PDRuntimeException ex) { // PD 自定义运行时异常处理
            Pdpb.Error error = Pdpb.Error.newBuilder().setTypeValue(ex.getErrorCode())
                                         .setMessage(ex.getMessage()).build();
            Pdpb.ResponseHeader header = Pdpb.ResponseHeader.newBuilder().setError(error).build();
            registerInfo = RegisterInfo.newBuilder().setHeader(header).build();
            log.debug("registerStore exception: ", ex);
        } catch (Exception e) { // 其他通用异常处理
            Pdpb.Error error =
                    Pdpb.Error.newBuilder().setTypeValue(Pdpb.ErrorType.UNKNOWN.getNumber())
                              .setMessage(e.getMessage()).build();
            Pdpb.ResponseHeader header = Pdpb.ResponseHeader.newBuilder().setError(error).build();
            registerInfo = RegisterInfo.newBuilder().setHeader(header).build();
        }
        observer.onNext(registerInfo);
        observer.onCompleted();
    }

    /**
     * 处理查询已注册节点信息的请求。
     * 客户端（如 HugeGraph Server）可以根据应用名称等条件查询当前已向 PD 注册的节点列表。
     *
     * @param request 包含查询条件的 Query 对象 (例如，appName)。
     * @param responseObserver 用于异步返回查询到的节点信息列表 (NodeInfos)。
     */
    @Override
    public void getNodes(Query request, io.grpc.stub.StreamObserver<NodeInfos> responseObserver) {
        // 检查当前 PD 节点是否为 Raft Leader。查询操作通常也建议在 Leader 上执行以获取最新数据。
        if (!isLeader()) {
            redirectToLeader(null, DiscoveryServiceGrpc.getGetNodesMethod(), request,
                             responseObserver);
            return;
        }
        // 调用底层的 RegistryService (DiscoveryMetaStore) 来执行查询。
        // DiscoveryMetaStore 会从 RocksDB 中读取节点信息。由于 RocksDB 的状态是通过 Raft 维护的，
        // 所以这里读取到的是集群一致性的数据。
        responseObserver.onNext(register.getNodes(request));
        responseObserver.onCompleted();
    }

    @Override
    public boolean isLeader() {
        return RaftEngine.getInstance().isLeader();
    }

}
