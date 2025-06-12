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

package org.apache.hugegraph.pd.raft;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.raft.auth.IpAuthHandler;

import com.alipay.remoting.ExtendedNettyChannelHandler;
import com.alipay.remoting.config.BoltServerOption;
import com.alipay.sofa.jraft.JRaftUtils;
import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.RaftGroupService;
import com.alipay.sofa.jraft.ReplicatorGroup;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.core.Replicator;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.option.RpcOptions;
import com.alipay.sofa.jraft.rpc.RaftRpcServerFactory;
import com.alipay.sofa.jraft.rpc.RpcServer;
import com.alipay.sofa.jraft.rpc.impl.BoltRpcServer;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.ThreadId;
import com.alipay.sofa.jraft.util.internal.ThrowUtil;

import io.netty.channel.ChannelHandler;
import lombok.extern.slf4j.Slf4j;

@Slf4j
// RaftEngine 是 PD (Placement Driver) 实现 Raft 分布式一致性算法的核心引擎。
// PD 作为一个独立的组件，负责管理 HugeGraph 集群的元数据、调度任务（如分区切分、负载均衡）等。
// 为了保证这些关键数据的强一致性和高可用性，PD 内部采用 Raft 协议。
// RaftEngine 封装了 JRaft 库的实现细节，提供了 Raft 节点的创建、启动、关闭，
// 以及处理 Raft 日志复制、领导者选举、成员变更等核心功能。
// 所有需要通过 Raft 达成共识的操作（例如，保存一个 PD 内部任务的请求），
// 最终都会通过 RaftEngine 提交到 Raft Group，并由其分发到各个 PD 节点的 RaftStateMachine 进行应用。
public class RaftEngine {

    private static final RaftEngine INSTANCE = new RaftEngine();
    private final RaftStateMachine stateMachine;
    private PDConfig.Raft config;
    private RaftGroupService raftGroupService;
    private RpcServer rpcServer;
    private Node raftNode;
    private RaftRpcClient raftRpcClient;

    public RaftEngine() {
        this.stateMachine = new RaftStateMachine();
    }

    public static RaftEngine getInstance() {
        return INSTANCE;
    }

    // 初始化 RaftEngine，包括设置 Raft 节点、启动 RPC 服务、初始化状态机等。
    public boolean init(PDConfig.Raft config) {
        if (this.raftNode != null) {
            return false;
        }
        this.config = config;

        raftRpcClient = new RaftRpcClient();
        raftRpcClient.init(new RpcOptions());

        String groupId = "pd_raft";
        String raftPath = config.getDataPath() + "/" + groupId;
        new File(raftPath).mkdirs();

        new File(config.getDataPath()).mkdirs();
        Configuration initConf = new Configuration();
        initConf.parse(config.getPeersList());
        if (config.isEnable() && config.getPeersList().length() < 3) {
            log.error("The RaftEngine parameter is incorrect." +
                      " When RAFT is enabled, the number of peers " + "cannot be less than 3");
        }
        // Set node parameters, including the log storage path and state machine instance
        NodeOptions nodeOptions = new NodeOptions();
        nodeOptions.setFsm(stateMachine);
        nodeOptions.setEnableMetrics(true);
        // Log path
        nodeOptions.setLogUri(raftPath + "/log");
        // raft metadata path
        nodeOptions.setRaftMetaUri(raftPath + "/meta");
        // Snapshot path
        nodeOptions.setSnapshotUri(raftPath + "/snapshot");
        // Initial cluster
        nodeOptions.setInitialConf(initConf);
        // Snapshot interval
        nodeOptions.setSnapshotIntervalSecs(config.getSnapshotInterval());

        nodeOptions.setRpcConnectTimeoutMs(config.getRpcTimeout());
        nodeOptions.setRpcDefaultTimeout(config.getRpcTimeout());
        nodeOptions.setRpcInstallSnapshotTimeout(config.getRpcTimeout());
        // Set the raft configuration
        RaftOptions raftOptions = nodeOptions.getRaftOptions();

        nodeOptions.setEnableMetrics(true);

        final PeerId serverId = JRaftUtils.getPeerId(config.getAddress());

        rpcServer = createRaftRpcServer(config.getAddress(), initConf.getPeers());
        // construct raft group and start raft
        this.raftGroupService =
                new RaftGroupService(groupId, serverId, nodeOptions, rpcServer, true);
        this.raftNode = raftGroupService.start(false);
        log.info("RaftEngine start successfully: id = {}, peers list = {}", groupId,
                 nodeOptions.getInitialConf().getPeers());
        return this.raftNode != null;
    }

    /**
     * Create a Raft RPC Server for communication between PDs
     */
    private RpcServer createRaftRpcServer(String raftAddr, List<PeerId> peers) {
        Endpoint endpoint = JRaftUtils.getEndPoint(raftAddr);
        RpcServer rpcServer = RaftRpcServerFactory.createRaftRpcServer(endpoint);
        configureRaftServerIpWhitelist(peers, rpcServer);
        RaftRpcProcessor.registerProcessor(rpcServer, this);
        rpcServer.init(null);
        return rpcServer;
    }

    private static void configureRaftServerIpWhitelist(List<PeerId> peers, RpcServer rpcServer) {
        if (rpcServer instanceof BoltRpcServer) {
            ((BoltRpcServer) rpcServer).getServer().option(
                    BoltServerOption.EXTENDED_NETTY_CHANNEL_HANDLER,
                    new ExtendedNettyChannelHandler() {
                        @Override
                        public List<ChannelHandler> frontChannelHandlers() {
                            return Collections.singletonList(
                                    IpAuthHandler.getInstance(
                                            peers.stream()
                                                 .map(PeerId::getIp)
                                                 .collect(Collectors.toSet())
                                    )
                            );
                        }

                        @Override
                        public List<ChannelHandler> backChannelHandlers() {
                            return Collections.emptyList();
                        }
                    }
            );
        }
    }

    public void shutDown() {
        if (this.raftGroupService != null) {
            this.raftGroupService.shutdown();
            try {
                this.raftGroupService.join();
            } catch (final InterruptedException e) {
                this.raftNode = null;
                ThrowUtil.throwException(e);
            }
            this.raftGroupService = null;
        }
        if (this.rpcServer != null) {
            this.rpcServer.shutdown();
            this.rpcServer = null;
        }
        if (this.raftNode != null) {
            this.raftNode.shutdown();
        }
        this.raftNode = null;
    }

    // 判断当前节点是否为 Raft 集群的领导者。
    public boolean isLeader() {
        return this.raftNode.isLeader(true);
    }

    /**
     * 向 Raft Group 提交一个任务 (Task) 以进行共识处理。
     * 注意：这里的 `Task` 是 JRaft 定义的 `com.alipay.sofa.jraft.entity.Task`，
     * 它通常包装了一个需要通过 Raft 协议复制和应用的具体操作，例如一个 `KVOperation`。
     * 这个 `KVOperation` 可能代表着对 PD 元数据的修改请求，比如创建一个新的 PD 内部任务（如分区切分任务）的持久化请求。
     *
     * 此方法是 PD 服务中所有需要通过 Raft 达成共识的操作的统一入口点。
     *
     * 工作流程：
     * 1. 检查当前 PD 节点是否为 Raft Group 的 Leader。
     *    - 如果不是 Leader，则直接拒绝该任务，并通过 `KVStoreClosure` 返回一个 "Not Leader" 错误。
     *      客户端（通常是 gRPC 服务层）需要将请求重定向到当前的 Leader 节点。
     * 2. 如果当前节点是 Leader，则调用 `this.raftNode.apply(task)` 将该任务提交给 Raft Group。
     *    - JRaft 库会负责将这个任务序列化成 Raft 日志条目。
     *    - Leader 节点会将这个日志条目复制到 Raft Group 中的其他 Follower 节点。
     *    - 一旦大多数节点确认接收了该日志条目 (即日志被 "committed")，
     *      RaftEngine (通过其内部的 RaftStateMachine) 会在所有节点上“应用”(apply) 这个日志条目中包含的操作。
     *      这意味着 `RaftStateMachine.onApply()` 方法会被调用，进而执行 `KVOperation` 中定义的数据修改逻辑。
     *
     * 这个机制确保了所有对 PD 关键数据的修改请求（如 PD 内部任务的创建、更新、删除等）
     * 都会以一致的方式在所有 PD 节点上被持久化和应用，从而保证了分布式环境下的数据一致性。
     * 这就是 PD 内部任务 *持久化请求* 如何在分布式环境中被发起和初步处理的过程。
     * 最终的“保存”动作发生在 RaftStateMachine 中。
     *
     * @param task 一个 JRaft Task 对象，通常包装了需要通过 Raft 共识处理的操作（如 KVOperation）。
     */
    public void addTask(Task task) {
        if (!isLeader()) {
            KVStoreClosure closure = (KVStoreClosure) task.getDone();
            closure.setError(Pdpb.Error.newBuilder().setType(Pdpb.ErrorType.NOT_LEADER).build());
            closure.run(new Status(RaftError.EPERM, "Not leader"));
            return;
        }
        this.raftNode.apply(task);
    }

    public void addStateListener(RaftStateListener listener) {
        this.stateMachine.addStateListener(listener);
    }

    public void addTaskHandler(RaftTaskHandler handler) {
        this.stateMachine.addTaskHandler(handler);
    }

    public PDConfig.Raft getConfig() {
        return this.config;
    }

    // 获取当前 Raft 集群的领导者 PeerId。
    public PeerId getLeader() {
        return raftNode.getLeaderId();
    }

    /**
     * Send a message to the leader to get the grpc address;
     */
    public String getLeaderGrpcAddress() throws ExecutionException, InterruptedException {
        if (isLeader()) {
            return config.getGrpcAddress();
        }

        if (raftNode.getLeaderId() == null) {
            waitingForLeader(10000);
        }

        return raftRpcClient.getGrpcAddress(raftNode.getLeaderId().getEndpoint().toString()).get()
                            .getGrpcAddress();
    }

    public Metapb.Member getLocalMember() {
        Metapb.Member.Builder builder = Metapb.Member.newBuilder();
        builder.setClusterId(config.getClusterId());
        builder.setRaftUrl(config.getAddress());
        builder.setDataPath(config.getDataPath());
        builder.setGrpcUrl(config.getGrpcAddress());
        builder.setState(Metapb.StoreState.Up);
        return builder.build();
    }

    public List<Metapb.Member> getMembers() {
        List<Metapb.Member> members = new ArrayList<>();

        List<PeerId> peers = raftNode.listPeers();
        peers.addAll(raftNode.listLearners());
        var learners = new HashSet<>(raftNode.listLearners());

        for (PeerId peerId : peers) {
            Metapb.Member.Builder builder = Metapb.Member.newBuilder();
            builder.setClusterId(config.getClusterId());
            CompletableFuture<RaftRpcProcessor.GetMemberResponse> future =
                    raftRpcClient.getGrpcAddress(peerId.getEndpoint().toString());

            Metapb.ShardRole role = Metapb.ShardRole.Follower;
            if (peerEquals(peerId, raftNode.getLeaderId())) {
                role = Metapb.ShardRole.Leader;
            } else if (learners.contains(peerId)) {
                role = Metapb.ShardRole.Learner;
                var state = getReplicatorState(peerId);
                if (state != null) {
                    builder.setReplicatorState(state.name());
                }
            }

            builder.setRole(role);

            try {
                if (future.isCompletedExceptionally()) {
                    log.error("failed to getGrpcAddress of {}", peerId.getEndpoint().toString());
                    builder.setState(Metapb.StoreState.Offline);
                    builder.setRaftUrl(peerId.getEndpoint().toString());
                    members.add(builder.build());
                } else {
                    RaftRpcProcessor.GetMemberResponse response = future.get();
                    builder.setState(Metapb.StoreState.Up);
                    builder.setRaftUrl(response.getRaftAddress());
                    builder.setDataPath(response.getDatePath());
                    builder.setGrpcUrl(response.getGrpcAddress());
                    builder.setRestUrl(response.getRestAddress());
                    members.add(builder.build());
                }
            } catch (Exception e) {
                log.error("failed to getGrpcAddress of {}.", peerId.getEndpoint().toString(), e);
                builder.setState(Metapb.StoreState.Offline);
                builder.setRaftUrl(peerId.getEndpoint().toString());
                members.add(builder.build());
            }

        }
        return members;
    }

    public Status changePeerList(String peerList) {
        AtomicReference<Status> result = new AtomicReference<>();
        try {
            String[] peers = peerList.split(",", -1);
            if ((peers.length & 1) != 1) {
                throw new PDException(-1, "the number of peer list must be odd.");
            }
            Configuration newPeers = new Configuration();
            newPeers.parse(peerList);
            CountDownLatch latch = new CountDownLatch(1);
            this.raftNode.changePeers(newPeers, status -> {
                result.set(status);
                latch.countDown();
            });
            latch.await();
        } catch (Exception e) {
            log.error("failed to changePeerList to {}", peerList, e);
            result.set(new Status(-1, e.getMessage()));
        }
        return result.get();
    }

    public PeerId waitingForLeader(long timeOut) {
        PeerId leader = getLeader();
        if (leader != null) {
            return leader;
        }

        synchronized (this) {
            leader = getLeader();
            long start = System.currentTimeMillis();
            while ((System.currentTimeMillis() - start < timeOut) && (leader == null)) {
                try {
                    this.wait(1000);
                } catch (InterruptedException e) {
                    log.error("Raft wait for leader exception", e);
                }
                leader = getLeader();
            }
            return leader;
        }

    }

    public Node getRaftNode() {
        return raftNode;
    }

    private boolean peerEquals(PeerId p1, PeerId p2) {
        if (p1 == null && p2 == null) {
            return true;
        }
        if (p1 == null || p2 == null) {
            return false;
        }
        return Objects.equals(p1.getIp(), p2.getIp()) && Objects.equals(p1.getPort(), p2.getPort());
    }

    private Replicator.State getReplicatorState(PeerId peerId) {
        var replicateGroup = getReplicatorGroup();
        if (replicateGroup == null) {
            return null;
        }

        ThreadId threadId = replicateGroup.getReplicator(peerId);
        if (threadId == null) {
            return null;
        } else {
            Replicator r = (Replicator) threadId.lock();
            if (r == null) {
                return Replicator.State.Probe;
            }
            Replicator.State result = getState(r);
            threadId.unlock();
            return result;
        }
    }

    private ReplicatorGroup getReplicatorGroup() {
        var clz = this.raftNode.getClass();
        try {
            var f = clz.getDeclaredField("replicatorGroup");
            f.setAccessible(true);
            var group = (ReplicatorGroup) f.get(this.raftNode);
            f.setAccessible(false);
            return group;
        } catch (NoSuchFieldException | IllegalAccessException e) {
            log.info("getReplicatorGroup: error {}", e.getMessage());
            return null;
        }
    }

    private Replicator.State getState(Replicator r) {
        var clz = r.getClass();
        try {
            var f = clz.getDeclaredField("state");
            f.setAccessible(true);
            var state = (Replicator.State) f.get(this.raftNode);
            f.setAccessible(false);
            return state;
        } catch (NoSuchFieldException | IllegalAccessException e) {
            log.info("getReplicatorGroup: error {}", e.getMessage());
            return null;
        }
    }
}
