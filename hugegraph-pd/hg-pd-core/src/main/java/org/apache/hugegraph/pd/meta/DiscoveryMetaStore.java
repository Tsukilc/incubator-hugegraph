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

package org.apache.hugegraph.pd.meta;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.common.Useless;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.discovery.NodeInfo;
import org.apache.hugegraph.pd.grpc.discovery.NodeInfos;
import org.apache.hugegraph.pd.grpc.discovery.Query;

import lombok.extern.slf4j.Slf4j;

/**
 * DiscoveryMetaStore 提供了节点发现和注册信息的持久化存储机制。
 * 它继承自 `MetadataRocksDBStore`，因此使用 RocksDB 作为底层 KV 存储。
 * 所有对节点信息的写操作（如注册新节点）都会通过 Raft 协议进行复制和共识，
 * 确保在整个 PD 集群中节点信息的一致性。
 *
 * 此类主要被 `DiscoveryService` 使用，用于：
 * - 存储已注册节点 (如 HugeGraph Server) 的详细信息 (`NodeInfo`)。
 * - 为每个注册的节点信息设置 TTL (Time-To-Live)，以实现节点的自动过期和清理。
 *   节点需要定期续约其注册信息（通常通过心跳机制），否则其信息会因 TTL 到期而被移除。
 * - 提供根据应用名称、版本等条件查询已注册节点列表的功能。
 *
 * 核心方法：
 * - `register(NodeInfo nodeInfo, int outTimes)`: 将节点信息序列化后存入 RocksDB，并设置 TTL。
 *   TTL 的计算基于节点自身上报的心跳间隔 (`nodeInfo.getInterval()`) 和 PD 配置的超时倍数 (`outTimes`)。
 *   这个写操作会触发 Raft 共识。
 * - `getNodes(Query query)`: 根据查询条件从 RocksDB 中扫描并返回匹配的节点信息列表。
 *   这个读操作读取的是通过 Raft 复制和应用到本地状态机的数据。
 *
 * 通过 `MetadataKeyHelper`（尽管在此类中未直接显示，但其模式类似）或内部的 `toKey` 方法
 * 生成用于存储节点信息的 RocksDB key，通常包含前缀、应用名、版本和节点地址等，以便于查询和管理。
 */
@Useless("discovery related")
@Slf4j
public class DiscoveryMetaStore extends MetadataRocksDBStore {

    /**
     * appName --> address --> registryInfo
     * 定义了存储节点注册信息时键 (key) 的基本前缀和分隔符。
     */
    private static final String PREFIX = "REGIS-"; // 注册信息统一前缀
    private static final String SPLITTER = "-";   // 不同部分之间的分隔符

    public DiscoveryMetaStore(PDConfig pdConfig) {
        super(pdConfig);
    }

    /**
     * 注册一个节点信息到持久化存储中，并为其设置 TTL (Time-To-Live)。
     * 当节点在 TTL 时间内没有续约（通常通过心跳），其注册信息会自动过期。
     *
     * @param nodeInfo 包含节点详细信息的 protobuf 对象。
     * @param outTimes 心跳超时倍数。TTL = (nodeInfo.getInterval() / 1000) * outTimes 秒。
     * @throws PDException 如果存储操作失败。
     */
    public void register(NodeInfo nodeInfo, int outTimes) throws PDException {
        // 1. 将节点信息 (appName, version, address) 组合成一个唯一的 RocksDB key。
        byte[] key = toKey(nodeInfo.getAppName(), nodeInfo.getVersion(), nodeInfo.getAddress());
        // 2. 将 NodeInfo protobuf 对象序列化为字节数组作为 value。
        byte[] value = nodeInfo.toByteArray();
        // 3. 计算 TTL (单位：秒)。nodeInfo.getInterval() 通常是节点上报的心跳间隔（毫秒）。
        long ttlInSeconds = (nodeInfo.getInterval() / 1000) * outTimes;

        // 4. 调用父类 MetadataRocksDBStore 的 putWithTTL 方法。
        //    - 此方法会将 key-value 对和 TTL 信息包装成 KVOperation。
        //    - 然后通过 RaftEngine.addTask() 提交给 Raft Group 进行共识。
        //    - RaftStateMachine.onApply() 在每个节点上应用此操作，最终调用 RocksDB 的带 TTL 的写入。
        putWithTTL(key, value, ttlInSeconds);
    }

    /**
     * 根据应用名称、版本和地址构造用于存储节点信息的 RocksDB key。
     * @param appName 应用名称
     * @param version 应用版本
     * @param address 节点地址
     * @return 序列化后的 key 字节数组。
     */
    byte[] toKey(String appName, String version, String address) {
        StringBuilder builder = getPrefixBuilder(appName, version);
        builder.append(SPLITTER);
        builder.append(address);
        return builder.toString().getBytes();
    }

    private StringBuilder getPrefixBuilder(String appName, String version) {
        StringBuilder builder = new StringBuilder();
        builder.append(PREFIX);
        if (!StringUtils.isEmpty(appName)) {
            builder.append(appName);
            builder.append(SPLITTER);
        }
        if (!StringUtils.isEmpty(version)) {
            builder.append(version);
        }
        return builder;
    }

    public NodeInfos getNodes(Query query) {
        List<NodeInfo> nodeInfos = null;
        try {
            // 1. 根据查询条件（应用名、版本）构造用于前缀扫描的 RocksDB key 前缀。
            StringBuilder builder = getPrefixBuilder(query.getAppName(),
                                                     query.getVersion());
            // 2. 调用父类的 getInstanceListWithTTL 方法 (或类似的扫描方法) 从 RocksDB 中获取所有匹配前缀的节点信息。
            //    - NodeInfo.parser() 用于将从 RocksDB 读取的字节数组反序列化为 NodeInfo 对象。
            //    - 这个读取操作访问的是已经通过 Raft 复制和应用到本地状态机的数据，因此保证了数据的一致性。
            //    - 底层实现会处理 TTL，确保过期的节点信息不会被返回。
            nodeInfos = getInstanceListWithTTL( // 假设 getInstanceListWithTTL 是父类提供的方法或本类实现
                    NodeInfo.parser(),
                    builder.toString().getBytes());
            builder.setLength(0); // 清理 StringBuilder
        } catch (PDException e) {
            log.error("An error occurred getting data from the store,{}", e);
            // 如果发生错误，返回空的节点列表或进行错误处理
            return NodeInfos.newBuilder().build();
        }

        // 3. 如果查询中包含标签 (labels) 条件，则对扫描结果进行进一步过滤。
        if (query.getLabelsMap() != null && !query.getLabelsMap().isEmpty()) {
            List<NodeInfo> result = new LinkedList<NodeInfo>();
            if (nodeInfos != null) { // 确保 nodeInfos 不是 null
                for (NodeInfo node : nodeInfos) {
                    if (labelMatch(node, query)) { // 检查节点的标签是否满足查询条件
                        result.add(node);
                    }
                }
            }
            return NodeInfos.newBuilder().addAllInfo(result).build();
        }

        // 4. 如果没有标签过滤，直接返回所有扫描到的节点信息。
        return NodeInfos.newBuilder().addAllInfo(nodeInfos == null ? List.of() : nodeInfos).build();
    }

    private boolean labelMatch(NodeInfo node, Query query) {
        Map<String, String> labelsMap = node.getLabelsMap();
        for (Map.Entry<String, String> entry : query.getLabelsMap().entrySet()) {
            if (!entry.getValue().equals(labelsMap.get(entry.getKey()))) {
                return false;
            }
        }
        return true;
    }
}
