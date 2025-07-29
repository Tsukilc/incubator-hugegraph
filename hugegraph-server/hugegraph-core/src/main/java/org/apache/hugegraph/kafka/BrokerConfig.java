/*
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.kafka;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.meta.MetaManager;
import org.apache.hugegraph.meta.PdMetaDriver;
import org.apache.hugegraph.pd.client.DiscoveryClient;
import org.apache.hugegraph.pd.client.DiscoveryClientImpl;
import org.apache.hugegraph.pd.client.PDConfig;
import org.apache.hugegraph.pd.grpc.discovery.NodeInfo;
import org.apache.hugegraph.pd.grpc.discovery.NodeInfos;
import org.apache.hugegraph.pd.grpc.discovery.Query;
import org.apache.hugegraph.pd.grpc.discovery.RegisterType;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

import com.google.common.collect.ImmutableMap;

/**
 * BrokerConfig used to init producer and consumer
 *
 * @author Scorpiour
 * @since 2022-01-18
 */
public final class BrokerConfig {

    private static final Logger LOG = Log.logger(BrokerConfig.class);
    private static final String KAFKA_APP_KEY = "HUGEGRAPH_KAFKA_APP";
    private static final String PD_KAFKA_HOST = "KAFKA_HOST";
    private static final String PD_KAFKA_PORT = "KAFKA_PORT";
    private static final String PD_KAFKA_CLUSTER_ROLE = "KAFKA_CLUSTER_ROLE";
    private static final String PD_KAFKA_PARTITION_COUNT = "PD_KAFKA_PARTITION_COUNT";
    private static String PD_PEERS;
    private final MetaManager manager;
    private final String SYNC_BROKER_KEY;
    private final String SYNC_STORAGE_KEY;
    private final String FILTER_GRAPH_KEY;
    private final String FILTER_GRAPH_SPACE_KEY;
    private final Set<String> filteredGraph = ConcurrentHashMap.newKeySet();
    private final Set<String> filteredGraphSpace = ConcurrentHashMap.newKeySet();
    private volatile boolean needSyncBroker = false;
    private volatile boolean needSyncStorage = false;
    private DiscoveryClient client;

    private BrokerConfig() {
        this.manager = MetaManager.instance();
        this.SYNC_BROKER_KEY = manager.kafkaSyncBrokerKey();
        this.SYNC_STORAGE_KEY = manager.kafkaSyncStorageKey();
        this.FILTER_GRAPH_KEY = manager.kafkaFilterGraphKey();
        this.FILTER_GRAPH_SPACE_KEY = manager.kafkaFilterGraphspaceKey();

        if (manager.isReady()) {
            this.updateNeedSyncBroker();
            this.updateNeedSyncStorage();
            manager.listenKafkaConfig(this::kafkaConfigEventHandler);
            // sync config to pd

            updatePDRegisterInfo();
        } else {
            needSyncBroker = true;
            needSyncStorage = false;
        }

    }

    public static void setPdPeers(String pdPeers) {
        PD_PEERS = pdPeers;
    }

    public static BrokerConfig getInstance() {
        return ConfigHolder.instance;
    }

    private void updatePDRegisterInfo() {
        if (StringUtils.isNotBlank(BrokerConfig.PD_PEERS) && (isMaster() || isSlave())) {
            try {
                String kafkaHost = ConfigHolder.getKafkaHost();
                String kafkaPort = ConfigHolder.getKafkaPort();
                String clusterRole = ConfigHolder.getClusterRole().name();
                int partitionCount = ConfigHolder.getPartitionCount();

                String address = kafkaHost + ":" + kafkaPort;
                if (null == this.client) {
                    PDConfig pdConfig = PDConfig.of(BrokerConfig.PD_PEERS);
                    pdConfig.setAuthority(PdMetaDriver.PDAuthConfig.service(),
                                          PdMetaDriver.PDAuthConfig.token());
                    this.client = DiscoveryClientImpl.newBuilder()
                                                     .setAppName(KAFKA_APP_KEY)
                                                     .setCenterAddress(BrokerConfig.PD_PEERS)
                                                     .setPdConfig(pdConfig)
                                                     .setAddress(address)
                                                     .setVersion("1.0.0")
                                                     .setType(RegisterType.Heartbeat)
                                                     .setId(KAFKA_APP_KEY)
                                                     .setDelay(15 * 1000)
                                                     .setLabels(
                                                             new ImmutableMap.Builder<String,
                                                                     String>()
                                                                     .put(PD_KAFKA_HOST, kafkaHost)
                                                                     .put(PD_KAFKA_PORT, kafkaPort)
                                                                     .put(PD_KAFKA_CLUSTER_ROLE,
                                                                          clusterRole)
                                                                     .put(PD_KAFKA_PARTITION_COUNT,
                                                                          String.valueOf(
                                                                                  partitionCount))
                                                                     .build()
                                                     )
                                                     .build();
                }
                Query query = Query.newBuilder()
                                   .setAppName(KAFKA_APP_KEY)
                                   .build();
                NodeInfos nodes = client.getNodeInfos(query);
                // ignore if exists, otherwise register it
                int count = nodes.getInfoCount();
                if (count != 0) {
                    return;
                }

                client.scheduleTask();

            } catch (Exception e) {
                LOG.error("Meet error when register kafka to pd {}", e);
            }
        }
    }

    private <T> void kafkaConfigEventHandler(T response) {
        Map<String, String> events = manager.extractKVFromResponse(response);
        for (Map.Entry<String, String> entry : events.entrySet()) {
            String key = entry.getKey();
            if (this.SYNC_BROKER_KEY.equals(key)) {
                this.needSyncBroker = "1".equals(entry.getValue());
            } else if (this.SYNC_STORAGE_KEY.equals(key)) {
                this.needSyncStorage = "1".equals(entry.getValue());
            } else if (this.FILTER_GRAPH_KEY.equals(key)) {
                String[] graphs = entry.getValue().split(",");
                this.filteredGraph.clear();
                this.filteredGraph.addAll(Arrays.asList(graphs));
            } else if (this.FILTER_GRAPH_SPACE_KEY.equals(key)) {
                String[] graphSpaces = entry.getValue().split(",");
                this.filteredGraphSpace.clear();
                this.filteredGraphSpace.addAll(Arrays.asList(graphSpaces));
            }
        }
    }

    private void updateNeedSyncBroker() {
        String res = manager.kafkaGetRaw(this.SYNC_BROKER_KEY);
        this.needSyncBroker = "1".equals(res);
    }

    private void updateNeedSyncStorage() {
        String res = manager.kafkaGetRaw(this.SYNC_STORAGE_KEY);
        this.needSyncStorage = "1".equals(res);
    }

    /**
     * Indicates when if need sync data between hugegraph-server & broker
     * Should be functioned dynamically
     * If returns true, both Master and slave will be produce topics to broker
     *
     * @return
     */
    public boolean needKafkaSyncBroker() {
        return this.needSyncBroker;
    }

    /**
     * Indicates when if need sync data between hugegraph-server & storage
     * Should be functioned dynamically
     * If returns true, Master's consumer will consume data from broker, then push to slave,
     * while Slave will consume data from broker, then commit them to storage
     *
     * @return
     */
    public boolean needKafkaSyncStorage() {
        return this.needSyncStorage;
    }

    public HugeGraphClusterRole getClusterRole() {
        return ConfigHolder.clusterRole;
    }

    public int getPartitionCount() {
        return ConfigHolder.partitionCount;
    }

    public String getKafkaHost() {
        return ConfigHolder.brokerHost;
    }

    public String getKafkaPort() {
        return ConfigHolder.brokerPort;
    }

    public Boolean isMaster() {
        return HugeGraphClusterRole.MASTER.equals(ConfigHolder.clusterRole);
    }

    public Boolean isSlave() {
        return HugeGraphClusterRole.SLAVE.equals(ConfigHolder.clusterRole);
    }

    public Boolean isKafkaEnabled() {
        return this.isMaster() || this.isSlave();
    }

    public String getSyncGroupId() {
        return "hugegraph-sync-consumer-group";
    }

    public String getSyncGroupInstanceId() {
        return "hugegraph-sync-consumer-instance-1";
    }

    public String getMutateGroupId() {
        return "hugegraph-mutate-consumer-group";
    }

    public String getMutateGroupInstanceId() {
        return "hugegraph-mutate-consumer-instance-1";
    }

    public String getConfGroupId() {
        return "hugegraph-conf-consumer-group";
    }

    public String getConfGroupInstanceId() {
        return "hugegraph-conf-consumer-instance-1";
    }

    public String getConfPrefix() {
        return "GLOBAL-";
    }

    public boolean graphSpaceFiltered(String graphSpace) {
        return filteredGraphSpace.contains(graphSpace);
    }

    public boolean graphFiltered(String graphSpace, String graph) {
        return this.filteredGraph.contains(graph);
    }

    public synchronized void close() {
        ConfigHolder.closeClient();
    }

    private static class ConfigHolder {

        public final static BrokerConfig instance = new BrokerConfig();
        private static DiscoveryClient client = null;
        private static Map<String, String> PD_CONFIG_MAP = null;
        public final static HugeGraphClusterRole clusterRole = ConfigHolder.getClusterRole();
        public final static String brokerHost = ConfigHolder.getKafkaHost();
        public final static String brokerPort = ConfigHolder.getKafkaPort();
        public final static int partitionCount = ConfigHolder.getPartitionCount();

        private synchronized static void loadPDRegisterInfo() {
            if (null != PD_CONFIG_MAP) {
                return;
            }
            try {
                if (null == client) {
                    PDConfig pdConfig = PDConfig.of(BrokerConfig.PD_PEERS);
                    pdConfig.setAuthority(PdMetaDriver.PDAuthConfig.service(),
                                          PdMetaDriver.PDAuthConfig.token());
                    client = DiscoveryClientImpl.newBuilder()
                                                .setAppName(KAFKA_APP_KEY)
                                                .setCenterAddress(BrokerConfig.PD_PEERS)
                                                .setPdConfig(pdConfig)
                                                .setDelay(15 * 1000)
                                                .setLabels(ImmutableMap.of())
                                                .build();
                }
                Query query = Query.newBuilder()
                                   .setAppName(KAFKA_APP_KEY)
                                   .build();
                NodeInfos nodes = client.getNodeInfos(query);
                int count = nodes.getInfoCount();
                if (count > 0) {
                    NodeInfo info = nodes.getInfo(0);
                    Map<String, String> map = info.getLabelsMap();
                    PD_CONFIG_MAP = map;
                }
            } catch (Exception e) {
                LOG.error("Meet error when load kafka config from pd {}", e);
                PD_CONFIG_MAP = null;
            }
        }

        public synchronized static void closeClient() {
            if (null != client) {
                client.close();
                client = null;
            }
        }

        private static HugeGraphClusterRole getClusterRole() {
            try {
                MetaManager manager = MetaManager.instance();
                if (!manager.isReady()) {
                    loadPDRegisterInfo();
                    if (null != PD_CONFIG_MAP) {
                        String clusterRole =
                                PD_CONFIG_MAP.getOrDefault(PD_KAFKA_CLUSTER_ROLE, "NONE");
                        return HugeGraphClusterRole.fromName(clusterRole);
                    } else {
                        return HugeGraphClusterRole.NONE;
                    }
                } else {
                    String val = manager.getHugeGraphClusterRole();
                    return HugeGraphClusterRole.fromName(val);
                }
            } catch (Exception e) {
                return HugeGraphClusterRole.NONE;
            }
        }

        private static String getKafkaHost() {
            MetaManager manager = MetaManager.instance();
            if (!manager.isReady()) {
                loadPDRegisterInfo();
                if (null != PD_CONFIG_MAP) {
                    return PD_CONFIG_MAP.getOrDefault(PD_KAFKA_HOST, "");
                } else {
                    return "";
                }
            } else {
                return manager.getKafkaBrokerHost();
            }
        }

        private static String getKafkaPort() {
            MetaManager manager = MetaManager.instance();
            if (!manager.isReady()) {
                loadPDRegisterInfo();
                if (null != PD_CONFIG_MAP) {
                    return PD_CONFIG_MAP.getOrDefault(PD_KAFKA_PORT, "9092");
                } else {
                    return "";
                }
            } else {
                return manager.getKafkaBrokerPort();
            }
        }

        private static Integer getPartitionCount() {
            MetaManager manager = MetaManager.instance();
            if (!manager.isReady()) {
                loadPDRegisterInfo();
                if (null != PD_CONFIG_MAP) {
                    String partitionCount =
                            PD_CONFIG_MAP.getOrDefault(PD_KAFKA_PARTITION_COUNT, "1");
                    return Integer.parseInt(partitionCount);
                } else {
                    return 1;
                }
            } else {
                return manager.getPartitionCount();
            }
        }
    }
}
