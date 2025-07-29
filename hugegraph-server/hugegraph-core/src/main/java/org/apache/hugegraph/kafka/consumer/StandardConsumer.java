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

package org.apache.hugegraph.kafka.consumer;

import java.nio.ByteBuffer;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.apache.hugegraph.kafka.BrokerConfig;
import org.apache.hugegraph.kafka.topic.HugeGraphSyncTopicBuilder;
import org.apache.hugegraph.meta.MetaManager;
import com.baidu.hugegraph.syncgateway.SyncMutationClient;

/**
 * Used to consume HugeGraphSyncTopic then send the data to slave cluster
 *
 * @author Scorpiour
 * @since 2022-01-25
 */
public class StandardConsumer extends ConsumerClient<String, ByteBuffer> {

    MetaManager manager = MetaManager.instance();
    private final SyncMutationClient client = new SyncMutationClient(
            MetaManager.instance().getKafkaSlaveServerHost(),
            MetaManager.instance().getKafkaSlaveServerPort());

    protected StandardConsumer(Properties props) {
        super(props);
    }

    @Override
    protected boolean handleRecord(ConsumerRecord<String, ByteBuffer> record) {
        if (BrokerConfig.getInstance().needKafkaSyncStorage()) {
            String[] graphInfo = HugeGraphSyncTopicBuilder.extractGraphs(record);
            String graphSpace = graphInfo[0];
            String graphName = graphInfo[1];
            ByteBuffer value = record.value();
            byte[] raw = value.array();

            client.sendMutation(graphSpace, graphName, raw);
            return true;
        }
        return false;
    }

}
