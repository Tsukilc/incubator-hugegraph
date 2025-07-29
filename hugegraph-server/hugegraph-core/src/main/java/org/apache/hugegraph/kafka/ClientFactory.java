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

import java.nio.ByteBuffer;

import org.apache.hugegraph.kafka.consumer.StandardConsumer;
import org.apache.hugegraph.kafka.consumer.StandardConsumerBuilder;
import org.apache.hugegraph.kafka.producer.ProducerClient;
import org.apache.hugegraph.kafka.producer.StandardProducerBuilder;
import org.apache.hugegraph.kafka.producer.SyncConfProducer;
import org.apache.hugegraph.kafka.producer.SyncConfProducerBuilder;

/**
 * @author Scorpiour
 * @since 2022-03-08
 */
public class ClientFactory {

    /**
     * Producers
     */
    public final ProducerClient<String, ByteBuffer> standardProducer;
    public final SyncConfProducer syncConfProducer;
    /**
     * Consumers
     */
    public final StandardConsumer standardConsumer;

    private ClientFactory() {
        standardProducer = new StandardProducerBuilder().build();
        syncConfProducer = new SyncConfProducerBuilder().build();
        standardConsumer = new StandardConsumerBuilder().build();
    }

    public static ClientFactory getInstance() {
        return ClientInstanceHolder.factory;
    }

    public ProducerClient<String, ByteBuffer> getStandardProducer() {
        return this.standardProducer;
    }

    public StandardConsumer getStandardConsumer() {
        return this.standardConsumer;
    }

    public SyncConfProducer getSyncConfProducer() {
        return this.syncConfProducer;
    }

    private static class ClientInstanceHolder {

        public static final ClientFactory factory = new ClientFactory();
    }
}
