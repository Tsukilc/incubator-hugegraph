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
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.apache.hugegraph.pd.store.HgKVStore;
import org.apache.hugegraph.pd.store.KV;

import com.google.protobuf.Parser;

public class MetadataRocksDBStore extends MetadataStoreBase {

    HgKVStore store;

    PDConfig pdConfig;

    public MetadataRocksDBStore(PDConfig pdConfig) {
        store = MetadataFactory.getStore(pdConfig);
        this.pdConfig = pdConfig;
    }

    public HgKVStore getStore() {
        if (store == null) {
            store = MetadataFactory.getStore(pdConfig);
        }
        return store;
    }

    @Override
    public byte[] getOne(byte[] key) throws PDException {
        try {
            byte[] bytes = store.get(key);
            return bytes;
        } catch (Exception e) {
            throw new PDException(Pdpb.ErrorType.ROCKSDB_READ_ERROR_VALUE, e);
        }
    }

    @Override
    public <E> E getOne(Parser<E> parser, byte[] key) throws PDException {
        try {
            byte[] bytes = store.get(key);
            if (ArrayUtils.isEmpty(bytes)) {
                return null;
            }
            return parser.parseFrom(bytes);
        } catch (Exception e) {
            throw new PDException(Pdpb.ErrorType.ROCKSDB_READ_ERROR_VALUE, e);
        }
    }

    /**
     * 将键值对持久化到 RocksDB。
     * 这是一个通用的存储方法，用于将任意的键值对写入底层的 RocksDB 实例。
     *
     * 在 PD 内部任务的保存流程中 (例如，当 TaskInfoMeta.addSplitTask() 被调用时)，
     * 此方法扮演着一个关键角色：
     * 1. TaskInfoMeta 将任务信息序列化为 value，并生成一个唯一的 key。
     * 2. TaskInfoMeta 调用本类的 `put(key, value)` 方法。
     * 3. **重要**：此 `put` 方法的调用路径实际上是 Raft 共识流程的一部分。
     *    它并不是直接写入本地 RocksDB 然后结束。相反，它的调用通常源于 `RaftStateMachine.onApply()`。
     *    - 当一个 "保存任务" 的操作通过 `RaftEngine.addTask()` 提交后，它会被复制到 Raft Group 的所有节点。
     *    - 一旦该操作对应的 Raft 日志条目被大多数节点确认 (committed)，`RaftStateMachine.onApply()` 会在每个节点上被触发。
     *    - 在 `onApply()` 内部，`KVOperation` (包含了这里的 key 和 value) 会被提取出来，
     *      并传递给相应的 `RaftTaskHandler` (例如 `TaskInfoMeta` 的 `invoke` 方法)。
     *    - 该 handler 最终会调用这里的 `put(key, value)` 方法 (通常是 `super.put(key, value)` 从 TaskInfoMeta 调用过来)，
     *      以在当前节点上实际执行对 RocksDB 的写入操作。
     *
     * 因此，虽然代码看起来像是直接写入 `store` (HgKVStore，封装了 RocksDB)，
     * 但这个写入动作是在 Raft 协议保证了该操作已在集群中达成共识之后才在每个节点上执行的。
     * `getStore().put(key, value)` 是这个特定节点在 Raft 复制和应用流程中，执行本地数据持久化的最后一步。
     *
     * @param key 要存储的键
     * @param value 要存储的值
     * @throws PDException 如果 RocksDB 写入操作失败
     */
    @Override
    public void put(byte[] key, byte[] value) throws PDException {
        try {
            // getStore() 获取 HgKVStore 实例，它封装了对 RocksDB 的直接操作。
            // 这里的 put 调用是实际将数据写入本节点 RocksDB 的操作。
            getStore().put(key, value);
        } catch (Exception e) {
            throw new PDException(Pdpb.ErrorType.ROCKSDB_WRITE_ERROR_VALUE, e);
        }
    }

    @Override
    public void putWithTTL(byte[] key, byte[] value, long ttl) throws PDException {
        this.store.putWithTTL(key, value, ttl);
    }

    @Override
    public void putWithTTL(byte[] key, byte[] value, long ttl, TimeUnit timeUnit) throws
                                                                                  PDException {
        this.store.putWithTTL(key, value, ttl, timeUnit);
    }

    @Override
    public byte[] getWithTTL(byte[] key) throws PDException {
        return this.store.getWithTTL(key);
    }

    @Override
    public List getListWithTTL(byte[] key) throws PDException {
        return this.store.getListWithTTL(key);
    }

    @Override
    public void removeWithTTL(byte[] key) throws PDException {
        this.store.removeWithTTL(key);
    }

    @Override
    public List<KV> scanPrefix(byte[] prefix) throws PDException {
        try {
            return this.store.scanPrefix(prefix);
        } catch (Exception e) {
            throw new PDException(Pdpb.ErrorType.ROCKSDB_READ_ERROR_VALUE, e);
        }
    }

    @Override
    public List<KV> scanRange(byte[] start, byte[] end) throws PDException {
        return this.store.scanRange(start, end);
    }

    @Override
    public <E> List<E> scanRange(Parser<E> parser, byte[] start, byte[] end) throws PDException {
        List<E> stores = new LinkedList<>();
        try {
            List<KV> kvs = this.scanRange(start, end);
            for (KV keyValue : kvs) {
                stores.add(parser.parseFrom(keyValue.getValue()));
            }
        } catch (Exception e) {
            throw new PDException(Pdpb.ErrorType.ROCKSDB_READ_ERROR_VALUE, e);
        }
        return stores;
    }

    @Override
    public <E> List<E> scanPrefix(Parser<E> parser, byte[] prefix) throws PDException {
        List<E> stores = new LinkedList<>();
        try {
            List<KV> kvs = this.scanPrefix(prefix);
            for (KV keyValue : kvs) {
                stores.add(parser.parseFrom(keyValue.getValue()));
            }
        } catch (Exception e) {
            throw new PDException(Pdpb.ErrorType.ROCKSDB_READ_ERROR_VALUE, e);
        }
        return stores;
    }

    @Override
    public boolean containsKey(byte[] key) throws PDException {
        return !ArrayUtils.isEmpty(store.get(key));
    }

    @Override
    public long remove(byte[] key) throws PDException {
        try {
            return this.store.remove(key);
        } catch (Exception e) {
            throw new PDException(Pdpb.ErrorType.ROCKSDB_WRITE_ERROR_VALUE, e);
        }
    }

    @Override
    public long removeByPrefix(byte[] prefix) throws PDException {
        try {
            return this.store.removeByPrefix(prefix);
        } catch (Exception e) {
            throw new PDException(Pdpb.ErrorType.ROCKSDB_WRITE_ERROR_VALUE, e);
        }
    }

    @Override
    public void clearAllCache() throws PDException {
        this.store.clear();
    }

    @Override
    public void close() {

    }
}
