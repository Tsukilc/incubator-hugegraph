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

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.PostConstruct;

import org.apache.hugegraph.pd.KvService;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.kv.K;
import org.apache.hugegraph.pd.grpc.kv.KResponse;
import org.apache.hugegraph.pd.grpc.kv.Kv;
import org.apache.hugegraph.pd.grpc.kv.KvResponse;
import org.apache.hugegraph.pd.grpc.kv.KvServiceGrpc;
import org.apache.hugegraph.pd.grpc.kv.LockRequest;
import org.apache.hugegraph.pd.grpc.kv.LockResponse;
import org.apache.hugegraph.pd.grpc.kv.ScanPrefixResponse;
import org.apache.hugegraph.pd.grpc.kv.TTLRequest;
import org.apache.hugegraph.pd.grpc.kv.TTLResponse;
import org.apache.hugegraph.pd.grpc.kv.WatchKv;
import org.apache.hugegraph.pd.grpc.kv.WatchRequest;
import org.apache.hugegraph.pd.grpc.kv.WatchResponse;
import org.apache.hugegraph.pd.grpc.kv.WatchState;
import org.apache.hugegraph.pd.grpc.kv.WatchType;
import org.apache.hugegraph.pd.raft.RaftEngine;
import org.apache.hugegraph.pd.raft.RaftStateListener;
import org.apache.hugegraph.pd.watch.KvWatchSubject;
import org.lognet.springboot.grpc.GRpcService;
import org.springframework.beans.factory.annotation.Autowired;

import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

/**
 * KvServiceGrpcImpl 是 PD 提供的 gRPC 服务接口的具体实现，主要负责处理键值 (KV) 相关的操作。
 * 这包括基本的 PUT, GET, DELETE 操作，以及更复杂的分布式锁（lock, unlock, isLocked, keepAlive）
 * 和带有 TTL (Time-To-Live) 的键值对操作。
 *
 * 对于分布式任务调度场景，HugeGraph Server 端的 `MetaManager` 会利用此服务提供的分布式锁功能
 * 来协调不同 Server 节点对任务的竞争和执行权。例如，在 `DistributedTaskScheduler` 中，
 * `tryLockTask` 和 `unlockTask` 方法会通过 gRPC调用这里的 `lock` 和 `unlock` 方法。
 *
 * 为了保证这些操作在 PD 集群中的一致性（例如，确保锁的唯一性），所有修改状态的操作
 * （如 put, delete, lock, unlock）都会通过 Raft 协议进行复制和共识。
 * 这意味着当一个 Server 请求锁时，该请求最终会被转发到 PD 的 Raft Leader 节点，
 * Leader 节点会将锁操作包装成一个 `KVOperation`，并通过 RaftEngine 提交到 Raft Group。
 * 只有当该操作被 Raft Group 提交后，锁才真正被授予或许可。
 * 查询操作 (如 get, isLocked) 则直接查询当前 Leader 节点上通过 Raft 复制和应用的状态。
 *
 * 如果当前 PD 节点不是 Leader，它会将请求重定向到 Leader 节点处理 (`redirectToLeader`)。
 */
@Slf4j
@GRpcService
public class KvServiceGrpcImpl extends KvServiceGrpc.KvServiceImplBase implements RaftStateListener,
                                                                                  ServiceGrpc {

    private final ManagedChannel channel = null;
    KvService kvService;
    AtomicLong count = new AtomicLong();
    String msg = "node is not leader,it is necessary to  redirect to the leader on the client";
    @Autowired
    private PDConfig pdConfig;
    private KvWatchSubject subjects;
    private ScheduledExecutorService executor;

    @PostConstruct
    public void init() {
        RaftEngine.getInstance().init(pdConfig.getRaft());
        RaftEngine.getInstance().addStateListener(this);
        kvService = new KvService(pdConfig);
        subjects = new KvWatchSubject(pdConfig);
        executor = Executors.newScheduledThreadPool(1);
        executor.scheduleWithFixedDelay(() -> {
            if (isLeader()) {
                subjects.keepClientAlive();
            }
        }, 0, KvWatchSubject.WATCH_TTL / 2, TimeUnit.MILLISECONDS);
    }

    /**
     * Ordinary put
     *
     * @param request
     * @param responseObserver
     */
    @Override
    public void put(Kv request, StreamObserver<KvResponse> responseObserver) {
        if (!isLeader()) {
            redirectToLeader(channel, KvServiceGrpc.getPutMethod(), request, responseObserver);
            return;
        }
        KvResponse response;
        KvResponse.Builder builder = KvResponse.newBuilder();
        try {
            String key = request.getKey();
            String value = request.getValue();
            this.kvService.put(key, value);
            WatchKv watchKV = getWatchKv(key, value);
            subjects.notifyAllObserver(key, WatchType.Put, new WatchKv[]{watchKV});
            response = builder.setHeader(getResponseHeader()).build();
        } catch (PDException e) {
            if (!isLeader()) {
                redirectToLeader(channel, KvServiceGrpc.getPutMethod(), request, responseObserver);
                return;
            }
            response = builder.setHeader(getResponseHeader(e)).build();
        }
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**
     * Ordinary get
     *
     * @param request
     * @param responseObserver
     */
    @Override
    public void get(K request, StreamObserver<KResponse> responseObserver) {
        if (!isLeader()) {
            redirectToLeader(channel, KvServiceGrpc.getGetMethod(), request, responseObserver);
            return;
        }
        KResponse response;
        KResponse.Builder builder = KResponse.newBuilder();
        try {
            String value = this.kvService.get(request.getKey());
            builder.setHeader(getResponseHeader());
            if (value != null) {
                builder.setValue(value);
            }
            response = builder.build();
        } catch (PDException e) {
            if (!isLeader()) {
                redirectToLeader(channel, KvServiceGrpc.getGetMethod(), request, responseObserver);
                return;
            }
            response = builder.setHeader(getResponseHeader(e)).build();
        }
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**
     * Ordinary delete
     *
     * @param request
     * @param responseObserver
     */
    @Override
    public void delete(K request, StreamObserver<KvResponse> responseObserver) {
        if (!isLeader()) {
            redirectToLeader(channel, KvServiceGrpc.getDeleteMethod(), request, responseObserver);
            return;
        }
        KvResponse response;
        KvResponse.Builder builder = KvResponse.newBuilder();
        try {
            String key = request.getKey();
            Kv deleted = this.kvService.delete(key);
            if (deleted.getValue() != null) {
                WatchKv watchKV = getWatchKv(deleted.getKey(), deleted.getValue());
                subjects.notifyAllObserver(key, WatchType.Delete, new WatchKv[]{watchKV});
            }
            response = builder.setHeader(getResponseHeader()).build();
        } catch (PDException e) {
            if (!isLeader()) {
                redirectToLeader(channel, KvServiceGrpc.getDeleteMethod(), request,
                                 responseObserver);
                return;
            }
            response = builder.setHeader(getResponseHeader(e)).build();
        }
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**
     * Delete by prefix
     *
     * @param request
     * @param responseObserver
     */
    @Override
    public void deletePrefix(K request, StreamObserver<KvResponse> responseObserver) {
        if (!isLeader()) {
            redirectToLeader(channel, KvServiceGrpc.getDeletePrefixMethod(), request,
                             responseObserver);
            return;
        }
        KvResponse response;
        KvResponse.Builder builder = KvResponse.newBuilder();
        try {
            String key = request.getKey();
            List<Kv> kvs = this.kvService.deleteWithPrefix(key);
            WatchKv[] watchKvs = new WatchKv[kvs.size()];
            int i = 0;
            for (Kv kv : kvs) {
                WatchKv watchKV = getWatchKv(kv.getKey(), kv.getValue());
                watchKvs[i++] = watchKV;
            }
            subjects.notifyAllObserver(key, WatchType.Delete, watchKvs);
            response = builder.setHeader(getResponseHeader()).build();
        } catch (PDException e) {
            if (!isLeader()) {
                redirectToLeader(channel, KvServiceGrpc.getDeletePrefixMethod(), request,
                                 responseObserver);
                return;
            }
            response = builder.setHeader(getResponseHeader(e)).build();
        }
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**
     * Search by prefix
     *
     * @param request
     * @param responseObserver
     */
    @Override
    public void scanPrefix(K request, StreamObserver<ScanPrefixResponse> responseObserver) {
        if (!isLeader()) {
            redirectToLeader(channel, KvServiceGrpc.getScanPrefixMethod(), request,
                             responseObserver);
            return;
        }
        ScanPrefixResponse response;
        ScanPrefixResponse.Builder builder = ScanPrefixResponse.newBuilder();
        try {
            Map kvs = this.kvService.scanWithPrefix(request.getKey());
            response = builder.setHeader(getResponseHeader()).putAllKvs(kvs).build();
        } catch (PDException e) {
            if (!isLeader()) {
                redirectToLeader(channel, KvServiceGrpc.getScanPrefixMethod(), request,
                                 responseObserver);
                return;
            }
            response = builder.setHeader(getResponseHeader(e)).build();
        }
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**
     * Obtain a random non-0 string as an Id
     *
     * @return
     */
    private long getRandomLong() {

        long result;
        Random random = new Random();
        while ((result = random.nextLong()) == 0) {
            continue;
        }
        return result;
    }

    /**
     * Ordinary watch
     *
     * @param request
     * @param responseObserver
     */
    @Override
    public void watch(WatchRequest request, StreamObserver<WatchResponse> responseObserver) {
        if (!isLeader()) {
            responseObserver.onError(new PDException(-1, msg));
            return;
        }
        try {
            clientWatch(request, responseObserver, false);
        } catch (PDException e) {
            if (!isLeader()) {
                try {
                    responseObserver.onError(new PDException(-1, msg));
                } catch (IllegalStateException ie) {

                } catch (Exception e1) {
                    log.error("redirect with error: ", e1);
                }
            }
        }
    }

    /**
     * Ordinary prefix watch
     *
     * @param request
     * @param responseObserver
     */
    @Override
    public void watchPrefix(WatchRequest request, StreamObserver<WatchResponse> responseObserver) {
        if (!isLeader()) {
            responseObserver.onError(new PDException(-1, msg));
            return;
        }
        try {
            clientWatch(request, responseObserver, true);
        } catch (PDException e) {
            if (!isLeader()) {
                try {
                    responseObserver.onError(new PDException(-1, msg));
                } catch (IllegalStateException ie) {

                } catch (Exception e1) {
                    log.error("redirect with error: ", e1);
                }
            }
        }
    }

    /**
     * A generic approach to the above two methods
     *
     * @param request
     * @param responseObserver
     * @param isPrefix
     * @throws PDException
     */
    private void clientWatch(WatchRequest request, StreamObserver<WatchResponse> responseObserver,
                             boolean isPrefix) throws PDException {
        try {
            String key = request.getKey();
            long clientId = request.getClientId();
            WatchResponse.Builder builder = WatchResponse.newBuilder();
            WatchResponse response;
            if (request.getState().equals(WatchState.Starting) && clientId == 0) {
                clientId = getRandomLong();
                response = builder.setClientId(clientId).setState(WatchState.Starting).build();
            } else {
                response = builder.setState(WatchState.Started).build();
            }
            String delimiter =
                    isPrefix ? KvWatchSubject.PREFIX_DELIMITER : KvWatchSubject.KEY_DELIMITER;
            subjects.addObserver(key, clientId, responseObserver, delimiter);
            synchronized (responseObserver) {
                responseObserver.onNext(response);
            }
        } catch (PDException e) {
            if (!isLeader()) {
                throw new PDException(-1, msg);
            }
            throw new PDException(e.getErrorCode(), e);
        }

    }

    /**
     * 处理来自 HugeGraph Server 的分布式锁获取请求。
     * 当 Server 端的 `DistributedTaskScheduler` (通过 `MetaManager`) 尝试获取一个任务锁时，
     * 会调用此 gRPC 方法。
     *
     * @param request 包含锁的键 (key, 通常是任务ID的字符串形式)、TTL (锁的租约时间) 和客户端ID。
     * @param responseObserver 用于异步返回锁操作的结果。
     */
    @Override
    public void lock(LockRequest request, StreamObserver<LockResponse> responseObserver) {
        // 1. 检查当前 PD 节点是否为 Raft Leader。
        //    所有写操作（包括获取锁，因为它会改变锁的状态）都必须由 Leader 处理以保证一致性。
        if (!isLeader()) {
            // 如果不是 Leader，则将请求重定向到已知的 Leader 节点。
            // redirectToLeader 是一个辅助方法，用于将 gRPC 请求转发。
            redirectToLeader(channel, KvServiceGrpc.getLockMethod(), request, responseObserver);
            return;
        }

        LockResponse response;
        LockResponse.Builder builder = LockResponse.newBuilder();
        try {
            long clientId = request.getClientId();
            // 如果客户端未提供ID，则生成一个随机ID。
            if (clientId == 0) {
                clientId = getRandomLong();
            }

            // 2. 调用底层的 kvService.lock() 方法来实际执行锁操作。
            //    - kvService.lock() 内部会将锁操作（例如，创建一个代表锁的键值对）
            //      包装成一个 KVOperation。
            //    - 然后，这个 KVOperation 会通过 RaftEngine.addTask() 提交给 Raft Group。
            //    - Raft 协议确保这个锁操作在整个 PD 集群中以一致的方式被应用。
            //      只有当 Raft 日志被提交并且 KVOperation 在状态机中成功应用后，
            //      这里的 this.kvService.lock() 才会返回 true (表示成功获取锁)。
            boolean locked = this.kvService.lock(request.getKey(), request.getTtl(), clientId);

            // 3. 构建并返回响应。
            //    如果 locked 为 true，表示 Server 成功获取了任务锁。
            response =
                    builder.setHeader(getResponseHeader()).setSucceed(locked).setClientId(clientId)
                           .build();
        } catch (PDException e) {
            // 如果在锁操作过程中（包括 Raft 提交或应用阶段）发生错误，
            // 再次检查是否因为 Leader 变更导致，如果是则重定向。
            if (!isLeader()) {
                redirectToLeader(channel, KvServiceGrpc.getLockMethod(), request, responseObserver);
                return;
            }
            log.error("lock with error :", e);
            response = builder.setHeader(getResponseHeader(e)).build();
        }
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void lockWithoutReentrant(LockRequest request,
                                     StreamObserver<LockResponse> responseObserver) {
        if (!isLeader()) {
            redirectToLeader(channel, KvServiceGrpc.getLockWithoutReentrantMethod(), request,
                             responseObserver);
            return;
        }
        LockResponse response;
        LockResponse.Builder builder = LockResponse.newBuilder();
        try {
            long clientId = request.getClientId();
            if (clientId == 0) {
                clientId = getRandomLong();
            }
            boolean locked = this.kvService.lockWithoutReentrant(request.getKey(), request.getTtl(),
                                                                 clientId);
            response =
                    builder.setHeader(getResponseHeader()).setSucceed(locked).setClientId(clientId)
                           .build();
        } catch (PDException e) {
            if (!isLeader()) {
                redirectToLeader(channel, KvServiceGrpc.getLockWithoutReentrantMethod(), request,
                                 responseObserver);
                return;
            }
            log.error("lock with error :", e);
            response = builder.setHeader(getResponseHeader(e)).build();
        }
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void isLocked(LockRequest request, StreamObserver<LockResponse> responseObserver) {
        // 检查是否为 Leader，如果不是则重定向。
        // 查询操作通常也建议在 Leader 上执行，以获取最新的、已提交的状态。
        // 虽然 Raft 允许在 Follower 上执行只读查询（如果满足线性一致性或顺序一致性要求），
        // 但最简单的做法是都重定向到 Leader。
        if (!isLeader()) {
            redirectToLeader(channel, KvServiceGrpc.getIsLockedMethod(), request, responseObserver);
            return;
        }
        LockResponse response;
        LockResponse.Builder builder = LockResponse.newBuilder();
        try {
            // 调用 kvService.locked() 查询指定 key (任务ID) 是否已被锁定。
            // 这个查询会访问 PD 节点本地（但通过 Raft 保证一致性）的 KV 存储。
            boolean locked = this.kvService.locked(request.getKey());
            response = builder.setHeader(getResponseHeader()).setSucceed(locked).build();
        } catch (PDException e) {
            log.error("lock with error :", e);
            if (!isLeader()) {
                redirectToLeader(channel, KvServiceGrpc.getIsLockedMethod(), request,
                                 responseObserver);
                return;
            }
            response = builder.setHeader(getResponseHeader(e)).build();
        }
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**
     * 处理来自 HugeGraph Server 的分布式锁释放请求。
     * 当 Server 端的 `DistributedTaskScheduler` (通过 `MetaManager`) 完成任务或不再需要锁时，
     * 会调用此 gRPC 方法。
     *
     * @param request 包含锁的键 (key) 和持有锁的客户端ID。
     * @param responseObserver 用于异步返回解锁操作的结果。
     */
    @Override
    public void unlock(LockRequest request, StreamObserver<LockResponse> responseObserver) {
        // 检查是否为 Leader，如果不是则重定向。
        if (!isLeader()) {
            redirectToLeader(channel, KvServiceGrpc.getUnlockMethod(), request, responseObserver);
            return;
        }
        LockResponse response;
        LockResponse.Builder builder = LockResponse.newBuilder();
        try {
            long clientId = request.getClientId();
            // 客户端ID必须提供，用于验证是否为锁的持有者。
            if (clientId == 0) {
                throw new PDException(-1, "incorrect clientId: 0");
            }

            // 调用底层的 kvService.unlock() 方法。
            // 与 lock() 类似，此操作也会通过 Raft 协议确保在集群中的一致性。
            // 它会删除或更新代表锁的键值对。
            boolean unlocked = this.kvService.unlock(request.getKey(), clientId);

            response = builder.setHeader(getResponseHeader()).setSucceed(unlocked)
                              .setClientId(clientId).build();
        } catch (PDException e) {
            if (!isLeader()) {
                redirectToLeader(channel, KvServiceGrpc.getUnlockMethod(), request,
                                 responseObserver);
                return;
            }
            response = builder.setHeader(getResponseHeader(e)).build();
        }
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**
     * Lock renewal
     *
     * @param request
     * @param responseObserver
     */
    @Override
    public void keepAlive(LockRequest request, StreamObserver<LockResponse> responseObserver) {
        if (!isLeader()) {
            redirectToLeader(channel, KvServiceGrpc.getKeepAliveMethod(), request,
                             responseObserver);
            return;
        }
        LockResponse response;
        LockResponse.Builder builder = LockResponse.newBuilder();
        try {
            long clientId = request.getClientId();
            if (clientId == 0) {
                throw new PDException(-1, "incorrect clientId: 0");
            }
            boolean alive = this.kvService.keepAlive(request.getKey(), clientId);
            response =
                    builder.setHeader(getResponseHeader()).setSucceed(alive).setClientId(clientId)
                           .build();
        } catch (PDException e) {
            if (!isLeader()) {
                redirectToLeader(channel, KvServiceGrpc.getKeepAliveMethod(), request,
                                 responseObserver);
                return;
            }
            response = builder.setHeader(getResponseHeader(e)).build();
        }
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**
     * PUT with timeout
     *
     * @param request
     * @param responseObserver
     */
    @Override
    public void putTTL(TTLRequest request, StreamObserver<TTLResponse> responseObserver) {
        if (!isLeader()) {
            redirectToLeader(channel, KvServiceGrpc.getPutTTLMethod(), request, responseObserver);
            return;
        }
        TTLResponse response;
        TTLResponse.Builder builder = TTLResponse.newBuilder();
        try {
            this.kvService.put(request.getKey(), request.getValue(), request.getTtl());
            response = builder.setHeader(getResponseHeader()).setSucceed(true).build();
        } catch (PDException e) {
            if (!isLeader()) {
                redirectToLeader(channel, KvServiceGrpc.getPutTTLMethod(), request,
                                 responseObserver);
                return;
            }
            response = builder.setHeader(getResponseHeader(e)).build();
        }
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**
     * Reactivate the key with a timeout period
     *
     * @param request
     * @param responseObserver
     */
    @Override
    public void keepTTLAlive(TTLRequest request, StreamObserver<TTLResponse> responseObserver) {
        if (!isLeader()) {
            redirectToLeader(channel, KvServiceGrpc.getKeepTTLAliveMethod(), request,
                             responseObserver);
            return;
        }
        TTLResponse response;
        TTLResponse.Builder builder = TTLResponse.newBuilder();
        try {
            this.kvService.keepAlive(request.getKey());
            response = builder.setHeader(getResponseHeader()).setSucceed(true).build();
        } catch (PDException e) {
            if (!isLeader()) {
                redirectToLeader(channel, KvServiceGrpc.getKeepTTLAliveMethod(), request,
                                 responseObserver);
                return;
            }
            response = builder.setHeader(getResponseHeader(e)).build();
        }
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private WatchKv getWatchKv(String key, String value) {
        WatchKv kv = WatchKv.newBuilder().setKey(key).setValue(value).build();
        return kv;
    }

    @Override
    public void onRaftLeaderChanged() {
        subjects.notifyClientChangeLeader();
    }
}
