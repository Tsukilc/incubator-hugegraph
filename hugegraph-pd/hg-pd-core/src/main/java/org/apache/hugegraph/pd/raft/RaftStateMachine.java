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
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.Checksum;

import org.apache.commons.io.FileUtils;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Pdpb;
import org.springframework.util.CollectionUtils;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.core.StateMachineAdapter;
import com.alipay.sofa.jraft.entity.LeaderChangeContext;
import com.alipay.sofa.jraft.entity.LocalFileMetaOutter;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.error.RaftException;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.alipay.sofa.jraft.util.CRC64;
import com.alipay.sofa.jraft.util.Utils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
// RaftStateMachine (FSM - Finite State Machine) 是 Raft 协议中至关重要的一个组件。
// 在 JRaft 实现中，每个 Raft 节点都拥有一个状态机实例。
// 它的核心职责是 **应用 (apply)** 那些已经被 Raft Group **提交 (committed)** 的日志条目 (log entries)。
// 当 Raft Leader 将一个操作（包装在日志条目中）成功复制到集群中的大多数节点后，该日志条目就被认为是“已提交”的。
// 此时，JRaft 会通知所有节点（包括 Leader 和 Followers）的状态机去应用这个已提交的日志条目。
//
// "应用" 通常意味着将日志条目中包含的操作实际执行到节点本地的持久化存储中（例如 RocksDB）。
// 由于 Raft 保证了所有节点提交的日志顺序是一致的，因此，只要状态机的应用逻辑是确定性的，
// 那么所有节点在应用完相同的日志序列后，其本地状态也必然是一致的。
// 这就是 Raft 如何实现分布式共识和数据一致性的关键。
//
// 在 PD 的场景下，当一个例如 "创建 PD 内部任务" 的请求通过 RaftEngine.addTask() 提交后，
// 它会被包装成 KVOperation，并通过 Raft 日志复制到所有 PD 节点。
// 一旦该日志条目被提交，每个 PD 节点的 RaftStateMachine 的 onApply() 方法就会被调用，
// 从而将这个 KVOperation（包含任务信息）应用到本地的 RocksDB 存储中。
public class RaftStateMachine extends StateMachineAdapter {

    private static final String SNAPSHOT_DIR_NAME = "snapshot";
    private static final String SNAPSHOT_ARCHIVE_NAME = "snapshot.zip";
    private final AtomicLong leaderTerm = new AtomicLong(-1);
    private final List<RaftTaskHandler> taskHandlers;
    private final List<RaftStateListener> stateListeners;

    public RaftStateMachine() {
        this.taskHandlers = new CopyOnWriteArrayList<>();
        this.stateListeners = new CopyOnWriteArrayList<>();
    }

    public void addTaskHandler(RaftTaskHandler handler) {
        taskHandlers.add(handler);
    }

    public void addStateListener(RaftStateListener listener) {
        stateListeners.add(listener);
    }

    public boolean isLeader() {
        return this.leaderTerm.get() > 0;
    }

    /**
     * 当 Raft 日志条目被 Raft Group 成功提交 (committed) 后，JRaft 会调用此方法。
     * 此方法负责将这些已提交的日志条目中包含的操作应用到当前节点的状态存储中。
     *
     * @param iter 一个迭代器，用于遍历一批已提交的 Raft 日志条目。
     *             每个日志条目通常包含一个通过 RaftEngine.addTask() 提交的 `KVOperation`。
     */
    @Override
    public void onApply(Iterator iter) {
        while (iter.hasNext()) {
            // `done` 是一个回调闭包 (Closure)，在操作应用完成后被调用，用于通知 JRaft 操作完成状态。
            // RaftClosureAdapter 包装了原始的 KVOperation 和用户的回调。
            final RaftClosureAdapter done = (RaftClosureAdapter) iter.done();
            try {
                KVOperation kvOp;
                if (done != null) {
                    // 如果有回调 (通常在 Leader 节点上，是原始提交的 Task)，直接获取其中的 KVOperation。
                    kvOp = done.op;
                } else {
                    // 如果没有回调 (通常在 Follower 节点上，或者 Leader 重启后应用旧日志)，
                    // 则从日志数据中反序列化出 KVOperation。
                    // iter.getData() 返回的是日志条目的原始数据 (ByteBuffer)。
                    kvOp = KVOperation.fromByteArray(iter.getData().array());
                }

                // 将提取出的 KVOperation 分发给所有注册的 RaftTaskHandler 进行处理。
                // 例如，如果 kvOp 是一个关于 PD 内部任务的操作，它会被 TaskInfoMeta (一个 RaftTaskHandler) 处理。
                // TaskInfoMeta 会根据 kvOp 的类型（如 PUT, DELETE）将其持久化到 RocksDB。
                // 这就是 PD 内部任务数据最终被 "保存" 到每个 PD 节点的本地存储的地方。
                for (RaftTaskHandler taskHandler : taskHandlers) {
                    taskHandler.invoke(kvOp, done);
                }

                // 如果有回调，并且前面的处理没有抛出异常，则标记操作成功。
                if (done != null) {
                    done.run(Status.OK());
                }
            } catch (Throwable t) {
                log.error("StateMachine encountered critical error", t);
                // 如果应用过程中发生错误，通过回调通知 JRaft 操作失败。
                if (done != null) {
                    done.run(new Status(RaftError.EINTERNAL, t.getMessage()));
                }
            }
            // 处理下一个日志条目
            iter.next();
        }
    }

    @Override
    public void onError(final RaftException e) {
        log.error("Raft StateMachine encountered an error", e);
    }

    @Override
    public void onShutdown() {
        super.onShutdown();
    }

    @Override
    public void onLeaderStart(final long term) {
        this.leaderTerm.set(term);
        super.onLeaderStart(term);

        log.info("Raft becomes leader");
        Utils.runInThread(() -> {
            if (!CollectionUtils.isEmpty(stateListeners)) {
                stateListeners.forEach(RaftStateListener::onRaftLeaderChanged);
            }
        });
    }

    @Override
    public void onLeaderStop(final Status status) {
        this.leaderTerm.set(-1);
        super.onLeaderStop(status);
        log.info("Raft  lost leader ");
    }

    @Override
    public void onStartFollowing(final LeaderChangeContext ctx) {
        super.onStartFollowing(ctx);
        Utils.runInThread(() -> {
            if (!CollectionUtils.isEmpty(stateListeners)) {
                stateListeners.forEach(RaftStateListener::onRaftLeaderChanged);
            }
        });
    }

    @Override
    public void onStopFollowing(final LeaderChangeContext ctx) {
        super.onStopFollowing(ctx);
    }

    @Override
    public void onConfigurationCommitted(final Configuration conf) {
        log.info("Raft  onConfigurationCommitted {}", conf);
    }

    @Override
    public void onSnapshotSave(final SnapshotWriter writer, final Closure done) {

        String snapshotDir = writer.getPath() + File.separator + SNAPSHOT_DIR_NAME;
        try {
            FileUtils.deleteDirectory(new File(snapshotDir));
            FileUtils.forceMkdir(new File(snapshotDir));
        } catch (IOException e) {
            log.error("Failed to create snapshot directory {}", snapshotDir);
            done.run(new Status(RaftError.EIO, e.toString()));
            return;
        }

        CountDownLatch latch = new CountDownLatch(taskHandlers.size());
        for (RaftTaskHandler taskHandler : taskHandlers) {
            Utils.runInThread(() -> {
                try {
                    KVOperation op = KVOperation.createSaveSnapshot(snapshotDir);
                    taskHandler.invoke(op, null);
                    log.info("Raft onSnapshotSave success");
                    latch.countDown();
                } catch (PDException e) {
                    log.error("Raft onSnapshotSave failed. {}", e.toString());
                    done.run(new Status(RaftError.EIO, e.toString()));
                }
            });
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            log.error("Raft onSnapshotSave failed. {}", e.toString());
            done.run(new Status(RaftError.EIO, e.toString()));
            return;
        }

        // compress
        try {
            compressSnapshot(writer);
            FileUtils.deleteDirectory(new File(snapshotDir));
        } catch (Exception e) {
            log.error("Failed to delete snapshot directory {}, {}", snapshotDir, e.toString());
            done.run(new Status(RaftError.EIO, e.toString()));
            return;
        }
        done.run(Status.OK());
    }

    @Override
    public boolean onSnapshotLoad(final SnapshotReader reader) {
        if (isLeader()) {
            log.warn("Leader is not supposed to load snapshot");
            return false;
        }
        String snapshotDir = reader.getPath() + File.separator + SNAPSHOT_DIR_NAME;
        String snapshotArchive = reader.getPath() + File.separator + SNAPSHOT_ARCHIVE_NAME;
        // 2. decompress snapshot archive
        try {
            decompressSnapshot(reader);
        } catch (PDException e) {
            log.error("Failed to delete snapshot directory {}, {}", snapshotDir, e.toString());
            return true;
        }

        CountDownLatch latch = new CountDownLatch(taskHandlers.size());
        for (RaftTaskHandler taskHandler : taskHandlers) {
            try {
                KVOperation op = KVOperation.createLoadSnapshot(snapshotDir);
                taskHandler.invoke(op, null);
                log.info("Raft onSnapshotLoad success");
                latch.countDown();
            } catch (PDException e) {
                log.error("Raft onSnapshotLoad failed. {}", e.toString());
                return false;
            }
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            log.error("Raft onSnapshotSave failed. {}", e.toString());
            return false;
        }

        try {
            // TODO: remove file from meta
            FileUtils.deleteDirectory(new File(snapshotDir));
            File file = new File(snapshotArchive);
            if (file.exists()) {
                FileUtils.forceDelete(file);
            }
        } catch (IOException e) {
            log.error("Failed to delete snapshot directory {} and file {}", snapshotDir,
                      snapshotArchive);
            return false;
        }

        return true;
    }

    private void compressSnapshot(final SnapshotWriter writer) throws PDException {
        final Checksum checksum = new CRC64();
        final String snapshotArchive = writer.getPath() + File.separator + SNAPSHOT_ARCHIVE_NAME;
        try {
            ZipUtils.compress(writer.getPath(), SNAPSHOT_DIR_NAME, snapshotArchive, checksum);
            LocalFileMetaOutter.LocalFileMeta.Builder metaBuild =
                    LocalFileMetaOutter.LocalFileMeta.newBuilder();
            metaBuild.setChecksum(Long.toHexString(checksum.getValue()));
            if (!writer.addFile(SNAPSHOT_ARCHIVE_NAME, metaBuild.build())) {
                throw new PDException(Pdpb.ErrorType.ROCKSDB_SAVE_SNAPSHOT_ERROR_VALUE,
                                      "failed to add file to LocalFileMeta");
            }
        } catch (IOException e) {
            throw new PDException(Pdpb.ErrorType.ROCKSDB_SAVE_SNAPSHOT_ERROR_VALUE, e);
        }
    }

    private void decompressSnapshot(final SnapshotReader reader) throws PDException {
        final LocalFileMetaOutter.LocalFileMeta meta =
                (LocalFileMetaOutter.LocalFileMeta) reader.getFileMeta(SNAPSHOT_ARCHIVE_NAME);
        final Checksum checksum = new CRC64();
        final String snapshotArchive = reader.getPath() + File.separator + SNAPSHOT_ARCHIVE_NAME;
        try {
            ZipUtils.decompress(snapshotArchive, new File(reader.getPath()), checksum);
            if (meta.hasChecksum()) {
                if (!meta.getChecksum().equals(Long.toHexString(checksum.getValue()))) {
                    throw new PDException(Pdpb.ErrorType.ROCKSDB_LOAD_SNAPSHOT_ERROR_VALUE,
                                          "Snapshot checksum failed");
                }
            }
        } catch (IOException e) {
            throw new PDException(Pdpb.ErrorType.ROCKSDB_LOAD_SNAPSHOT_ERROR_VALUE, e);
        }
    }

    public static class RaftClosureAdapter implements KVStoreClosure {

        private final KVOperation op;
        private final KVStoreClosure closure;

        public RaftClosureAdapter(KVOperation op, KVStoreClosure closure) {
            this.op = op;
            this.closure = closure;
        }

        public KVStoreClosure getClosure() {
            return closure;
        }

        @Override
        public void run(Status status) {
            closure.run(status);
        }

        @Override
        public Pdpb.Error getError() {
            return null;
        }

        @Override
        public void setError(Pdpb.Error error) {

        }

        @Override
        public Object getData() {
            return null;
        }

        @Override
        public void setData(Object data) {

        }
    }
}
