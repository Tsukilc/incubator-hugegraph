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

import java.util.List;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.grpc.MetaTask;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.pulse.MovePartition;
import org.apache.hugegraph.pd.grpc.pulse.SplitPartition;

/**
 * TaskInfoMeta 负责管理 PD 内部任务（例如分区切分、分区迁移等）的元数据信息。
 * "PD 内部任务" 指的是由 PD 自身发起和调度的、用于维护 HugeGraph 集群状态和数据分布的任务，
 * 这与 HugeGraph Server 中用户提交的图计算任务 (HugeTask) 是不同的概念。
 *
 *此类继承自 MetadataRocksDBStore，利用 RocksDB 作为底层存储来持久化这些内部任务的信息。
 * 它提供了针对特定任务类型（如 Split_Partition, Move_Partition）的增删改查接口。
 *
 * 当需要创建一个新的 PD 内部任务时（例如，通过调用 addSplitTask），TaskInfoMeta 会：
 * 1. 构建一个代表该任务的 protobuf 对象 (MetaTask.Task)。
 * 2. 将该 protobuf 对象序列化成字节数组。
 * 3. 调用父类 MetadataRocksDBStore 的 put() 方法，将序列化后的任务数据存入 RocksDB。
 *
 * put() 方法的调用最终会触发 Raft 共识流程：
 *   - MetadataRocksDBStore.put() -> RaftEngine.addTask() -> RaftStateMachine.onApply()
 *   - 在 onApply() 中，KVOperation (包装了任务数据) 会被传递给 TaskInfoMeta 的 invoke() 方法（通过 RaftTaskHandler 接口）。
 *   - TaskInfoMeta.invoke() 最终会再次调用其内部的 RocksDB 操作方法（如 super.put()），
 *     在当前节点实际执行数据的写入。
 * 这样就确保了 PD 内部任务的创建和状态变更都在 PD 集群中达成一致。
 */
public class TaskInfoMeta extends MetadataRocksDBStore {

    public TaskInfoMeta(PDConfig pdConfig) {
        super(pdConfig);
    }

    /**
     * 添加一个分区切分任务 (Split Partition Task)。
     *
     * @param groupID 分区组ID (虽然参数名是 groupID，但通常对应 partition.getId())
     * @param partition 要被切分的原始分区信息
     * @param splitPartition 定义了切分操作的具体参数（如新的分区边界等）
     * @throws PDException 如果存储操作失败
     */
    public void addSplitTask(int groupID, Metapb.Partition partition, SplitPartition splitPartition)
            throws PDException {
        // 1. 使用 MetadataKeyHelper 生成用于存储此任务的 RocksDB key。
        //    key 通常会包含图名和分区ID，以确保唯一性。
        byte[] key = MetadataKeyHelper.getSplitTaskKey(partition.getGraphName(), groupID);

        // 2. 构建 MetaTask.Task protobuf 对象，用于描述这个切分任务。
        //    设置任务类型为 Split_Partition，初始状态为 Task_Doing，记录开始时间戳等。
        MetaTask.Task task = MetaTask.Task.newBuilder()
                                          .setType(MetaTask.TaskType.Split_Partition)
                                          .setState(MetaTask.TaskState.Task_Doing)
                                          .setStartTimestamp(System.currentTimeMillis())
                                          .setPartition(partition) // 记录原始分区信息
                                          .setSplitPartition(splitPartition) // 记录切分参数
                                          .build();

        // 3. 调用父类 MetadataRocksDBStore 的 put() 方法。
        //    - task.toByteString().toByteArray() 将 protobuf 对象序列化为字节数组作为 value。
        //    - put() 方法内部会将这个 key-value 对包装成 KVOperation，
        //      然后通过 RaftEngine.addTask() 提交给 Raft Group 进行共识。
        //    - 一旦 Raft 日志被提交，RaftStateMachine.onApply() 会在每个节点上被调用，
        //      最终通过 TaskInfoMeta 的 RaftTaskHandler 实现，将这个任务数据写入本地 RocksDB。
        put(key, task.toByteString().toByteArray());
    }

    /**
     * 更新一个已存在的分区切分任务的状态或信息。
     *
     * @param task 包含更新后信息的 MetaTask.Task 对象。其内部的 partition 和 splitPartition 信息应保持一致，主要更新 state 或时间戳等。
     * @throws PDException 如果存储操作失败
     */
    public void updateSplitTask(MetaTask.Task task) throws PDException {
        var partition = task.getPartition();
        // 1. 根据任务关联的分区信息生成对应的 RocksDB key。
        byte[] key = MetadataKeyHelper.getSplitTaskKey(partition.getGraphName(), partition.getId());
        // 2. 调用父类的 put() 方法，用新的任务信息覆盖旧的。
        //    同样，这个操作会经过 Raft 共识流程，确保所有节点上任务信息的更新是一致的。
        put(key, task.toByteString().toByteArray());
    }

    public MetaTask.Task getSplitTask(String graphName, int groupID) throws PDException {
        byte[] key = MetadataKeyHelper.getSplitTaskKey(graphName, groupID);
        return getOne(MetaTask.Task.parser(), key);
    }

    public List<MetaTask.Task> scanSplitTask(String graphName) throws PDException {
        byte[] prefix = MetadataKeyHelper.getSplitTaskPrefix(graphName);
        return scanPrefix(MetaTask.Task.parser(), prefix);
    }

    public void removeSplitTaskPrefix(String graphName) throws PDException {
        byte[] key = MetadataKeyHelper.getSplitTaskPrefix(graphName);
        removeByPrefix(key);
    }

    public boolean hasSplitTaskDoing() throws PDException {
        byte[] key = MetadataKeyHelper.getAllSplitTaskPrefix();
        return scanPrefix(key).size() > 0;
    }

    public void addMovePartitionTask(Metapb.Partition partition, MovePartition movePartition)
            throws PDException {
        byte[] key = MetadataKeyHelper.getMoveTaskKey(partition.getGraphName(),
                                                      movePartition.getTargetPartition().getId(),
                                                      partition.getId());

        MetaTask.Task task = MetaTask.Task.newBuilder()
                                          .setType(MetaTask.TaskType.Move_Partition)
                                          .setState(MetaTask.TaskState.Task_Doing)
                                          .setStartTimestamp(System.currentTimeMillis())
                                          .setPartition(partition)
                                          .setMovePartition(movePartition)
                                          .build();
        put(key, task.toByteArray());
    }

    public void updateMovePartitionTask(MetaTask.Task task)
            throws PDException {

        byte[] key = MetadataKeyHelper.getMoveTaskKey(task.getPartition().getGraphName(),
                                                      task.getMovePartition().getTargetPartition()
                                                          .getId(),
                                                      task.getPartition().getId());
        put(key, task.toByteArray());
    }

    public MetaTask.Task getMovePartitionTask(String graphName, int targetId, int partId) throws
                                                                                          PDException {
        byte[] key = MetadataKeyHelper.getMoveTaskKey(graphName, targetId, partId);
        return getOne(MetaTask.Task.parser(), key);
    }

    public List<MetaTask.Task> scanMoveTask(String graphName) throws PDException {
        byte[] prefix = MetadataKeyHelper.getMoveTaskPrefix(graphName);
        return scanPrefix(MetaTask.Task.parser(), prefix);
    }

    /**
     * Delete the migration task by prefixing it and group them all at once
     *
     * @param graphName graphName
     * @throws PDException io error
     */
    public void removeMoveTaskPrefix(String graphName) throws PDException {
        byte[] key = MetadataKeyHelper.getMoveTaskPrefix(graphName);
        removeByPrefix(key);
    }

    public boolean hasMoveTaskDoing() throws PDException {
        byte[] key = MetadataKeyHelper.getAllMoveTaskPrefix();
        return scanPrefix(key).size() > 0;
    }

}
