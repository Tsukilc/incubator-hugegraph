/*
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

package org.apache.hugegraph.task;

import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.HugeGraphParams;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.query.QueryResults;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.exception.ConnectionException;
import org.apache.hugegraph.exception.NotFoundException;
import org.apache.hugegraph.meta.MetaManager;
import org.apache.hugegraph.meta.lock.LockResult;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.LockUtil;
import org.apache.hugegraph.util.Log;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;

public class DistributedTaskScheduler extends TaskAndResultScheduler {
    private final long schedulePeriod;
    private static final Logger LOG = Log.logger(DistributedTaskScheduler.class);
    private final ExecutorService taskDbExecutor;
    private final ExecutorService schemaTaskExecutor;
    private final ExecutorService olapTaskExecutor;
    private final ExecutorService ephemeralTaskExecutor;
    private final ExecutorService gremlinTaskExecutor;
    private final ScheduledThreadPoolExecutor schedulerExecutor;
    private final ScheduledFuture<?> cronFuture;

    /**
     * the status of scheduler
     */
    private final AtomicBoolean closed = new AtomicBoolean(true);

    private final ConcurrentHashMap<Id, HugeTask<?>> runningTasks = new ConcurrentHashMap<>();

    /**
     * 分布式任务调度器构造函数
     * 
     * @param graph 图参数，包含配置信息
     * @param schedulerExecutor 调度器线程池，用于定时任务
     * @param taskDbExecutor 任务数据库操作线程池
     * @param schemaTaskExecutor Schema操作任务专用线程池
     * @param olapTaskExecutor OLAP计算任务专用线程池  
     * @param gremlinTaskExecutor Gremlin查询任务专用线程池
     * @param ephemeralTaskExecutor 临时任务专用线程池
     * @param serverInfoDbExecutor 服务器信息数据库操作线程池
     */
    public DistributedTaskScheduler(HugeGraphParams graph,
                                    ScheduledThreadPoolExecutor schedulerExecutor,
                                    ExecutorService taskDbExecutor,
                                    ExecutorService schemaTaskExecutor,
                                    ExecutorService olapTaskExecutor,
                                    ExecutorService gremlinTaskExecutor,
                                    ExecutorService ephemeralTaskExecutor,
                                    ExecutorService serverInfoDbExecutor) {
        // 调用父类构造函数，初始化基础功能
        super(graph, serverInfoDbExecutor);

        // === 初始化各种类型的线程池 ===
        // 任务数据库操作线程池：用于任务的持久化操作
        this.taskDbExecutor = taskDbExecutor;
        // Schema任务线程池：处理图模式相关的任务
        this.schemaTaskExecutor = schemaTaskExecutor;
        // OLAP任务线程池：处理大规模图计算任务
        this.olapTaskExecutor = olapTaskExecutor;
        // Gremlin任务线程池：处理图查询任务
        this.gremlinTaskExecutor = gremlinTaskExecutor;
        // 临时任务线程池：处理不需要持久化的轻量任务
        this.ephemeralTaskExecutor = ephemeralTaskExecutor;

        // 调度器线程池：用于定时任务调度
        this.schedulerExecutor = schedulerExecutor;

        // 设置调度器为开启状态
        this.closed.set(false);

        // 从配置中获取调度周期
        this.schedulePeriod = this.graph.configuration()
                                        .get(CoreOptions.TASK_SCHEDULE_PERIOD);

        // === 启动定时调度任务 ===
        // 创建周期性任务，每隔schedulePeriod秒执行一次cronSchedule
        this.cronFuture = this.schedulerExecutor.scheduleWithFixedDelay(
            () -> {
                // TODO: uncomment later - graph space
                // 获取图级别的锁，防止多个调度器同时操作同一个图的任务
                // LockUtil.lock(this.graph().spaceGraphName(), LockUtil.GRAPH_LOCK);
                LockUtil.lock("", LockUtil.GRAPH_LOCK);
                try {
                    // TODO: Use super administrator privileges to query tasks.
                    // TaskManager.useAdmin();
                    // 执行核心调度逻辑
                    this.cronSchedule();
                } catch (Throwable t) {
                    // TODO: log with graph space
                    // 记录调度过程中的异常，但不影响后续调度
                    LOG.info("cronScheduler exception graph: {}", this.graphName(), t);
                } finally {
                    // TODO: uncomment later - graph space
                    // 无论成功还是失败都要释放锁
                    LockUtil.unlock("", LockUtil.GRAPH_LOCK);
                    // LockUtil.unlock(this.graph().spaceGraphName(), LockUtil.GRAPH_LOCK);
                }
            },
            10L, schedulePeriod, // 延迟10秒启动，然后每schedulePeriod秒执行一次
            TimeUnit.SECONDS);
    }

    private static boolean sleep(long ms) {
        try {
            Thread.sleep(ms);
            return true;
        } catch (InterruptedException ignored) {
            // Ignore InterruptedException
            return false;
        }
    }

    /**
     * 定时调度方法：这是分布式任务调度的核心逻辑
     * 每个节点定期执行此方法，扫描数据库中的任务并进行状态管理
     */
    public void cronSchedule() {
        // Perform periodic scheduling tasks

        // 前置检查：如果图实例未启动或已关闭，跳过调度
        if (!this.graph.started() || this.graph.closed()) {
            return;
        }

        // === 第一阶段：处理NEW状态的任务 ===
        // 扫描数据库中状态为NEW的任务，尝试启动执行
        // Handle tasks in NEW status
        Iterator<HugeTask<Object>> news = queryTaskWithoutResultByStatus(
            TaskStatus.NEW);

        while (!this.closed.get() && news.hasNext()) {
            HugeTask<?> newTask = news.next();
            LOG.info("Try to start task({})@({}/{})", newTask.id(),
                     this.graphSpace, this.graphName);
            // 尝试启动任务，如果线程池满了会返回false
            if (!tryStartHugeTask(newTask)) {
                // Task submission failed when the thread pool is full.
                break; // 线程池满了，停止处理更多任务
            }
        }

        // === 第二阶段：处理RUNNING状态的任务 ===
        // 检查运行中的任务是否还在正常执行，如果节点已释放锁则标记为失败
        // Handling tasks in RUNNING state
        Iterator<HugeTask<Object>> runnings =
            queryTaskWithoutResultByStatus(TaskStatus.RUNNING);

        while (!this.closed.get() && runnings.hasNext()) {
            HugeTask<?> running = runnings.next();
            initTaskParams(running);
            // 检查任务是否还被当前节点锁定
            if (!isLockedTask(running.id().toString())) {
                // 如果锁已释放，说明执行节点可能已故障，将任务标记为失败
                LOG.info("Try to update task({})@({}/{}) status" +
                         "(RUNNING->FAILED)", running.id(), this.graphSpace,
                         this.graphName);
                if (updateStatusWithLock(running.id(), TaskStatus.RUNNING,
                                         TaskStatus.FAILED)) {
                    runningTasks.remove(running.id());
                } else {
                    LOG.warn("Update task({})@({}/{}) status" +
                             "(RUNNING->FAILED) failed",
                             running.id(), this.graphSpace, this.graphName);
                }
            }
        }

        // === 第三阶段：处理FAILED状态的任务 ===
        // 失败的任务如果还有重试次数，重新标记为NEW等待执行
        // Handle tasks in FAILED/HANGING state
        Iterator<HugeTask<Object>> faileds =
            queryTaskWithoutResultByStatus(TaskStatus.FAILED);

        while (!this.closed.get() && faileds.hasNext()) {
            HugeTask<?> failed = faileds.next();
            initTaskParams(failed);
            // 检查重试次数是否超限
            if (failed.retries() < this.graph().option(CoreOptions.TASK_RETRY)) {
                LOG.info("Try to update task({})@({}/{}) status(FAILED->NEW)",
                         failed.id(), this.graphSpace, this.graphName);
                // 重置为NEW状态，等待重新执行
                updateStatusWithLock(failed.id(), TaskStatus.FAILED,
                                     TaskStatus.NEW);
            }
        }

        // === 第四阶段：处理CANCELLING状态的任务 ===
        // 处理正在取消的任务
        // Handling tasks in CANCELLING state
        Iterator<HugeTask<Object>> cancellings = queryTaskWithoutResultByStatus(
            TaskStatus.CANCELLING);

        while (!this.closed.get() && cancellings.hasNext()) {
            Id cancellingId = cancellings.next().id();
            if (runningTasks.containsKey(cancellingId)) {
                // 如果任务在本地运行，直接取消
                HugeTask<?> cancelling = runningTasks.get(cancellingId);
                initTaskParams(cancelling);
                LOG.info("Try to cancel task({})@({}/{})",
                         cancelling.id(), this.graphSpace, this.graphName);
                cancelling.cancel(true);

                runningTasks.remove(cancellingId);
            } else {
                // 本地没有执行该任务，但如果没有其他节点持有锁，直接标记为已取消
                // Local no execution task, but the current task has no nodes executing.
                if (!isLockedTask(cancellingId.toString())) {
                    updateStatusWithLock(cancellingId, TaskStatus.CANCELLING,
                                         TaskStatus.CANCELLED);
                }
            }
        }

        // === 第五阶段：处理DELETING状态的任务 ===
        // 删除标记为删除的任务
        // Handling tasks in DELETING status
        Iterator<HugeTask<Object>> deletings = queryTaskWithoutResultByStatus(
            TaskStatus.DELETING);

        while (!this.closed.get() && deletings.hasNext()) {
            Id deletingId = deletings.next().id();
            if (runningTasks.containsKey(deletingId)) {
                // 如果任务在本地运行，先取消再删除
                HugeTask<?> deleting = runningTasks.get(deletingId);
                initTaskParams(deleting);
                deleting.cancel(true);

                // Delete storage information
                deleteFromDB(deletingId);

                runningTasks.remove(deletingId);
            } else {
                // 本地没有执行该任务，如果没有其他节点持有锁，直接从数据库删除
                // Local has no task execution, but the current task has no nodes executing anymore.
                if (!isLockedTask(deletingId.toString())) {
                    deleteFromDB(deletingId);
                }
            }
        }
    }

    protected <V> Iterator<HugeTask<V>> queryTaskWithoutResultByStatus(TaskStatus status) {
        if (this.closed.get()) {
            return QueryResults.emptyIterator();
        }
        return queryTaskWithoutResult(HugeTask.P.STATUS, status.code(), NO_LIMIT, null);
    }

    @Override
    public HugeGraph graph() {
        return this.graph.graph();
    }

    @Override
    public int pendingTasks() {
        return this.runningTasks.size();
    }

    @Override
    public <V> void restoreTasks() {
        // DO Nothing!
    }

    @Override
    public <V> Future<?> schedule(HugeTask<V> task) {
        // 1. 参数校验：确保任务不为空
        E.checkArgumentNotNull(task, "Task can't be null");

        // 2. 初始化任务参数：绑定调度器、图实例等运行环境
        initTaskParams(task);

        // 3. 特殊处理：临时任务(ephemeralTask)直接在本地执行，无需调度
        // 临时任务通常是轻量级、快速执行的任务，不需要持久化和分布式调度
        if (task.ephemeralTask()) {
            // Handle ephemeral tasks, no scheduling needed, execute directly
            return this.ephemeralTaskExecutor.submit(task);
        }

        // 4. 任务持久化：将任务保存到数据库，状态设置为 NEW
        // 这样其他节点也能看到这个任务，是分布式调度的基础
        // Process schema task
        // Handle gremlin task
        // Handle OLAP calculation tasks
        // Add task to DB, current task status is NEW
        // TODO: save server id for task  // 注释：将来需要保存服务器ID，用于任务分配跟踪
        this.save(task);

        // 5. 本地执行尝试：如果调度器未关闭，尝试在当前节点立即执行任务
        // 这是一种优化策略：优先在创建任务的节点执行，避免不必要的调度延迟
        if (!this.closed.get()) {
            LOG.info("Try to start task({})@({}/{}) immediately", task.id(),
                     this.graphSpace, this.graphName);
            tryStartHugeTask(task);
        } else {
            LOG.info("TaskScheduler has closed");
        }

        // 6. 返回值：当前实现返回null，表示任务已提交但不提供Future跟踪
        // 任务状态跟踪需要通过数据库查询实现
        return null;
    }

    /**
     * 初始化任务参数：为任务绑定执行所需的环境变量
     * 这个方法在任务反序列化和执行前必须调用
     */
    protected <V> void initTaskParams(HugeTask<V> task) {
        // Bind the environment variables required for the current task execution
        // Before task deserialization and execution, this method needs to be called.
        
        // 绑定任务调度器：让任务知道由哪个调度器管理
        task.scheduler(this);
        
        // 获取任务的可调用对象(实际执行逻辑)
        TaskCallable<V> callable = task.callable();
        
        // 为可调用对象绑定任务实例和图实例
        callable.task(task);
        callable.graph(this.graph());

        // 特殊处理：如果是系统任务，需要绑定图参数
        if (callable instanceof TaskCallable.SysTaskCallable) {
            ((TaskCallable.SysTaskCallable<?>) callable).params(this.graph);
        }
    }

    @Override
    public <V> void cancel(HugeTask<V> task) {
        // Update status to CANCELLING
        if (!task.completed()) {
            // Task not completed, can only execute status not CANCELLING
            this.updateStatus(task.id(), null, TaskStatus.CANCELLING);
        } else {
            LOG.info("cancel task({}) error, task has completed", task.id());
        }
    }

    @Override
    public void init() {
        this.call(() -> this.tx().initSchema());
    }

    protected <V> HugeTask<V> deleteFromDB(Id id) {
        // Delete Task from DB, without checking task status
        return this.call(() -> {
            Iterator<Vertex> vertices = this.tx().queryTaskInfos(id);
            HugeVertex vertex = (HugeVertex) QueryResults.one(vertices);
            if (vertex == null) {
                return null;
            }
            HugeTask<V> result = HugeTask.fromVertex(vertex);
            this.tx().removeVertex(vertex);
            return result;
        });
    }

    @Override
    public <V> HugeTask<V> delete(Id id, boolean force) {
        if (!force) {
            // Change status to DELETING, perform the deletion operation through automatic scheduling.
            this.updateStatus(id, null, TaskStatus.DELETING);
            return null;
        } else {
            return this.deleteFromDB(id);
        }
    }

    @Override
    public boolean close() {
        if (this.closed.get()) {
            return true;
        }

        // set closed
        this.closed.set(true);

        // cancel all running tasks
        for (HugeTask<?> task : this.runningTasks.values()) {
            LOG.info("cancel task({}) @({}/{}) when closing scheduler",
                     task.id(), graphSpace, graphName);
            this.cancel(task);
        }

        try {
            this.waitUntilAllTasksCompleted(10);
        } catch (TimeoutException e) {
            LOG.warn("Tasks not completed when close distributed task scheduler", e);
        }

        // cancel cron thread
        if (!cronFuture.isDone() && !cronFuture.isCancelled()) {
            cronFuture.cancel(false);
        }

        if (!this.taskDbExecutor.isShutdown()) {
            this.call(() -> {
                try {
                    this.tx().close();
                } catch (ConnectionException ignored) {
                    // ConnectionException means no connection established
                }
                this.graph.closeTx();
            });
        }
        return true;
    }

    @Override
    public <V> HugeTask<V> waitUntilTaskCompleted(Id id, long seconds)
        throws TimeoutException {
        return this.waitUntilTaskCompleted(id, seconds, QUERY_INTERVAL);
    }

    @Override
    public <V> HugeTask<V> waitUntilTaskCompleted(Id id)
        throws TimeoutException {
        // This method is just used by tests
        long timeout = this.graph.configuration()
                                 .get(CoreOptions.TASK_WAIT_TIMEOUT);
        return this.waitUntilTaskCompleted(id, timeout, 1L);
    }

    private <V> HugeTask<V> waitUntilTaskCompleted(Id id, long seconds,
                                                   long intervalMs)
        throws TimeoutException {
        long passes = seconds * 1000 / intervalMs;
        HugeTask<V> task = null;
        for (long pass = 0; ; pass++) {
            try {
                task = this.taskWithoutResult(id);
            } catch (NotFoundException e) {
                if (task != null && task.completed()) {
                    assert task.id().asLong() < 0L : task.id();
                    sleep(intervalMs);
                    return task;
                }
                throw e;
            }
            if (task.completed()) {
                // Wait for task result being set after status is completed
                sleep(intervalMs);
                // Query task information with results
                task = this.task(id);
                return task;
            }
            if (pass >= passes) {
                break;
            }
            sleep(intervalMs);
        }
        throw new TimeoutException(String.format(
            "Task '%s' was not completed in %s seconds", id, seconds));
    }

    @Override
    public void waitUntilAllTasksCompleted(long seconds)
        throws TimeoutException {
        long passes = seconds * 1000 / QUERY_INTERVAL;
        int taskSize = 0;
        for (long pass = 0; ; pass++) {
            taskSize = this.pendingTasks();
            if (taskSize == 0) {
                sleep(QUERY_INTERVAL);
                return;
            }
            if (pass >= passes) {
                break;
            }
            sleep(QUERY_INTERVAL);
        }
        throw new TimeoutException(String.format(
            "There are still %s incomplete tasks after %s seconds",
            taskSize, seconds));

    }

    @Override
    public void checkRequirement(String op) {
        if (!this.serverManager().selfIsMaster()) {
            throw new HugeException("Can't %s task on non-master server", op);
        }
    }

    @Override
    public <V> V call(Callable<V> callable) {
        return this.call(callable, this.taskDbExecutor);
    }

    @Override
    public <V> V call(Runnable runnable) {
        return this.call(Executors.callable(runnable, null));
    }

    private <V> V call(Callable<V> callable, ExecutorService executor) {
        try {
            callable = new TaskManager.ContextCallable<>(callable);
            return executor.submit(callable).get();
        } catch (Exception e) {
            throw new HugeException("Failed to update/query TaskStore for " +
                                    "graph(%s/%s): %s", e, this.graphSpace,
                                    this.graph.name(), e.toString());
        }
    }

    protected boolean updateStatus(Id id, TaskStatus prestatus,
                                   TaskStatus status) {
        HugeTask<Object> task = this.taskWithoutResult(id);
        initTaskParams(task);
        if (prestatus == null || task.status() == prestatus) {
            task.overwriteStatus(status);
            // If the status is updated to FAILED -> NEW, then increase the retry count.
            if (prestatus == TaskStatus.FAILED && status == TaskStatus.NEW) {
                task.retry();
            }
            this.save(task);
            LOG.info("Update task({}) success: pre({}), status({})",
                     id, prestatus, status);

            return true;
        } else {
            LOG.warn("Update task({}) status conflict: current({}), " +
                     "pre({}), status({})", id, task.status(),
                     prestatus, status);
            return false;
        }
    }

    protected boolean updateStatusWithLock(Id id, TaskStatus prestatus,
                                           TaskStatus status) {

        LockResult lockResult = tryLockTask(id.asString());

        if (lockResult.lockSuccess()) {
            try {
                return updateStatus(id, prestatus, status);
            } finally {
                unlockTask(id.asString(), lockResult);
            }
        }

        return false;
    }

    /**
     * 尝试启动HugeTask：这是任务执行的核心方法
     * 根据任务类型选择合适的线程池，如果有可用资源则立即执行
     *
     * @param task 要执行的任务
     * @return true 如果任务成功提交执行；false 如果线程池已满
     */
    private boolean tryStartHugeTask(HugeTask<?> task) {
        // Print Scheduler status
        logCurrentState(); // 打印当前调度器状态，用于监控和调试

        // 初始化任务参数
        initTaskParams(task);

        // === 选择执行器策略 ===
        // 根据任务类型选择合适的线程池，实现任务隔离和资源管理
        ExecutorService chosenExecutor = gremlinTaskExecutor; // 默认使用gremlin执行器

        // OLAP计算任务：使用专门的OLAP线程池，通常资源更多
        if (task.computer()) {
            chosenExecutor = this.olapTaskExecutor;
        }

        // TODO: uncomment later - vermeer job
        // Vermeer任务：未来支持的分布式图计算框架
        //if (task.vermeer()) {
        //    chosenExecutor = this.olapTaskExecutor;
        //}

        // Gremlin查询任务：使用Gremlin专用线程池
        if (task.gremlinTask()) {
            chosenExecutor = this.gremlinTaskExecutor;
        }

        // Schema操作任务：使用Schema专用线程池，避免影响其他操作
        if (task.schemaTask()) {
            chosenExecutor = schemaTaskExecutor;
        }

        // === 资源检查和任务提交 ===
        // 检查选定的线程池是否有可用资源
        ThreadPoolExecutor executor = (ThreadPoolExecutor) chosenExecutor;
        if (executor.getActiveCount() < executor.getMaximumPoolSize()) {
            // 有可用资源，创建任务运行器并提交执行
            TaskRunner<?> runner = new TaskRunner<>(task);
            chosenExecutor.submit(runner);
            LOG.info("Submit task({})@({}/{})", task.id(),
                     this.graphSpace, this.graphName);

            return true; // 任务成功提交
        }

        return false; // 线程池已满，任务未能提交
    }

    protected void logCurrentState() {
        int gremlinActive =
            ((ThreadPoolExecutor) gremlinTaskExecutor).getActiveCount();
        int schemaActive =
            ((ThreadPoolExecutor) schemaTaskExecutor).getActiveCount();
        int ephemeralActive =
            ((ThreadPoolExecutor) ephemeralTaskExecutor).getActiveCount();
        int olapActive =
            ((ThreadPoolExecutor) olapTaskExecutor).getActiveCount();

        LOG.info("Current State: gremlinTaskExecutor({}), schemaTaskExecutor" +
                 "({}), ephemeralTaskExecutor({}), olapTaskExecutor({})",
                 gremlinActive, schemaActive, ephemeralActive, olapActive);
    }

    /**
     * 尝试锁定任务：在分布式环境中确保任务只在一个节点执行
     * 
     * @param taskId 任务ID
     * @return 锁结果对象，包含是否成功获取锁等信息
     */
    private LockResult tryLockTask(String taskId) {

        LockResult lockResult = new LockResult();

        try {
            // 通过MetaManager获取分布式锁，防止同一任务在多个节点同时执行
            lockResult =
                MetaManager.instance().tryLockTask(graphSpace, graphName,
                                                   taskId);
        } catch (Throwable t) {
            LOG.warn(String.format("try to lock task(%s) error", taskId), t);
        }

        return lockResult;
    }

    /**
     * 释放任务锁：任务执行完成或失败后释放锁，允许其他操作
     * 
     * @param taskId 任务ID
     * @param lockResult 之前获取锁时返回的结果对象
     */
    private void unlockTask(String taskId, LockResult lockResult) {

        try {
            // 释放之前获取的分布式锁
            MetaManager.instance().unlockTask(graphSpace, graphName, taskId,
                                              lockResult);
        } catch (Throwable t) {
            LOG.warn(String.format("try to unlock task(%s) error",
                                   taskId), t);
        }
    }

    /**
     * 检查任务是否被锁定：用于判断任务是否正在其他节点执行
     * 
     * @param taskId 任务ID
     * @return true 如果任务被锁定；false 如果任务未被锁定
     */
    private boolean isLockedTask(String taskId) {
        // 查询任务是否在分布式环境中被某个节点锁定
        return MetaManager.instance().isLockedTask(graphSpace,
                                                   graphName, taskId);
    }

    /**
     * 任务运行器：包装实际的任务执行逻辑
     * 负责任务的生命周期管理、锁管理、异常处理等
     */
    private class TaskRunner<V> implements Runnable {

        private final HugeTask<V> task;

        public TaskRunner(HugeTask<V> task) {
            this.task = task;
        }

        @Override
        public void run() {
            // === 第一步：尝试获取任务锁 ===
            // 使用分布式锁确保同一任务只在一个节点执行
            LockResult lockResult = tryLockTask(task.id().asString());

            // 重新初始化任务参数（因为可能跨线程传递）
            initTaskParams(task);
            
            // === 第二步：检查锁获取结果和任务状态 ===
            if (lockResult.lockSuccess() && !task.completed()) {

                LOG.info("Start task({})", task.id());

                // 设置任务上下文到当前线程
                TaskManager.setContext(task.context());
                try {
                    // === 第三步：二次状态检查（防止并发问题） ===
                    // 1. start task can be from schedule() & cronSchedule()
                    // 2. recheck the status of task, in case one same task
                    // called by both methods at same time;
                    
                    // 从数据库重新查询任务状态，确保状态一致性
                    HugeTask<Object> queryTask = task(this.task.id());
                    if (queryTask != null &&
                        !TaskStatus.NEW.equals(queryTask.status())) {
                        // 任务状态已变更，可能被其他节点处理了，直接返回
                        return;
                    }

                    // === 第四步：标记任务为运行中 ===
                    // 将任务添加到本地运行任务列表，用于状态跟踪
                    runningTasks.put(task.id(), task);

                    // === 第五步：执行任务 ===
                    // Task execution will not throw exceptions, HugeTask will catch exceptions during execution and store them in the DB.
                    // 任务执行不会抛出异常，HugeTask会捕获执行过程中的异常并存储到数据库
                    task.run();
                } catch (Throwable t) {
                    // === 异常处理 ===
                    // 记录执行过程中的异常（虽然正常情况下task.run()不应该抛异常）
                    LOG.warn("exception when execute task", t);
                } finally {
                    // === 第六步：清理资源 ===
                    // 无论执行成功还是失败，都要进行清理
                    
                    // 从本地运行列表中移除任务
                    runningTasks.remove(task.id());
                    
                    // 释放分布式锁，允许其他操作
                    unlockTask(task.id().asString(), lockResult);

                    LOG.info("task({}) finished.", task.id().toString());
                }
            }
            // 如果没有获取到锁或任务已完成，直接结束（其他节点可能已经在处理）
        }
    }

    @Override
    public String graphName() {
        return this.graph.name();
    }

    @Override
    public void taskDone(HugeTask<?> task) {
        // DO Nothing
    }
}
