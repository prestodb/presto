/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.execution;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.memory.LocalMemoryManager;
import com.facebook.presto.memory.MemoryPool;
import com.facebook.presto.memory.TaskRevocableMemoryListener;
import com.facebook.presto.memory.VoidTraversingQueryContextVisitor;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.facebook.presto.execution.MemoryRevokingUtils.getMemoryPools;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TaskThresholdMemoryRevokingScheduler
{
    private static final Logger log = Logger.get(TaskThresholdMemoryRevokingScheduler.class);

    private final Supplier<List<SqlTask>> allTasksSupplier;
    private final Function<TaskId, SqlTask> taskSupplier;
    private final ScheduledExecutorService taskManagementExecutor;
    private final long maxRevocableMemoryPerTask;

    // Technically not thread safe but should be fine since we only call this on PostConstruct and PreDestroy.
    // PreDestroy isn't called until server shuts down/ in between tests.
    @Nullable
    private ScheduledFuture<?> scheduledFuture;

    private final AtomicBoolean checkPending = new AtomicBoolean();
    private final List<MemoryPool> memoryPools;
    private final TaskRevocableMemoryListener taskRevocableMemoryListener = this::onMemoryReserved;

    @Inject
    public TaskThresholdMemoryRevokingScheduler(
            LocalMemoryManager localMemoryManager,
            SqlTaskManager sqlTaskManager,
            TaskManagementExecutor taskManagementExecutor,
            FeaturesConfig config)
    {
        this(
                ImmutableList.copyOf(getMemoryPools(localMemoryManager)),
                requireNonNull(sqlTaskManager, "sqlTaskManager cannot be null")::getAllTasks,
                requireNonNull(sqlTaskManager, "sqlTaskManager cannot be null")::getTask,
                requireNonNull(taskManagementExecutor, "taskManagementExecutor cannot be null").getExecutor(),
                requireNonNull(config.getMaxRevocableMemoryPerTask(), "maxRevocableMemoryPerTask cannot be null").toBytes());
        log.debug("Using TaskThresholdMemoryRevokingScheduler spilling strategy");
    }

    @VisibleForTesting
    TaskThresholdMemoryRevokingScheduler(
            List<MemoryPool> memoryPools,
            Supplier<List<SqlTask>> allTasksSupplier,
            Function<TaskId, SqlTask> taskSupplier,
            ScheduledExecutorService taskManagementExecutor,
            long maxRevocableMemoryPerTask)
    {
        this.memoryPools = ImmutableList.copyOf(requireNonNull(memoryPools, "memoryPools is null"));
        this.allTasksSupplier = requireNonNull(allTasksSupplier, "allTasksSupplier is null");
        this.taskSupplier = requireNonNull(taskSupplier, "taskSupplier is null");
        this.taskManagementExecutor = requireNonNull(taskManagementExecutor, "taskManagementExecutor is null");
        this.maxRevocableMemoryPerTask = maxRevocableMemoryPerTask;
    }

    @PostConstruct
    public void start()
    {
        registerTaskMemoryPeriodicCheck();
        registerPoolListeners();
    }

    private void registerTaskMemoryPeriodicCheck()
    {
        this.scheduledFuture = taskManagementExecutor.scheduleWithFixedDelay(() -> {
            try {
                revokeHighMemoryTasksIfNeeded();
            }
            catch (Exception e) {
                log.error(e, "Error requesting task memory revoking");
            }
        }, 1, 1, SECONDS);
    }

    @PreDestroy
    public void stop()
    {
        if (scheduledFuture != null) {
            scheduledFuture.cancel(true);
            scheduledFuture = null;
        }

        memoryPools.forEach(memoryPool -> memoryPool.removeTaskRevocableMemoryListener(taskRevocableMemoryListener));
    }

    @VisibleForTesting
    void registerPoolListeners()
    {
        memoryPools.forEach(memoryPool -> memoryPool.addTaskRevocableMemoryListener(taskRevocableMemoryListener));
    }

    @VisibleForTesting
    void revokeHighMemoryTasksIfNeeded()
    {
        if (checkPending.compareAndSet(false, true)) {
            revokeHighMemoryTasks();
        }
    }

    private void onMemoryReserved(TaskId taskId)
    {
        try {
            SqlTask task = taskSupplier.apply(taskId);
            if (!memoryRevokingNeeded(task)) {
                return;
            }

            if (checkPending.compareAndSet(false, true)) {
                log.debug("Scheduling check for %s", taskId);
                scheduleRevoking();
            }
        }
        catch (Exception e) {
            log.error(e, "Error when acting on memory pool reservation");
        }
    }

    private void scheduleRevoking()
    {
        taskManagementExecutor.execute(() -> {
            try {
                revokeHighMemoryTasks();
            }
            catch (Exception e) {
                log.error(e, "Error requesting memory revoking");
            }
        });
    }

    private boolean memoryRevokingNeeded(SqlTask task)
    {
        return task.getTaskContext().filter(taskContext -> taskContext.getTaskMemoryContext().getRevocableMemory() >= maxRevocableMemoryPerTask).isPresent();
    }

    private synchronized void revokeHighMemoryTasks()
    {
        if (checkPending.getAndSet(false)) {
            Collection<SqlTask> sqlTasks = requireNonNull(allTasksSupplier.get());
            for (SqlTask task : sqlTasks) {
                Optional<TaskContext> taskContext = task.getTaskContext();
                if (!taskContext.isPresent()) {
                    continue;
                }
                long currentTaskRevocableMemory = taskContext.get().getTaskMemoryContext().getRevocableMemory();
                if (currentTaskRevocableMemory < maxRevocableMemoryPerTask) {
                    continue;
                }

                AtomicLong remainingBytesToRevokeAtomic = new AtomicLong(currentTaskRevocableMemory - maxRevocableMemoryPerTask);
                taskContext.get().accept(new VoidTraversingQueryContextVisitor<AtomicLong>()
                {
                    @Override
                    public Void visitOperatorContext(OperatorContext operatorContext, AtomicLong remainingBytesToRevoke)
                    {
                        if (remainingBytesToRevoke.get() > 0) {
                            long revokedBytes = operatorContext.requestMemoryRevoking();
                            if (revokedBytes > 0) {
                                remainingBytesToRevoke.addAndGet(-revokedBytes);
                                log.debug("taskId=%s: requested revoking %s; remaining %s", task.getTaskId(), revokedBytes, remainingBytesToRevoke.get());
                            }
                        }
                        return null;
                    }
                }, remainingBytesToRevokeAtomic);
            }
        }
    }
}
