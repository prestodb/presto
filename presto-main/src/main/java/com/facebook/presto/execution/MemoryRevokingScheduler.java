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
import com.facebook.presto.memory.MemoryPoolListener;
import com.facebook.presto.memory.TraversingQueryContextVisitor;
import com.facebook.presto.memory.VoidTraversingQueryContextVisitor;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.PipelineContext;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.spi.memory.MemoryPoolId;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.FeaturesConfig.TaskSpillingStrategy;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;

import javax.annotation.Nullable;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static com.facebook.presto.execution.MemoryRevokingUtils.getMemoryPools;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.TaskSpillingStrategy.PER_TASK_MEMORY_THRESHOLD;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public class MemoryRevokingScheduler
{
    private static final Logger log = Logger.get(MemoryRevokingScheduler.class);

    private static final Ordering<SqlTask> ORDER_BY_CREATE_TIME = Ordering.natural().onResultOf(SqlTask::getTaskCreatedTime);

    private final List<MemoryPool> memoryPools;
    private final Supplier<List<SqlTask>> currentTasksSupplier;
    private final ScheduledExecutorService taskManagementExecutor;
    private final double memoryRevokingThreshold;
    private final double memoryRevokingTarget;
    private final TaskSpillingStrategy spillingStrategy;

    private final MemoryPoolListener memoryPoolListener = MemoryPoolListener.onMemoryReserved(this::onMemoryReserved);

    @Nullable
    private ScheduledFuture<?> scheduledFuture;

    private final AtomicBoolean checkPending = new AtomicBoolean();

    @Inject
    public MemoryRevokingScheduler(
            LocalMemoryManager localMemoryManager,
            SqlTaskManager sqlTaskManager,
            TaskManagementExecutor taskManagementExecutor,
            FeaturesConfig config)
    {
        this(
                ImmutableList.copyOf(getMemoryPools(localMemoryManager)),
                requireNonNull(sqlTaskManager, "sqlTaskManager cannot be null")::getAllTasks,
                requireNonNull(taskManagementExecutor, "taskManagementExecutor cannot be null").getExecutor(),
                config.getMemoryRevokingThreshold(),
                config.getMemoryRevokingTarget(),
                config.getTaskSpillingStrategy());
    }

    @VisibleForTesting
    MemoryRevokingScheduler(
            List<MemoryPool> memoryPools,
            Supplier<List<SqlTask>> currentTasksSupplier,
            ScheduledExecutorService taskManagementExecutor,
            double memoryRevokingThreshold,
            double memoryRevokingTarget,
            TaskSpillingStrategy taskSpillingStrategy)
    {
        this.memoryPools = ImmutableList.copyOf(requireNonNull(memoryPools, "memoryPools is null"));
        this.currentTasksSupplier = requireNonNull(currentTasksSupplier, "currentTasksSupplier is null");
        this.taskManagementExecutor = requireNonNull(taskManagementExecutor, "taskManagementExecutor is null");
        this.memoryRevokingThreshold = checkFraction(memoryRevokingThreshold, "memoryRevokingThreshold");
        this.memoryRevokingTarget = checkFraction(memoryRevokingTarget, "memoryRevokingTarget");
        this.spillingStrategy = requireNonNull(taskSpillingStrategy, "taskSpillingStrategy is null");
        checkArgument(spillingStrategy != PER_TASK_MEMORY_THRESHOLD, "spilling strategy cannot be PER_TASK_MEMORY_THRESHOLD in MemoryRevokingScheduler");
        checkArgument(
                memoryRevokingTarget <= memoryRevokingThreshold,
                "memoryRevokingTarget should be less than or equal memoryRevokingThreshold, but got %s and %s respectively",
                memoryRevokingTarget, memoryRevokingThreshold);
    }

    private static double checkFraction(double value, String valueName)
    {
        requireNonNull(valueName, "valueName is null");
        checkArgument(0 <= value && value <= 1, "%s should be within [0, 1] range, got %s", valueName, value);
        return value;
    }

    @PostConstruct
    public void start()
    {
        registerPeriodicCheck();
        registerPoolListeners();
    }

    private void registerPeriodicCheck()
    {
        this.scheduledFuture = taskManagementExecutor.scheduleWithFixedDelay(() -> {
            try {
                requestMemoryRevokingIfNeeded();
            }
            catch (Throwable e) {
                log.error(e, "Error requesting system memory revoking");
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

        memoryPools.forEach(memoryPool -> memoryPool.removeListener(memoryPoolListener));
    }

    @VisibleForTesting
    void registerPoolListeners()
    {
        memoryPools.forEach(memoryPool -> memoryPool.addListener(memoryPoolListener));
    }

    private void onMemoryReserved(MemoryPool memoryPool)
    {
        try {
            if (!memoryRevokingNeeded(memoryPool)) {
                return;
            }

            if (checkPending.compareAndSet(false, true)) {
                log.debug("Scheduling check for %s", memoryPool);
                scheduleRevoking();
            }
        }
        catch (Throwable e) {
            log.error(e, "Error when acting on memory pool reservation");
        }
    }

    @VisibleForTesting
    void requestMemoryRevokingIfNeeded()
    {
        if (checkPending.compareAndSet(false, true)) {
            runMemoryRevoking();
        }
    }

    private void scheduleRevoking()
    {
        taskManagementExecutor.execute(() -> {
            try {
                runMemoryRevoking();
            }
            catch (Throwable e) {
                log.error(e, "Error requesting memory revoking");
            }
        });
    }

    private synchronized void runMemoryRevoking()
    {
        if (checkPending.getAndSet(false)) {
            Collection<SqlTask> allTasks = null;
            for (MemoryPool memoryPool : memoryPools) {
                if (!memoryRevokingNeeded(memoryPool)) {
                    continue;
                }

                if (allTasks == null) {
                    allTasks = requireNonNull(currentTasksSupplier.get());
                }

                requestMemoryRevoking(memoryPool, allTasks);
            }
        }
    }

    private void requestMemoryRevoking(MemoryPool memoryPool, Collection<SqlTask> allTasks)
    {
        long remainingBytesToRevoke = (long) (-memoryPool.getFreeBytes() + (memoryPool.getMaxBytes() * (1.0 - memoryRevokingTarget)));
        ArrayList<SqlTask> runningTasksInPool = findRunningTasksInMemoryPool(allTasks, memoryPool);
        remainingBytesToRevoke -= getMemoryAlreadyBeingRevoked(runningTasksInPool, remainingBytesToRevoke);
        if (remainingBytesToRevoke > 0) {
            requestRevoking(memoryPool.getId(), runningTasksInPool, remainingBytesToRevoke);
        }
    }

    private boolean memoryRevokingNeeded(MemoryPool memoryPool)
    {
        return memoryPool.getReservedRevocableBytes() > 0
                && memoryPool.getFreeBytes() <= memoryPool.getMaxBytes() * (1.0 - memoryRevokingThreshold);
    }

    private long getMemoryAlreadyBeingRevoked(List<SqlTask> sqlTasks, long targetRevokingLimit)
    {
        TraversingQueryContextVisitor<Void, Long> visitor = new TraversingQueryContextVisitor<Void, Long>()
        {
            @Override
            public Long visitOperatorContext(OperatorContext operatorContext, Void context)
            {
                if (operatorContext.isMemoryRevokingRequested()) {
                    return operatorContext.getReservedRevocableBytes();
                }
                return 0L;
            }

            @Override
            public Long mergeResults(List<Long> childrenResults)
            {
                return childrenResults.stream()
                        .mapToLong(i -> i).sum();
            }
        };

        long currentRevoking = 0;
        for (SqlTask task : sqlTasks) {
            Optional<TaskContext> taskContext = task.getTaskContext();
            if (taskContext.isPresent()) {
                currentRevoking += taskContext.get().accept(visitor, null);
                if (currentRevoking > targetRevokingLimit) {
                    // Return early, target value exceeded and revoking will not occur
                    return currentRevoking;
                }
            }
        }
        return currentRevoking;
    }

    private void requestRevoking(MemoryPoolId memoryPoolId, ArrayList<SqlTask> sqlTasks, long remainingBytesToRevoke)
    {
        VoidTraversingQueryContextVisitor<AtomicLong> visitor = new VoidTraversingQueryContextVisitor<AtomicLong>()
        {
            @Override
            public Void visitPipelineContext(PipelineContext pipelineContext, AtomicLong remainingBytesToRevoke)
            {
                if (remainingBytesToRevoke.get() <= 0) {
                    // exit immediately if no work needs to be done
                    return null;
                }
                return super.visitPipelineContext(pipelineContext, remainingBytesToRevoke);
            }

            @Override
            public Void visitOperatorContext(OperatorContext operatorContext, AtomicLong remainingBytesToRevoke)
            {
                if (remainingBytesToRevoke.get() > 0) {
                    long revokedBytes = operatorContext.requestMemoryRevoking();
                    if (revokedBytes > 0) {
                        remainingBytesToRevoke.addAndGet(-revokedBytes);
                        log.debug("memoryPool=%s: requested revoking %s; remaining %s", memoryPoolId, revokedBytes, remainingBytesToRevoke.get());
                    }
                }
                return null;
            }
        };

        // Sort the tasks into their traversal order
        log.debug("Ordering by %s", spillingStrategy);
        sortTasksToTraversalOrder(sqlTasks, spillingStrategy);

        AtomicLong remainingBytesToRevokeAtomic = new AtomicLong(remainingBytesToRevoke);
        for (SqlTask task : sqlTasks) {
            Optional<TaskContext> taskContext = task.getTaskContext();
            if (taskContext.isPresent()) {
                taskContext.get().accept(visitor, remainingBytesToRevokeAtomic);
                if (remainingBytesToRevokeAtomic.get() <= 0) {
                    // No further revoking required
                    return;
                }
            }
        }
    }

    private static void sortTasksToTraversalOrder(ArrayList<SqlTask> sqlTasks, TaskSpillingStrategy spillingStrategy)
    {
        switch (spillingStrategy) {
            case ORDER_BY_CREATE_TIME:
                sqlTasks.sort(ORDER_BY_CREATE_TIME);
                break;
            case ORDER_BY_REVOCABLE_BYTES:
                // To avoid repeatedly generating the task info, we have to compare by their mapping
                HashMap<TaskId, Long> taskRevocableReservations = new HashMap<>();
                for (SqlTask sqlTask : sqlTasks) {
                    taskRevocableReservations.put(sqlTask.getTaskId(), sqlTask.getTaskInfo().getStats().getRevocableMemoryReservationInBytes());
                }
                sqlTasks.sort(Ordering.natural().reverse().onResultOf(task -> task == null ? 0L : taskRevocableReservations.getOrDefault(task.getTaskId(), 0L)));
                break;
            case PER_TASK_MEMORY_THRESHOLD:
                throw new IllegalArgumentException("spilling strategy cannot be PER_TASK_MEMORY_THRESHOLD in MemoryRevokingScheduler");
            default:
                throw new UnsupportedOperationException("Unexpected spilling strategy in MemoryRevokingScheduler");
        }
    }

    private static ArrayList<SqlTask> findRunningTasksInMemoryPool(Collection<SqlTask> allCurrentTasks, MemoryPool memoryPool)
    {
        ArrayList<SqlTask> sqlTasks = new ArrayList<>();
        allCurrentTasks.stream()
                .filter(task -> task.getTaskState() == TaskState.RUNNING && task.getQueryContext().getMemoryPool() == memoryPool)
                .forEach(sqlTasks::add); // Resulting list must be mutable to enable sorting after the fact
        return sqlTasks;
    }
}
