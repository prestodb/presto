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
import com.facebook.presto.memory.QueryContext;
import com.facebook.presto.memory.VoidTraversingQueryContextVisitor;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.PipelineContext;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.memory.MemoryPoolId;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.FeaturesConfig.TaskSpillingStrategy;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.facebook.airlift.concurrent.Threads.threadsNamed;
import static com.facebook.presto.execution.MemoryRevokingUtils.getMemoryPools;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.TaskSpillingStrategy.PER_TASK_MEMORY_THRESHOLD;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

public class MemoryRevokingScheduler
{
    private static final Logger log = Logger.get(MemoryRevokingScheduler.class);

    private static final Ordering<SqlTask> ORDER_BY_CREATE_TIME = Ordering.natural().onResultOf(SqlTask::getTaskCreatedTime);

    private final Function<QueryId, QueryContext> queryContextSupplier;
    private final Supplier<List<SqlTask>> currentTasksSupplier;
    private final ExecutorService memoryRevocationExecutor;
    private final double memoryRevokingThreshold;
    private final double memoryRevokingTarget;
    private final TaskSpillingStrategy spillingStrategy;

    private final List<MemoryPool> memoryPools;
    private final MemoryPoolListener memoryPoolListener = this::onMemoryReserved;

    private final boolean queryLimitSpillEnabled;

    @Inject
    public MemoryRevokingScheduler(
            LocalMemoryManager localMemoryManager,
            SqlTaskManager sqlTaskManager,
            FeaturesConfig config)
    {
        this(
                ImmutableList.copyOf(getMemoryPools(localMemoryManager)),
                requireNonNull(sqlTaskManager, "sqlTaskManager cannot be null")::getAllTasks,
                requireNonNull(sqlTaskManager, "sqlTaskManager cannot be null")::getQueryContext,
                config.getMemoryRevokingThreshold(),
                config.getMemoryRevokingTarget(),
                config.getTaskSpillingStrategy(),
                config.isQueryLimitSpillEnabled());
    }

    @VisibleForTesting
    MemoryRevokingScheduler(
            List<MemoryPool> memoryPools,
            Supplier<List<SqlTask>> currentTasksSupplier,
            Function<QueryId, QueryContext> queryContextSupplier,
            double memoryRevokingThreshold,
            double memoryRevokingTarget,
            TaskSpillingStrategy taskSpillingStrategy,
            boolean queryLimitSpillEnabled)
    {
        this.memoryPools = ImmutableList.copyOf(requireNonNull(memoryPools, "memoryPools is null"));
        this.currentTasksSupplier = requireNonNull(currentTasksSupplier, "allTasksSupplier is null");
        this.queryContextSupplier = requireNonNull(queryContextSupplier, "queryContextSupplier is null");
        this.memoryRevokingThreshold = checkFraction(memoryRevokingThreshold, "memoryRevokingThreshold");
        this.memoryRevokingTarget = checkFraction(memoryRevokingTarget, "memoryRevokingTarget");
        // by using a single thread executor, we don't need to worry about locking to ensure only
        // one revocation request per-query/memory pool is processed at a time.
        this.memoryRevocationExecutor = newSingleThreadExecutor(threadsNamed("memory-revocation"));
        this.spillingStrategy = requireNonNull(taskSpillingStrategy, "taskSpillingStrategy is null");
        checkArgument(spillingStrategy != PER_TASK_MEMORY_THRESHOLD, "spilling strategy cannot be PER_TASK_MEMORY_THRESHOLD in MemoryRevokingScheduler");
        checkArgument(
                memoryRevokingTarget <= memoryRevokingThreshold,
                "memoryRevokingTarget should be less than or equal memoryRevokingThreshold, but got %s and %s respectively",
                memoryRevokingTarget, memoryRevokingThreshold);
        this.queryLimitSpillEnabled = queryLimitSpillEnabled;
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
        registerPoolListeners();
    }

    @PreDestroy
    public void stop()
    {
        memoryPools.forEach(memoryPool -> memoryPool.removeListener(memoryPoolListener));
        memoryRevocationExecutor.shutdown();
    }

    private void registerPoolListeners()
    {
        memoryPools.forEach(memoryPool -> memoryPool.addListener(memoryPoolListener));
    }

    @VisibleForTesting
    void awaitAsynchronousCallbacksRun()
            throws InterruptedException
    {
        memoryRevocationExecutor.invokeAll(singletonList((Callable<?>) () -> null));
    }

    private void onMemoryReserved(MemoryPool memoryPool, QueryId queryId, long queryMemoryReservation)
    {
        try {
            if (queryLimitSpillEnabled) {
                QueryContext queryContext = queryContextSupplier.apply(queryId);
                verify(queryContext != null, "QueryContext not found for queryId %s", queryId);
                long maxTotalMemory = queryContext.getMaxTotalMemory();
                if (memoryRevokingNeededForQuery(queryMemoryReservation, maxTotalMemory)) {
                    log.debug("Scheduling check for %s", queryId);
                    scheduleQueryRevoking(queryContext, maxTotalMemory);
                }
            }
            if (memoryRevokingNeededForPool(memoryPool)) {
                log.debug("Scheduling check for %s", memoryPool);
                scheduleMemoryPoolRevoking(memoryPool);
            }
        }
        catch (Exception e) {
            log.error(e, "Error when acting on memory pool reservation");
        }
    }

    private boolean memoryRevokingNeededForQuery(long queryMemoryReservation, long maxTotalMemory)
    {
        return queryMemoryReservation >= maxTotalMemory;
    }

    private void scheduleQueryRevoking(QueryContext queryContext, long maxTotalMemory)
    {
        memoryRevocationExecutor.execute(() -> {
            try {
                revokeQueryMemory(queryContext, maxTotalMemory);
            }
            catch (Exception e) {
                log.error(e, "Error requesting memory revoking");
            }
        });
    }

    private void revokeQueryMemory(QueryContext queryContext, long maxTotalMemory)
    {
        QueryId queryId = queryContext.getQueryId();
        MemoryPool memoryPool = queryContext.getMemoryPool();
        // get a fresh value for queryTotalMemory in case it's changed (e.g. by a previous revocation request)
        long queryTotalMemory = getTotalQueryMemoryReservation(queryId, memoryPool);
        // order tasks by decreasing revocableMemory so that we don't spill more tasks than needed
        SortedMap<Long, TaskContext> queryTaskContextsMap = new TreeMap<>(Comparator.reverseOrder());
        queryContext.getAllTaskContexts()
                .forEach(taskContext -> queryTaskContextsMap.put(taskContext.getTaskMemoryContext().getRevocableMemory(), taskContext));

        AtomicLong remainingBytesToRevoke = new AtomicLong(queryTotalMemory - maxTotalMemory);
        Collection<TaskContext> queryTaskContexts = queryTaskContextsMap.values();
        remainingBytesToRevoke.addAndGet(-MemoryRevokingSchedulerUtils.getMemoryAlreadyBeingRevoked(queryTaskContexts, remainingBytesToRevoke.get()));
        for (TaskContext taskContext : queryTaskContexts) {
            if (remainingBytesToRevoke.get() <= 0) {
                break;
            }
            taskContext.accept(new VoidTraversingQueryContextVisitor<AtomicLong>()
            {
                @Override
                public Void visitOperatorContext(OperatorContext operatorContext, AtomicLong remainingBytesToRevoke)
                {
                    if (remainingBytesToRevoke.get() > 0) {
                        long revokedBytes = operatorContext.requestMemoryRevoking();
                        if (revokedBytes > 0) {
                            remainingBytesToRevoke.addAndGet(-revokedBytes);
                            log.debug("taskId=%s: requested revoking %s; remaining %s", taskContext.getTaskId(), revokedBytes, remainingBytesToRevoke);
                        }
                    }
                    return null;
                }
            }, remainingBytesToRevoke);
        }
    }

    private static long getTotalQueryMemoryReservation(QueryId queryId, MemoryPool memoryPool)
    {
        return memoryPool.getQueryMemoryReservation(queryId) + memoryPool.getQueryRevocableMemoryReservation(queryId);
    }

    private void scheduleMemoryPoolRevoking(MemoryPool memoryPool)
    {
        memoryRevocationExecutor.execute(() -> {
            try {
                runMemoryPoolRevoking(memoryPool);
            }
            catch (Exception e) {
                log.error(e, "Error requesting memory revoking");
            }
        });
    }

    @VisibleForTesting
    void runMemoryPoolRevoking(MemoryPool memoryPool)
    {
        if (!memoryRevokingNeededForPool(memoryPool)) {
            return;
        }
        Collection<SqlTask> allTasks = requireNonNull(currentTasksSupplier.get());
        requestMemoryPoolRevoking(memoryPool, allTasks);
    }

    private void requestMemoryPoolRevoking(MemoryPool memoryPool, Collection<SqlTask> allTasks)
    {
        long remainingBytesToRevoke = (long) (-memoryPool.getFreeBytes() + (memoryPool.getMaxBytes() * (1.0 - memoryRevokingTarget)));
        ArrayList<SqlTask> runningTasksInPool = findRunningTasksInMemoryPool(allTasks, memoryPool);
        remainingBytesToRevoke -= getMemoryAlreadyBeingRevoked(runningTasksInPool, remainingBytesToRevoke);
        if (remainingBytesToRevoke > 0) {
            requestRevoking(memoryPool.getId(), runningTasksInPool, remainingBytesToRevoke);
        }
    }

    private boolean memoryRevokingNeededForPool(MemoryPool memoryPool)
    {
        return memoryPool.getReservedRevocableBytes() > 0
                && memoryPool.getFreeBytes() <= memoryPool.getMaxBytes() * (1.0 - memoryRevokingThreshold);
    }

    private long getMemoryAlreadyBeingRevoked(List<SqlTask> sqlTasks, long targetRevokingLimit)
    {
        List<TaskContext> taskContexts = sqlTasks.stream()
                .map(SqlTask::getTaskContext)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toImmutableList());
        return MemoryRevokingSchedulerUtils.getMemoryAlreadyBeingRevoked(taskContexts, targetRevokingLimit);
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
