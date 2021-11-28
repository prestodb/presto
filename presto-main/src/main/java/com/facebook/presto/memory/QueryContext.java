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
package com.facebook.presto.memory;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.stats.GcMonitor;
import com.facebook.presto.Session;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskStateMachine;
import com.facebook.presto.memory.context.MemoryReservationHandler;
import com.facebook.presto.memory.context.MemoryTrackingContext;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.operator.TaskMemoryReservationSummary;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spiller.SpillSpaceTracker;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static com.facebook.presto.ExceededMemoryLimitException.exceededLocalBroadcastMemoryLimit;
import static com.facebook.presto.ExceededMemoryLimitException.exceededLocalRevocableMemoryLimit;
import static com.facebook.presto.ExceededMemoryLimitException.exceededLocalTotalMemoryLimit;
import static com.facebook.presto.ExceededMemoryLimitException.exceededLocalUserMemoryLimit;
import static com.facebook.presto.ExceededSpillLimitException.exceededPerQueryLocalLimit;
import static com.facebook.presto.memory.context.AggregatedMemoryContext.newRootAggregatedMemoryContext;
import static com.facebook.presto.operator.Operator.NOT_BLOCKED;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.airlift.units.DataSize.succinctBytes;
import static java.lang.String.format;
import static java.util.Comparator.comparing;
import static java.util.Map.Entry.comparingByValue;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

@ThreadSafe
public class QueryContext
{
    private static final long GUARANTEED_MEMORY = new DataSize(1, MEGABYTE).toBytes();

    private final QueryId queryId;
    private final GcMonitor gcMonitor;
    private final Executor notificationExecutor;
    private final ScheduledExecutorService yieldExecutor;
    private final long maxSpill;
    private final SpillSpaceTracker spillSpaceTracker;
    private final JsonCodec<List<TaskMemoryReservationSummary>> memoryReservationSummaryJsonCodec;
    private final Map<TaskId, TaskContext> taskContexts = new ConcurrentHashMap<>();

    @GuardedBy("this")
    private boolean resourceOverCommit;
    private volatile boolean memoryLimitsInitialized;

    // TODO: This field should be final. However, due to the way QueryContext is constructed the memory limit is not known in advance
    @GuardedBy("this")
    private long maxUserMemory;
    @GuardedBy("this")
    private long maxTotalMemory;
    @GuardedBy("this")
    private long peakNodeTotalMemory;
    @GuardedBy("this")
    private long maxRevocableMemory;

    @GuardedBy("this")
    private long broadcastUsed;
    @GuardedBy("this")
    private long maxBroadcastUsedMemory;

    private final MemoryTrackingContext queryMemoryContext;

    @GuardedBy("this")
    private MemoryPool memoryPool;

    @GuardedBy("this")
    private long spillUsed;

    @GuardedBy("this")
    private boolean verboseExceededMemoryLimitErrorsEnabled;

    @GuardedBy("this")
    private boolean heapDumpOnExceededMemoryLimitEnabled;

    @GuardedBy("this")
    private Optional<String> heapDumpFilePath;

    public QueryContext(
            QueryId queryId,
            DataSize maxUserMemory,
            DataSize maxTotalMemory,
            DataSize maxBroadcastUsedMemory,
            DataSize maxRevocableMemory,
            MemoryPool memoryPool,
            GcMonitor gcMonitor,
            Executor notificationExecutor,
            ScheduledExecutorService yieldExecutor,
            DataSize maxSpill,
            SpillSpaceTracker spillSpaceTracker,
            JsonCodec<List<TaskMemoryReservationSummary>> memoryReservationSummaryJsonCodec)
    {
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.maxUserMemory = requireNonNull(maxUserMemory, "maxUserMemory is null").toBytes();
        this.maxTotalMemory = requireNonNull(maxTotalMemory, "maxTotalMemory is null").toBytes();
        this.maxBroadcastUsedMemory = requireNonNull(maxBroadcastUsedMemory, "maxBroadcastUsedMemory is null").toBytes();
        this.maxRevocableMemory = requireNonNull(maxRevocableMemory, "maxRevocableMemory is null").toBytes();
        this.memoryPool = requireNonNull(memoryPool, "memoryPool is null");
        this.gcMonitor = requireNonNull(gcMonitor, "gcMonitor is null");
        this.notificationExecutor = requireNonNull(notificationExecutor, "notificationExecutor is null");
        this.yieldExecutor = requireNonNull(yieldExecutor, "yieldExecutor is null");
        this.maxSpill = requireNonNull(maxSpill, "maxSpill is null").toBytes();
        this.spillSpaceTracker = requireNonNull(spillSpaceTracker, "spillSpaceTracker is null");
        this.memoryReservationSummaryJsonCodec = requireNonNull(memoryReservationSummaryJsonCodec, "memoryReservationSummaryJsonCodec is null");
        this.queryMemoryContext = new MemoryTrackingContext(
                newRootAggregatedMemoryContext(new QueryMemoryReservationHandler(this::updateUserMemory, this::tryUpdateUserMemory, this::updateBroadcastMemory, this::tryUpdateBroadcastMemory), GUARANTEED_MEMORY),
                newRootAggregatedMemoryContext(new QueryMemoryReservationHandler(this::updateRevocableMemory, this::tryReserveMemoryNotSupported, this::updateBroadcastMemory, this::tryUpdateBroadcastMemory), 0L),
                newRootAggregatedMemoryContext(new QueryMemoryReservationHandler(this::updateSystemMemory, this::tryReserveMemoryNotSupported, this::updateBroadcastMemory, this::tryUpdateBroadcastMemory), 0L));
    }

    public boolean isMemoryLimitsInitialized()
    {
        return memoryLimitsInitialized;
    }

    // TODO: This method should be removed, and the correct limit set in the constructor. However, due to the way QueryContext is constructed the memory limit is not known in advance
    public synchronized void setResourceOvercommit()
    {
        resourceOverCommit = true;
        // Allow the query to use the entire pool. This way the worker will kill the query, if it uses the entire local memory pool.
        // The coordinator will kill the query if the cluster runs out of memory.
        maxUserMemory = memoryPool.getMaxBytes();
        maxTotalMemory = memoryPool.getMaxBytes();
        //  Mark future memory limit updates as unnecessary
        memoryLimitsInitialized = true;
    }

    @VisibleForTesting
    MemoryTrackingContext getQueryMemoryContext()
    {
        return queryMemoryContext;
    }

    public synchronized void updateBroadcastMemory(long delta)
    {
        if (delta >= 0) {
            enforceBroadcastMemoryLimit(broadcastUsed, delta, maxBroadcastUsedMemory);
        }
        broadcastUsed += delta;
    }

    /**
     * Deadlock is possible for concurrent user and system allocations when updateSystemMemory()/updateUserMemory
     * calls queryMemoryContext.getUserMemory()/queryMemoryContext.getSystemMemory(), respectively.
     *
     * @see this#updateSystemMemory(String, long) for details.
     */
    private synchronized ListenableFuture<?> updateUserMemory(String allocationTag, long delta)
    {
        if (delta >= 0) {
            enforceUserMemoryLimit(queryMemoryContext.getUserMemory(), delta, maxUserMemory);
            long totalMemory = memoryPool.getQueryMemoryReservation(queryId);
            enforceTotalMemoryLimit(totalMemory, delta, maxTotalMemory);
            return memoryPool.reserve(queryId, allocationTag, delta);
        }
        memoryPool.free(queryId, allocationTag, -delta);
        return NOT_BLOCKED;
    }

    //TODO Add tagging support for revocable memory reservations if needed
    private synchronized ListenableFuture<?> updateRevocableMemory(String allocationTag, long delta)
    {
        long totalRevocableMemory = memoryPool.getQueryRevocableMemoryReservation(queryId);
        if (delta >= 0) {
            enforceRevocableMemoryLimit(totalRevocableMemory, delta, maxRevocableMemory);
            return memoryPool.reserveRevocable(queryId, delta);
        }
        memoryPool.freeRevocable(queryId, -delta);
        return NOT_BLOCKED;
    }

    private synchronized ListenableFuture<?> updateSystemMemory(String allocationTag, long delta)
    {
        // We call memoryPool.getQueryMemoryReservation(queryId) instead of calling queryMemoryContext.getUserMemory() to
        // calculate the total memory size.
        //
        // Calling the latter can result in a deadlock:
        // * A thread doing a user allocation will acquire locks in this order:
        //   1. monitor of queryMemoryContext.userAggregateMemoryContext
        //   2. monitor of this (QueryContext)
        // * The current thread doing a system allocation will acquire locks in this order:
        //   1. monitor of this (QueryContext)
        //   2. monitor of queryMemoryContext.userAggregateMemoryContext

        // Deadlock is possible for concurrent user and system allocations when updateSystemMemory()/updateUserMemory
        // calls queryMemoryContext.getUserMemory()/queryMemoryContext.getSystemMemory(), respectively. For concurrent
        // allocations of the same type (e.g., tryUpdateUserMemory/updateUserMemory) it is not possible as they share
        // the same RootAggregatedMemoryContext instance, and one of the threads will be blocked on the monitor of that
        // RootAggregatedMemoryContext instance even before calling the QueryContext methods (the monitors of
        // RootAggregatedMemoryContext instance and this will be acquired in the same order).
        if (delta >= 0) {
            long totalMemory = memoryPool.getQueryMemoryReservation(queryId);
            enforceTotalMemoryLimit(totalMemory, delta, maxTotalMemory);
            return memoryPool.reserve(queryId, allocationTag, delta);
        }
        memoryPool.free(queryId, allocationTag, -delta);
        return NOT_BLOCKED;
    }

    //TODO move spill tracking to the new memory tracking framework
    public synchronized ListenableFuture<?> reserveSpill(long bytes)
    {
        checkArgument(bytes >= 0, "bytes is negative");
        if (spillUsed + bytes > maxSpill) {
            throw exceededPerQueryLocalLimit(succinctBytes(maxSpill));
        }
        ListenableFuture<?> future = spillSpaceTracker.reserve(bytes);
        spillUsed += bytes;
        return future;
    }

    private synchronized boolean tryUpdateBroadcastMemory(long delta)
    {
        if (delta <= 0) {
            broadcastUsed += delta;
            return true;
        }
        if (broadcastUsed + delta > maxBroadcastUsedMemory) {
            return false;
        }
        broadcastUsed += delta;
        return true;
    }

    private synchronized boolean tryUpdateUserMemory(String allocationTag, long delta)
    {
        if (delta <= 0) {
            ListenableFuture<?> future = updateUserMemory(allocationTag, delta);
            // When delta == 0 and the pool is full the future can still not be done,
            // but, for negative deltas it must always be done.
            if (delta < 0) {
                verify(future.isDone(), "future should be done");
            }
            return true;
        }
        if (queryMemoryContext.getUserMemory() + delta > maxUserMemory) {
            return false;
        }

        long totalMemory = memoryPool.getQueryMemoryReservation(queryId);
        if (totalMemory + delta > maxTotalMemory) {
            return false;
        }
        return memoryPool.tryReserve(queryId, allocationTag, delta);
    }

    public synchronized void freeSpill(long bytes)
    {
        checkArgument(spillUsed - bytes >= 0, "tried to free more memory than is reserved");
        spillUsed -= bytes;
        spillSpaceTracker.free(bytes);
    }

    public synchronized void setMemoryPool(MemoryPool newMemoryPool)
    {
        // This method first acquires the monitor of this instance.
        // After that in this method if we acquire the monitors of the
        // user/revocable memory contexts in the queryMemoryContext instance
        // (say, by calling queryMemoryContext.getUserMemory()) it's possible
        // to have a deadlock. Because, the driver threads running the operators
        // will allocate memory concurrently through the child memory context -> ... ->
        // root memory context -> this.updateUserMemory() calls, and will acquire
        // the monitors of the user/revocable memory contexts in the queryMemoryContext instance
        // first, and then the monitor of this, which may cause deadlocks.
        // That's why instead of calling methods on queryMemoryContext to get the
        // user/revocable memory reservations, we call the MemoryPool to get the same
        // information.
        requireNonNull(newMemoryPool, "newMemoryPool is null");
        if (memoryPool == newMemoryPool) {
            // Don't unblock our tasks and thrash the pools, if this is a no-op
            return;
        }
        ListenableFuture<?> future = memoryPool.moveQuery(queryId, newMemoryPool);
        memoryPool = newMemoryPool;
        if (resourceOverCommit) {
            // Reset the memory limits based on the new pool assignment
            setResourceOvercommit();
        }
        future.addListener(() -> {
            // Unblock all the tasks, if they were waiting for memory, since we're in a new pool.
            taskContexts.values().forEach(TaskContext::moreMemoryAvailable);
        }, directExecutor());
    }

    public synchronized MemoryPool getMemoryPool()
    {
        return memoryPool;
    }

    public long getMaxUserMemory()
    {
        return maxUserMemory;
    }

    public long getMaxTotalMemory()
    {
        return maxTotalMemory;
    }

    public synchronized void setHeapDumpOnExceededMemoryLimitEnabled(boolean heapDumpOnExceededMemoryLimitEnabled)
    {
        this.heapDumpOnExceededMemoryLimitEnabled = heapDumpOnExceededMemoryLimitEnabled;
    }

    public synchronized void setHeapDumpFilePath(String heapDumpFilePath)
    {
        this.heapDumpFilePath = Optional.ofNullable(heapDumpFilePath);
    }

    public TaskContext addTaskContext(
            TaskStateMachine taskStateMachine,
            Session session,
            Optional<PlanNode> taskPlan,
            boolean perOperatorCpuTimerEnabled,
            boolean cpuTimerEnabled,
            boolean perOperatorAllocationTrackingEnabled,
            boolean allocationTrackingEnabled,
            boolean legacyLifespanCompletionCondition)
    {
        TaskContext taskContext = TaskContext.createTaskContext(
                this,
                taskStateMachine,
                gcMonitor,
                notificationExecutor,
                yieldExecutor,
                session,
                queryMemoryContext.newMemoryTrackingContext(),
                taskPlan,
                perOperatorCpuTimerEnabled,
                cpuTimerEnabled,
                perOperatorAllocationTrackingEnabled,
                allocationTrackingEnabled,
                legacyLifespanCompletionCondition);
        taskContexts.put(taskStateMachine.getTaskId(), taskContext);
        return taskContext;
    }

    public <C, R> R accept(QueryContextVisitor<C, R> visitor, C context)
    {
        return visitor.visitQueryContext(this, context);
    }

    public <C, R> List<R> acceptChildren(QueryContextVisitor<C, R> visitor, C context)
    {
        return taskContexts.values()
                .stream()
                .map(taskContext -> taskContext.accept(visitor, context))
                .collect(toList());
    }

    public TaskContext getTaskContextByTaskId(TaskId taskId)
    {
        TaskContext taskContext = taskContexts.get(taskId);
        verify(taskContext != null, "task does not exist");
        return taskContext;
    }

    public Collection<TaskContext> getAllTaskContexts()
    {
        return ImmutableList.copyOf(taskContexts.values());
    }

    public QueryId getQueryId()
    {
        return queryId;
    }

    public synchronized void setMemoryLimits(
            DataSize queryMaxTaskMemory,
            DataSize queryMaxTotalTaskMemory,
            DataSize queryMaxBroadcastMemory,
            DataSize queryMaxRevocableMemory)
    {
        // Don't allow session properties to increase memory beyond configured limits
        maxUserMemory = Math.min(maxUserMemory, queryMaxTaskMemory.toBytes());
        maxTotalMemory = Math.min(maxTotalMemory, queryMaxTotalTaskMemory.toBytes());
        maxBroadcastUsedMemory = Math.min(maxBroadcastUsedMemory, queryMaxBroadcastMemory.toBytes());
        maxRevocableMemory = Math.min(maxRevocableMemory, queryMaxRevocableMemory.toBytes());
        //  Mark future memory limit updates as unnecessary
        memoryLimitsInitialized = true;
    }

    public synchronized void setVerboseExceededMemoryLimitErrorsEnabled(boolean verboseExceededMemoryLimitErrorsEnabled)
    {
        this.verboseExceededMemoryLimitErrorsEnabled = verboseExceededMemoryLimitErrorsEnabled;
    }

    public synchronized long getPeakNodeTotalMemory()
    {
        return peakNodeTotalMemory;
    }

    public synchronized void setPeakNodeTotalMemory(long peakNodeTotalMemoryInBytes)
    {
        this.peakNodeTotalMemory = peakNodeTotalMemoryInBytes;
    }

    private static class QueryMemoryReservationHandler
            implements MemoryReservationHandler
    {
        private final BiFunction<String, Long, ListenableFuture<?>> reserveMemoryFunction;
        private final BiPredicate<String, Long> tryReserveMemoryFunction;
        private final Consumer<Long> updateBroadcastMemoryFunction;
        private final Predicate<Long> tryUpdateBroadcastMemoryFunction;

        public QueryMemoryReservationHandler(
                BiFunction<String, Long, ListenableFuture<?>> reserveMemoryFunction,
                BiPredicate<String, Long> tryReserveMemoryFunction,
                Consumer<Long> updateBroadcastMemoryFunction,
                Predicate<Long> tryUpdateBroadcastMemoryFunction)
        {
            this.reserveMemoryFunction = requireNonNull(reserveMemoryFunction, "reserveMemoryFunction is null");
            this.tryReserveMemoryFunction = requireNonNull(tryReserveMemoryFunction, "tryReserveMemoryFunction is null");
            this.updateBroadcastMemoryFunction = requireNonNull(updateBroadcastMemoryFunction, "updateBroadcastMemoryFunction is null");
            this.tryUpdateBroadcastMemoryFunction = requireNonNull(tryUpdateBroadcastMemoryFunction, "tryUpdateBroadcastMemoryFunction is null");
        }

        @Override
        public ListenableFuture<?> reserveMemory(String allocationTag, long delta, boolean enforceBroadcastMemoryLimit)
        {
            ListenableFuture<?> future = reserveMemoryFunction.apply(allocationTag, delta);
            if (enforceBroadcastMemoryLimit) {
                updateBroadcastMemoryFunction.accept(delta);
            }
            return future;
        }

        @Override
        public boolean tryReserveMemory(String allocationTag, long delta, boolean enforceBroadcastMemoryLimit)
        {
            if (!tryReserveMemoryFunction.test(allocationTag, delta)) {
                return false;
            }
            return !enforceBroadcastMemoryLimit || tryUpdateBroadcastMemoryFunction.test(delta);
        }
    }

    private boolean tryReserveMemoryNotSupported(String allocationTag, long bytes)
    {
        throw new UnsupportedOperationException("tryReserveMemory is not supported");
    }

    @GuardedBy("this")
    private void enforceBroadcastMemoryLimit(long allocated, long delta, long maxMemory)
    {
        if (allocated + delta > maxMemory) {
            throw exceededLocalBroadcastMemoryLimit(succinctBytes(maxMemory), getAdditionalFailureInfo(allocated, delta));
        }
    }

    @GuardedBy("this")
    private void enforceUserMemoryLimit(long allocated, long delta, long maxMemory)
    {
        if (allocated + delta > maxMemory) {
            throw exceededLocalUserMemoryLimit(succinctBytes(maxMemory), getAdditionalFailureInfo(allocated, delta), heapDumpOnExceededMemoryLimitEnabled, heapDumpFilePath);
        }
    }

    @GuardedBy("this")
    private void enforceTotalMemoryLimit(long allocated, long delta, long maxMemory)
    {
        long totalMemory = allocated + delta;
        peakNodeTotalMemory = Math.max(totalMemory, peakNodeTotalMemory);
        if (totalMemory > maxMemory) {
            throw exceededLocalTotalMemoryLimit(succinctBytes(maxMemory), getAdditionalFailureInfo(allocated, delta), heapDumpOnExceededMemoryLimitEnabled, heapDumpFilePath);
        }
    }

    @GuardedBy("this")
    private void enforceRevocableMemoryLimit(long allocated, long delta, long maxMemory)
    {
        if (allocated + delta > maxMemory) {
            throw exceededLocalRevocableMemoryLimit(succinctBytes(maxMemory), getAdditionalFailureInfo(allocated, delta), heapDumpOnExceededMemoryLimitEnabled, heapDumpFilePath);
        }
    }

    @GuardedBy("this")
    public String getAdditionalFailureInfo(long allocated, long delta)
    {
        Map<String, Long> queryAllocations = memoryPool.getTaggedMemoryAllocations(queryId);

        String additionalInfo = format("Allocated: %s, Delta: %s", succinctBytes(allocated), succinctBytes(delta));

        // It's possible that a query tries allocating more than the available memory
        // failing immediately before any allocation of that query is tagged
        if (queryAllocations == null) {
            return additionalInfo;
        }

        String topConsumers = queryAllocations.entrySet().stream()
                .sorted(comparingByValue(Comparator.reverseOrder()))
                .limit(3)
                .collect(toImmutableMap(Entry::getKey, e -> succinctBytes(e.getValue())))
                .toString();

        String message = format("%s, Top Consumers: %s", additionalInfo, topConsumers);

        if (verboseExceededMemoryLimitErrorsEnabled) {
            List<TaskMemoryReservationSummary> memoryReservationSummaries = taskContexts.values().stream()
                    .map(TaskContext::getMemoryReservationSummary)
                    .filter(summary -> summary.getReservation().toBytes() > 0)
                    .sorted(comparing(TaskMemoryReservationSummary::getReservation).reversed())
                    .limit(3)
                    .collect(toImmutableList());
            message += ", Details: " + memoryReservationSummaryJsonCodec.toJson(memoryReservationSummaries);
        }
        return message;
    }
}
