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
package com.facebook.presto.operator;

import com.facebook.presto.Session;
import com.facebook.presto.memory.QueryContextVisitor;
import com.facebook.presto.memory.context.AggregatedMemoryContext;
import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.memory.context.MemoryTrackingContext;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.stats.CounterStat;
import io.airlift.units.Duration;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static com.facebook.presto.operator.BlockedReason.WAITING_FOR_MEMORY;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.units.DataSize.succinctBytes;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Only {@link #getOperatorStats()} and revocable-memory-related operations are ThreadSafe
 */
public class OperatorContext
{
    private static final ThreadMXBean THREAD_MX_BEAN = ManagementFactory.getThreadMXBean();

    private final int operatorId;
    private final PlanNodeId planNodeId;
    private final String operatorType;
    private final DriverContext driverContext;
    private final Executor executor;

    private final AtomicLong intervalWallStart = new AtomicLong();
    private final AtomicLong intervalCpuStart = new AtomicLong();
    private final AtomicLong intervalUserStart = new AtomicLong();

    private final AtomicLong addInputCalls = new AtomicLong();
    private final AtomicLong addInputWallNanos = new AtomicLong();
    private final AtomicLong addInputCpuNanos = new AtomicLong();
    private final AtomicLong addInputUserNanos = new AtomicLong();
    private final CounterStat inputDataSize = new CounterStat();
    private final CounterStat inputPositions = new CounterStat();

    private final AtomicLong getOutputCalls = new AtomicLong();
    private final AtomicLong getOutputWallNanos = new AtomicLong();
    private final AtomicLong getOutputCpuNanos = new AtomicLong();
    private final AtomicLong getOutputUserNanos = new AtomicLong();
    private final CounterStat outputDataSize = new CounterStat();
    private final CounterStat outputPositions = new CounterStat();

    private final AtomicLong physicalWrittenDataSize = new AtomicLong();

    private final AtomicReference<SettableFuture<?>> memoryFuture;
    private final AtomicReference<SettableFuture<?>> revocableMemoryFuture;
    private final AtomicReference<BlockedMonitor> blockedMonitor = new AtomicReference<>();
    private final AtomicLong blockedWallNanos = new AtomicLong();

    private final AtomicLong finishCalls = new AtomicLong();
    private final AtomicLong finishWallNanos = new AtomicLong();
    private final AtomicLong finishCpuNanos = new AtomicLong();
    private final AtomicLong finishUserNanos = new AtomicLong();

    private final SpillContext spillContext;
    private final AtomicReference<Supplier<OperatorInfo>> infoSupplier = new AtomicReference<>();
    private final boolean collectTimings;

    private final AtomicLong peakUserMemoryReservation = new AtomicLong();
    private final AtomicLong peakSystemMemoryReservation = new AtomicLong();
    private final AtomicLong peakTotalMemoryReservation = new AtomicLong();

    @GuardedBy("this")
    private boolean memoryRevokingRequested;

    @Nullable
    @GuardedBy("this")
    private Runnable memoryRevocationRequestListener;

    private final MemoryTrackingContext operatorMemoryContext;

    public OperatorContext(
            int operatorId,
            PlanNodeId planNodeId,
            String operatorType,
            DriverContext driverContext,
            Executor executor,
            MemoryTrackingContext operatorMemoryContext)
    {
        checkArgument(operatorId >= 0, "operatorId is negative");
        this.operatorId = operatorId;
        this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
        this.operatorType = requireNonNull(operatorType, "operatorType is null");
        this.driverContext = requireNonNull(driverContext, "driverContext is null");
        this.spillContext = new OperatorSpillContext(this.driverContext);
        this.executor = requireNonNull(executor, "executor is null");
        this.memoryFuture = new AtomicReference<>(SettableFuture.create());
        this.memoryFuture.get().set(null);
        this.revocableMemoryFuture = new AtomicReference<>(SettableFuture.create());
        this.revocableMemoryFuture.get().set(null);
        this.operatorMemoryContext = requireNonNull(operatorMemoryContext, "operatorMemoryContext is null");

        collectTimings = driverContext.isVerboseStats() && driverContext.isCpuTimerEnabled();
    }

    public int getOperatorId()
    {
        return operatorId;
    }

    public String getOperatorType()
    {
        return operatorType;
    }

    public DriverContext getDriverContext()
    {
        return driverContext;
    }

    public Session getSession()
    {
        return driverContext.getSession();
    }

    public boolean isDone()
    {
        return driverContext.isDone();
    }

    public void startIntervalTimer()
    {
        intervalWallStart.set(System.nanoTime());
        intervalCpuStart.set(currentThreadCpuTime());
        intervalUserStart.set(currentThreadUserTime());
    }

    public void recordAddInput(Page page)
    {
        addInputCalls.incrementAndGet();
        recordInputWallNanos(nanosBetween(intervalWallStart.get(), System.nanoTime()));
        addInputCpuNanos.getAndAdd(nanosBetween(intervalCpuStart.get(), currentThreadCpuTime()));
        addInputUserNanos.getAndAdd(nanosBetween(intervalUserStart.get(), currentThreadUserTime()));

        if (page != null) {
            inputDataSize.update(page.getSizeInBytes());
            inputPositions.update(page.getPositionCount());
        }
    }

    public void recordGeneratedInput(long sizeInBytes, long positions)
    {
        recordGeneratedInput(sizeInBytes, positions, 0);
    }

    public void recordGeneratedInput(long sizeInBytes, long positions, long readNanos)
    {
        inputDataSize.update(sizeInBytes);
        inputPositions.update(positions);
        recordInputWallNanos(readNanos);
    }

    public long recordInputWallNanos(long readNanos)
    {
        return addInputWallNanos.getAndAdd(readNanos);
    }

    public void recordGetOutput(Page page)
    {
        getOutputCalls.incrementAndGet();
        getOutputWallNanos.getAndAdd(nanosBetween(intervalWallStart.get(), System.nanoTime()));
        getOutputCpuNanos.getAndAdd(nanosBetween(intervalCpuStart.get(), currentThreadCpuTime()));
        getOutputUserNanos.getAndAdd(nanosBetween(intervalUserStart.get(), currentThreadUserTime()));

        if (page != null) {
            outputDataSize.update(page.getSizeInBytes());
            outputPositions.update(page.getPositionCount());
        }
    }

    public void recordGeneratedOutput(long sizeInBytes, long positions)
    {
        outputDataSize.update(sizeInBytes);
        outputPositions.update(positions);
    }

    public void recordPhysicalWrittenData(long sizeInBytes)
    {
        physicalWrittenDataSize.getAndAdd(sizeInBytes);
    }

    public void recordBlocked(ListenableFuture<?> blocked)
    {
        requireNonNull(blocked, "blocked is null");

        BlockedMonitor monitor = new BlockedMonitor();

        BlockedMonitor oldMonitor = blockedMonitor.getAndSet(monitor);
        if (oldMonitor != null) {
            oldMonitor.run();
        }

        blocked.addListener(monitor, executor);
        // Do not register blocked with driver context.  The driver handles this directly.
    }

    public void recordFinish()
    {
        finishCalls.incrementAndGet();
        finishWallNanos.getAndAdd(nanosBetween(intervalWallStart.get(), System.nanoTime()));
        finishCpuNanos.getAndAdd(nanosBetween(intervalCpuStart.get(), currentThreadCpuTime()));
        finishUserNanos.getAndAdd(nanosBetween(intervalUserStart.get(), currentThreadUserTime()));
    }

    public ListenableFuture<?> isWaitingForMemory()
    {
        return memoryFuture.get();
    }

    public ListenableFuture<?> isWaitingForRevocableMemory()
    {
        return revocableMemoryFuture.get();
    }

    // caller should close this context as it's a new context
    public LocalMemoryContext newLocalSystemMemoryContext()
    {
        return new InternalLocalMemoryContext(operatorMemoryContext.newSystemMemoryContext(), memoryFuture, this::updatePeakMemoryReservations);
    }

    // caller shouldn't close this context as it's managed by the OperatorContext
    public LocalMemoryContext localUserMemoryContext()
    {
        return new InternalLocalMemoryContext(operatorMemoryContext.localUserMemoryContext(), memoryFuture, this::updatePeakMemoryReservations);
    }

    // caller shouldn't close this context as it's managed by the OperatorContext
    public LocalMemoryContext localSystemMemoryContext()
    {
        return new InternalLocalMemoryContext(operatorMemoryContext.localSystemMemoryContext(), memoryFuture, this::updatePeakMemoryReservations);
    }

    // caller shouldn't close this context as it's managed by the OperatorContext
    public LocalMemoryContext localRevocableMemoryContext()
    {
        return new InternalLocalMemoryContext(operatorMemoryContext.localRevocableMemoryContext(), revocableMemoryFuture, () -> {});
    }

    // caller shouldn't close this context as it's managed by the OperatorContext
    public AggregatedMemoryContext aggregateUserMemoryContext()
    {
        return new InternalAggregatedMemoryContext(operatorMemoryContext.aggregateUserMemoryContext(), memoryFuture, this::updatePeakMemoryReservations);
    }

    // caller should close this context as it's a new context
    public AggregatedMemoryContext newAggregateSystemMemoryContext()
    {
        return new InternalAggregatedMemoryContext(operatorMemoryContext.newAggregateSystemMemoryContext(), memoryFuture, this::updatePeakMemoryReservations);
    }

    // listen to all memory allocations and update the peak memory reservations accordingly
    private void updatePeakMemoryReservations()
    {
        long userMemory = operatorMemoryContext.getUserMemory();
        long systemMemory = operatorMemoryContext.getSystemMemory();
        long totalMemory = userMemory + systemMemory;
        peakUserMemoryReservation.accumulateAndGet(userMemory, Math::max);
        peakSystemMemoryReservation.accumulateAndGet(systemMemory, Math::max);
        peakTotalMemoryReservation.accumulateAndGet(totalMemory, Math::max);
    }

    public long getReservedRevocableBytes()
    {
        return operatorMemoryContext.getRevocableMemory();
    }

    private static void updateMemoryFuture(ListenableFuture<?> memoryPoolFuture, AtomicReference<SettableFuture<?>> targetFutureReference)
    {
        if (!memoryPoolFuture.isDone()) {
            SettableFuture<?> currentMemoryFuture = targetFutureReference.get();
            while (currentMemoryFuture.isDone()) {
                SettableFuture<?> settableFuture = SettableFuture.create();
                // We can't replace one that's not done, because the task may be blocked on that future
                if (targetFutureReference.compareAndSet(currentMemoryFuture, settableFuture)) {
                    currentMemoryFuture = settableFuture;
                }
                else {
                    currentMemoryFuture = targetFutureReference.get();
                }
            }

            SettableFuture<?> finalMemoryFuture = currentMemoryFuture;
            // Create a new future, so that this operator can un-block before the pool does, if it's moved to a new pool
            memoryPoolFuture.addListener(() -> finalMemoryFuture.set(null), directExecutor());
        }
    }

    public void destroy()
    {
        // reset memory revocation listener so that OperatorContext doesn't hold any references to Driver instance
        synchronized (this) {
            memoryRevocationRequestListener = null;
        }

        operatorMemoryContext.close();

        if (operatorMemoryContext.getSystemMemory() != 0) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, format("Operator %s has non-zero system memory (%d bytes) after destroy()", this, operatorMemoryContext.getSystemMemory()));
        }

        if (operatorMemoryContext.getUserMemory() != 0) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, format("Operator %s has non-zero user memory (%d bytes) after destroy()", this, operatorMemoryContext.getUserMemory()));
        }

        if (operatorMemoryContext.getRevocableMemory() != 0) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, format("Operator %s has non-zero revocable memory (%d bytes) after destroy()", this, operatorMemoryContext.getRevocableMemory()));
        }
    }

    public SpillContext getSpillContext()
    {
        return spillContext;
    }

    public void moreMemoryAvailable()
    {
        memoryFuture.get().set(null);
    }

    public synchronized boolean isMemoryRevokingRequested()
    {
        return memoryRevokingRequested;
    }

    /**
     * Returns how much revocable memory will be revoked by the operator
     */
    public long requestMemoryRevoking()
    {
        long revokedMemory = 0L;
        Runnable listener = null;
        synchronized (this) {
            if (!isMemoryRevokingRequested() && operatorMemoryContext.getRevocableMemory() > 0) {
                memoryRevokingRequested = true;
                revokedMemory = operatorMemoryContext.getRevocableMemory();
                listener = memoryRevocationRequestListener;
            }
        }
        if (listener != null) {
            runListener(listener);
        }
        return revokedMemory;
    }

    public synchronized void resetMemoryRevokingRequested()
    {
        memoryRevokingRequested = false;
    }

    public void setMemoryRevocationRequestListener(Runnable listener)
    {
        requireNonNull(listener, "listener is null");

        boolean shouldNotify;
        synchronized (this) {
            checkState(memoryRevocationRequestListener == null, "listener already set");
            memoryRevocationRequestListener = listener;
            shouldNotify = memoryRevokingRequested;
        }
        // if memory revoking is requested immediately run the listener
        if (shouldNotify) {
            runListener(listener);
        }
    }

    private static void runListener(Runnable listener)
    {
        requireNonNull(listener, "listener is null");
        try {
            listener.run();
        }
        catch (RuntimeException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Exception while running the listener", e);
        }
    }

    public void setInfoSupplier(Supplier<OperatorInfo> infoSupplier)
    {
        requireNonNull(infoSupplier, "infoProvider is null");
        this.infoSupplier.set(infoSupplier);
    }

    public CounterStat getInputDataSize()
    {
        return inputDataSize;
    }

    public CounterStat getInputPositions()
    {
        return inputPositions;
    }

    public CounterStat getOutputDataSize()
    {
        return outputDataSize;
    }

    public CounterStat getOutputPositions()
    {
        return outputPositions;
    }

    public long getPhysicalWrittenDataSize()
    {
        return physicalWrittenDataSize.get();
    }

    @Override
    public String toString()
    {
        return format("%s-%s", operatorType, planNodeId);
    }

    public OperatorStats getOperatorStats()
    {
        Supplier<OperatorInfo> infoSupplier = this.infoSupplier.get();
        OperatorInfo info = Optional.ofNullable(infoSupplier).map(Supplier::get).orElse(null);

        long inputPositionsCount = inputPositions.getTotalCount();

        return new OperatorStats(
                driverContext.getPipelineContext().getPipelineId(),
                operatorId,
                planNodeId,
                operatorType,

                1,

                addInputCalls.get(),
                new Duration(addInputWallNanos.get(), NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(addInputCpuNanos.get(), NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(addInputUserNanos.get(), NANOSECONDS).convertToMostSuccinctTimeUnit(),
                succinctBytes(inputDataSize.getTotalCount()),
                inputPositionsCount,
                (double) inputPositionsCount * inputPositionsCount,

                getOutputCalls.get(),
                new Duration(getOutputWallNanos.get(), NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(getOutputCpuNanos.get(), NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(getOutputUserNanos.get(), NANOSECONDS).convertToMostSuccinctTimeUnit(),
                succinctBytes(outputDataSize.getTotalCount()),
                outputPositions.getTotalCount(),

                succinctBytes(physicalWrittenDataSize.get()),

                new Duration(blockedWallNanos.get(), NANOSECONDS).convertToMostSuccinctTimeUnit(),

                finishCalls.get(),
                new Duration(finishWallNanos.get(), NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(finishCpuNanos.get(), NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(finishUserNanos.get(), NANOSECONDS).convertToMostSuccinctTimeUnit(),

                succinctBytes(operatorMemoryContext.getUserMemory()),
                succinctBytes(getReservedRevocableBytes()),
                succinctBytes(operatorMemoryContext.getSystemMemory()),

                succinctBytes(peakUserMemoryReservation.get()),
                succinctBytes(peakSystemMemoryReservation.get()),
                succinctBytes(peakTotalMemoryReservation.get()),

                memoryFuture.get().isDone() ? Optional.empty() : Optional.of(WAITING_FOR_MEMORY),
                info);
    }

    public <C, R> R accept(QueryContextVisitor<C, R> visitor, C context)
    {
        return visitor.visitOperatorContext(this, context);
    }

    private long currentThreadUserTime()
    {
        if (!collectTimings) {
            return 0;
        }
        return THREAD_MX_BEAN.getCurrentThreadUserTime();
    }

    private long currentThreadCpuTime()
    {
        if (!collectTimings) {
            return 0;
        }
        return THREAD_MX_BEAN.getCurrentThreadCpuTime();
    }

    private static long nanosBetween(long start, long end)
    {
        return Math.abs(end - start);
    }

    private class BlockedMonitor
            implements Runnable
    {
        private final long start = System.nanoTime();
        private boolean finished;

        @Override
        public synchronized void run()
        {
            if (finished) {
                return;
            }
            finished = true;
            blockedMonitor.compareAndSet(this, null);
            blockedWallNanos.getAndAdd(getBlockedTime());
        }

        public long getBlockedTime()
        {
            return nanosBetween(start, System.nanoTime());
        }
    }

    @ThreadSafe
    private static class OperatorSpillContext
            implements SpillContext
    {
        private final DriverContext driverContext;
        private final AtomicLong reservedBytes = new AtomicLong();

        public OperatorSpillContext(DriverContext driverContext)
        {
            this.driverContext = driverContext;
        }

        @Override
        public void updateBytes(long bytes)
        {
            if (bytes >= 0) {
                reservedBytes.addAndGet(bytes);
                driverContext.reserveSpill(bytes);
            }
            else {
                reservedBytes.accumulateAndGet(-bytes, this::decrementSpilledReservation);
                driverContext.freeSpill(-bytes);
            }
        }

        private long decrementSpilledReservation(long reservedBytes, long bytesBeingFreed)
        {
            checkArgument(bytesBeingFreed >= 0);
            checkArgument(bytesBeingFreed <= reservedBytes, "tried to free %s spilled bytes from %s bytes reserved", bytesBeingFreed, reservedBytes);
            return reservedBytes - bytesBeingFreed;
        }

        @Override
        public void close()
        {
            // Only products of SpillContext.newLocalSpillContext() should be closed.
            throw new UnsupportedOperationException(format("%s should not be closed directly", getClass()));
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("usedBytes", reservedBytes.get())
                    .toString();
        }
    }

    private static class InternalLocalMemoryContext
            implements LocalMemoryContext
    {
        private final LocalMemoryContext delegate;
        private final AtomicReference<SettableFuture<?>> memoryFuture;
        private final Runnable allocationListener;

        InternalLocalMemoryContext(LocalMemoryContext delegate, AtomicReference<SettableFuture<?>> memoryFuture, Runnable allocationListener)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
            this.memoryFuture = requireNonNull(memoryFuture, "memoryFuture is null");
            this.allocationListener = requireNonNull(allocationListener, "allocationListener is null");
        }

        @Override
        public long getBytes()
        {
            return delegate.getBytes();
        }

        @Override
        public ListenableFuture<?> setBytes(long bytes)
        {
            ListenableFuture<?> blocked = delegate.setBytes(bytes);
            updateMemoryFuture(blocked, memoryFuture);
            allocationListener.run();
            return blocked;
        }

        @Override
        public boolean trySetBytes(long bytes)
        {
            return delegate.trySetBytes(bytes);
        }

        @Override
        public void close()
        {
            delegate.close();
        }
    }

    private static class InternalAggregatedMemoryContext
            implements AggregatedMemoryContext
    {
        private final AggregatedMemoryContext delegate;
        private final AtomicReference<SettableFuture<?>> memoryFuture;
        private final Runnable allocationListener;

        InternalAggregatedMemoryContext(AggregatedMemoryContext delegate, AtomicReference<SettableFuture<?>> memoryFuture, Runnable allocationListener)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
            this.memoryFuture = requireNonNull(memoryFuture, "memoryFuture is null");
            this.allocationListener = requireNonNull(allocationListener, "allocationListener is null");
        }

        @Override
        public AggregatedMemoryContext newAggregatedMemoryContext()
        {
            return delegate.newAggregatedMemoryContext();
        }

        @Override
        public LocalMemoryContext newLocalMemoryContext()
        {
            return new InternalLocalMemoryContext(delegate.newLocalMemoryContext(), memoryFuture, allocationListener);
        }

        @Override
        public long getBytes()
        {
            return delegate.getBytes();
        }

        @Override
        public void close()
        {
            delegate.close();
        }
    }

    @VisibleForTesting
    public MemoryTrackingContext getOperatorMemoryContext()
    {
        return operatorMemoryContext;
    }
}
