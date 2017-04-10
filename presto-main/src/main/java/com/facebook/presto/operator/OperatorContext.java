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

import com.facebook.presto.ExceededMemoryLimitException;
import com.facebook.presto.Session;
import com.facebook.presto.memory.AbstractAggregatedMemoryContext;
import com.facebook.presto.spi.Page;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.stats.CounterStat;
import io.airlift.units.Duration;

import javax.annotation.concurrent.ThreadSafe;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static com.facebook.presto.operator.BlockedReason.WAITING_FOR_MEMORY;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.units.DataSize.succinctBytes;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Only calling getOperatorStats is ThreadSafe
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

    private final AtomicReference<SettableFuture<?>> memoryFuture = new AtomicReference<>();
    private final AtomicReference<BlockedMonitor> blockedMonitor = new AtomicReference<>();
    private final AtomicLong blockedWallNanos = new AtomicLong();

    private final AtomicLong finishCalls = new AtomicLong();
    private final AtomicLong finishWallNanos = new AtomicLong();
    private final AtomicLong finishCpuNanos = new AtomicLong();
    private final AtomicLong finishUserNanos = new AtomicLong();

    private final AtomicLong memoryReservation = new AtomicLong();
    private final OperatorSystemMemoryContext systemMemoryContext;
    private final SpillContext spillContext;

    private final AtomicReference<Supplier<OperatorInfo>> infoSupplier = new AtomicReference<>();
    private final boolean collectTimings;

    public OperatorContext(int operatorId, PlanNodeId planNodeId, String operatorType, DriverContext driverContext, Executor executor)
    {
        checkArgument(operatorId >= 0, "operatorId is negative");
        this.operatorId = operatorId;
        this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
        this.operatorType = requireNonNull(operatorType, "operatorType is null");
        this.driverContext = requireNonNull(driverContext, "driverContext is null");
        this.systemMemoryContext = new OperatorSystemMemoryContext(this.driverContext);
        this.spillContext = new OperatorSpillContext(this.driverContext);
        this.executor = requireNonNull(executor, "executor is null");
        SettableFuture<Object> future = SettableFuture.create();
        future.set(null);
        this.memoryFuture.set(future);

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

    public void reserveMemory(long bytes)
    {
        ListenableFuture<?> future = driverContext.reserveMemory(bytes);
        if (!future.isDone()) {
            SettableFuture<?> currentMemoryFuture = memoryFuture.get();
            while (currentMemoryFuture.isDone()) {
                SettableFuture<?> settableFuture = SettableFuture.create();
                // We can't replace one that's not done, because the task may be blocked on that future
                if (memoryFuture.compareAndSet(currentMemoryFuture, settableFuture)) {
                    currentMemoryFuture = settableFuture;
                }
                else {
                    currentMemoryFuture = memoryFuture.get();
                }
            }

            SettableFuture<?> finalMemoryFuture = currentMemoryFuture;
            // Create a new future, so that this operator can un-block before the pool does, if it's moved to a new pool
            Futures.addCallback(future, new FutureCallback<Object>()
            {
                @Override
                public void onSuccess(Object result)
                {
                    finalMemoryFuture.set(null);
                }

                @Override
                public void onFailure(Throwable t)
                {
                    finalMemoryFuture.set(null);
                }
            });
        }
        memoryReservation.addAndGet(bytes);
    }

    public void freeMemory(long bytes)
    {
        checkArgument(bytes >= 0, "bytes is negative");
        checkArgument(bytes <= memoryReservation.get(), "tried to free more memory than is reserved");
        driverContext.freeMemory(bytes);
        memoryReservation.getAndAdd(-bytes);
    }

    public AbstractAggregatedMemoryContext getSystemMemoryContext()
    {
        return systemMemoryContext;
    }

    public void closeSystemMemoryContext()
    {
        systemMemoryContext.close();
    }

    public SpillContext getSpillContext()
    {
        return spillContext;
    }

    public void moreMemoryAvailable()
    {
        memoryFuture.get().set(null);
    }

    public void transferMemoryToTaskContext(long taskBytes)
    {
        long bytes = memoryReservation.getAndSet(0);
        driverContext.transferMemoryToTaskContext(bytes);

        TaskContext taskContext = driverContext.getPipelineContext().getTaskContext();
        if (taskBytes > bytes) {
            try {
                taskContext.reserveMemory(taskBytes - bytes);
            }
            catch (ExceededMemoryLimitException e) {
                taskContext.freeMemory(bytes);
                throw e;
            }
        }
        else {
            taskContext.freeMemory(bytes - taskBytes);
        }
    }

    public void setMemoryReservation(long newMemoryReservation)
    {
        checkArgument(newMemoryReservation >= 0, "newMemoryReservation is negative");

        long delta = newMemoryReservation - memoryReservation.get();

        if (delta > 0) {
            reserveMemory(delta);
        }
        else {
            freeMemory(-delta);
        }
    }

    public boolean trySetMemoryReservation(long newMemoryReservation)
    {
        checkArgument(newMemoryReservation >= 0, "newMemoryReservation is negative");

        long delta = newMemoryReservation - memoryReservation.get();

        if (delta > 0) {
            if (!driverContext.tryReserveMemory(delta)) {
                return false;
            }

            memoryReservation.addAndGet(delta);
        }
        else {
            freeMemory(-delta);
        }
        return true;
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

                new Duration(blockedWallNanos.get(), NANOSECONDS).convertToMostSuccinctTimeUnit(),

                finishCalls.get(),
                new Duration(finishWallNanos.get(), NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(finishCpuNanos.get(), NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(finishUserNanos.get(), NANOSECONDS).convertToMostSuccinctTimeUnit(),

                succinctBytes(memoryReservation.get()),
                succinctBytes(systemMemoryContext.getReservedBytes()),
                memoryFuture.get().isDone() ? Optional.empty() : Optional.of(WAITING_FOR_MEMORY),
                info);
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

    private static class OperatorSystemMemoryContext
            extends AbstractAggregatedMemoryContext
    {
        // TODO: remove this class. See comment in AbstractAggregatedMemoryContext

        private final DriverContext driverContext;

        private boolean closed;
        private long reservedBytes;

        public OperatorSystemMemoryContext(DriverContext driverContext)
        {
            this.driverContext = driverContext;
        }

        public void close()
        {
            if (closed) {
                return;
            }
            closed = true;
            driverContext.freeSystemMemory(reservedBytes);
            reservedBytes = 0;
        }

        @Override
        protected void updateBytes(long bytes)
        {
            checkState(!closed);
            if (bytes > 0) {
                driverContext.reserveSystemMemory(bytes);
            }
            else {
                checkArgument(reservedBytes + bytes >= 0, "tried to free %s bytes of memory from %s bytes reserved", -bytes, reservedBytes);
                driverContext.freeSystemMemory(-bytes);
            }
            reservedBytes += bytes;
        }

        public long getReservedBytes()
        {
            return reservedBytes;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("usedBytes", reservedBytes)
                    .add("closed", closed)
                    .toString();
        }
    }

    @ThreadSafe
    private class OperatorSpillContext
        implements SpillContext
    {
        private final DriverContext driverContext;

        private long reservedBytes;

        public OperatorSpillContext(DriverContext driverContext)
        {
            this.driverContext = driverContext;
        }

        @Override
        public void updateBytes(long bytes)
        {
            if (bytes > 0) {
                driverContext.reserveSpill(bytes);
            }
            else {
                checkArgument(reservedBytes + bytes >= 0, "tried to free %s spilled bytes from %s bytes reserved", -bytes, reservedBytes);
                driverContext.freeSpill(-bytes);
            }
            reservedBytes += bytes;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("usedBytes", reservedBytes)
                    .toString();
        }
    }
}
