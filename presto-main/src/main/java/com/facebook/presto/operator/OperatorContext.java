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
import com.facebook.presto.spi.Page;
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.stats.CounterStat;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.annotation.concurrent.ThreadSafe;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.operator.BlockedReason.WAITING_FOR_MEMORY;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@ThreadSafe
public class OperatorContext
{
    private static final ThreadMXBean THREAD_MX_BEAN = ManagementFactory.getThreadMXBean();

    private final int operatorId;
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
    private final long maxMemoryReservation;

    private final AtomicReference<Supplier<Object>> infoSupplier = new AtomicReference<>();
    private final boolean collectTimings;

    public OperatorContext(int operatorId, String operatorType, DriverContext driverContext, Executor executor, long maxMemoryReservation)
    {
        checkArgument(operatorId >= 0, "operatorId is negative");
        this.operatorId = operatorId;
        this.maxMemoryReservation = maxMemoryReservation;
        this.operatorType = checkNotNull(operatorType, "operatorType is null");
        this.driverContext = checkNotNull(driverContext, "driverContext is null");
        this.executor = checkNotNull(executor, "executor is null");
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
        checkNotNull(blocked, "blocked is null");

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

    public DataSize getMaxMemorySize()
    {
        return driverContext.getMaxMemorySize();
    }

    public DataSize getOperatorPreAllocatedMemory()
    {
        return driverContext.getOperatorPreAllocatedMemory();
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
        long newReservation = memoryReservation.getAndAdd(bytes);
        if (newReservation > maxMemoryReservation) {
            memoryReservation.getAndAdd(-bytes);
            throw new ExceededMemoryLimitException(getMaxMemorySize());
        }
    }

    public boolean tryReserveMemory(long bytes)
    {
        if (!driverContext.tryReserveMemory(bytes)) {
            return false;
        }

        long newReservation = memoryReservation.getAndAdd(bytes);
        if (newReservation > maxMemoryReservation) {
            memoryReservation.getAndAdd(-bytes);
            return false;
        }
        return true;
    }

    public void freeMemory(long bytes)
    {
        checkArgument(bytes >= 0, "bytes is negative");
        checkArgument(bytes <= memoryReservation.get(), "tried to free more memory than is reserved");
        driverContext.freeMemory(bytes);
        memoryReservation.getAndAdd(-bytes);
    }

    public void moreMemoryAvailable()
    {
        memoryFuture.get().set(null);
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
            return tryReserveMemory(delta);
        }
        else {
            freeMemory(-delta);
            return true;
        }
    }

    public void setInfoSupplier(Supplier<Object> infoSupplier)
    {
        checkNotNull(infoSupplier, "infoProvider is null");
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
        Supplier<Object> infoSupplier = this.infoSupplier.get();
        Object info = null;
        if (infoSupplier != null) {
            info = infoSupplier.get();
        }

        return new OperatorStats(
                operatorId,
                operatorType,

                addInputCalls.get(),
                new Duration(addInputWallNanos.get(), NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(addInputCpuNanos.get(), NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(addInputUserNanos.get(), NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new DataSize(inputDataSize.getTotalCount(), BYTE).convertToMostSuccinctDataSize(),
                inputPositions.getTotalCount(),

                getOutputCalls.get(),
                new Duration(getOutputWallNanos.get(), NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(getOutputCpuNanos.get(), NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(getOutputUserNanos.get(), NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new DataSize(outputDataSize.getTotalCount(), BYTE).convertToMostSuccinctDataSize(),
                outputPositions.getTotalCount(),

                new Duration(blockedWallNanos.get(), NANOSECONDS).convertToMostSuccinctTimeUnit(),

                finishCalls.get(),
                new Duration(finishWallNanos.get(), NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(finishCpuNanos.get(), NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(finishUserNanos.get(), NANOSECONDS).convertToMostSuccinctTimeUnit(),

                new DataSize(memoryReservation.get(), BYTE).convertToMostSuccinctDataSize(),
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
            synchronized (this) {
                if (finished) {
                    return;
                }
                finished = true;
                blockedMonitor.compareAndSet(this, null);
                blockedWallNanos.getAndAdd(getBlockedTime());
            }
        }

        public long getBlockedTime()
        {
            return nanosBetween(start, System.nanoTime());
        }
    }
}
