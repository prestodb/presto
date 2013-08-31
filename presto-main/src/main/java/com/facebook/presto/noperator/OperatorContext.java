package com.facebook.presto.noperator;

import com.facebook.presto.operator.Page;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.annotation.concurrent.ThreadSafe;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
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

    private final AtomicLong getOutputWallNanos = new AtomicLong();
    private final AtomicLong getOutputCpuNanos = new AtomicLong();
    private final AtomicLong getOutputUserNanos = new AtomicLong();
    private final AtomicLong outputDataSize = new AtomicLong();
    private final AtomicLong outputPositions = new AtomicLong();

    private final AtomicLong addInputWallNanos = new AtomicLong();
    private final AtomicLong addInputCpuNanos = new AtomicLong();
    private final AtomicLong addInputUserNanos = new AtomicLong();
    private final AtomicLong inputDataSize = new AtomicLong();
    private final AtomicLong inputPositions = new AtomicLong();

    private final AtomicLong blockedWallNanos = new AtomicLong();

    private final AtomicLong finishWallNanos = new AtomicLong();
    private final AtomicLong finishCpuNanos = new AtomicLong();
    private final AtomicLong finishUserNanos = new AtomicLong();

    private final AtomicLong memoryReservation = new AtomicLong();

    private final AtomicReference<Supplier<Object>> infoSupplier = new AtomicReference<>();

    public OperatorContext(int operatorId, String operatorType, DriverContext driverContext, Executor executor)
    {
        checkArgument(operatorId >= 0, "operatorId is negative");
        this.operatorId = operatorId;
        this.operatorType = checkNotNull(operatorType, "operatorType is null");
        this.driverContext = checkNotNull(driverContext, "driverContext is null");
        this.executor = checkNotNull(executor, "executor is null");
    }

    public int getOperatorId()
    {
        return operatorId;
    }

    public String getOperatorType()
    {
        return operatorType;
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
        intervalCpuStart.set(THREAD_MX_BEAN.getCurrentThreadCpuTime());
        intervalUserStart.set(THREAD_MX_BEAN.getCurrentThreadUserTime());
    }

    public void recordGetOutput(Page page)
    {
        getOutputWallNanos.getAndAdd((nanosBetween(intervalWallStart.get(), System.nanoTime())));
        getOutputCpuNanos.getAndAdd((nanosBetween(intervalCpuStart.get(), THREAD_MX_BEAN.getCurrentThreadCpuTime())));
        getOutputUserNanos.getAndAdd((nanosBetween(intervalUserStart.get(), THREAD_MX_BEAN.getCurrentThreadUserTime())));

        if (page != null) {
            outputDataSize.getAndAdd((page.getDataSize().toBytes()));
            outputPositions.getAndAdd((page.getPositionCount()));
        }
    }

    public void recordAddInput(Page page)
    {
        addInputWallNanos.getAndAdd((nanosBetween(intervalWallStart.get(), System.nanoTime())));
        addInputCpuNanos.getAndAdd((nanosBetween(intervalCpuStart.get(), THREAD_MX_BEAN.getCurrentThreadCpuTime())));
        addInputUserNanos.getAndAdd((nanosBetween(intervalUserStart.get(), THREAD_MX_BEAN.getCurrentThreadUserTime())));

        if (page != null) {
            inputDataSize.getAndAdd((page.getDataSize().toBytes()));
            inputPositions.getAndAdd((page.getPositionCount()));
        }
    }

    public void recordBlocked(ListenableFuture<?> blocked)
    {
        checkNotNull(blocked, "blocked is null");
        blocked.addListener(new Runnable()
        {
            private final long start = System.nanoTime();

            @Override
            public void run()
            {
                blockedWallNanos.getAndAdd((nanosBetween(start, System.nanoTime())));
            }
        }, executor);
    }

    public void recordFinish()
    {
        finishWallNanos.getAndAdd((nanosBetween(intervalWallStart.get(), System.nanoTime())));
        finishCpuNanos.getAndAdd((nanosBetween(intervalCpuStart.get(), THREAD_MX_BEAN.getCurrentThreadCpuTime())));
        finishUserNanos.getAndAdd((nanosBetween(intervalUserStart.get(), THREAD_MX_BEAN.getCurrentThreadUserTime())));
    }

    public DataSize getMaxMemorySize()
    {
        return driverContext.getMaxMemorySize();
    }

    public DataSize getOperatorPreAllocatedMemory()
    {
        return driverContext.getOperatorPreAllocatedMemory();
    }

    public boolean reserveMemory(long bytes)
    {
        boolean result = driverContext.reserveMemory(bytes);
        if (result) {
            memoryReservation.getAndAdd((bytes));
        }
        return result;
    }

    public synchronized long setMemoryReservation(long newMemoryReservation)
    {
        checkArgument(newMemoryReservation >= 0, "newMemoryReservation is negative");

        long delta = newMemoryReservation - memoryReservation.get();

        // currently, operator memory is not be released
        if (delta > 0) {
            checkState(reserveMemory(delta), "Task exceeded max memory size of %s", getMaxMemorySize());
        }

        return newMemoryReservation;
    }

    public void setInfoSupplier(Supplier<Object> infoSupplier)
    {
        checkNotNull(infoSupplier, "infoProvider is null");
        this.infoSupplier.set(infoSupplier);
    }

    @Deprecated
    public void addOutputItems(PlanNodeId id, Set<?> output)
    {
        driverContext.addOutputItems(id, output);
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
                new Duration(getOutputWallNanos.get(), NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(getOutputCpuNanos.get(), NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(getOutputUserNanos.get(), NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new DataSize(outputDataSize.get(), BYTE).convertToMostSuccinctDataSize(),
                outputPositions.get(),

                new Duration(addInputWallNanos.get(), NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(addInputCpuNanos.get(), NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(addInputUserNanos.get(), NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new DataSize(inputDataSize.get(), BYTE).convertToMostSuccinctDataSize(),
                inputPositions.get(),

                new Duration(blockedWallNanos.get(), NANOSECONDS).convertToMostSuccinctTimeUnit(),

                new Duration(finishWallNanos.get(), NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(finishCpuNanos.get(), NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(finishUserNanos.get(), NANOSECONDS).convertToMostSuccinctTimeUnit(),

                new DataSize(memoryReservation.get(), BYTE).convertToMostSuccinctDataSize(),
                info);
    }

    private static long nanosBetween(long start, long end)
    {
        return Math.abs(end - start);
    }

    public static Function<OperatorContext, OperatorStats> operatorStatsGetter()
    {
        return new Function<OperatorContext, OperatorStats>()
        {
            public OperatorStats apply(OperatorContext operatorContext)
            {
                return operatorContext.getOperatorStats();
            }
        };
    }
}
