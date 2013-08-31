package com.facebook.presto.noperator;

import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.base.Function;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import io.airlift.stats.Distribution;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.presto.noperator.OperatorContext.operatorStatsGetter;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.getFirst;
import static com.google.common.collect.Iterables.getLast;
import static com.google.common.collect.Iterables.transform;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@ThreadSafe
public class PipelineContext
{
    private final TaskContext taskContext;
    private final Executor executor;

    private final List<DriverContext> runningDrivers = new CopyOnWriteArrayList<>();

    private final AtomicInteger completedDrivers = new AtomicInteger();

    private final AtomicLong memoryReservation = new AtomicLong();

    private final Distribution queuedTime = new Distribution();
    private final Distribution elapsedTime = new Distribution();

    private final AtomicLong totalScheduledTime = new AtomicLong();
    private final AtomicLong totalCpuTime = new AtomicLong();
    private final AtomicLong totalUserTime = new AtomicLong();
    private final AtomicLong totalBlockedTime = new AtomicLong();

    private final AtomicLong inputDataSize = new AtomicLong();
    private final AtomicLong inputPositions = new AtomicLong();

    private final AtomicLong outputDataSize = new AtomicLong();
    private final AtomicLong outputPositions = new AtomicLong();

    private final ConcurrentMap<Integer, OperatorStats> operatorSummaries = new ConcurrentHashMap<>();

    public PipelineContext(TaskContext taskContext, Executor executor)
    {
        this.taskContext = checkNotNull(taskContext, "taskContext is null");
        this.executor = checkNotNull(executor, "executor is null");
    }

    public DriverContext addDriverContext()
    {
        DriverContext driverContext = new DriverContext(this, executor);
        runningDrivers.add(driverContext);
        return driverContext;
    }

    public List<DriverContext> getRunningDrivers()
    {
        return ImmutableList.copyOf(runningDrivers);
    }

    public Session getSession()
    {
        return taskContext.getSession();
    }

    public void driverFinished(DriverContext driverContext)
    {
        checkNotNull(driverContext, "driverContext is null");

        if (!runningDrivers.remove(driverContext)) {
            throw new IllegalArgumentException("Unknown driver " + driverContext);
        }

        DriverStats driverStats = driverContext.getDriverStats();

        completedDrivers.getAndIncrement();

        // remove the memory reservation
        memoryReservation.getAndAdd(-driverStats.getMemoryReservation().toBytes());

        queuedTime.add(driverStats.getQueuedTime().roundTo(NANOSECONDS));
        elapsedTime.add(driverStats.getElapsedTime().roundTo(NANOSECONDS));

        totalScheduledTime.getAndAdd((driverStats.getTotalScheduledTime().roundTo(NANOSECONDS)));
        totalCpuTime.getAndAdd((driverStats.getTotalCpuTime().roundTo(NANOSECONDS)));
        totalUserTime.getAndAdd((driverStats.getTotalUserTime().roundTo(NANOSECONDS)));

        totalBlockedTime.getAndAdd((driverStats.getTotalBlockedTime().roundTo(NANOSECONDS)));

        // merge the operator stats into the operator summary
        List<OperatorStats> operators = driverStats.getOperatorStats();
        for (OperatorStats operator : operators) {
            OperatorStats operatorSummary = operatorSummaries.get(operator.getOperatorId());
            if (operatorSummary != null) {
                operatorSummary = operatorSummary.add(operator);
            } else {
                operatorSummary = operator;
            }
            operatorSummaries.put(operator.getOperatorId(), operatorSummary);
        }

        OperatorStats inputOperator = getFirst(operators, null);
        if (inputOperator != null) {
            inputDataSize.getAndAdd((inputOperator.getInputDataSize().toBytes()));
            inputPositions.getAndAdd((inputOperator.getInputPositions()));

            OperatorStats outputOperator = checkNotNull(getLast(operators, null));
            outputDataSize.getAndAdd((outputOperator.getOutputDataSize().toBytes()));
            outputPositions.getAndAdd((outputOperator.getOutputPositions()));
        }
    }

    public void start()
    {
        taskContext.start();
    }

    public void failed(Throwable cause)
    {
        taskContext.failed(cause);
    }

    public boolean isDone()
    {
        return taskContext.isDone();
    }

    public DataSize getMaxMemorySize()
    {
        return taskContext.getMaxMemorySize();
    }

    public DataSize getOperatorPreAllocatedMemory()
    {
        return taskContext.getOperatorPreAllocatedMemory();
    }

    public synchronized boolean reserveMemory(long bytes)
    {
        boolean result = taskContext.reserveMemory(bytes);
        if (result) {
            memoryReservation.getAndAdd((bytes));
        }
        return result;
    }

    @Deprecated
    public void addOutputItems(PlanNodeId id, Iterable<?> outputItems)
    {
        taskContext.addOutputItems(id, outputItems);
    }

    public PipelineStats getPipelineStats()
    {
        List<DriverContext> runningDrivers = ImmutableList.copyOf(this.runningDrivers);

        int totalDriers = completedDrivers.get() + runningDrivers.size();

        int queuedDrivers = 0;
        int startedDrivers = 0;
        int completedDrivers = this.completedDrivers.get();

        Distribution queuedTime = new Distribution(this.queuedTime);
        Distribution elapsedTime = new Distribution(this.elapsedTime);

        long totalScheduledTime = this.totalScheduledTime.get();
        long totalCpuTime = this.totalCpuTime.get();
        long totalUserTime = this.totalUserTime.get();
        long totalBlockedTime = this.totalBlockedTime.get();

        long inputDataSize = this.inputDataSize.get();
        long inputPositions = this.inputPositions.get();

        long outputDataSize = this.outputDataSize.get();
        long outputPositions = this.outputPositions.get();

        List<DriverStats> drivers = new ArrayList<>();

        Multimap<Integer, OperatorStats> runningOperators = ArrayListMultimap.create();
        for (DriverContext driverContext : runningDrivers) {
            DriverStats driverStats = driverContext.getDriverStats();
            drivers.add(driverStats);

            if (driverStats.getStartTime() == null) {
                queuedDrivers++;
            }
            else {
                startedDrivers++;
            }

            queuedTime.add(driverStats.getQueuedTime().roundTo(NANOSECONDS));
            elapsedTime.add(driverStats.getElapsedTime().roundTo(NANOSECONDS));

            totalScheduledTime += driverStats.getTotalScheduledTime().roundTo(NANOSECONDS);
            totalCpuTime += driverStats.getTotalCpuTime().roundTo(NANOSECONDS);
            totalUserTime += driverStats.getTotalUserTime().roundTo(NANOSECONDS);
            totalBlockedTime += driverStats.getTotalBlockedTime().roundTo(NANOSECONDS);

            List<OperatorStats> operators = ImmutableList.copyOf(transform(driverContext.getOperatorContexts(), operatorStatsGetter()));
            for (OperatorStats operator : operators) {
                runningOperators.put(operator.getOperatorId(), operator);
            }

            OperatorStats inputOperator = getFirst(operators, null);
            if (inputOperator != null) {
                inputDataSize += inputOperator.getInputDataSize().toBytes();
                inputPositions += inputOperator.getInputPositions();

                OperatorStats outputOperator = checkNotNull(getLast(operators, null));
                outputDataSize += outputOperator.getOutputDataSize().toBytes();
                outputPositions += outputOperator.getOutputPositions();
            }
        }

        // merge the operator stats into the operator summary
        TreeMap<Integer, OperatorStats> operatorSummaries = new TreeMap<>();
        for (Entry<Integer, OperatorStats> entry : this.operatorSummaries.entrySet()) {
            OperatorStats operator = entry.getValue();
            operator.add(runningOperators.get(entry.getKey()));
            operatorSummaries.put(entry.getKey(), operator);
        }

        return new PipelineStats(
                totalDriers,
                queuedDrivers,
                startedDrivers,
                completedDrivers,
                new DataSize(memoryReservation.get(), BYTE).convertToMostSuccinctDataSize(),
                queuedTime.snapshot(),
                elapsedTime.snapshot(),
                new Duration(totalScheduledTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(totalCpuTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(totalUserTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(totalBlockedTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new DataSize(inputDataSize, BYTE).convertToMostSuccinctDataSize(),
                inputPositions,
                new DataSize(outputDataSize, BYTE).convertToMostSuccinctDataSize(),
                outputPositions,
                ImmutableList.copyOf(operatorSummaries.values()),
                drivers);
    }

    public static Function<PipelineContext, PipelineStats> pipelineStatsGetter()
    {
        return new Function<PipelineContext, PipelineStats>()
        {
            public PipelineStats apply(PipelineContext pipelineContext)
            {
                return pipelineContext.getPipelineStats();
            }
        };
    }

}
