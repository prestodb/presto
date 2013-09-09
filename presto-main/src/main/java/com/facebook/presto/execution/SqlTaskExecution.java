/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.facebook.presto.OutputBuffers;
import com.facebook.presto.ScheduledSplit;
import com.facebook.presto.TaskSource;
import com.facebook.presto.client.FailureInfo;
import com.facebook.presto.event.query.QueryMonitor;
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.execution.TaskExecutor.TaskHandle;
import com.facebook.presto.noperator.Driver;
import com.facebook.presto.noperator.DriverFactory;
import com.facebook.presto.noperator.PipelineContext;
import com.facebook.presto.noperator.TaskContext;
import com.facebook.presto.noperator.TaskOutputOperator.TaskOutputFactory;
import com.facebook.presto.spi.Split;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.planner.NewLocalExecutionPlanner;
import com.facebook.presto.sql.planner.NewLocalExecutionPlanner.NewLocalExecutionPlan;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.util.SetThreadName;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import javax.annotation.concurrent.GuardedBy;

import java.lang.ref.WeakReference;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.presto.util.Failures.toFailures;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.Math.max;

public class SqlTaskExecution
        implements TaskExecution
{
    private final TaskId taskId;
    private final URI location;
    private final TaskExecutor taskExecutor;
    private final TaskStateMachine taskStateMachine;
    private final TaskContext taskContext;
    private final SharedBuffer sharedBuffer;

    private final QueryMonitor queryMonitor;

    private final TaskHandle taskHandle;

    private final AtomicInteger pendingWorkerCount = new AtomicInteger();

    @GuardedBy("this")
    private final Set<PlanNodeId> noMoreSplits = new HashSet<>();
    @GuardedBy("this")
    private final Set<PlanNodeId> completedUnpartitionedSources = new HashSet<>();
    @GuardedBy("this")
    private final SetMultimap<PlanNodeId, Split> unpartitionedSources = HashMultimap.create();
    @GuardedBy("this")
    private final List<WeakReference<Driver>> drivers = new ArrayList<>();
    @GuardedBy("this")
    private long maxAcknowledgedSplit = Long.MIN_VALUE;
    @GuardedBy("this")
    private DateTime lastHeartbeat = DateTime.now();

    private final PlanNodeId partitionedSourceId;
    private final PipelineContext partitionedPipelineContext;
    private final DriverFactory partitionedDriverFactory;
    private final List<Driver> unpartitionedDrivers;

    private final AtomicLong nextTaskInfoVersion = new AtomicLong(TaskInfo.STARTING_VERSION);

    public static SqlTaskExecution createSqlTaskExecution(Session session,
            TaskId taskId,
            URI location,
            PlanFragment fragment,
            NewLocalExecutionPlanner planner,
            DataSize maxBufferSize,
            TaskExecutor taskExecutor,
            ExecutorService notificationExecutor,
            DataSize maxTaskMemoryUsage,
            DataSize operatorPreAllocatedMemory,
            QueryMonitor queryMonitor)
    {
        SqlTaskExecution task = new SqlTaskExecution(session,
                taskId,
                location,
                fragment,
                planner,
                maxBufferSize,
                taskExecutor,
                maxTaskMemoryUsage,
                operatorPreAllocatedMemory,
                queryMonitor,
                notificationExecutor
        );

        try (SetThreadName setThreadName = new SetThreadName("Task-%s", taskId)) {
            task.start();
            return task;
        }
    }

    private SqlTaskExecution(Session session,
            TaskId taskId,
            URI location,
            PlanFragment fragment,
            NewLocalExecutionPlanner planner,
            DataSize maxBufferSize,
            TaskExecutor taskExecutor,
            DataSize maxTaskMemoryUsage,
            DataSize operatorPreAllocatedMemory,
            QueryMonitor queryMonitor,
            Executor notificationExecutor)
    {
        try (SetThreadName setThreadName = new SetThreadName("Task-%s", taskId)) {
            this.taskId = checkNotNull(taskId, "taskId is null");
            this.location = checkNotNull(location, "location is null");
            this.taskExecutor = checkNotNull(taskExecutor, "driverExecutor is null");

            this.taskStateMachine = new TaskStateMachine(taskId, notificationExecutor);
            taskStateMachine.addStateChangeListener(new StateChangeListener<TaskState>()
            {
                @Override
                public void stateChanged(TaskState taskState)
                {
                    if (taskState.isDone()) {
                        SqlTaskExecution.this.taskExecutor.removeTask(taskHandle);
                    }
                }
            });

            this.taskContext = new TaskContext(taskStateMachine,
                    notificationExecutor,
                    session,
                    checkNotNull(maxTaskMemoryUsage, "maxTaskMemoryUsage is null"),
                    checkNotNull(operatorPreAllocatedMemory, "operatorPreAllocatedMemory is null"));

            this.sharedBuffer = new SharedBuffer(checkNotNull(maxBufferSize, "maxBufferSize is null"));

            this.queryMonitor = checkNotNull(queryMonitor, "queryMonitor is null");

            taskHandle = taskExecutor.addTask(taskId);

            NewLocalExecutionPlan localExecutionPlan = planner.plan(session, fragment.getRoot(), fragment.getSymbols(), new TaskOutputFactory(sharedBuffer));
            List<DriverFactory> driverFactories = localExecutionPlan.getDriverFactories();

            // index driver factories
            DriverFactory partitionedDriverFactory = null;
            List<Driver> unpartitionedDrivers = new ArrayList<>();
            for (DriverFactory driverFactory : driverFactories) {
                if (driverFactory.getSourceIds().contains(fragment.getPartitionedSource())) {
                    partitionedDriverFactory = driverFactory;
                }
                else {
                    PipelineContext pipelineContext = taskContext.addPipelineContext(driverFactory.isInputDriver(), driverFactory.isOutputDriver());
                    Driver driver = driverFactory.createDriver(pipelineContext.addDriverContext());
                    unpartitionedDrivers.add(driver);
                }
            }
            this.unpartitionedDrivers = ImmutableList.copyOf(unpartitionedDrivers);

            if (fragment.isPartitioned()) {
                this.partitionedSourceId = fragment.getPartitionedSource();
                this.partitionedDriverFactory = partitionedDriverFactory;
                this.partitionedPipelineContext = taskContext.addPipelineContext(partitionedDriverFactory.isInputDriver(), partitionedDriverFactory.isOutputDriver());
            } else {
                this.partitionedSourceId = null;
                this.partitionedDriverFactory = null;
                this.partitionedPipelineContext = null;
            }
        }
    }

    //
    // This code starts threads so it can not be in the constructor
    private void start()
    {
        // start unpartitioned drivers
        for (Driver driver : unpartitionedDrivers) {
            enqueueDriver(driver);
        }
    }

    @Override
    public TaskId getTaskId()
    {
        return taskId;
    }

    @Override
    public void waitForStateChange(TaskState currentState, Duration maxWait)
            throws InterruptedException
    {
        try (SetThreadName setThreadName = new SetThreadName("Task-%s", taskId)) {
            taskStateMachine.waitForStateChange(currentState, maxWait);
        }
    }

    @Override
    public TaskInfo getTaskInfo(boolean full)
    {
        try (SetThreadName setThreadName = new SetThreadName("Task-%s", taskId)) {
            checkTaskCompletion();

            TaskState state = taskStateMachine.getState();
            List<FailureInfo> failures = ImmutableList.of();
            if (state == TaskState.FAILED) {
                failures = toFailures(taskStateMachine.getFailureCauses());
            }

            return new TaskInfo(
                    taskStateMachine.getTaskId(),
                    nextTaskInfoVersion.getAndIncrement(),
                    state,
                    location,
                    lastHeartbeat,
                    sharedBuffer.getInfo(),
                    getNoMoreSplits(),
                    taskContext.getTaskStats(),
                    failures,
                    taskContext.getOutputItems());
        }
    }

    @Override
    public synchronized void addSources(List<TaskSource> sources)
    {
        checkNotNull(sources, "sources is null");

        try (SetThreadName setThreadName = new SetThreadName("Task-%s", taskId)) {
            long newMaxAcknowledgedSplit = maxAcknowledgedSplit;
            for (TaskSource source : sources) {
                PlanNodeId sourceId = source.getPlanNodeId();
                for (ScheduledSplit scheduledSplit : source.getSplits()) {
                    // only add a split if we have not already scheduled it
                    if (scheduledSplit.getSequenceId() > maxAcknowledgedSplit) {
                        addSplit(sourceId, scheduledSplit.getSplit());
                        newMaxAcknowledgedSplit = max(scheduledSplit.getSequenceId(), newMaxAcknowledgedSplit);
                    }
                }
                if (source.isNoMoreSplits()) {
                    noMoreSplits(sourceId);
                }
            }
            maxAcknowledgedSplit = newMaxAcknowledgedSplit;
        }
    }

    @Override
    public synchronized void addResultQueue(OutputBuffers outputIds)
    {
        checkNotNull(outputIds, "outputIds is null");

        try (SetThreadName setThreadName = new SetThreadName("Task-%s", taskId)) {
            for (String bufferId : outputIds.getBufferIds()) {
                sharedBuffer.addQueue(bufferId);
            }
            if (outputIds.isNoMoreBufferIds()) {
                sharedBuffer.noMoreQueues();
            }
        }
    }

    private synchronized void addSplit(PlanNodeId sourceId, Split split)
    {
        // is this a partitioned source
        if (sourceId.equals(partitionedSourceId)) {
            addPartitionDriver(split);
        }
        else {
            // add this to all of the existing workers
            if (!unpartitionedSources.put(sourceId, split)) {
                return;
            }
            for (WeakReference<Driver> driverReference : drivers) {
                Driver driver = driverReference.get();
                // the driver can be GCed due to a failure or a limit
                if (driver != null) {
                    driver.addSplit(sourceId, split);
                }
            }
        }
    }

    private synchronized void addPartitionDriver(Split partitionedSplit)
    {
        // create a new split worker
        Driver driver = partitionedDriverFactory.createDriver(partitionedPipelineContext.addDriverContext());

        // TableScanOperator requires partitioned split to be added before task is started
        driver.addSplit(partitionedSourceId, partitionedSplit);

        // add unpartitioned sources
        for (Entry<PlanNodeId, Split> entry : unpartitionedSources.entries()) {
            driver.addSplit(entry.getKey(), entry.getValue());
        }

        // mark completed sources
        for (PlanNodeId completedUnpartitionedSource : completedUnpartitionedSources) {
            driver.noMoreSplits(completedUnpartitionedSource);
        }

        // add the worker
        enqueueDriver(driver);
    }

    private synchronized void enqueueDriver(final Driver driver)
    {
        // record new worker
        drivers.add(new WeakReference<>(driver));
        pendingWorkerCount.incrementAndGet();

        // execute worker
        final ListenableFuture<?> finishedFuture = taskExecutor.addSplit(taskHandle, new SplitRunner()
        {
            @Override
            public boolean isFinished()
            {
                return driver.isFinished();
            }

            @Override
            public ListenableFuture<?> process()
                    throws Exception
            {
                if (driver == null) {
                    driver = buildDriver();
                }
                return driver.process();
            }
        });

        Futures.addCallback(finishedFuture, new FutureCallback<Object>()
        {
            @Override
            public void onSuccess(Object result)
            {
                try (SetThreadName setThreadName = new SetThreadName("Task-%s", taskId)) {
                    pendingWorkerCount.decrementAndGet();
                    checkTaskCompletion();

                    // todo add failure info to split completion event
                    queryMonitor.splitCompletionEvent(taskId, driver.getDriverContext().getDriverStats());
                }
            }

            @Override
            public void onFailure(Throwable cause)
            {
                try (SetThreadName setThreadName = new SetThreadName("Task-%s", taskId)) {
                    taskStateMachine.failed(cause);
                    pendingWorkerCount.decrementAndGet();

                    // todo add failure info to split completion event
                    queryMonitor.splitCompletionEvent(taskId, driver.getDriverContext().getDriverStats());
                }
            }
        });
    }

    private synchronized void noMoreSplits(PlanNodeId sourceId)
    {
        // don't bother updating is this source has already been closed
        if (!noMoreSplits.add(sourceId)) {
            return;
        }

        if (sourceId.equals(partitionedSourceId)) {
            // all workers have been created
            // clear hash provider since it has a hard reference to every hash table
            partitionedDriverFactory.close();
        }
        else {
            completedUnpartitionedSources.add(sourceId);

            // tell all this existing workers this source is finished
            for (WeakReference<Driver> driverReference : drivers) {
                Driver driver = driverReference.get();
                // the driver can be GCed due to a failure or a limit
                if (driver != null) {
                    driver.noMoreSplits(sourceId);
                }
            }
        }
    }

    private synchronized Set<PlanNodeId> getNoMoreSplits()
    {
        return noMoreSplits;
    }

    private synchronized void checkTaskCompletion()
    {
        // are there more partition splits expected?
        if (partitionedSourceId != null && !noMoreSplits.contains(partitionedSourceId)) {
            return;
        }
        // do we still have running tasks?
        if (pendingWorkerCount.get() != 0) {
            return;
        }

        // no more output will be created
        sharedBuffer.finish();

        // are there still pages in the output buffer
        if (!sharedBuffer.isFinished()) {
            return;
        }

        // Cool! All done!
        taskStateMachine.finished();
    }

    @Override
    public void cancel()
    {
        try (SetThreadName setThreadName = new SetThreadName("Task-%s", taskId)) {
            taskStateMachine.cancel();
        }
    }

    @Override
    public void fail(Throwable cause)
    {
        try (SetThreadName setThreadName = new SetThreadName("Task-%s", taskId)) {
            taskStateMachine.failed(cause);
        }
    }

    @Override
    public BufferResult getResults(String outputId, long startingSequenceId, DataSize maxSize, Duration maxWait)
            throws InterruptedException
    {
        checkNotNull(outputId, "outputId is null");
        Preconditions.checkArgument(maxSize.toBytes() > 0, "maxSize must be at least 1 byte");
        checkNotNull(maxWait, "maxWait is null");

        try (SetThreadName setThreadName = new SetThreadName("Task-%s", taskId)) {
            return sharedBuffer.get(outputId, startingSequenceId, maxSize, maxWait);
        }
    }

    @Override
    public void abortResults(String outputId)
    {
        try (SetThreadName setThreadName = new SetThreadName("Task-%s", taskId)) {
            sharedBuffer.abort(outputId);
        }
    }

    @Override
    public synchronized void recordHeartbeat()
    {
        this.lastHeartbeat = DateTime.now();
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("taskId", taskId)
                .add("unpartitionedSources", unpartitionedSources)
                .toString();
    }
}
