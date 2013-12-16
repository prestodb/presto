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

import com.facebook.presto.OutputBuffers;
import com.facebook.presto.ScheduledSplit;
import com.facebook.presto.TaskSource;
import com.facebook.presto.client.FailureInfo;
import com.facebook.presto.event.query.QueryMonitor;
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.execution.TaskExecutor.TaskHandle;
import com.facebook.presto.operator.Driver;
import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.DriverFactory;
import com.facebook.presto.operator.PipelineContext;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.operator.TaskOutputOperator.TaskOutputFactory;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.planner.LocalExecutionPlanner;
import com.facebook.presto.sql.planner.LocalExecutionPlanner.LocalExecutionPlan;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.PlanFragment.PlanDistribution;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.util.SetThreadName;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.util.Failures.toFailures;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
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

    private final List<WeakReference<Driver>> drivers = new CopyOnWriteArrayList<>();

    /**
     * Number of drivers that have been sent to the TaskExecutor that have not finished.
     */
    private final AtomicInteger remainingDrivers = new AtomicInteger();
    private final AtomicInteger remainingPartitionedDrivers = new AtomicInteger();

    // guarded for update only
    @GuardedBy("this")
    private final ConcurrentMap<PlanNodeId, TaskSource> unpartitionedSources = new ConcurrentHashMap<>();
    @GuardedBy("this")
    private long maxAcknowledgedSplit = Long.MIN_VALUE;

    private final AtomicReference<DateTime> lastHeartbeat = new AtomicReference<>(DateTime.now());

    private final PlanNodeId partitionedSourceId;
    private final PipelineContext partitionedPipelineContext;
    private final DriverFactory partitionedDriverFactory;
    private final AtomicBoolean noMorePartitionedSplits = new AtomicBoolean();

    private final List<Driver> unpartitionedDrivers;

    private final AtomicLong nextTaskInfoVersion = new AtomicLong(TaskInfo.STARTING_VERSION);

    public static SqlTaskExecution createSqlTaskExecution(Session session,
            TaskId taskId,
            URI location,
            PlanFragment fragment,
            List<TaskSource> sources,
            OutputBuffers outputBuffers,
            LocalExecutionPlanner planner,
            DataSize maxBufferSize,
            TaskExecutor taskExecutor,
            ExecutorService notificationExecutor,
            DataSize maxTaskMemoryUsage,
            DataSize operatorPreAllocatedMemory,
            QueryMonitor queryMonitor,
            boolean cpuTimerEnabled)
    {
        SqlTaskExecution task = new SqlTaskExecution(session,
                taskId,
                location,
                fragment,
                outputBuffers,
                planner,
                maxBufferSize,
                taskExecutor,
                maxTaskMemoryUsage,
                operatorPreAllocatedMemory,
                queryMonitor,
                notificationExecutor,
                cpuTimerEnabled
        );

        try (SetThreadName setThreadName = new SetThreadName("Task-%s", taskId)) {
            task.start();
            task.addSources(sources);
            task.recordHeartbeat();
            return task;
        }
    }

    private SqlTaskExecution(Session session,
            TaskId taskId,
            URI location,
            PlanFragment fragment,
            OutputBuffers outputBuffers,
            LocalExecutionPlanner planner,
            DataSize maxBufferSize,
            TaskExecutor taskExecutor,
            DataSize maxTaskMemoryUsage,
            DataSize operatorPreAllocatedMemory,
            QueryMonitor queryMonitor,
            Executor notificationExecutor,
            boolean cpuTimerEnabled)
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
                        // make sure buffers are cleaned up
                        sharedBuffer.destroy();
                    }
                }
            });

            this.taskContext = new TaskContext(taskStateMachine,
                    notificationExecutor,
                    session,
                    checkNotNull(maxTaskMemoryUsage, "maxTaskMemoryUsage is null"),
                    checkNotNull(operatorPreAllocatedMemory, "operatorPreAllocatedMemory is null"),
                    cpuTimerEnabled);

            this.sharedBuffer = new SharedBuffer(
                    checkNotNull(maxBufferSize, "maxBufferSize is null"),
                    outputBuffers);

            this.queryMonitor = checkNotNull(queryMonitor, "queryMonitor is null");

            taskHandle = taskExecutor.addTask(taskId);

            LocalExecutionPlan localExecutionPlan = planner.plan(session, fragment.getRoot(), fragment.getSymbols(), new TaskOutputFactory(sharedBuffer));
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

            if (fragment.getDistribution() == PlanDistribution.SOURCE) {
                checkArgument(partitionedDriverFactory != null, "Fragment is partitioned, but no partitioned driver found");
                this.partitionedSourceId = fragment.getPartitionedSource();
                this.partitionedDriverFactory = partitionedDriverFactory;
                this.partitionedPipelineContext = taskContext.addPipelineContext(partitionedDriverFactory.isInputDriver(), partitionedDriverFactory.isOutputDriver());
            }
            else {
                this.partitionedSourceId = null;
                this.partitionedDriverFactory = null;
                this.partitionedPipelineContext = null;
            }
        }
    }

    //
    // This code starts registers a callback with access to this class, and this
    // call back is access from another thread, so this code can not be placed in the constructor
    private void start()
    {
        // start unpartitioned drivers
        for (Driver driver : unpartitionedDrivers) {
            drivers.add(new WeakReference<>(driver));
            enqueueDriver(true, false, new DriverSplitRunner(driver));
        }
    }

    @Override
    public TaskId getTaskId()
    {
        return taskId;
    }

    @Override
    public TaskContext getTaskContext()
    {
        return taskContext;
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
                    lastHeartbeat.get(),
                    sharedBuffer.getInfo(),
                    getNoMoreSplits(),
                    taskContext.getTaskStats(),
                    failures,
                    taskContext.getOutputItems());
        }
    }

    @Override
    public void addSources(List<TaskSource> sources)
    {
        checkNotNull(sources, "sources is null");
        checkState(!Thread.holdsLock(this), "Can not add sources while holding a lock on the %s", getClass().getSimpleName());

        try (SetThreadName setThreadName = new SetThreadName("Task-%s", taskId)) {

            // update our record of sources and schedule drivers for new partitioned splits
            Map<PlanNodeId, TaskSource> updatedUnpartitionedSources = updateSources(sources);

            // tell existing drivers about the new splits; it is safe to update drivers
            // multiple times and out of order because sources contain full record of
            // the unpartitioned splits
            for (TaskSource source : updatedUnpartitionedSources.values()) {
                // tell all the existing drivers this source is finished
                for (WeakReference<Driver> driverReference : drivers) {
                    Driver driver = driverReference.get();
                    // the driver can be GCed due to a failure or a limit
                    if (driver != null) {
                        driver.updateSource(source);
                    } else {
                        // remove the weak reference from the list to avoid a memory leak
                        // NOTE: this is a concurrent safe operation on a CopyOnWriteArrayList
                        drivers.remove(driverReference);
                    }
                }
            }
        }
    }

    private synchronized Map<PlanNodeId, TaskSource> updateSources(List<TaskSource> sources)
    {
        Map<PlanNodeId, TaskSource> updatedUnpartitionedSources = new HashMap<>();

        // don't update maxAcknowledgedSplit until the end because task sources may not
        // be in sorted order and if we updated early we could skip splits
        long newMaxAcknowledgedSplit = maxAcknowledgedSplit;

        for (TaskSource source : sources) {
            PlanNodeId sourceId = source.getPlanNodeId();
            if (sourceId.equals(partitionedSourceId)) {
                // partitioned split
                for (final ScheduledSplit scheduledSplit : source.getSplits()) {
                    // only add a split if we have not already scheduled it
                    if (scheduledSplit.getSequenceId() > maxAcknowledgedSplit) {
                        // create a new driver for the split
                        enqueueDriver(false, true, new DriverSplitRunner(partitionedPipelineContext.addDriverContext(), new Function<DriverContext, Driver>()
                        {
                            @Override
                            public Driver apply(DriverContext driverContext)
                            {
                                return createDriver(partitionedDriverFactory, driverContext, scheduledSplit);
                            }
                        }));

                        newMaxAcknowledgedSplit = max(scheduledSplit.getSequenceId(), newMaxAcknowledgedSplit);
                    }
                }

                if (source.isNoMoreSplits()) {
                    noMorePartitionedSplits.set(true);
                    checkNoMorePartitionedSplits();
                }
            }
            else {
                // unpartitioned split

                // update newMaxAcknowledgedSplit
                for (ScheduledSplit scheduledSplit : source.getSplits()) {
                    newMaxAcknowledgedSplit = max(scheduledSplit.getSequenceId(), newMaxAcknowledgedSplit);
                }

                // create new source
                TaskSource newSource;
                TaskSource currentSource = unpartitionedSources.get(sourceId);
                if (currentSource == null) {
                    newSource = source;
                }
                else {
                    newSource = currentSource.update(source);
                }

                // only record new source if something changed
                if (newSource != currentSource) {
                    unpartitionedSources.put(sourceId, newSource);
                    updatedUnpartitionedSources.put(sourceId, newSource);
                }
            }
        }

        maxAcknowledgedSplit = newMaxAcknowledgedSplit;
        return updatedUnpartitionedSources;
    }

    @Override
    public synchronized void addResultQueue(OutputBuffers outputBuffers)
    {
        checkNotNull(outputBuffers, "outputBuffers is null");

        try (SetThreadName setThreadName = new SetThreadName("Task-%s", taskId)) {
            sharedBuffer.setOutputBuffers(outputBuffers);
        }
    }

    private synchronized void enqueueDriver(boolean forceRunSplit, final boolean partitioned, final DriverSplitRunner splitRunner)
    {
        // schedule driver to be executed
        ListenableFuture<?> finishedFuture;
        if (forceRunSplit) {
            finishedFuture = taskExecutor.forceRunSplit(taskHandle, splitRunner);
        }
        else {
            finishedFuture = taskExecutor.enqueueSplit(taskHandle, splitRunner);
        }

        // record new driver
        remainingDrivers.incrementAndGet();
        if (partitioned) {
            remainingPartitionedDrivers.incrementAndGet();
        }

        // when driver completes, update state and fire events
        Futures.addCallback(finishedFuture, new FutureCallback<Object>()
        {
            @Override
            public void onSuccess(Object result)
            {
                try (SetThreadName setThreadName = new SetThreadName("Task-%s", taskId)) {
                    // if all drivers have been created, close the factory so it can perform cleanup
                    remainingDrivers.decrementAndGet();
                    if (partitioned) {
                        int runningCount = remainingPartitionedDrivers.decrementAndGet();
                        if (runningCount <= 0) {
                            checkNoMorePartitionedSplits();
                        }
                    }

                    checkTaskCompletion();

                    queryMonitor.splitCompletionEvent(taskId, splitRunner.getDriverContext().getDriverStats());
                }
            }

            @Override
            public void onFailure(Throwable cause)
            {
                try (SetThreadName setThreadName = new SetThreadName("Task-%s", taskId)) {
                    taskStateMachine.failed(cause);

                    // record driver is finished
                    remainingDrivers.decrementAndGet();
                    if (partitioned) {
                        remainingPartitionedDrivers.decrementAndGet();
                    }

                    // check if partitioned driver
                    checkNoMorePartitionedSplits();

                    // todo add failure info to split completion event
                    queryMonitor.splitFailedEvent(taskId, splitRunner.getDriverContext().getDriverStats(), cause);
                }
            }
        });
    }

    private void checkNoMorePartitionedSplits()
    {
        // todo this is not exactly correct, we should be closing when all drivers have been created, but
        // we check against running count which means we are waiting until all drivers are finished
        if (partitionedDriverFactory != null && noMorePartitionedSplits.get() && remainingPartitionedDrivers.get() <= 0) {
            partitionedDriverFactory.close();
        }
    }

    private Driver createDriver(DriverFactory driverFactory, DriverContext driverContext, ScheduledSplit partitionedSplit)
    {
        checkState(!Thread.holdsLock(this), "Can not crete a driver while holding a lock on the %s", getClass().getSimpleName());

        Driver driver = driverFactory.createDriver(driverContext);

        if (partitionedSplit != null) {
            // TableScanOperator requires partitioned split to be added before task is started
            driver.updateSource(new TaskSource(partitionedSourceId, ImmutableSet.of(partitionedSplit), true));
        }

        // record driver so other threads add unpartitioned sources can see the driver
        // NOTE: this MUST be done before reading unpartitionedSources, so we see a consistent view of the unpartitioned sources
        drivers.add(new WeakReference<>(driver));

        // add unpartitioned sources
        for (TaskSource source : unpartitionedSources.values()) {
            driver.updateSource(source);
        }

        return driver;
    }

    private Set<PlanNodeId> getNoMoreSplits()
    {
        ImmutableSet.Builder<PlanNodeId> noMoreSplits = ImmutableSet.builder();
        if (partitionedSourceId != null && noMorePartitionedSplits.get()) {
            noMoreSplits.add(partitionedSourceId);
        }
        for (TaskSource taskSource : unpartitionedSources.values()) {
            if (taskSource.isNoMoreSplits()) {
                noMoreSplits.add(taskSource.getPlanNodeId());
            }
        }
        return noMoreSplits.build();
    }

    private synchronized void checkTaskCompletion()
    {
        // are there more partition splits expected?
        if (partitionedSourceId != null && !noMorePartitionedSplits.get()) {
            return;
        }
        // do we still have running tasks?
        if (remainingDrivers.get() != 0) {
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
        checkState(!Thread.holdsLock(this), "Can not get result data while holding a lock on the %s", getClass().getSimpleName());

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
    public void recordHeartbeat()
    {
        this.lastHeartbeat.set(DateTime.now());
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("taskId", taskId)
                .add("unpartitionedSources", unpartitionedSources)
                .toString();
    }

    private static class DriverSplitRunner
            implements SplitRunner
    {
        private final DriverContext driverContext;
        private final Function<? super DriverContext, Driver> driverSupplier;
        private Driver driver;

        public DriverSplitRunner(Driver driver)
        {
            this(driver.getDriverContext(), Functions.constant(driver));
        }

        private DriverSplitRunner(DriverContext driverContext, Function<? super DriverContext, Driver> driverFactory)
        {
            this.driverContext = checkNotNull(driverContext, "driverContext is null");
            this.driverSupplier = checkNotNull(driverFactory, "driverFactory is null");
        }

        public DriverContext getDriverContext()
        {
            return driverContext;
        }

        @Override
        public synchronized void initialize()
        {
            try {
                driver = driverSupplier.apply(driverContext);
            }
            catch (Throwable e) {
                driverContext.failed(e);
                throw e;
            }
        }

        @Override
        public synchronized boolean isFinished()
        {
            return driver.isFinished();
        }

        @Override
        public ListenableFuture<?> processFor(Duration duration)
        {
            return driver.processFor(duration);
        }

        @Override
        public void close()
        {
            if (driver != null) {
                driver.close();
            }
        }
    }
}
