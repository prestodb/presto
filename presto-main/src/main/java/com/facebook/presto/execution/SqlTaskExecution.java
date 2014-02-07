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
import com.facebook.presto.execution.SharedBuffer.QueueState;
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.execution.TaskExecutor.TaskHandle;
import com.facebook.presto.operator.Driver;
import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.DriverFactory;
import com.facebook.presto.operator.DriverStats;
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
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
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
    private final Executor notificationExecutor;
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

    // guarded for update only
    @GuardedBy("this")
    private final ConcurrentMap<PlanNodeId, TaskSource> unpartitionedSources = new ConcurrentHashMap<>();

    @GuardedBy("this")
    private long maxAcknowledgedSplit = Long.MIN_VALUE;

    private final AtomicReference<DateTime> lastHeartbeat = new AtomicReference<>(DateTime.now());

    private final PlanNodeId partitionedSourceId;
    private final DriverSplitRunnerFactory partitionedDriverFactory;

    private final List<DriverSplitRunnerFactory> unpartitionedDriverFactories;

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
            this.notificationExecutor = checkNotNull(notificationExecutor, "notificationExecutor is null");

            this.taskStateMachine = new TaskStateMachine(taskId, notificationExecutor);
            taskStateMachine.addStateChangeListener(new StateChangeListener<TaskState>()
            {
                @Override
                public void stateChanged(TaskState taskState)
                {
                    if (taskState.isDone()) {
                        SqlTaskExecution.this.taskExecutor.removeTask(taskHandle);
                        // make sure buffers are cleaned up
                        if (taskState != TaskState.FAILED) {
                            // don't close buffers for a failed query
                            // closed buffers signal to upstream tasks that everything finished cleanly
                            sharedBuffer.destroy();
                        }
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
                    taskId,
                    notificationExecutor,
                    checkNotNull(maxBufferSize, "maxBufferSize is null"),
                    outputBuffers);
            sharedBuffer.addStateChangeListener(new StateChangeListener<QueueState>()
            {
                @Override
                public void stateChanged(QueueState taskState)
                {
                    if (taskState == QueueState.FINISHED) {
                        checkTaskCompletion();
                    }
                }
            });

            this.queryMonitor = checkNotNull(queryMonitor, "queryMonitor is null");

            taskHandle = taskExecutor.addTask(taskId);

            LocalExecutionPlan localExecutionPlan = planner.plan(session, fragment.getRoot(), fragment.getSymbols(), new TaskOutputFactory(sharedBuffer));
            List<DriverFactory> driverFactories = localExecutionPlan.getDriverFactories();

            // index driver factories
            DriverSplitRunnerFactory partitionedDriverFactory = null;
            ImmutableList.Builder<DriverSplitRunnerFactory> unpartitionedDriverFactories = ImmutableList.builder();
            for (DriverFactory driverFactory : driverFactories) {
                if (driverFactory.getSourceIds().contains(fragment.getPartitionedSource())) {
                    checkState(partitionedDriverFactory == null, "multiple partitioned sources are not supported");
                    partitionedDriverFactory = new DriverSplitRunnerFactory(driverFactory);
                }
                else {
                    unpartitionedDriverFactories.add(new DriverSplitRunnerFactory(driverFactory));
                }
            }
            this.unpartitionedDriverFactories = unpartitionedDriverFactories.build();

            if (fragment.getDistribution() == PlanDistribution.SOURCE) {
                checkArgument(partitionedDriverFactory != null, "Fragment is partitioned, but no partitioned driver found");
            }
            this.partitionedSourceId = fragment.getPartitionedSource();
            this.partitionedDriverFactory = partitionedDriverFactory;
        }
    }

    //
    // This code starts registers a callback with access to this class, and this
    // call back is access from another thread, so this code can not be placed in the constructor
    private void start()
    {
        // start unpartitioned drivers
        List<DriverSplitRunner> runners = new ArrayList<>();
        for (DriverSplitRunnerFactory driverFactory : unpartitionedDriverFactories) {
            runners.add(driverFactory.createDriverRunner(null));
            driverFactory.setNoMoreSplits();
        }
        enqueueDrivers(true, runners);
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
                    }
                    else {
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
                ImmutableList.Builder<DriverSplitRunner> runners = ImmutableList.builder();
                for (ScheduledSplit scheduledSplit : source.getSplits()) {
                    // only add a split if we have not already scheduled it
                    if (scheduledSplit.getSequenceId() > maxAcknowledgedSplit) {
                        // create a new driver for the split
                        runners.add(partitionedDriverFactory.createDriverRunner(scheduledSplit));
                        newMaxAcknowledgedSplit = max(scheduledSplit.getSequenceId(), newMaxAcknowledgedSplit);
                    }
                }

                enqueueDrivers(false, runners.build());
                if (source.isNoMoreSplits()) {
                    partitionedDriverFactory.setNoMoreSplits();
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

    private synchronized void enqueueDrivers(boolean forceRunSplit, List<DriverSplitRunner> runners)
    {
        // schedule driver to be executed
        List<ListenableFuture<?>> finishedFutures = taskExecutor.enqueueSplits(taskHandle, forceRunSplit, runners);
        checkState(finishedFutures.size() == runners.size(), "Expected %s futures but got %s", runners.size(), finishedFutures.size());

        // record new driver
        remainingDrivers.addAndGet(finishedFutures.size());

        // when driver completes, update state and fire events
        for (int i = 0; i < finishedFutures.size(); i++) {
            ListenableFuture<?> finishedFuture = finishedFutures.get(i);
            final DriverSplitRunner splitRunner = runners.get(i);
            Futures.addCallback(finishedFuture, new FutureCallback<Object>()
            {
                @Override
                public void onSuccess(Object result)
                {
                    try (SetThreadName setThreadName = new SetThreadName("Task-%s", taskId)) {
                        // record driver is finished
                        remainingDrivers.decrementAndGet();

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

                        DriverContext driverContext = splitRunner.getDriverContext();
                        DriverStats driverStats;
                        if (driverContext != null) {
                            driverStats = driverContext.getDriverStats();
                        }
                        else {
                            // split runner did not start successfully
                            driverStats = new DriverStats();
                        }

                        // fire failed event with cause
                        queryMonitor.splitFailedEvent(taskId, driverStats, cause);
                    }
                }
            }, notificationExecutor);
        }
    }

    private Set<PlanNodeId> getNoMoreSplits()
    {
        ImmutableSet.Builder<PlanNodeId> noMoreSplits = ImmutableSet.builder();
        if (partitionedDriverFactory != null && partitionedDriverFactory.isNoMoreSplits()) {
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
        if (taskStateMachine.getState().isDone()) {
            return;
        }

        // are there more partition splits expected?
        if (partitionedDriverFactory != null && !partitionedDriverFactory.isNoMoreSplits()) {
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
        checkArgument(maxSize.toBytes() > 0, "maxSize must be at least 1 byte");
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
                .add("remainingDrivers", remainingDrivers)
                .add("unpartitionedSources", unpartitionedSources)
                .toString();
    }

    private class DriverSplitRunnerFactory
    {
        private final DriverFactory driverFactory;
        private final PipelineContext pipelineContext;

        private final AtomicInteger pendingCreation = new AtomicInteger();
        private final AtomicBoolean noMoreSplits = new AtomicBoolean();

        private DriverSplitRunnerFactory(DriverFactory driverFactory)
        {
            this.driverFactory = driverFactory;
            this.pipelineContext = taskContext.addPipelineContext(driverFactory.isInputDriver(), driverFactory.isOutputDriver());
        }

        private DriverSplitRunner createDriverRunner(@Nullable ScheduledSplit partitionedSplit)
        {
            pendingCreation.incrementAndGet();
            return new DriverSplitRunner(this, partitionedSplit);
        }

        private Driver createDriver(@Nullable ScheduledSplit partitionedSplit)
        {
            Driver driver = driverFactory.createDriver(pipelineContext.addDriverContext());

            // record driver so other threads add unpartitioned sources can see the driver
            // NOTE: this MUST be done before reading unpartitionedSources, so we see a consistent view of the unpartitioned sources
            drivers.add(new WeakReference<>(driver));

            if (partitionedSplit != null) {
                // TableScanOperator requires partitioned split to be added before the first call to process
                driver.updateSource(new TaskSource(partitionedSourceId, ImmutableSet.of(partitionedSplit), true));
            }

            // add unpartitioned sources
            for (TaskSource source : unpartitionedSources.values()) {
                driver.updateSource(source);
            }

            pendingCreation.decrementAndGet();
            closeDriverFactoryIfFullyCreated();

            return driver;
        }

        private boolean isNoMoreSplits()
        {
            return noMoreSplits.get();
        }

        private void setNoMoreSplits()
        {
            noMoreSplits.set(true);
            closeDriverFactoryIfFullyCreated();
        }

        private void closeDriverFactoryIfFullyCreated()
        {
            if (isNoMoreSplits() && pendingCreation.get() <= 0) {
                driverFactory.close();
            }
        }
    }

    private static class DriverSplitRunner
            implements SplitRunner
    {
        private final DriverSplitRunnerFactory driverSplitRunnerFactory;

        @Nullable
        private final ScheduledSplit partitionedSplit;

        @GuardedBy("this")
        private Driver driver;

        private DriverSplitRunner(DriverSplitRunnerFactory driverSplitRunnerFactory, @Nullable ScheduledSplit partitionedSplit)
        {
            this.driverSplitRunnerFactory = checkNotNull(driverSplitRunnerFactory, "driverFactory is null");
            this.partitionedSplit = partitionedSplit;
        }

        public synchronized DriverContext getDriverContext()
        {
            if (driver == null) {
                return null;
            }
            return driver.getDriverContext();
        }

        @Override
        public synchronized boolean isFinished()
        {
            return driver.isFinished();
        }

        @Override
        public synchronized ListenableFuture<?> processFor(Duration duration)
        {
            if (driver == null) {
                driver = driverSplitRunnerFactory.createDriver(partitionedSplit);
            }

            return driver.processFor(duration);
        }

        @Override
        public synchronized void close()
        {
            if (driver != null) {
                driver.close();
            }
        }
    }
}
