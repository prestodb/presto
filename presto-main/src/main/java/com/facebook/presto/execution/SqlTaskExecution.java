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

import com.facebook.presto.ScheduledSplit;
import com.facebook.presto.TaskSource;
import com.facebook.presto.event.query.QueryMonitor;
import com.facebook.presto.execution.SharedBuffer.BufferState;
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.execution.TaskExecutor.TaskHandle;
import com.facebook.presto.operator.Driver;
import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.DriverFactory;
import com.facebook.presto.operator.DriverStats;
import com.facebook.presto.operator.OutputFactory;
import com.facebook.presto.operator.PartitionedOutputOperator.PartitionedOutputFactory;
import com.facebook.presto.operator.PipelineContext;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.operator.TaskOutputOperator.TaskOutputFactory;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.sql.planner.LocalExecutionPlanner;
import com.facebook.presto.sql.planner.LocalExecutionPlanner.LocalExecutionPlan;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.PlanFragment.OutputPartitioning;
import com.facebook.presto.sql.planner.PlanFragment.PlanDistribution;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.SetThreadName;
import io.airlift.units.Duration;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Math.max;
import static java.lang.String.format;

public class SqlTaskExecution
{
    private final TaskId taskId;
    private final TaskStateMachine taskStateMachine;
    private final TaskContext taskContext;
    private final SharedBuffer sharedBuffer;

    private final TaskHandle taskHandle;
    private final TaskExecutor taskExecutor;

    private final Executor notificationExecutor;

    private final QueryMonitor queryMonitor;

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

    private final PlanNodeId partitionedSourceId;
    private final DriverSplitRunnerFactory partitionedDriverFactory;

    private final List<DriverSplitRunnerFactory> unpartitionedDriverFactories;

    public static SqlTaskExecution createSqlTaskExecution(
            TaskStateMachine taskStateMachine,
            TaskContext taskContext,
            SharedBuffer sharedBuffer,
            PlanFragment fragment,
            List<TaskSource> sources,
            LocalExecutionPlanner planner,
            TaskExecutor taskExecutor,
            Executor notificationExecutor,
            QueryMonitor queryMonitor)
    {
        SqlTaskExecution task = new SqlTaskExecution(
                taskStateMachine, taskContext, sharedBuffer, fragment,
                planner,
                taskExecutor,
                queryMonitor,
                notificationExecutor
        );

        try (SetThreadName ignored = new SetThreadName("Task-%s", task.getTaskId())) {
            task.start();
            task.addSources(sources);
            return task;
        }
    }

    private SqlTaskExecution(
            TaskStateMachine taskStateMachine,
            TaskContext taskContext,
            SharedBuffer sharedBuffer,
            PlanFragment fragment,
            LocalExecutionPlanner planner,
            TaskExecutor taskExecutor,
            QueryMonitor queryMonitor,
            Executor notificationExecutor)
    {
        this.taskStateMachine = checkNotNull(taskStateMachine, "taskStateMachine is null");
        this.taskId = taskStateMachine.getTaskId();
        this.taskContext = checkNotNull(taskContext, "taskContext is null");
        this.sharedBuffer = checkNotNull(sharedBuffer, "sharedBuffer is null");

        this.taskExecutor = checkNotNull(taskExecutor, "driverExecutor is null");
        this.notificationExecutor = checkNotNull(notificationExecutor, "notificationExecutor is null");

        this.queryMonitor = checkNotNull(queryMonitor, "queryMonitor is null");

        try (SetThreadName ignored = new SetThreadName("Task-%s", taskId)) {
            List<DriverFactory> driverFactories;
            try {
                OutputFactory outputOperatorFactory;
                if (fragment.getOutputPartitioning() == OutputPartitioning.NONE) {
                    outputOperatorFactory = new TaskOutputFactory(sharedBuffer);
                }
                else if (fragment.getOutputPartitioning() == OutputPartitioning.HASH) {
                    outputOperatorFactory = new PartitionedOutputFactory(sharedBuffer);
                }
                else {
                    throw new PrestoException(NOT_SUPPORTED, format("OutputPartitioning %s is not supported", fragment.getOutputPartitioning()));
                }

                LocalExecutionPlan localExecutionPlan = planner.plan(
                        taskContext.getSession(),
                        fragment.getRoot(),
                        fragment.getOutputLayout(),
                        fragment.getSymbols(),
                        fragment.getDistribution(),
                        outputOperatorFactory);
                driverFactories = localExecutionPlan.getDriverFactories();
            }
            catch (Throwable e) {
                // planning failed
                taskStateMachine.failed(e);
                throw Throwables.propagate(e);
            }

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

            // don't register the task if it is already completed (most likely failed during planning above)
            if (!taskStateMachine.getState().isDone()) {
                taskHandle = taskExecutor.addTask(taskId);
                taskStateMachine.addStateChangeListener(new RemoveTaskHandleWhenDone(taskExecutor, taskHandle));
            }
            else {
                taskHandle = null;
            }

            sharedBuffer.addStateChangeListener(new CheckTaskCompletionOnBufferFinish(SqlTaskExecution.this));
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
            for (int i = 0; i < driverFactory.getDriverInstances(); i++) {
                runners.add(driverFactory.createDriverRunner(null, false));
            }
            driverFactory.setNoMoreSplits();
        }
        enqueueDrivers(true, runners);
    }

    public TaskId getTaskId()
    {
        return taskId;
    }

    public TaskContext getTaskContext()
    {
        return taskContext;
    }

    public void addSources(List<TaskSource> sources)
    {
        checkNotNull(sources, "sources is null");
        checkState(!Thread.holdsLock(this), "Can not add sources while holding a lock on the %s", getClass().getSimpleName());

        try (SetThreadName ignored = new SetThreadName("Task-%s", taskId)) {
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

            // we may have transitioned to no more splits, so check for completion
            checkTaskCompletion();
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
                        runners.add(partitionedDriverFactory.createDriverRunner(scheduledSplit, true));
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
                    try (SetThreadName ignored = new SetThreadName("Task-%s", taskId)) {
                        // record driver is finished
                        remainingDrivers.decrementAndGet();

                        checkTaskCompletion();

                        queryMonitor.splitCompletionEvent(taskId, getDriverStats());
                    }
                }

                @Override
                public void onFailure(Throwable cause)
                {
                    try (SetThreadName ignored = new SetThreadName("Task-%s", taskId)) {
                        taskStateMachine.failed(cause);

                        // record driver is finished
                        remainingDrivers.decrementAndGet();

                        // fire failed event with cause
                        queryMonitor.splitFailedEvent(taskId, getDriverStats(), cause);
                    }
                }

                private DriverStats getDriverStats()
                {
                    DriverContext driverContext = splitRunner.getDriverContext();
                    DriverStats driverStats;
                    if (driverContext != null) {
                        driverStats = driverContext.getDriverStats();
                    }
                    else {
                        // split runner did not start successfully
                        driverStats = new DriverStats();
                    }

                    return driverStats;
                }
            }, notificationExecutor);
        }
    }

    public Set<PlanNodeId> getNoMoreSplits()
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
        sharedBuffer.setNoMorePages();

        // are there still pages in the output buffer
        if (!sharedBuffer.isFinished()) {
            return;
        }

        // Cool! All done!
        taskStateMachine.finished();
    }

    public void cancel()
    {
        // todo this should finish all input sources and let the task finish naturally
        try (SetThreadName ignored = new SetThreadName("Task-%s", taskId)) {
            taskStateMachine.cancel();
        }
    }

    public void fail(Throwable cause)
    {
        try (SetThreadName ignored = new SetThreadName("Task-%s", taskId)) {
            taskStateMachine.failed(cause);
        }
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
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

        private DriverSplitRunner createDriverRunner(@Nullable ScheduledSplit partitionedSplit, boolean partitioned)
        {
            pendingCreation.incrementAndGet();
            // create driver context immediately so the driver existence is recorded in the stats
            // the number of drivers is used to balance work across nodes
            DriverContext driverContext = pipelineContext.addDriverContext(partitioned);
            return new DriverSplitRunner(this, driverContext, partitionedSplit);
        }

        private Driver createDriver(DriverContext driverContext, @Nullable ScheduledSplit partitionedSplit)
        {
            Driver driver = driverFactory.createDriver(driverContext);

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

        public int getDriverInstances()
        {
            return driverFactory.getDriverInstances();
        }
    }

    private static class DriverSplitRunner
            implements SplitRunner
    {
        private final DriverSplitRunnerFactory driverSplitRunnerFactory;
        private final DriverContext driverContext;

        @GuardedBy("this")
        private boolean closed;

        @Nullable
        private final ScheduledSplit partitionedSplit;

        @GuardedBy("this")
        private Driver driver;

        private DriverSplitRunner(DriverSplitRunnerFactory driverSplitRunnerFactory, DriverContext driverContext, @Nullable ScheduledSplit partitionedSplit)
        {
            this.driverSplitRunnerFactory = checkNotNull(driverSplitRunnerFactory, "driverFactory is null");
            this.driverContext = checkNotNull(driverContext, "driverContext is null");
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
            if (closed) {
                return true;
            }

            if (driver == null) {
                return false;
            }

            return driver.isFinished();
        }

        @Override
        public ListenableFuture<?> processFor(Duration duration)
        {
            Driver driver;
            synchronized (this) {
                // if close() was called before we get here, there's not point in even creating the driver
                if (closed) {
                    return Futures.immediateFuture(null);
                }

                if (this.driver == null) {
                    this.driver = driverSplitRunnerFactory.createDriver(driverContext, partitionedSplit);
                }

                driver = this.driver;
            }

            return driver.processFor(duration);
        }

        @Override
        public void close()
        {
            Driver driver;
            synchronized (this) {
                closed = true;
                driver = this.driver;
            }

            if (driver != null) {
                driver.close();
            }
        }
    }

    private static final class RemoveTaskHandleWhenDone
            implements StateChangeListener<TaskState>
    {
        private final TaskExecutor taskExecutor;
        private final TaskHandle taskHandle;

        private RemoveTaskHandleWhenDone(TaskExecutor taskExecutor, TaskHandle taskHandle)
        {
            this.taskExecutor = checkNotNull(taskExecutor, "taskExecutor is null");
            this.taskHandle = checkNotNull(taskHandle, "taskHandle is null");
        }

        @Override
        public void stateChanged(TaskState taskState)
        {
            if (taskState.isDone()) {
                taskExecutor.removeTask(taskHandle);
            }
        }
    }

    private static final class CheckTaskCompletionOnBufferFinish
            implements StateChangeListener<BufferState>
    {
        private final WeakReference<SqlTaskExecution> sqlTaskExecutionReference;

        public CheckTaskCompletionOnBufferFinish(SqlTaskExecution sqlTaskExecution)
        {
            // we are only checking for completion of the task, so don't hold up GC if the task is dead
            this.sqlTaskExecutionReference = new WeakReference<>(sqlTaskExecution);
        }

        @Override
        public void stateChanged(BufferState taskState)
        {
            if (taskState == BufferState.FINISHED) {
                SqlTaskExecution sqlTaskExecution = sqlTaskExecutionReference.get();
                if (sqlTaskExecution != null) {
                    sqlTaskExecution.checkTaskCompletion();
                }
            }
        }
    }
}
