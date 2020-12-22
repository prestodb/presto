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
package com.facebook.presto.spark.execution;

import com.facebook.airlift.concurrent.SetThreadName;
import com.facebook.presto.event.SplitMonitor;
import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.execution.ScheduledSplit;
import com.facebook.presto.execution.SplitRunner;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.execution.TaskStateMachine;
import com.facebook.presto.execution.executor.TaskExecutor;
import com.facebook.presto.execution.executor.TaskHandle;
import com.facebook.presto.operator.Driver;
import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.DriverFactory;
import com.facebook.presto.operator.DriverStats;
import com.facebook.presto.operator.PipelineContext;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.sql.planner.LocalExecutionPlanner.LocalExecutionPlan;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.Duration;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.presto.SystemSessionProperties.getInitialSplitsPerNode;
import static com.facebook.presto.SystemSessionProperties.getMaxDriversPerTask;
import static com.facebook.presto.SystemSessionProperties.getSplitConcurrencyAdjustmentInterval;
import static com.facebook.presto.operator.PipelineExecutionStrategy.UNGROUPED_EXECUTION;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * The PrestoSparkTaskExecution is a simplified version of SqlTaskExecution.
 * It doesn't support grouped execution that is not needed on Presto on Spark.
 * Unlike the SqlTaskExecution the PrestoSparkTaskExecution does not require
 * the output buffer to be drained to mark the task as finished. As long as
 * all driver as finished the task execution is marked as finished. That allows to
 * have more control over the output Iterator lifecycle in the PrestoSparkTaskExecutor
 */
public class PrestoSparkTaskExecution
{
    private final TaskId taskId;
    private final TaskStateMachine taskStateMachine;
    private final TaskContext taskContext;

    private final TaskHandle taskHandle;
    private final TaskExecutor taskExecutor;

    private final Executor notificationExecutor;

    private final SplitMonitor splitMonitor;

    private final List<PlanNodeId> schedulingOrder;
    private final Map<PlanNodeId, DriverSplitRunnerFactory> driverRunnerFactoriesWithSplitLifeCycle;
    private final List<DriverSplitRunnerFactory> driverRunnerFactoriesWithTaskLifeCycle;

    /**
     * Number of drivers that have been sent to the TaskExecutor that have not finished.
     */
    private final AtomicInteger remainingDrivers = new AtomicInteger();

    private final AtomicBoolean started = new AtomicBoolean();

    public PrestoSparkTaskExecution(
            TaskStateMachine taskStateMachine,
            TaskContext taskContext,
            LocalExecutionPlan localExecutionPlan,
            TaskExecutor taskExecutor,
            SplitMonitor splitMonitor,
            Executor notificationExecutor,
            ScheduledExecutorService memoryUpdateExecutor)
    {
        this.taskStateMachine = requireNonNull(taskStateMachine, "taskStateMachine is null");
        this.taskId = taskStateMachine.getTaskId();
        this.taskContext = requireNonNull(taskContext, "taskContext is null");

        this.taskExecutor = requireNonNull(taskExecutor, "driverExecutor is null");
        this.notificationExecutor = requireNonNull(notificationExecutor, "notificationExecutor is null");

        this.splitMonitor = requireNonNull(splitMonitor, "splitMonitor is null");

        // index driver factories
        schedulingOrder = localExecutionPlan.getTableScanSourceOrder();
        Set<PlanNodeId> tableScanSources = ImmutableSet.copyOf(schedulingOrder);
        ImmutableMap.Builder<PlanNodeId, DriverSplitRunnerFactory> driverRunnerFactoriesWithSplitLifeCycle = ImmutableMap.builder();
        ImmutableList.Builder<DriverSplitRunnerFactory> driverRunnerFactoriesWithTaskLifeCycle = ImmutableList.builder();
        for (DriverFactory driverFactory : localExecutionPlan.getDriverFactories()) {
            Optional<PlanNodeId> sourceId = driverFactory.getSourceId();
            if (sourceId.isPresent() && tableScanSources.contains(sourceId.get())) {
                driverRunnerFactoriesWithSplitLifeCycle.put(sourceId.get(), new DriverSplitRunnerFactory(driverFactory, true));
            }
            else {
                checkArgument(
                        driverFactory.getPipelineExecutionStrategy() == UNGROUPED_EXECUTION,
                        "unexpected pipeline execution strategy: %s",
                        driverFactory.getPipelineExecutionStrategy());
                driverRunnerFactoriesWithTaskLifeCycle.add(new DriverSplitRunnerFactory(driverFactory, false));
            }
        }
        this.driverRunnerFactoriesWithSplitLifeCycle = driverRunnerFactoriesWithSplitLifeCycle.build();
        this.driverRunnerFactoriesWithTaskLifeCycle = driverRunnerFactoriesWithTaskLifeCycle.build();

        checkArgument(this.driverRunnerFactoriesWithSplitLifeCycle.keySet().equals(tableScanSources),
                "Fragment is partitioned, but not all partitioned drivers were found");

        taskHandle = createTaskHandle(taskStateMachine, taskContext, localExecutionPlan, taskExecutor);

        requireNonNull(memoryUpdateExecutor, "memoryUpdateExecutor is null");
        memoryUpdateExecutor.schedule(taskContext::updatePeakMemory, 1, SECONDS);
    }

    // this is a separate method to ensure that the `this` reference is not leaked during construction
    private static TaskHandle createTaskHandle(
            TaskStateMachine taskStateMachine,
            TaskContext taskContext,
            LocalExecutionPlan localExecutionPlan,
            TaskExecutor taskExecutor)
    {
        TaskHandle taskHandle = taskExecutor.addTask(
                taskStateMachine.getTaskId(),
                () -> 0,
                getInitialSplitsPerNode(taskContext.getSession()),
                getSplitConcurrencyAdjustmentInterval(taskContext.getSession()),
                getMaxDriversPerTask(taskContext.getSession()));
        taskStateMachine.addStateChangeListener(state -> {
            if (state.isDone()) {
                taskExecutor.removeTask(taskHandle);
                for (DriverFactory factory : localExecutionPlan.getDriverFactories()) {
                    factory.noMoreDrivers();
                }
            }
        });
        return taskHandle;
    }

    public void start(List<TaskSource> sources)
    {
        requireNonNull(sources, "sources is null");

        checkState(started.compareAndSet(false, true), "already started");

        scheduleDriversForTaskLifeCycle();
        scheduleDriversForSplitLifeCycle(sources);
        checkTaskCompletion();
    }

    private void scheduleDriversForTaskLifeCycle()
    {
        List<DriverSplitRunner> runners = new ArrayList<>();
        for (DriverSplitRunnerFactory driverRunnerFactory : driverRunnerFactoriesWithTaskLifeCycle) {
            for (int i = 0; i < driverRunnerFactory.getDriverInstances().orElse(1); i++) {
                runners.add(driverRunnerFactory.createDriverRunner(null));
            }
        }
        enqueueDriverSplitRunner(true, runners);
        for (DriverSplitRunnerFactory driverRunnerFactory : driverRunnerFactoriesWithTaskLifeCycle) {
            driverRunnerFactory.noMoreDriverRunner();
            verify(driverRunnerFactory.isNoMoreDriverRunner());
        }
    }

    private synchronized void scheduleDriversForSplitLifeCycle(List<TaskSource> sources)
    {
        checkArgument(sources.stream().allMatch(TaskSource::isNoMoreSplits), "All task sources are expected to be final");

        ListMultimap<PlanNodeId, ScheduledSplit> splits = ArrayListMultimap.create();
        for (TaskSource taskSource : sources) {
            splits.putAll(taskSource.getPlanNodeId(), taskSource.getSplits());
        }

        for (PlanNodeId planNodeId : schedulingOrder) {
            DriverSplitRunnerFactory driverSplitRunnerFactory = driverRunnerFactoriesWithSplitLifeCycle.get(planNodeId);
            List<ScheduledSplit> planNodeSplits = splits.get(planNodeId);
            scheduleTableScanSource(driverSplitRunnerFactory, planNodeSplits);
        }
    }

    private synchronized void scheduleTableScanSource(DriverSplitRunnerFactory factory, List<ScheduledSplit> splits)
    {
        factory.splitsAdded(splits.size());

        // Enqueue driver runners with split lifecycle for this plan node and driver life cycle combination.
        ImmutableList.Builder<DriverSplitRunner> runners = ImmutableList.builder();
        for (ScheduledSplit scheduledSplit : splits) {
            // create a new driver for the split
            runners.add(factory.createDriverRunner(scheduledSplit));
        }
        enqueueDriverSplitRunner(false, runners.build());

        factory.noMoreDriverRunner();
    }

    private synchronized void enqueueDriverSplitRunner(boolean forceRunSplit, List<DriverSplitRunner> runners)
    {
        // schedule driver to be executed
        List<ListenableFuture<?>> finishedFutures = taskExecutor.enqueueSplits(taskHandle, forceRunSplit, runners);
        checkState(finishedFutures.size() == runners.size(), "Expected %s futures but got %s", runners.size(), finishedFutures.size());

        // when driver completes, update state and fire events
        for (int i = 0; i < finishedFutures.size(); i++) {
            ListenableFuture<?> finishedFuture = finishedFutures.get(i);
            final DriverSplitRunner splitRunner = runners.get(i);

            // record new driver
            remainingDrivers.incrementAndGet();

            Futures.addCallback(finishedFuture, new FutureCallback<Object>()
            {
                @Override
                public void onSuccess(Object result)
                {
                    try (SetThreadName ignored = new SetThreadName("Task-%s", taskId)) {
                        // record driver is finished
                        remainingDrivers.decrementAndGet();

                        checkTaskCompletion();

                        splitMonitor.splitCompletedEvent(taskId, getDriverStats());
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
                        splitMonitor.splitFailedEvent(taskId, getDriverStats(), cause);
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

    private synchronized void checkTaskCompletion()
    {
        if (taskStateMachine.getState().isDone()) {
            return;
        }

        // are there more partition splits expected?
        for (DriverSplitRunnerFactory driverSplitRunnerFactory : driverRunnerFactoriesWithSplitLifeCycle.values()) {
            if (!driverSplitRunnerFactory.isNoMoreDriverRunner()) {
                return;
            }
        }
        // do we still have running tasks?
        if (remainingDrivers.get() != 0) {
            return;
        }

        // Cool! All done!
        taskStateMachine.finished();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("taskId", taskId)
                .add("remainingDrivers", remainingDrivers.get())
                .toString();
    }

    private class DriverSplitRunnerFactory
    {
        private final DriverFactory driverFactory;
        private final PipelineContext pipelineContext;

        private final AtomicInteger pendingCreation = new AtomicInteger();
        private final AtomicBoolean noMoreDriverRunner = new AtomicBoolean();
        private final AtomicBoolean closed = new AtomicBoolean();

        private DriverSplitRunnerFactory(DriverFactory driverFactory, boolean partitioned)
        {
            this.driverFactory = requireNonNull(driverFactory, "driverFactory is null");
            this.pipelineContext = taskContext.addPipelineContext(driverFactory.getPipelineId(), driverFactory.isInputDriver(), driverFactory.isOutputDriver(), partitioned);
        }

        public DriverSplitRunner createDriverRunner(@Nullable ScheduledSplit partitionedSplit)
        {
            checkState(!noMoreDriverRunner.get(), "Cannot create driver for pipeline: %s", pipelineContext.getPipelineId());
            pendingCreation.incrementAndGet();
            // create driver context immediately so the driver existence is recorded in the stats
            // the number of drivers is used to balance work across nodes
            DriverContext driverContext = pipelineContext.addDriverContext(Lifespan.taskWide(), driverFactory.getFragmentResultCacheContext());
            return new DriverSplitRunner(this, driverContext, partitionedSplit);
        }

        public Driver createDriver(DriverContext driverContext, @Nullable ScheduledSplit partitionedSplit)
        {
            Driver driver = driverFactory.createDriver(driverContext);

            if (partitionedSplit != null) {
                // TableScanOperator requires partitioned split to be added before the first call to process
                driver.updateSource(new TaskSource(partitionedSplit.getPlanNodeId(), ImmutableSet.of(partitionedSplit), true));
            }

            verify(pendingCreation.get() > 0, "pendingCreation is expected to be greater than zero");
            pendingCreation.decrementAndGet();

            closeDriverFactoryIfFullyCreated();

            return driver;
        }

        public void noMoreDriverRunner()
        {
            if (noMoreDriverRunner.get()) {
                return;
            }
            noMoreDriverRunner.set(true);
            closeDriverFactoryIfFullyCreated();
        }

        public boolean isNoMoreDriverRunner()
        {
            return noMoreDriverRunner.get();
        }

        public void closeDriverFactoryIfFullyCreated()
        {
            if (closed.get()) {
                return;
            }
            if (isNoMoreDriverRunner() && pendingCreation.get() == 0) {
                // ensure noMoreDrivers is called only once
                if (!closed.compareAndSet(false, true)) {
                    return;
                }
                driverFactory.noMoreDrivers(Lifespan.taskWide());
                driverFactory.noMoreDrivers();
            }
        }

        public OptionalInt getDriverInstances()
        {
            return driverFactory.getDriverInstances();
        }

        public void splitsAdded(int count)
        {
            pipelineContext.splitsAdded(count);
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
            this.driverSplitRunnerFactory = requireNonNull(driverSplitRunnerFactory, "driverFactory is null");
            this.driverContext = requireNonNull(driverContext, "driverContext is null");
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

            return driver != null && driver.isFinished();
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
        public String getInfo()
        {
            return (partitionedSplit == null) ? "" : partitionedSplit.getSplit().getInfo().toString();
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
}
