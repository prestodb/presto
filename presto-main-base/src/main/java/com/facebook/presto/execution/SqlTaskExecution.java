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

import com.facebook.airlift.concurrent.NotThreadSafe;
import com.facebook.airlift.concurrent.SetThreadName;
import com.facebook.airlift.units.Duration;
import com.facebook.presto.event.SplitMonitor;
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.execution.buffer.BufferState;
import com.facebook.presto.execution.buffer.OutputBuffer;
import com.facebook.presto.execution.executor.TaskExecutor;
import com.facebook.presto.execution.executor.TaskHandle;
import com.facebook.presto.operator.Driver;
import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.DriverFactory;
import com.facebook.presto.operator.DriverStats;
import com.facebook.presto.operator.PipelineContext;
import com.facebook.presto.operator.PipelineExecutionStrategy;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.spi.SplitWeight;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.StageExecutionDescriptor;
import com.facebook.presto.sql.planner.LocalExecutionPlanner.LocalExecutionPlan;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import jakarta.annotation.Nullable;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

import static com.facebook.presto.SystemSessionProperties.getInitialSplitsPerNode;
import static com.facebook.presto.SystemSessionProperties.getMaxDriversPerTask;
import static com.facebook.presto.SystemSessionProperties.getSplitConcurrencyAdjustmentInterval;
import static com.facebook.presto.execution.SqlTaskExecution.SplitsState.ADDING_SPLITS;
import static com.facebook.presto.execution.SqlTaskExecution.SplitsState.FINISHED;
import static com.facebook.presto.execution.SqlTaskExecution.SplitsState.NO_MORE_SPLITS;
import static com.facebook.presto.operator.PipelineExecutionStrategy.UNGROUPED_EXECUTION;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;

public class SqlTaskExecution
{
    // For each driver in a task, it belong to a pipeline and a driver life cycle.
    // Pipeline and driver life cycle are two perpendicular organizations of tasks.
    //
    // * All drivers in the same pipeline has the same shape.
    //   (i.e. operators are constructed from the same set of operator factories)
    // * All drivers in the same driver life cycle are responsible for processing a group of data.
    //   (e.g. all rows that fall in bucket 42)
    //
    // Take a task with the following set of pipelines for example:
    //
    //   pipeline 0       pipeline 1       pipeline 2    pipeline 3    ... pipeline id
    //     grouped          grouped          grouped      ungrouped    ... execution strategy
    //
    // PartitionedOutput
    //        |
    //    LookupJoin  ..................................  HashBuild
    //        |                                               |
    //    LookupJoin  ...  HashBuild                     ExchangeSrc
    //        |               |
    //    TableScan       LocalExSrc  ...  LocalExSink
    //                                          |
    //                                      TableScan
    //
    // In this case,
    // * a driver could belong to pipeline 1 and driver life cycle 42.
    // * another driver could belong to pipeline 3 and task-wide driver life cycle.

    private final TaskId taskId;
    private final TaskStateMachine taskStateMachine;
    private final TaskContext taskContext;
    private final OutputBuffer outputBuffer;

    private final TaskHandle taskHandle;
    private final TaskExecutor taskExecutor;

    private final Executor notificationExecutor;

    private final SplitMonitor splitMonitor;

    private final List<WeakReference<Driver>> drivers = new CopyOnWriteArrayList<>();

    private final Map<PlanNodeId, DriverSplitRunnerFactory> driverRunnerFactoriesWithSplitLifeCycle;
    private final List<DriverSplitRunnerFactory> driverRunnerFactoriesWithDriverGroupLifeCycle;
    private final List<DriverSplitRunnerFactory> driverRunnerFactoriesWithTaskLifeCycle;

    // guarded for update only
    @GuardedBy("this")
    private final ConcurrentMap<PlanNodeId, TaskSource> remoteSources = new ConcurrentHashMap<>();

    @GuardedBy("this")
    private long maxAcknowledgedSplit = Long.MIN_VALUE;

    @GuardedBy("this")
    private final SchedulingLifespanManager schedulingLifespanManager;

    @GuardedBy("this")
    private final Map<PlanNodeId, PendingSplitsForPlanNode> pendingSplitsByPlanNode;

    private final Status status;

    static SqlTaskExecution createSqlTaskExecution(
            TaskStateMachine taskStateMachine,
            TaskContext taskContext,
            OutputBuffer outputBuffer,
            List<TaskSource> sources,
            LocalExecutionPlan localExecutionPlan,
            TaskExecutor taskExecutor,
            Executor notificationExecutor,
            SplitMonitor queryMonitor)
    {
        SqlTaskExecution task = new SqlTaskExecution(
                taskStateMachine,
                taskContext,
                outputBuffer,
                localExecutionPlan,
                taskExecutor,
                queryMonitor,
                notificationExecutor);
        try (SetThreadName ignored = new SetThreadName("Task-%s", task.getTaskId())) {
            // The scheduleDriversForTaskLifeCycle method calls enqueueDriverSplitRunner, which registers a callback with access to this object.
            // The call back is accessed from another thread, so this code can not be placed in the constructor.
            task.scheduleDriversForTaskLifeCycle();
            task.addSources(sources);
            return task;
        }
    }

    private SqlTaskExecution(
            TaskStateMachine taskStateMachine,
            TaskContext taskContext,
            OutputBuffer outputBuffer,
            LocalExecutionPlan localExecutionPlan,
            TaskExecutor taskExecutor,
            SplitMonitor splitMonitor,
            Executor notificationExecutor)
    {
        this.taskStateMachine = requireNonNull(taskStateMachine, "taskStateMachine is null");
        this.taskId = taskStateMachine.getTaskId();
        this.taskContext = requireNonNull(taskContext, "taskContext is null");
        this.outputBuffer = requireNonNull(outputBuffer, "outputBuffer is null");

        this.taskExecutor = requireNonNull(taskExecutor, "driverExecutor is null");
        this.notificationExecutor = requireNonNull(notificationExecutor, "notificationExecutor is null");

        this.splitMonitor = requireNonNull(splitMonitor, "splitMonitor is null");

        try (SetThreadName ignored = new SetThreadName("Task-%s", taskId)) {
            // index driver factories
            Set<PlanNodeId> tableScanSources = ImmutableSet.copyOf(localExecutionPlan.getTableScanSourceOrder());
            ImmutableMap.Builder<PlanNodeId, DriverSplitRunnerFactory> driverRunnerFactoriesWithSplitLifeCycle = ImmutableMap.builder();
            ImmutableList.Builder<DriverSplitRunnerFactory> driverRunnerFactoriesWithTaskLifeCycle = ImmutableList.builder();
            ImmutableList.Builder<DriverSplitRunnerFactory> driverRunnerFactoriesWithDriverGroupLifeCycle = ImmutableList.builder();
            for (DriverFactory driverFactory : localExecutionPlan.getDriverFactories()) {
                Optional<PlanNodeId> sourceId = driverFactory.getSourceId();
                if (sourceId.isPresent() && tableScanSources.contains(sourceId.get())) {
                    driverRunnerFactoriesWithSplitLifeCycle.put(sourceId.get(), new DriverSplitRunnerFactory(driverFactory, true));
                }
                else {
                    switch (driverFactory.getPipelineExecutionStrategy()) {
                        case GROUPED_EXECUTION:
                            driverRunnerFactoriesWithDriverGroupLifeCycle.add(new DriverSplitRunnerFactory(driverFactory, false));
                            break;
                        case UNGROUPED_EXECUTION:
                            driverRunnerFactoriesWithTaskLifeCycle.add(new DriverSplitRunnerFactory(driverFactory, false));
                            break;
                        default:
                            throw new UnsupportedOperationException();
                    }
                }
            }
            this.driverRunnerFactoriesWithSplitLifeCycle = driverRunnerFactoriesWithSplitLifeCycle.build();
            this.driverRunnerFactoriesWithDriverGroupLifeCycle = driverRunnerFactoriesWithDriverGroupLifeCycle.build();
            this.driverRunnerFactoriesWithTaskLifeCycle = driverRunnerFactoriesWithTaskLifeCycle.build();

            this.pendingSplitsByPlanNode = this.driverRunnerFactoriesWithSplitLifeCycle.keySet().stream()
                    .collect(toImmutableMap(identity(), ignore -> new PendingSplitsForPlanNode()));
            this.status = new Status(
                    taskContext,
                    outputBuffer,
                    localExecutionPlan.getDriverFactories().stream()
                            .collect(toImmutableMap(DriverFactory::getPipelineId, DriverFactory::getPipelineExecutionStrategy)));
            this.schedulingLifespanManager = new SchedulingLifespanManager(localExecutionPlan.getTableScanSourceOrder(), localExecutionPlan.getStageExecutionDescriptor(), this.status);

            checkArgument(this.driverRunnerFactoriesWithSplitLifeCycle.keySet().equals(tableScanSources),
                    "Fragment is partitioned, but not all partitioned drivers were found");

            // Pre-register Lifespans for ungrouped partitioned drivers in case they end up get no splits.
            for (Entry<PlanNodeId, DriverSplitRunnerFactory> entry : this.driverRunnerFactoriesWithSplitLifeCycle.entrySet()) {
                PlanNodeId planNodeId = entry.getKey();
                DriverSplitRunnerFactory driverSplitRunnerFactory = entry.getValue();
                if (driverSplitRunnerFactory.getPipelineExecutionStrategy() == UNGROUPED_EXECUTION) {
                    this.schedulingLifespanManager.addLifespanIfAbsent(Lifespan.taskWide());
                    this.pendingSplitsByPlanNode.get(planNodeId).getLifespan(Lifespan.taskWide());
                }
            }

            // don't register the task if it is already completed (most likely failed during planning above)
            if (!taskStateMachine.getState().isDone()) {
                taskHandle = createTaskHandle(taskStateMachine, taskContext, outputBuffer, localExecutionPlan, taskExecutor);
            }
            else {
                taskHandle = null;
            }

            outputBuffer.addStateChangeListener(new CheckTaskCompletionOnBufferFinish(SqlTaskExecution.this));
            outputBuffer.registerLifespanCompletionCallback(status::checkLifespanCompletion);
        }
    }

    // this is a separate method to ensure that the `this` reference is not leaked during construction
    private static TaskHandle createTaskHandle(
            TaskStateMachine taskStateMachine,
            TaskContext taskContext,
            OutputBuffer outputBuffer,
            LocalExecutionPlan localExecutionPlan,
            TaskExecutor taskExecutor)
    {
        TaskHandle taskHandle = taskExecutor.addTask(
                taskStateMachine.getTaskId(),
                outputBuffer::getUtilization,
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
        requireNonNull(sources, "sources is null");
        checkState(!Thread.holdsLock(this), "Can not add sources while holding a lock on the %s", getClass().getSimpleName());

        try (SetThreadName ignored = new SetThreadName("Task-%s", taskId)) {
            // update our record of sources and schedule drivers for new partitioned splits
            Map<PlanNodeId, TaskSource> updatedRemoteSources = updateSources(sources);

            // tell existing drivers about the new splits; it is safe to update drivers
            // multiple times and out of order because sources contain full record of
            // the unpartitioned splits
            for (WeakReference<Driver> driverReference : drivers) {
                Driver driver = driverReference.get();
                // the driver can be GCed due to a failure or a limit
                if (driver == null) {
                    // remove the weak reference from the list to avoid a memory leak
                    // NOTE: this is a concurrent safe operation on a CopyOnWriteArrayList
                    drivers.remove(driverReference);
                    continue;
                }
                Optional<PlanNodeId> sourceId = driver.getSourceId();
                if (!sourceId.isPresent()) {
                    continue;
                }
                TaskSource sourceUpdate = updatedRemoteSources.get(sourceId.get());
                if (sourceUpdate == null) {
                    continue;
                }
                driver.updateSource(sourceUpdate);
            }

            // we may have transitioned to no more splits, so check for completion
            checkTaskCompletion();
        }
    }

    private synchronized Map<PlanNodeId, TaskSource> updateSources(List<TaskSource> sources)
    {
        Map<PlanNodeId, TaskSource> updatedRemoteSources = new HashMap<>();

        // first remove any split that was already acknowledged
        long currentMaxAcknowledgedSplit = this.maxAcknowledgedSplit;
        sources = sources.stream()
                .map(source -> new TaskSource(
                        source.getPlanNodeId(),
                        source.getSplits().stream()
                                .filter(scheduledSplit -> scheduledSplit.getSequenceId() > currentMaxAcknowledgedSplit)
                                .collect(Collectors.toSet()),
                        // Like splits, noMoreSplitsForLifespan could be pruned so that only new items will be processed.
                        // This is not happening here because correctness won't be compromised due to duplicate events for noMoreSplitsForLifespan.
                        source.getNoMoreSplitsForLifespan(),
                        source.isNoMoreSplits()))
                .collect(toList());

        // update task with new sources
        for (TaskSource source : sources) {
            if (driverRunnerFactoriesWithSplitLifeCycle.containsKey(source.getPlanNodeId())) {
                scheduleTableScanSource(source);
            }
            else {
                scheduleRemoteSource(source, updatedRemoteSources);
            }
        }

        for (DriverSplitRunnerFactory driverSplitRunnerFactory :
                Iterables.concat(driverRunnerFactoriesWithSplitLifeCycle.values(), driverRunnerFactoriesWithTaskLifeCycle, driverRunnerFactoriesWithDriverGroupLifeCycle)) {
            driverSplitRunnerFactory.closeDriverFactoryIfFullyCreated();
        }

        // update maxAcknowledgedSplit
        maxAcknowledgedSplit = sources.stream()
                .flatMap(source -> source.getSplits().stream())
                .mapToLong(ScheduledSplit::getSequenceId)
                .max()
                .orElse(maxAcknowledgedSplit);
        return updatedRemoteSources;
    }

    @GuardedBy("this")
    private void mergeIntoPendingSplits(PlanNodeId planNodeId, Set<ScheduledSplit> scheduledSplits, Set<Lifespan> noMoreSplitsForLifespan, boolean noMoreSplits)
    {
        checkHoldsLock();

        DriverSplitRunnerFactory partitionedDriverFactory = driverRunnerFactoriesWithSplitLifeCycle.get(planNodeId);
        PendingSplitsForPlanNode pendingSplitsForPlanNode = pendingSplitsByPlanNode.get(planNodeId);

        partitionedDriverFactory.splitsAdded(scheduledSplits.size(), SplitWeight.rawValueSum(scheduledSplits, scheduledSplit -> scheduledSplit.getSplit().getSplitWeight()));
        for (ScheduledSplit scheduledSplit : scheduledSplits) {
            Lifespan lifespan = scheduledSplit.getSplit().getLifespan();
            checkLifespan(partitionedDriverFactory.getPipelineExecutionStrategy(), lifespan);
            pendingSplitsForPlanNode.getLifespan(lifespan).addSplit(scheduledSplit);
            schedulingLifespanManager.addLifespanIfAbsent(lifespan);
        }
        for (Lifespan lifespanWithNoMoreSplits : noMoreSplitsForLifespan) {
            checkLifespan(partitionedDriverFactory.getPipelineExecutionStrategy(), lifespanWithNoMoreSplits);
            pendingSplitsForPlanNode.getLifespan(lifespanWithNoMoreSplits).noMoreSplits();
            schedulingLifespanManager.addLifespanIfAbsent(lifespanWithNoMoreSplits);
        }
        if (noMoreSplits) {
            pendingSplitsForPlanNode.setNoMoreSplits();
        }
    }

    private synchronized void scheduleTableScanSource(TaskSource sourceUpdate)
    {
        mergeIntoPendingSplits(sourceUpdate.getPlanNodeId(), sourceUpdate.getSplits(), sourceUpdate.getNoMoreSplitsForLifespan(), sourceUpdate.isNoMoreSplits());

        while (true) {
            // SchedulingLifespanManager tracks how far each Lifespan has been scheduled. Here is an example.
            // Let's say there are 4 source pipelines/nodes: A, B, C, and D, in scheduling order.
            // And we're processing 3 concurrent lifespans at a time. In this case, we could have
            //
            // * Lifespan 10:  A   B  [C]  D; i.e. Pipeline A and B has finished scheduling (but not necessarily finished running).
            // * Lifespan 20: [A]  B   C   D
            // * Lifespan 30:  A  [B]  C   D
            //
            // To recap, SchedulingLifespanManager records the next scheduling source node for each lifespan.
            Iterator<SchedulingLifespan> activeLifespans = schedulingLifespanManager.getActiveLifespans();

            boolean madeProgress = false;

            while (activeLifespans.hasNext()) {
                SchedulingLifespan schedulingLifespan = activeLifespans.next();
                Lifespan lifespan = schedulingLifespan.getLifespan();

                // Continue using the example from above. Let's say the sourceUpdate adds some new splits for source node B.
                //
                // For lifespan 30, it could start new drivers and assign a pending split to each.
                // Pending splits could include both pre-existing pending splits, and the new ones from sourceUpdate.
                // If there is enough driver slots to deplete pending splits, one of the below would happen.
                // * If it is marked that all splits for node B in lifespan 30 has been received, SchedulingLifespanManager
                //   will be updated so that lifespan 30 now processes source node C. It will immediately start processing them.
                // * Otherwise, processing of lifespan 30 will be shelved for now.
                //
                // It is possible that the following loop would be a no-op for a particular lifespan.
                // It is also possible that a single lifespan can proceed through multiple source nodes in one run.
                //
                // When different drivers in the task has different pipelineExecutionStrategy, it adds additional complexity.
                // For example, when driver B is ungrouped and driver A, C, D is grouped, you could have something like this:
                //     TaskWide   :     [B]
                //     Lifespan 10:  A  [ ]  C   D
                //     Lifespan 20: [A]      C   D
                //     Lifespan 30:  A  [ ]  C   D
                // In this example, Lifespan 30 cannot start executing drivers in pipeline C because pipeline B
                // hasn't finished scheduling yet (albeit in a different lifespan).
                // Similarly, it wouldn't make sense for TaskWide to start executing drivers in pipeline B until at least
                // one lifespan has finished scheduling pipeline A.
                // This is why getSchedulingPlanNode returns an Optional.
                while (true) {
                    Optional<PlanNodeId> optionalSchedulingPlanNode = schedulingLifespan.getSchedulingPlanNode();
                    if (!optionalSchedulingPlanNode.isPresent()) {
                        break;
                    }
                    PlanNodeId schedulingPlanNode = optionalSchedulingPlanNode.get();

                    DriverSplitRunnerFactory partitionedDriverRunnerFactory = driverRunnerFactoriesWithSplitLifeCycle.get(schedulingPlanNode);

                    PendingSplits pendingSplits = pendingSplitsByPlanNode.get(schedulingPlanNode).getLifespan(lifespan);

                    // Enqueue driver runners with driver group lifecycle for this driver life cycle, if not already enqueued.
                    if (!lifespan.isTaskWide() && !schedulingLifespan.getAndSetDriversForDriverGroupLifeCycleScheduled()) {
                        scheduleDriversForDriverGroupLifeCycle(lifespan);
                    }

                    // Enqueue driver runners with split lifecycle for this plan node and driver life cycle combination.
                    ImmutableList.Builder<DriverSplitRunner> runners = ImmutableList.builder();
                    for (ScheduledSplit scheduledSplit : pendingSplits.removeAllSplits()) {
                        // create a new driver for the split
                        runners.add(partitionedDriverRunnerFactory.createDriverRunner(scheduledSplit, lifespan));
                    }
                    enqueueDriverSplitRunner(false, runners.build());

                    // If all driver runners have been enqueued for this plan node and driver life cycle combination,
                    // move on to the next plan node.
                    if (pendingSplits.getState() != NO_MORE_SPLITS) {
                        break;
                    }
                    partitionedDriverRunnerFactory.noMoreDriverRunner(ImmutableList.of(lifespan));
                    pendingSplits.markAsCleanedUp();

                    schedulingLifespan.nextPlanNode();
                    madeProgress = true;
                    if (schedulingLifespan.isDone()) {
                        break;
                    }
                }
            }

            if (!madeProgress) {
                break;
            }
        }

        if (sourceUpdate.isNoMoreSplits()) {
            schedulingLifespanManager.noMoreSplits(sourceUpdate.getPlanNodeId());
        }
    }

    private synchronized void scheduleRemoteSource(TaskSource sourceUpdate, Map<PlanNodeId, TaskSource> updatedRemoteSources)
    {
        // create new source
        TaskSource newSource;
        TaskSource currentSource = remoteSources.get(sourceUpdate.getPlanNodeId());
        if (currentSource == null) {
            newSource = sourceUpdate;
        }
        else {
            newSource = currentSource.update(sourceUpdate);
        }

        // only record new source if something changed
        if (newSource != currentSource) {
            remoteSources.put(sourceUpdate.getPlanNodeId(), newSource);
            updatedRemoteSources.put(sourceUpdate.getPlanNodeId(), newSource);
        }
    }

    // scheduleDriversForTaskLifeCycle and scheduleDriversForDriverGroupLifeCycle are similar.
    // They are invoked under different circumstances, and schedules a disjoint set of drivers, as suggested by their names.
    // They also have a few differences, making it more convenient to keep the two methods separate.
    private void scheduleDriversForTaskLifeCycle()
    {
        // This method is called at the beginning of the task.
        // It schedules drivers for all the pipelines that have task life cycle.
        List<DriverSplitRunner> runners = new ArrayList<>();
        for (DriverSplitRunnerFactory driverRunnerFactory : driverRunnerFactoriesWithTaskLifeCycle) {
            for (int i = 0; i < driverRunnerFactory.getDriverInstances().orElse(1); i++) {
                runners.add(driverRunnerFactory.createDriverRunner(null, Lifespan.taskWide()));
            }
        }
        enqueueDriverSplitRunner(true, runners);
        for (DriverSplitRunnerFactory driverRunnerFactory : driverRunnerFactoriesWithTaskLifeCycle) {
            driverRunnerFactory.noMoreDriverRunner(ImmutableList.of(Lifespan.taskWide()));
            verify(driverRunnerFactory.isNoMoreDriverRunner());
        }
    }

    private void scheduleDriversForDriverGroupLifeCycle(Lifespan lifespan)
    {
        // This method is called when a split that belongs to a previously unseen driver group is scheduled.
        // It schedules drivers for all the pipelines that have driver group life cycle.
        if (lifespan.isTaskWide()) {
            checkArgument(driverRunnerFactoriesWithDriverGroupLifeCycle.isEmpty(), "Instantiating pipeline of driver group lifecycle at task level is not allowed");
            return;
        }

        List<DriverSplitRunner> runners = new ArrayList<>();
        for (DriverSplitRunnerFactory driverSplitRunnerFactory : driverRunnerFactoriesWithDriverGroupLifeCycle) {
            for (int i = 0; i < driverSplitRunnerFactory.getDriverInstances().orElse(1); i++) {
                runners.add(driverSplitRunnerFactory.createDriverRunner(null, lifespan));
            }
        }
        enqueueDriverSplitRunner(true, runners);
        for (DriverSplitRunnerFactory driverRunnerFactory : driverRunnerFactoriesWithDriverGroupLifeCycle) {
            driverRunnerFactory.noMoreDriverRunner(ImmutableList.of(lifespan));
        }
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
            status.incrementRemainingDriver(splitRunner.getLifespan());

            Futures.addCallback(finishedFuture, new FutureCallback<Object>()
            {
                @Override
                public void onSuccess(Object result)
                {
                    try (SetThreadName ignored = new SetThreadName("Task-%s", taskId)) {
                        // record driver is finished
                        status.decrementRemainingDriver(splitRunner.getLifespan());

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
                        status.decrementRemainingDriver(splitRunner.getLifespan());

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

    public synchronized Set<PlanNodeId> getNoMoreSplits()
    {
        ImmutableSet.Builder<PlanNodeId> noMoreSplits = ImmutableSet.builder();
        for (Entry<PlanNodeId, DriverSplitRunnerFactory> entry : driverRunnerFactoriesWithSplitLifeCycle.entrySet()) {
            if (entry.getValue().isNoMoreDriverRunner()) {
                noMoreSplits.add(entry.getKey());
            }
        }
        for (TaskSource taskSource : remoteSources.values()) {
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
        for (DriverSplitRunnerFactory driverSplitRunnerFactory : driverRunnerFactoriesWithSplitLifeCycle.values()) {
            if (!driverSplitRunnerFactory.isNoMoreDriverRunner()) {
                return;
            }
        }
        // do we still have running tasks?
        if (status.getRemainingDriver() != 0) {
            return;
        }

        // no more output will be created
        outputBuffer.setNoMorePages();

        // are there still pages in the output buffer?
        if (!outputBuffer.isFinished()) {
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
                .add("remainingDrivers", status.getRemainingDriver())
                .add("remoteSources", remoteSources)
                .toString();
    }

    private void checkLifespan(PipelineExecutionStrategy executionStrategy, Lifespan lifespan)
    {
        switch (executionStrategy) {
            case GROUPED_EXECUTION:
                checkArgument(!lifespan.isTaskWide(), "Expect driver-group life cycle for grouped ExecutionStrategy. Got task-wide life cycle.");
                break;
            case UNGROUPED_EXECUTION:
                checkArgument(lifespan.isTaskWide(), "Expect task-wide life cycle for ungrouped ExecutionStrategy. Got driver-group life cycle.");
                break;
            default:
                throw new IllegalArgumentException("Unknown executionStrategy: " + executionStrategy);
        }
    }

    private void checkHoldsLock()
    {
        // This method serves a similar purpose at runtime as GuardedBy on method serves during static analysis.
        // This method should not have significant performance impact. If it does, it may be reasonably to remove this method.
        // This intentionally does not use checkState.
        if (!Thread.holdsLock(this)) {
            throw new IllegalStateException(format("Thread must hold a lock on the %s", getClass().getSimpleName()));
        }
    }

    // Splits for a particular plan node (all driver groups)
    @NotThreadSafe
    private static class PendingSplitsForPlanNode
    {
        private final Map<Lifespan, PendingSplits> splitsByLifespan = new HashMap<>();
        private boolean noMoreSplits;

        public PendingSplits getLifespan(Lifespan lifespan)
        {
            return splitsByLifespan.computeIfAbsent(lifespan, ignored -> new PendingSplits());
        }

        public void setNoMoreSplits()
        {
            if (noMoreSplits) {
                return;
            }
            noMoreSplits = true;
            for (PendingSplits splitsForLifespan : splitsByLifespan.values()) {
                splitsForLifespan.noMoreSplits();
            }
        }
    }

    // Splits for a particular plan node and driver group combination
    @NotThreadSafe
    private static class PendingSplits
    {
        private Set<ScheduledSplit> splits = new HashSet<>();
        private SplitsState state = ADDING_SPLITS;

        public SplitsState getState()
        {
            return state;
        }

        public void addSplit(ScheduledSplit scheduledSplit)
        {
            checkState(state == ADDING_SPLITS);
            splits.add(scheduledSplit);
        }

        public Set<ScheduledSplit> removeAllSplits()
        {
            checkState(state == ADDING_SPLITS || state == NO_MORE_SPLITS);
            Set<ScheduledSplit> result = splits;
            splits = new HashSet<>();
            return result;
        }

        public void noMoreSplits()
        {
            if (state == ADDING_SPLITS) {
                state = NO_MORE_SPLITS;
            }
        }

        public void markAsCleanedUp()
        {
            checkState(splits.isEmpty());
            checkState(state == NO_MORE_SPLITS);
            state = FINISHED;
        }
    }

    enum SplitsState
    {
        ADDING_SPLITS,
        // All splits have been received from scheduler.
        // No more splits will be added to the pendingSplits set.
        NO_MORE_SPLITS,
        // All splits has been turned into DriverSplitRunner.
        FINISHED,
    }

    private static class SchedulingLifespanManager
    {
        // SchedulingLifespanManager only contains partitioned drivers.
        // Note that different drivers in a task may have different pipelineExecutionStrategy.

        private final List<PlanNodeId> sourceStartOrder;
        private final StageExecutionDescriptor stageExecutionDescriptor;
        private final Status status;

        private final Map<Lifespan, SchedulingLifespan> lifespans = new HashMap<>();
        // driver groups whose scheduling is done (all splits for all plan nodes)
        private final Set<Lifespan> completedLifespans = new HashSet<>();

        private final Set<PlanNodeId> noMoreSplits = new HashSet<>();

        private int maxScheduledPlanNodeOrdinal;

        public SchedulingLifespanManager(List<PlanNodeId> sourceStartOrder, StageExecutionDescriptor stageExecutionDescriptor, Status status)
        {
            this.sourceStartOrder = ImmutableList.copyOf(sourceStartOrder);
            this.stageExecutionDescriptor = stageExecutionDescriptor;
            this.status = requireNonNull(status, "status is null");
        }

        public int getMaxScheduledPlanNodeOrdinal()
        {
            return maxScheduledPlanNodeOrdinal;
        }

        public void updateMaxScheduledPlanNodeOrdinalIfNecessary(int scheduledPlanNodeOrdinal)
        {
            if (maxScheduledPlanNodeOrdinal < scheduledPlanNodeOrdinal) {
                maxScheduledPlanNodeOrdinal = scheduledPlanNodeOrdinal;
            }
        }

        public void noMoreSplits(PlanNodeId planNodeId)
        {
            if (noMoreSplits.contains(planNodeId)) {
                return;
            }
            noMoreSplits.add(planNodeId);
            if (noMoreSplits.size() < sourceStartOrder.size()) {
                return;
            }
            checkState(noMoreSplits.size() == sourceStartOrder.size());
            checkState(noMoreSplits.containsAll(sourceStartOrder));
            status.setNoMoreLifespans();
        }

        public void addLifespanIfAbsent(Lifespan lifespan)
        {
            if (completedLifespans.contains(lifespan) || lifespans.containsKey(lifespan)) {
                return;
            }
            checkState(!status.isNoMoreLifespans());
            checkState(!sourceStartOrder.isEmpty());
            lifespans.put(lifespan, new SchedulingLifespan(lifespan, this));
        }

        public Iterator<SchedulingLifespan> getActiveLifespans()
        {
            // This function returns an iterator that iterates through active driver groups.
            // Before it advances to the next item, it checks whether the previous returned driver group is done scheduling.
            // If so, the completed SchedulingLifespan is removed so that it will not be returned again.
            Iterator<SchedulingLifespan> lifespansIterator = lifespans.values().iterator();
            return new AbstractIterator<SchedulingLifespan>()
            {
                SchedulingLifespan lastSchedulingLifespan;

                @Override
                protected SchedulingLifespan computeNext()
                {
                    if (lastSchedulingLifespan != null) {
                        if (lastSchedulingLifespan.isDone()) {
                            completedLifespans.add(lastSchedulingLifespan.getLifespan());
                            lifespansIterator.remove();
                        }
                    }
                    if (!lifespansIterator.hasNext()) {
                        return endOfData();
                    }
                    lastSchedulingLifespan = lifespansIterator.next();
                    return lastSchedulingLifespan;
                }
            };
        }
    }

    private static class SchedulingLifespan
    {
        private final Lifespan lifespan;
        private final SchedulingLifespanManager manager;
        private int schedulingPlanNodeOrdinal;
        private boolean unpartitionedDriversScheduled;

        public SchedulingLifespan(Lifespan lifespan, SchedulingLifespanManager manager)
        {
            this.lifespan = requireNonNull(lifespan, "lifespan is null");
            this.manager = requireNonNull(manager, "manager is null");
        }

        public Lifespan getLifespan()
        {
            return lifespan;
        }

        public Optional<PlanNodeId> getSchedulingPlanNode()
        {
            checkState(!isDone());
            while (!isDone()) {
                // Return current plan node if this lifespan is compatible with the plan node.
                // i.e. One of the following bullet points is true:
                // * The execution strategy of the plan node is grouped. And lifespan represents a driver group.
                // * The execution strategy of the plan node is ungrouped. And lifespan is task wide.
                if (manager.stageExecutionDescriptor.isScanGroupedExecution(manager.sourceStartOrder.get(schedulingPlanNodeOrdinal)) != lifespan.isTaskWide()) {
                    return Optional.of(manager.sourceStartOrder.get(schedulingPlanNodeOrdinal));
                }
                // This lifespan is incompatible with the plan node. As a result, this method should either
                // return empty to indicate that scheduling for this lifespan is blocked, or skip the current
                // plan node and went on to the next one. Which one of the two happens is dependent on whether
                // the current plan node has finished scheduling in any other lifespan.
                // If so, the lifespan can advance to the next plan node.
                // If not, it should not advance because doing so would violate scheduling order.
                if (manager.getMaxScheduledPlanNodeOrdinal() == schedulingPlanNodeOrdinal) {
                    return Optional.empty();
                }
                verify(manager.getMaxScheduledPlanNodeOrdinal() > schedulingPlanNodeOrdinal);
                nextPlanNode();
            }
            return Optional.empty();
        }

        public void nextPlanNode()
        {
            checkState(!isDone());
            schedulingPlanNodeOrdinal++;
            manager.updateMaxScheduledPlanNodeOrdinalIfNecessary(schedulingPlanNodeOrdinal);
        }

        public boolean isDone()
        {
            return schedulingPlanNodeOrdinal >= manager.sourceStartOrder.size();
        }

        public boolean getAndSetDriversForDriverGroupLifeCycleScheduled()
        {
            if (unpartitionedDriversScheduled) {
                return true;
            }
            unpartitionedDriversScheduled = true;
            return false;
        }
    }

    private class DriverSplitRunnerFactory
    {
        private final DriverFactory driverFactory;
        private final PipelineContext pipelineContext;
        private boolean closed;

        private DriverSplitRunnerFactory(DriverFactory driverFactory, boolean partitioned)
        {
            this.driverFactory = driverFactory;
            this.pipelineContext = taskContext.addPipelineContext(driverFactory.getPipelineId(), driverFactory.isInputDriver(), driverFactory.isOutputDriver(), partitioned);
        }

        // TODO: split this method into two: createPartitionedDriverRunner and createUnpartitionedDriverRunner.
        // The former will take two arguments, and the latter will take one. This will simplify the signature quite a bit.
        public DriverSplitRunner createDriverRunner(@Nullable ScheduledSplit partitionedSplit, Lifespan lifespan)
        {
            checkLifespan(driverFactory.getPipelineExecutionStrategy(), lifespan);
            status.incrementPendingCreation(pipelineContext.getPipelineId(), lifespan);
            // create driver context immediately so the driver existence is recorded in the stats
            // the number of drivers is used to balance work across nodes
            long splitWeight = partitionedSplit == null ? 0 : partitionedSplit.getSplit().getSplitWeight().getRawValue();
            DriverContext driverContext = pipelineContext.addDriverContext(splitWeight, lifespan, driverFactory.getFragmentResultCacheContext());
            return new DriverSplitRunner(this, driverContext, partitionedSplit, lifespan);
        }

        public Driver createDriver(DriverContext driverContext, @Nullable ScheduledSplit partitionedSplit)
        {
            Driver driver = driverFactory.createDriver(driverContext);

            // record driver so other threads add remote sources can see the driver
            // NOTE: this MUST be done before reading remoteSources, so we see a consistent view of the remote sources
            drivers.add(new WeakReference<>(driver));

            if (partitionedSplit != null) {
                // TableScanOperator requires partitioned split to be added before the first call to process
                driver.updateSource(new TaskSource(partitionedSplit.getPlanNodeId(), ImmutableSet.of(partitionedSplit), true));
            }

            // add remote sources
            Optional<PlanNodeId> sourceId = driver.getSourceId();
            if (sourceId.isPresent()) {
                TaskSource taskSource = remoteSources.get(sourceId.get());
                if (taskSource != null) {
                    driver.updateSource(taskSource);
                }
            }

            status.decrementPendingCreation(pipelineContext.getPipelineId(), driverContext.getLifespan());
            closeDriverFactoryIfFullyCreated();

            return driver;
        }

        public void noMoreDriverRunner(Iterable<Lifespan> lifespans)
        {
            for (Lifespan lifespan : lifespans) {
                status.setNoMoreDriverRunner(pipelineContext.getPipelineId(), lifespan);
            }
            closeDriverFactoryIfFullyCreated();
        }

        public boolean isNoMoreDriverRunner()
        {
            return status.isNoMoreDriverRunners(pipelineContext.getPipelineId());
        }

        public void closeDriverFactoryIfFullyCreated()
        {
            if (closed) {
                return;
            }
            for (Lifespan lifespan : status.getAndAcknowledgeLifespansWithNoMoreDrivers(pipelineContext.getPipelineId())) {
                driverFactory.noMoreDrivers(lifespan);
            }
            if (!isNoMoreDriverRunner() || status.getPendingCreation(pipelineContext.getPipelineId()) != 0) {
                return;
            }
            driverFactory.noMoreDrivers();
            closed = true;
        }

        public PipelineExecutionStrategy getPipelineExecutionStrategy()
        {
            return driverFactory.getPipelineExecutionStrategy();
        }

        public OptionalInt getDriverInstances()
        {
            return driverFactory.getDriverInstances();
        }

        public void splitsAdded(int count, long weightSum)
        {
            pipelineContext.splitsAdded(count, weightSum);
        }
    }

    private static class DriverSplitRunner
            implements SplitRunner
    {
        private final DriverSplitRunnerFactory driverSplitRunnerFactory;
        private final DriverContext driverContext;
        private final Lifespan lifespan;

        @GuardedBy("this")
        private boolean closed;

        @Nullable
        private final ScheduledSplit partitionedSplit;

        @GuardedBy("this")
        private Driver driver;

        private DriverSplitRunner(DriverSplitRunnerFactory driverSplitRunnerFactory, DriverContext driverContext, @Nullable ScheduledSplit partitionedSplit, Lifespan lifespan)
        {
            this.driverSplitRunnerFactory = requireNonNull(driverSplitRunnerFactory, "driverFactory is null");
            this.driverContext = requireNonNull(driverContext, "driverContext is null");
            this.partitionedSplit = partitionedSplit;
            this.lifespan = requireNonNull(lifespan, "lifespan is null");
        }

        public synchronized DriverContext getDriverContext()
        {
            if (driver == null) {
                return null;
            }
            return driver.getDriverContext();
        }

        public Lifespan getLifespan()
        {
            return lifespan;
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
            if (partitionedSplit != null && partitionedSplit.getSplit() != null && partitionedSplit.getSplit().getInfo() != null) {
                return partitionedSplit.getSplit().getInfo().toString();
            }
            return "";
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
        public void stateChanged(BufferState newState)
        {
            if (newState == BufferState.FINISHED) {
                SqlTaskExecution sqlTaskExecution = sqlTaskExecutionReference.get();
                if (sqlTaskExecution != null) {
                    sqlTaskExecution.checkTaskCompletion();
                }
            }
        }
    }

    @ThreadSafe
    private static class Status
    {
        // no more driver runner: true if no more DriverSplitRunners will be created.
        // pending creation: number of created DriverSplitRunners that haven't created underlying Driver.
        // remaining driver: number of created Drivers that haven't yet finished.

        private final TaskContext taskContext;
        private final OutputBuffer outputBuffer;

        @GuardedBy("this")
        private final int pipelineWithTaskLifeCycleCount;
        @GuardedBy("this")
        private final int pipelineWithDriverGroupLifeCycleCount;

        // For these 3 perX fields, they are populated lazily. If enumeration operations on the
        // map can lead to side effects, no new entries can be created after such enumeration has
        // happened. Otherwise, the order of entry creation and the enumeration operation will
        // lead to different outcome.
        @GuardedBy("this")
        private final Map<Integer, Map<Lifespan, PerPipelineAndLifespanStatus>> perPipelineAndLifespan;
        @GuardedBy("this")
        private final Map<Integer, PerPipelineStatus> perPipeline;
        @GuardedBy("this")
        private final Map<Lifespan, PerLifespanStatus> perLifespan = new HashMap<>();

        @GuardedBy("this")
        private int overallRemainingDriver;

        @GuardedBy("this")
        private boolean noMoreLifespans;

        public Status(TaskContext taskContext, OutputBuffer outputBuffer, Map<Integer, PipelineExecutionStrategy> pipelineToExecutionStrategy)
        {
            this.taskContext = requireNonNull(taskContext, "taskContext is null");
            this.outputBuffer = requireNonNull(outputBuffer, "outputBuffer is null");

            int pipelineWithTaskLifeCycleCount = 0;
            int pipelineWithDriverGroupLifeCycleCount = 0;
            ImmutableMap.Builder<Integer, Map<Lifespan, PerPipelineAndLifespanStatus>> perPipelineAndLifespan = ImmutableMap.builder();
            ImmutableMap.Builder<Integer, PerPipelineStatus> perPipeline = ImmutableMap.builder();
            for (Entry<Integer, PipelineExecutionStrategy> entry : pipelineToExecutionStrategy.entrySet()) {
                int pipelineId = entry.getKey();
                PipelineExecutionStrategy executionStrategy = entry.getValue();
                perPipelineAndLifespan.put(pipelineId, new HashMap<>());
                perPipeline.put(pipelineId, new PerPipelineStatus(executionStrategy));
                switch (executionStrategy) {
                    case UNGROUPED_EXECUTION:
                        pipelineWithTaskLifeCycleCount++;
                        break;
                    case GROUPED_EXECUTION:
                        pipelineWithDriverGroupLifeCycleCount++;
                        break;
                    default:
                        throw new IllegalArgumentException(format("Unknown ExecutionStrategy (%s) for pipeline %s.", executionStrategy, pipelineId));
                }
            }
            this.pipelineWithTaskLifeCycleCount = pipelineWithTaskLifeCycleCount;
            this.pipelineWithDriverGroupLifeCycleCount = pipelineWithDriverGroupLifeCycleCount;
            this.perPipelineAndLifespan = perPipelineAndLifespan.build();
            this.perPipeline = perPipeline.build();
        }

        public synchronized void setNoMoreLifespans()
        {
            if (noMoreLifespans) {
                return;
            }
            noMoreLifespans = true;
        }

        public synchronized void setNoMoreDriverRunner(int pipelineId, Lifespan lifespan)
        {
            if (per(pipelineId, lifespan).noMoreDriverRunner) {
                return;
            }
            per(pipelineId, lifespan).noMoreDriverRunner = true;
            if (per(pipelineId, lifespan).pendingCreation == 0) {
                per(pipelineId).unacknowledgedLifespansWithNoMoreDrivers.add(lifespan);
            }
            per(pipelineId).lifespansWithNoMoreDriverRunners++;
            per(lifespan).pipelinesWithNoMoreDriverRunners++;
            checkLifespanCompletion(lifespan);
        }

        public synchronized void incrementPendingCreation(int pipelineId, Lifespan lifespan)
        {
            checkState(!per(pipelineId, lifespan).noMoreDriverRunner, "Cannot increment pendingCreation for Pipeline %s Lifespan %s. NoMoreSplits is set.", pipelineId, lifespan);
            per(pipelineId, lifespan).pendingCreation++;
            per(pipelineId).pendingCreation++;
        }

        public synchronized void decrementPendingCreation(int pipelineId, Lifespan lifespan)
        {
            checkState(per(pipelineId, lifespan).pendingCreation > 0, "Cannot decrement pendingCreation for Pipeline %s Lifespan %s. Value is 0.", pipelineId, lifespan);
            per(pipelineId, lifespan).pendingCreation--;
            if (per(pipelineId, lifespan).pendingCreation == 0 && per(pipelineId, lifespan).noMoreDriverRunner) {
                per(pipelineId).unacknowledgedLifespansWithNoMoreDrivers.add(lifespan);
            }
            per(pipelineId).pendingCreation--;
        }

        public synchronized void incrementRemainingDriver(Lifespan lifespan)
        {
            checkState(!isNoMoreDriverRunners(lifespan), "Cannot increment remainingDriver for Lifespan %s. NoMoreSplits is set.", lifespan);
            per(lifespan).remainingDriver++;
            overallRemainingDriver++;
        }

        public synchronized void decrementRemainingDriver(Lifespan lifespan)
        {
            checkState(per(lifespan).remainingDriver > 0, "Cannot decrement remainingDriver for Lifespan %s. Value is 0.", lifespan);
            per(lifespan).remainingDriver--;
            overallRemainingDriver--;
            checkLifespanCompletion(lifespan);
        }

        public synchronized boolean isNoMoreLifespans()
        {
            return noMoreLifespans;
        }

        public synchronized int getPendingCreation(int pipelineId)
        {
            return per(pipelineId).pendingCreation;
        }

        public synchronized int getRemainingDriver(Lifespan lifespan)
        {
            return per(lifespan).remainingDriver;
        }

        public synchronized int getRemainingDriver()
        {
            return overallRemainingDriver;
        }

        public synchronized boolean isNoMoreDriverRunners(int pipelineId)
        {
            int driverGroupCount;
            switch (per(pipelineId).executionStrategy) {
                case UNGROUPED_EXECUTION:
                    // Even if noMoreLifespans is not set, UNGROUPED_EXECUTION pipelines can only have 1 driver group by nature.
                    driverGroupCount = 1;
                    break;
                case GROUPED_EXECUTION:
                    if (!noMoreLifespans) {
                        // There may still be new driver groups, which means potentially new splits.
                        return false;
                    }

                    // We are trying to figure out the number of driver life cycles that has this pipeline here.
                    // Since the pipeline has grouped execution strategy, all Lifespans except for the task-wide one
                    // should have this pipeline.
                    // Therefore, we get the total number of Lifespans in the task, and deduct 1 if the task-wide one exists.
                    driverGroupCount = perLifespan.size();
                    if (perLifespan.containsKey(Lifespan.taskWide())) {
                        driverGroupCount--;
                    }
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
            return per(pipelineId).lifespansWithNoMoreDriverRunners == driverGroupCount;
        }

        public synchronized boolean isNoMoreDriverRunners(Lifespan lifespan)
        {
            if (!lifespan.isTaskWide()) {
                return per(lifespan).pipelinesWithNoMoreDriverRunners == pipelineWithDriverGroupLifeCycleCount;
            }
            else {
                return per(lifespan).pipelinesWithNoMoreDriverRunners == pipelineWithTaskLifeCycleCount;
            }
        }

        /**
         * Return driver groups who recently became known to not need any new drivers.
         * Once it is determined that a driver group will not need any new driver groups,
         * the driver group will be returned in the next invocation of this method.
         * Once a driver group is returned, it is considered acknowledged, and will not be returned again.
         * In other words, each driver group will be returned by this method only once.
         */
        public synchronized List<Lifespan> getAndAcknowledgeLifespansWithNoMoreDrivers(int pipelineId)
        {
            List<Lifespan> result = ImmutableList.copyOf(per(pipelineId).unacknowledgedLifespansWithNoMoreDrivers);
            per(pipelineId).unacknowledgedLifespansWithNoMoreDrivers.clear();
            return result;
        }

        private void checkLifespanCompletion(Lifespan lifespan)
        {
            if (lifespan.isTaskWide()) {
                return; // not a driver group
            }

            // are there more partition splits expected?
            if (!isNoMoreDriverRunners(lifespan)) {
                return;
            }

            // do we still have running tasks?
            if (getRemainingDriver(lifespan) != 0) {
                return;
            }

            // no more output will be created
            outputBuffer.setNoMorePagesForLifespan(lifespan);

            if (!taskContext.isLegacyLifespanCompletionCondition()) {
                // are there still pages in the output buffer?
                if (!outputBuffer.isFinishedForLifespan(lifespan)) {
                    return;
                }
            }

            // Cool! All done!
            taskContext.addCompletedDriverGroup(lifespan);
        }

        @GuardedBy("this")
        private PerPipelineAndLifespanStatus per(int pipelineId, Lifespan lifespan)
        {
            return perPipelineAndLifespan.get(pipelineId).computeIfAbsent(lifespan, ignored -> new PerPipelineAndLifespanStatus());
        }

        @GuardedBy("this")
        private PerPipelineStatus per(int pipelineId)
        {
            return perPipeline.get(pipelineId);
        }

        @GuardedBy("this")
        private PerLifespanStatus per(Lifespan lifespan)
        {
            if (perLifespan.containsKey(lifespan)) {
                return perLifespan.get(lifespan);
            }
            PerLifespanStatus result = new PerLifespanStatus();
            perLifespan.put(lifespan, result);
            return result;
        }
    }

    private static class PerPipelineStatus
    {
        final PipelineExecutionStrategy executionStrategy;

        int pendingCreation;
        int lifespansWithNoMoreDriverRunners;
        final List<Lifespan> unacknowledgedLifespansWithNoMoreDrivers = new ArrayList<>();

        public PerPipelineStatus(PipelineExecutionStrategy executionStrategy)
        {
            this.executionStrategy = requireNonNull(executionStrategy, "executionStrategy is null");
        }
    }

    private static class PerLifespanStatus
    {
        int remainingDriver;
        int pipelinesWithNoMoreDriverRunners;
    }

    private static class PerPipelineAndLifespanStatus
    {
        int pendingCreation;
        boolean noMoreDriverRunner;
    }
}
