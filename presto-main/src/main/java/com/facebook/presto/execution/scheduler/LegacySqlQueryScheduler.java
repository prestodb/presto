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
package com.facebook.presto.execution.scheduler;

import com.facebook.airlift.concurrent.SetThreadName;
import com.facebook.airlift.log.Logger;
import com.facebook.airlift.stats.TimeStat;
import com.facebook.presto.Session;
import com.facebook.presto.execution.BasicStageExecutionStats;
import com.facebook.presto.execution.LocationFactory;
import com.facebook.presto.execution.PartialResultQueryManager;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.QueryStateMachine;
import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.execution.RemoteTaskFactory;
import com.facebook.presto.execution.SqlStageExecution;
import com.facebook.presto.execution.StageExecutionInfo;
import com.facebook.presto.execution.StageExecutionState;
import com.facebook.presto.execution.StageId;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.execution.buffer.OutputBuffers.OutputBufferId;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.SplitSourceFactory;
import com.facebook.presto.sql.planner.SubPlan;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.sanity.PlanChecker;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.facebook.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static com.facebook.airlift.concurrent.MoreFutures.whenAnyComplete;
import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static com.facebook.presto.SystemSessionProperties.getMaxConcurrentMaterializations;
import static com.facebook.presto.SystemSessionProperties.getPartialResultsCompletionRatioThreshold;
import static com.facebook.presto.SystemSessionProperties.getPartialResultsMaxExecutionTimeMultiplier;
import static com.facebook.presto.SystemSessionProperties.isPartialResultsEnabled;
import static com.facebook.presto.SystemSessionProperties.isRuntimeOptimizerEnabled;
import static com.facebook.presto.execution.BasicStageExecutionStats.aggregateBasicStageStats;
import static com.facebook.presto.execution.StageExecutionState.ABORTED;
import static com.facebook.presto.execution.StageExecutionState.CANCELED;
import static com.facebook.presto.execution.StageExecutionState.FAILED;
import static com.facebook.presto.execution.StageExecutionState.FINISHED;
import static com.facebook.presto.execution.StageExecutionState.PLANNED;
import static com.facebook.presto.execution.StageExecutionState.RUNNING;
import static com.facebook.presto.execution.StageExecutionState.SCHEDULED;
import static com.facebook.presto.execution.buffer.OutputBuffers.BROADCAST_PARTITION_ID;
import static com.facebook.presto.execution.buffer.OutputBuffers.createDiscardingOutputBuffers;
import static com.facebook.presto.execution.buffer.OutputBuffers.createInitialEmptyOutputBuffers;
import static com.facebook.presto.execution.scheduler.StreamingPlanSection.extractStreamingSections;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.sql.planner.PlanFragmenter.ROOT_FRAGMENT_ID;
import static com.facebook.presto.sql.planner.SchedulingOrderVisitor.scheduleOrder;
import static com.facebook.presto.sql.planner.planPrinter.PlanPrinter.jsonFragmentPlan;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Streams.stream;
import static com.google.common.graph.Traverser.forTree;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.function.Function.identity;

@Deprecated
public class LegacySqlQueryScheduler
        implements SqlQuerySchedulerInterface
{
    private static final Logger log = Logger.get(LegacySqlQueryScheduler.class);

    private final LocationFactory locationFactory;
    private final ExecutionPolicy executionPolicy;

    private final SplitSchedulerStats schedulerStats;

    private final QueryStateMachine queryStateMachine;
    private final AtomicReference<SubPlan> plan = new AtomicReference<>();
    private final StreamingPlanSection sectionedPlan;
    private final StageId rootStageId;
    private final boolean summarizeTaskInfo;
    private final int maxConcurrentMaterializations;

    // The following fields are required by adaptive optimization in runtime.
    private final Session session;
    private final FunctionAndTypeManager functionAndTypeManager;
    private final List<PlanOptimizer> runtimePlanOptimizers;
    private final WarningCollector warningCollector;
    private final PlanNodeIdAllocator idAllocator;
    private final PlanVariableAllocator variableAllocator;
    private final SectionExecutionFactory sectionExecutionFactory;
    private final RemoteTaskFactory remoteTaskFactory;
    private final SplitSourceFactory splitSourceFactory;
    private final Set<StageId> runtimeOptimizedStages = Collections.synchronizedSet(new HashSet<>());
    private final PlanChecker planChecker;
    private final Metadata metadata;
    private final SqlParser sqlParser;

    private final Map<StageId, StageExecutionAndScheduler> stageExecutions = new ConcurrentHashMap<>();
    private final ExecutorService executor;
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean scheduling = new AtomicBoolean();

    private final PartialResultQueryTaskTracker partialResultQueryTaskTracker;

    public static LegacySqlQueryScheduler createSqlQueryScheduler(
            LocationFactory locationFactory,
            ExecutionPolicy executionPolicy,
            ExecutorService queryExecutor,
            SplitSchedulerStats schedulerStats,
            SectionExecutionFactory sectionExecutionFactory,
            RemoteTaskFactory remoteTaskFactory,
            SplitSourceFactory splitSourceFactory,
            Session session,
            FunctionAndTypeManager functionAndTypeManager,
            QueryStateMachine queryStateMachine,
            SubPlan plan,
            OutputBuffers rootOutputBuffers,
            boolean summarizeTaskInfo,
            List<PlanOptimizer> runtimePlanOptimizers,
            WarningCollector warningCollector,
            PlanNodeIdAllocator idAllocator,
            PlanVariableAllocator variableAllocator,
            PlanChecker planChecker,
            Metadata metadata,
            SqlParser sqlParser,
            PartialResultQueryManager partialResultQueryManager)
    {
        LegacySqlQueryScheduler sqlQueryScheduler = new LegacySqlQueryScheduler(
                locationFactory,
                executionPolicy,
                queryExecutor,
                schedulerStats,
                sectionExecutionFactory,
                remoteTaskFactory,
                splitSourceFactory,
                session,
                functionAndTypeManager,
                queryStateMachine,
                plan,
                summarizeTaskInfo,
                rootOutputBuffers,
                runtimePlanOptimizers,
                warningCollector,
                idAllocator,
                variableAllocator,
                planChecker,
                metadata,
                sqlParser,
                partialResultQueryManager);
        sqlQueryScheduler.initialize();
        return sqlQueryScheduler;
    }

    private LegacySqlQueryScheduler(
            LocationFactory locationFactory,
            ExecutionPolicy executionPolicy,
            ExecutorService queryExecutor,
            SplitSchedulerStats schedulerStats,
            SectionExecutionFactory sectionExecutionFactory,
            RemoteTaskFactory remoteTaskFactory,
            SplitSourceFactory splitSourceFactory,
            Session session,
            FunctionAndTypeManager functionAndTypeManager,
            QueryStateMachine queryStateMachine,
            SubPlan plan,
            boolean summarizeTaskInfo,
            OutputBuffers rootOutputBuffers,
            List<PlanOptimizer> runtimePlanOptimizers,
            WarningCollector warningCollector,
            PlanNodeIdAllocator idAllocator,
            PlanVariableAllocator variableAllocator,
            PlanChecker planChecker,
            Metadata metadata,
            SqlParser sqlParser,
            PartialResultQueryManager partialResultQueryManager)
    {
        this.locationFactory = requireNonNull(locationFactory, "locationFactory is null");
        this.executionPolicy = requireNonNull(executionPolicy, "schedulerPolicyFactory is null");
        this.executor = queryExecutor;
        this.schedulerStats = requireNonNull(schedulerStats, "schedulerStats is null");
        this.queryStateMachine = requireNonNull(queryStateMachine, "queryStateMachine is null");
        this.plan.compareAndSet(null, requireNonNull(plan, "plan is null"));
        this.session = requireNonNull(session, "session is null");
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionManager is null");
        this.runtimePlanOptimizers = requireNonNull(runtimePlanOptimizers, "runtimePlanOptimizers is null");
        this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
        this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
        this.variableAllocator = requireNonNull(variableAllocator, "variableAllocator is null");
        this.planChecker = requireNonNull(planChecker, "planChecker is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.sectionExecutionFactory = requireNonNull(sectionExecutionFactory, "sectionExecutionFactory is null");
        this.remoteTaskFactory = requireNonNull(remoteTaskFactory, "remoteTaskFactory is null");
        this.splitSourceFactory = requireNonNull(splitSourceFactory, "splitSourceFactory is null");
        this.sectionedPlan = extractStreamingSections(plan);
        this.summarizeTaskInfo = summarizeTaskInfo;

        OutputBufferId rootBufferId = getOnlyElement(rootOutputBuffers.getBuffers().keySet());
        List<StageExecutionAndScheduler> stageExecutions = createStageExecutions(
                sectionExecutionFactory,
                (fragmentId, tasks, noMoreExchangeLocations) -> updateQueryOutputLocations(queryStateMachine, rootBufferId, tasks, noMoreExchangeLocations),
                sectionedPlan,
                Optional.of(new int[1]),
                rootOutputBuffers,
                remoteTaskFactory,
                splitSourceFactory,
                session);

        this.rootStageId = Iterables.getLast(stageExecutions).getStageExecution().getStageExecutionId().getStageId();

        stageExecutions.stream()
                .forEach(execution -> this.stageExecutions.put(execution.getStageExecution().getStageExecutionId().getStageId(), execution));

        this.maxConcurrentMaterializations = getMaxConcurrentMaterializations(session);
        this.partialResultQueryTaskTracker = new PartialResultQueryTaskTracker(partialResultQueryManager, getPartialResultsCompletionRatioThreshold(session), getPartialResultsMaxExecutionTimeMultiplier(session), warningCollector);
    }

    // this is a separate method to ensure that the `this` reference is not leaked during construction
    private void initialize()
    {
        SqlStageExecution rootStage = stageExecutions.get(rootStageId).getStageExecution();
        rootStage.addStateChangeListener(state -> {
            if (state == FINISHED) {
                queryStateMachine.transitionToFinishing();
            }
            else if (state == CANCELED) {
                // output stage was canceled
                queryStateMachine.transitionToCanceled();
            }
        });

        for (StageExecutionAndScheduler stageExecutionInfo : stageExecutions.values()) {
            SqlStageExecution stageExecution = stageExecutionInfo.getStageExecution();
            stageExecution.addStateChangeListener(state -> {
                if (queryStateMachine.isDone()) {
                    return;
                }
                if (state == FAILED) {
                    queryStateMachine.transitionToFailed(stageExecution.getStageExecutionInfo().getFailureCause().get().toException());
                }
                else if (state == ABORTED) {
                    // this should never happen, since abort can only be triggered in query clean up after the query is finished
                    queryStateMachine.transitionToFailed(new PrestoException(GENERIC_INTERNAL_ERROR, "Query stage was aborted"));
                }
                else if (state == FINISHED) {
                    // checks if there's any new sections available for execution and starts the scheduling if any
                    startScheduling();
                }
                else if (queryStateMachine.getQueryState() == QueryState.STARTING) {
                    // if the stage has at least one task, we are running
                    if (stageExecution.hasTasks()) {
                        queryStateMachine.transitionToRunning();
                    }
                }
            });
            stageExecution.addFinalStageInfoListener(status -> queryStateMachine.updateQueryInfo(Optional.of(getStageInfo())));
        }

        // when query is done or any time a stage completes, attempt to transition query to "final query info ready"
        queryStateMachine.addStateChangeListener(newState -> {
            if (newState.isDone()) {
                queryStateMachine.updateQueryInfo(Optional.of(getStageInfo()));
            }
        });
    }

    private static void updateQueryOutputLocations(QueryStateMachine queryStateMachine, OutputBufferId rootBufferId, Set<RemoteTask> tasks, boolean noMoreExchangeLocations)
    {
        Map<URI, TaskId> bufferLocations = tasks.stream()
                .collect(toImmutableMap(
                        task -> getBufferLocation(task, rootBufferId),
                        RemoteTask::getTaskId));
        queryStateMachine.updateOutputLocations(bufferLocations, noMoreExchangeLocations);
    }

    private static URI getBufferLocation(RemoteTask remoteTask, OutputBufferId rootBufferId)
    {
        URI location = remoteTask.getTaskStatus().getSelf();
        return uriBuilderFrom(location).appendPath("results").appendPath(rootBufferId.toString()).build();
    }

    /**
     * returns a List of SqlStageExecutionInfos in a postorder representation of the tree
     */
    private List<StageExecutionAndScheduler> createStageExecutions(
            SectionExecutionFactory sectionExecutionFactory,
            ExchangeLocationsConsumer locationsConsumer,
            StreamingPlanSection section,
            Optional<int[]> bucketToPartition,
            OutputBuffers outputBuffers,
            RemoteTaskFactory remoteTaskFactory,
            SplitSourceFactory splitSourceFactory,
            Session session)
    {
        ImmutableList.Builder<StageExecutionAndScheduler> stages = ImmutableList.builder();

        for (StreamingPlanSection childSection : section.getChildren()) {
            ExchangeLocationsConsumer childLocationsConsumer = (fragmentId, tasks, noMoreExhchangeLocations) -> {};
            stages.addAll(createStageExecutions(
                    sectionExecutionFactory,
                    childLocationsConsumer,
                    childSection,
                    Optional.empty(),
                    createDiscardingOutputBuffers(),
                    remoteTaskFactory,
                    splitSourceFactory,
                    session));
        }
        List<StageExecutionAndScheduler> sectionStages =
                sectionExecutionFactory.createSectionExecutions(
                        session,
                        section,
                        locationsConsumer,
                        bucketToPartition,
                        outputBuffers,
                        summarizeTaskInfo,
                        remoteTaskFactory,
                        splitSourceFactory,
                        0).getSectionStages();
        stages.addAll(sectionStages);

        return stages.build();
    }

    public void start()
    {
        if (started.compareAndSet(false, true)) {
            startScheduling();
        }
    }

    private void startScheduling()
    {
        requireNonNull(stageExecutions);
        // still scheduling the previous batch of stages
        if (scheduling.get()) {
            return;
        }
        executor.submit(this::schedule);
    }

    private void schedule()
    {
        if (!scheduling.compareAndSet(false, true)) {
            // still scheduling the previous batch of stages
            return;
        }

        List<StageExecutionAndScheduler> scheduledStageExecutions = new ArrayList<>();

        try (SetThreadName ignored = new SetThreadName("Query-%s", queryStateMachine.getQueryId())) {
            Set<StageId> completedStages = new HashSet<>();

            List<ExecutionSchedule> sectionExecutionSchedules = new LinkedList<>();

            while (!Thread.currentThread().isInterrupted()) {
                // remove finished section
                sectionExecutionSchedules.removeIf(ExecutionSchedule::isFinished);

                // try to pull more section that are ready to be run
                List<StreamingPlanSection> sectionsReadyForExecution = getSectionsReadyForExecution();

                // all finished
                if (sectionsReadyForExecution.isEmpty() && sectionExecutionSchedules.isEmpty()) {
                    break;
                }

                List<List<StageExecutionAndScheduler>> sectionStageExecutions = getStageExecutions(sectionsReadyForExecution);
                sectionStageExecutions.forEach(scheduledStageExecutions::addAll);
                sectionStageExecutions.stream()
                        .map(executionInfos -> executionInfos.stream()
                                .collect(toImmutableList()))
                        .map(executionPolicy::createExecutionSchedule)
                        .forEach(sectionExecutionSchedules::add);

                while (sectionExecutionSchedules.stream().noneMatch(ExecutionSchedule::isFinished)) {
                    List<ListenableFuture<?>> blockedStages = new ArrayList<>();

                    List<StageExecutionAndScheduler> executionsToSchedule = sectionExecutionSchedules.stream()
                            .flatMap(schedule -> schedule.getStagesToSchedule().stream())
                            .collect(toImmutableList());

                    for (StageExecutionAndScheduler stageExecutionAndScheduler : executionsToSchedule) {
                        SqlStageExecution stageExecution = stageExecutionAndScheduler.getStageExecution();
                        StageId stageId = stageExecution.getStageExecutionId().getStageId();
                        stageExecution.beginScheduling();

                        // perform some scheduling work
                        ScheduleResult result = stageExecutionAndScheduler.getStageScheduler()
                                .schedule();

                        // Track leaf tasks if partial results are enabled
                        if (isPartialResultsEnabled(session) && stageExecutionAndScheduler.getStageExecution().getFragment().isLeaf()) {
                            for (RemoteTask task : result.getNewTasks()) {
                                partialResultQueryTaskTracker.trackTask(task);
                                task.addFinalTaskInfoListener(partialResultQueryTaskTracker::recordTaskFinish);
                            }
                        }

                        // modify parent and children based on the results of the scheduling
                        if (result.isFinished()) {
                            stageExecution.schedulingComplete();
                        }
                        else if (!result.getBlocked().isDone()) {
                            blockedStages.add(result.getBlocked());
                        }
                        stageExecutionAndScheduler.getStageLinkage()
                                .processScheduleResults(stageExecution.getState(), result.getNewTasks());
                        schedulerStats.getSplitsScheduledPerIteration().add(result.getSplitsScheduled());
                        if (result.getBlockedReason().isPresent()) {
                            switch (result.getBlockedReason().get()) {
                                case WRITER_SCALING:
                                    // no-op
                                    break;
                                case WAITING_FOR_SOURCE:
                                    schedulerStats.getWaitingForSource().update(1);
                                    break;
                                case SPLIT_QUEUES_FULL:
                                    schedulerStats.getSplitQueuesFull().update(1);
                                    break;
                                case MIXED_SPLIT_QUEUES_FULL_AND_WAITING_FOR_SOURCE:
                                    schedulerStats.getMixedSplitQueuesFullAndWaitingForSource().update(1);
                                    break;
                                case NO_ACTIVE_DRIVER_GROUP:
                                    schedulerStats.getNoActiveDriverGroup().update(1);
                                    break;
                                default:
                                    throw new UnsupportedOperationException("Unknown blocked reason: " + result.getBlockedReason().get());
                            }
                        }
                    }

                    // make sure to update stage linkage at least once per loop to catch async state changes (e.g., partial cancel)
                    boolean stageFinishedExecution = false;
                    for (StageExecutionAndScheduler stageExecutionInfo : scheduledStageExecutions) {
                        SqlStageExecution stageExecution = stageExecutionInfo.getStageExecution();
                        StageId stageId = stageExecution.getStageExecutionId().getStageId();
                        if (!completedStages.contains(stageId) && stageExecution.getState().isDone()) {
                            stageExecutionInfo.getStageLinkage()
                                    .processScheduleResults(stageExecution.getState(), ImmutableSet.of());
                            completedStages.add(stageId);
                            stageFinishedExecution = true;
                        }
                    }

                    // if any stage has just finished execution try to pull more sections for scheduling
                    if (stageFinishedExecution) {
                        break;
                    }

                    // wait for a state change and then schedule again
                    if (!blockedStages.isEmpty()) {
                        try (TimeStat.BlockTimer timer = schedulerStats.getSleepTime().time()) {
                            tryGetFutureValue(whenAnyComplete(blockedStages), 1, SECONDS);
                        }
                        for (ListenableFuture<?> blockedStage : blockedStages) {
                            blockedStage.cancel(true);
                        }
                    }
                }
            }

            for (StageExecutionAndScheduler stageExecutionInfo : scheduledStageExecutions) {
                StageExecutionState state = stageExecutionInfo.getStageExecution().getState();
                if (state != SCHEDULED && state != RUNNING && !state.isDone()) {
                    throw new PrestoException(GENERIC_INTERNAL_ERROR, format("Scheduling is complete, but stage execution %s is in state %s", stageExecutionInfo.getStageExecution().getStageExecutionId(), state));
                }
            }

            scheduling.set(false);

            // Inform the tracker that task scheduling has completed
            partialResultQueryTaskTracker.completeTaskScheduling();

            if (!getSectionsReadyForExecution().isEmpty()) {
                startScheduling();
            }
        }
        catch (Throwable t) {
            scheduling.set(false);
            queryStateMachine.transitionToFailed(t);
            throw t;
        }
        finally {
            RuntimeException closeError = new RuntimeException();
            for (StageExecutionAndScheduler stageExecutionInfo : scheduledStageExecutions) {
                try {
                    stageExecutionInfo.getStageScheduler().close();
                }
                catch (Throwable t) {
                    queryStateMachine.transitionToFailed(t);
                    // Self-suppression not permitted
                    if (closeError != t) {
                        closeError.addSuppressed(t);
                    }
                }
            }
            if (closeError.getSuppressed().length > 0) {
                throw closeError;
            }
        }
    }

    private List<StreamingPlanSection> getSectionsReadyForExecution()
    {
        long runningPlanSections =
                stream(forTree(StreamingPlanSection::getChildren).depthFirstPreOrder(sectionedPlan))
                        .map(section -> getStageExecution(section.getPlan().getFragment().getId()).getState())
                        .filter(state -> !state.isDone() && state != PLANNED)
                        .count();
        return stream(forTree(StreamingPlanSection::getChildren).depthFirstPreOrder(sectionedPlan))
                // get all sections ready for execution
                .filter(this::isReadyForExecution)
                .limit(maxConcurrentMaterializations - runningPlanSections)
                .map(this::tryCostBasedOptimize)
                .collect(toImmutableList());
    }

    /**
     * A general purpose utility function to invoke runtime cost-based optimizer.
     * (right now there is only one plan optimizer which determines if the probe and build side of a JoinNode should be swapped
     * based on the statistics of the temporary table holding materialized exchange outputs from finished children sections)
     */
    private StreamingPlanSection tryCostBasedOptimize(StreamingPlanSection section)
    {
        // no need to do runtime optimization if no materialized exchange data is utilized by the section.
        if (!isRuntimeOptimizerEnabled(session) || section.getChildren().isEmpty()) {
            return section;
        }

        // Apply runtime optimization on each StreamingSubPlan and generate optimized new fragments
        Map<PlanFragment, PlanFragment> oldToNewFragment = new HashMap<>();
        stream(forTree(StreamingSubPlan::getChildren).depthFirstPreOrder(section.getPlan()))
                .forEach(currentSubPlan -> {
                    Optional<PlanFragment> newPlanFragment = performRuntimeOptimizations(currentSubPlan);
                    if (newPlanFragment.isPresent()) {
                        planChecker.validatePlanFragment(newPlanFragment.get().getRoot(), session, metadata, sqlParser, variableAllocator.getTypes(), warningCollector);
                        oldToNewFragment.put(currentSubPlan.getFragment(), newPlanFragment.get());
                    }
                });

        // Early exit when no stage's fragment is changed
        if (oldToNewFragment.isEmpty()) {
            return section;
        }

        oldToNewFragment.forEach((oldFragment, newFragment) -> runtimeOptimizedStages.add(getStageId(oldFragment.getId())));

        // Update SubPlan so that getStageInfo will reflect the latest optimized plan when query is finished.
        updatePlan(oldToNewFragment);

        // Rebuild and update entries of the stageExecutions map.
        updateStageExecutions(section, oldToNewFragment);
        log.debug("Invoked CBO during runtime, optimized stage IDs: " + oldToNewFragment.keySet().stream()
                .map(PlanFragment::getId)
                .map(PlanFragmentId::toString)
                .collect(Collectors.joining(", ")));
        return section;
    }

    private Optional<PlanFragment> performRuntimeOptimizations(StreamingSubPlan subPlan)
    {
        PlanFragment fragment = subPlan.getFragment();
        PlanNode newRoot = fragment.getRoot();
        for (PlanOptimizer optimizer : runtimePlanOptimizers) {
            newRoot = optimizer.optimize(newRoot, session, variableAllocator.getTypes(), variableAllocator, idAllocator, warningCollector);
        }
        if (newRoot != fragment.getRoot()) {
            return Optional.of(
                    // The partitioningScheme should stay the same
                    // even if the root's outputVariable layout is changed.
                    new PlanFragment(
                            fragment.getId(),
                            newRoot,
                            fragment.getVariables(),
                            fragment.getPartitioning(),
                            scheduleOrder(newRoot),
                            fragment.getPartitioningScheme(),
                            fragment.getStageExecutionDescriptor(),
                            fragment.isOutputTableWriterFragment(),
                            fragment.getStatsAndCosts(),
                            Optional.of(jsonFragmentPlan(newRoot, fragment.getVariables(), functionAndTypeManager, session))));
        }
        return Optional.empty();
    }

    /**
     * Utility function that rebuild a StreamingPlanSection, re-create stageExecutionAndScheduler for each of its stage, and finally update the stageExecutions map.
     */
    private void updateStageExecutions(StreamingPlanSection section, Map<PlanFragment, PlanFragment> oldToNewFragment)
    {
        StreamingPlanSection newSection = new StreamingPlanSection(rewriteStreamingSubPlan(section.getPlan(), oldToNewFragment), section.getChildren());
        PlanFragment sectionRootFragment = newSection.getPlan().getFragment();
        Optional<int[]> bucketToPartition;
        OutputBuffers outputBuffers;
        ExchangeLocationsConsumer locationsConsumer;
        if (isRootFragment(sectionRootFragment)) {
            bucketToPartition = Optional.of(new int[1]);
            outputBuffers = createInitialEmptyOutputBuffers(sectionRootFragment.getPartitioningScheme().getPartitioning().getHandle())
                    .withBuffer(new OutputBufferId(0), BROADCAST_PARTITION_ID)
                    .withNoMoreBufferIds();
            OutputBufferId rootBufferId = getOnlyElement(outputBuffers.getBuffers().keySet());
            locationsConsumer = (fragmentId, tasks, noMoreExchangeLocations) ->
                    updateQueryOutputLocations(queryStateMachine, rootBufferId, tasks, noMoreExchangeLocations);
        }
        else {
            bucketToPartition = Optional.empty();
            outputBuffers = createDiscardingOutputBuffers();
            locationsConsumer = (fragmentId, tasks, noMoreExchangeLocations) -> {};
        }
        SectionExecution sectionExecution = sectionExecutionFactory.createSectionExecutions(
                session,
                newSection,
                locationsConsumer,
                bucketToPartition,
                outputBuffers,
                summarizeTaskInfo,
                remoteTaskFactory,
                splitSourceFactory,
                0);
        addStateChangeListeners(sectionExecution);
        Map<StageId, StageExecutionAndScheduler> updatedStageExecutions = sectionExecution.getSectionStages().stream()
                .collect(toImmutableMap(execution -> execution.getStageExecution().getStageExecutionId().getStageId(), identity()));
        synchronized (this) {
            stageExecutions.putAll(updatedStageExecutions);
        }
    }

    private void updatePlan(Map<PlanFragment, PlanFragment> oldToNewFragments)
    {
        plan.getAndUpdate(value -> rewritePlan(value, oldToNewFragments));
    }

    private SubPlan rewritePlan(SubPlan root, Map<PlanFragment, PlanFragment> oldToNewFragments)
    {
        ImmutableList.Builder<SubPlan> children = ImmutableList.builder();
        for (SubPlan child : root.getChildren()) {
            children.add(rewritePlan(child, oldToNewFragments));
        }
        if (oldToNewFragments.containsKey(root.getFragment())) {
            return new SubPlan(oldToNewFragments.get(root.getFragment()), children.build());
        }
        else {
            return new SubPlan(root.getFragment(), children.build());
        }
    }

    // Only used for adaptive optimization, to register listeners to new stageExecutions generated in runtime.
    private void addStateChangeListeners(SectionExecution sectionExecution)
    {
        for (StageExecutionAndScheduler stageExecutionAndScheduler : sectionExecution.getSectionStages()) {
            SqlStageExecution stageExecution = stageExecutionAndScheduler.getStageExecution();
            if (isRootFragment(stageExecution.getFragment())) {
                stageExecution.addStateChangeListener(state -> {
                    if (state == FINISHED) {
                        queryStateMachine.transitionToFinishing();
                    }
                    else if (state == CANCELED) {
                        // output stage was canceled
                        queryStateMachine.transitionToCanceled();
                    }
                });
            }
            stageExecution.addStateChangeListener(state -> {
                if (queryStateMachine.isDone()) {
                    return;
                }
                if (state == FAILED) {
                    queryStateMachine.transitionToFailed(stageExecution.getStageExecutionInfo().getFailureCause().get().toException());
                }
                else if (state == ABORTED) {
                    // this should never happen, since abort can only be triggered in query clean up after the query is finished
                    queryStateMachine.transitionToFailed(new PrestoException(GENERIC_INTERNAL_ERROR, "Query stage was aborted"));
                }
                else if (state == FINISHED) {
                    // checks if there's any new sections available for execution and starts the scheduling if any
                    startScheduling();
                }
                else if (queryStateMachine.getQueryState() == QueryState.STARTING) {
                    // if the stage has at least one task, we are running
                    if (stageExecution.hasTasks()) {
                        queryStateMachine.transitionToRunning();
                    }
                }
            });
            stageExecution.addFinalStageInfoListener(status -> queryStateMachine.updateQueryInfo(Optional.of(getStageInfo())));
        }
    }

    private StreamingSubPlan rewriteStreamingSubPlan(StreamingSubPlan root, Map<PlanFragment, PlanFragment> oldToNewFragment)
    {
        ImmutableList.Builder<StreamingSubPlan> childrenPlans = ImmutableList.builder();
        for (StreamingSubPlan child : root.getChildren()) {
            childrenPlans.add(rewriteStreamingSubPlan(child, oldToNewFragment));
        }
        if (oldToNewFragment.containsKey(root.getFragment())) {
            return new StreamingSubPlan(oldToNewFragment.get(root.getFragment()), childrenPlans.build());
        }
        else {
            return new StreamingSubPlan(root.getFragment(), childrenPlans.build());
        }
    }

    private static boolean isRootFragment(PlanFragment fragment)
    {
        return fragment.getId().getId() == ROOT_FRAGMENT_ID;
    }

    private boolean isReadyForExecution(StreamingPlanSection section)
    {
        SqlStageExecution stageExecution = getStageExecution(section.getPlan().getFragment().getId());
        if (stageExecution.getState() != PLANNED) {
            // already scheduled
            return false;
        }
        for (StreamingPlanSection child : section.getChildren()) {
            SqlStageExecution rootStageExecution = getStageExecution(child.getPlan().getFragment().getId());
            if (rootStageExecution.getState() != FINISHED) {
                return false;
            }
        }
        return true;
    }

    private List<List<StageExecutionAndScheduler>> getStageExecutions(List<StreamingPlanSection> sections)
    {
        return sections.stream()
                .map(section -> stream(forTree(StreamingSubPlan::getChildren).depthFirstPreOrder(section.getPlan())).collect(toImmutableList()))
                .map(plans -> plans.stream()
                        .map(StreamingSubPlan::getFragment)
                        .map(PlanFragment::getId)
                        .map(this::getStageExecutionInfo)
                        .collect(toImmutableList()))
                .collect(toImmutableList());
    }

    private SqlStageExecution getStageExecution(PlanFragmentId planFragmentId)
    {
        return stageExecutions.get(getStageId(planFragmentId)).getStageExecution();
    }

    private StageExecutionAndScheduler getStageExecutionInfo(PlanFragmentId planFragmentId)
    {
        return stageExecutions.get(getStageId(planFragmentId));
    }

    private StageId getStageId(PlanFragmentId fragmentId)
    {
        return new StageId(queryStateMachine.getQueryId(), fragmentId.getId());
    }

    public long getUserMemoryReservation()
    {
        return stageExecutions.values().stream()
                .mapToLong(stageExecutionInfo -> stageExecutionInfo.getStageExecution().getUserMemoryReservation())
                .sum();
    }

    public long getTotalMemoryReservation()
    {
        return stageExecutions.values().stream()
                .mapToLong(stageExecutionInfo -> stageExecutionInfo.getStageExecution().getTotalMemoryReservation())
                .sum();
    }

    public Duration getTotalCpuTime()
    {
        long millis = stageExecutions.values().stream()
                .mapToLong(stage -> stage.getStageExecution().getTotalCpuTime().toMillis())
                .sum();
        return new Duration(millis, MILLISECONDS);
    }

    @Override
    public DataSize getRawInputDataSize()
    {
        long datasize = stageExecutions.values().stream()
                .mapToLong(stage -> stage.getStageExecution().getRawInputDataSize().toBytes())
                .sum();
        return DataSize.succinctBytes(datasize);
    }

    @Override
    public DataSize getOutputDataSize()
    {
        return stageExecutions.get(rootStageId).getStageExecution().getStageExecutionInfo().getStats().getOutputDataSize();
    }

    public BasicStageExecutionStats getBasicStageStats()
    {
        List<BasicStageExecutionStats> stageStats = stageExecutions.values().stream()
                .map(stageExecutionInfo -> stageExecutionInfo.getStageExecution().getBasicStageStats())
                .collect(toImmutableList());

        return aggregateBasicStageStats(stageStats);
    }

    public StageInfo getStageInfo()
    {
        Map<StageId, StageExecutionInfo> stageInfos = stageExecutions.values().stream()
                .map(StageExecutionAndScheduler::getStageExecution)
                .collect(toImmutableMap(execution -> execution.getStageExecutionId().getStageId(), SqlStageExecution::getStageExecutionInfo));

        return buildStageInfo(plan.get(), stageInfos);
    }

    private StageInfo buildStageInfo(SubPlan subPlan, Map<StageId, StageExecutionInfo> stageExecutionInfos)
    {
        StageId stageId = getStageId(subPlan.getFragment().getId());
        StageExecutionInfo stageExecutionInfo = stageExecutionInfos.get(stageId);
        checkArgument(stageExecutionInfo != null, "No stageExecutionInfo for %s", stageId);
        return new StageInfo(
                stageId,
                locationFactory.createStageLocation(stageId),
                Optional.of(subPlan.getFragment()),
                stageExecutionInfo,
                ImmutableList.of(),
                subPlan.getChildren().stream()
                        .map(plan -> buildStageInfo(plan, stageExecutionInfos))
                        .collect(toImmutableList()),
                runtimeOptimizedStages.contains(stageId));
    }

    public void cancelStage(StageId stageId)
    {
        try (SetThreadName ignored = new SetThreadName("Query-%s", queryStateMachine.getQueryId())) {
            SqlStageExecution execution = stageExecutions.get(stageId).getStageExecution();
            SqlStageExecution stage = requireNonNull(execution, () -> format("Stage %s does not exist", stageId));
            stage.cancel();
        }
    }

    public void abort()
    {
        try (SetThreadName ignored = new SetThreadName("Query-%s", queryStateMachine.getQueryId())) {
            stageExecutions.values().forEach(stageExecutionInfo -> stageExecutionInfo.getStageExecution().abort());
        }
    }
}
