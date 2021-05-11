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
import com.facebook.presto.execution.ExecutionFailureInfo;
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
import com.facebook.presto.metadata.InternalNodeManager;
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
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.facebook.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static com.facebook.airlift.concurrent.MoreFutures.whenAnyComplete;
import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static com.facebook.presto.SystemSessionProperties.getMaxConcurrentMaterializations;
import static com.facebook.presto.SystemSessionProperties.getMaxStageRetries;
import static com.facebook.presto.SystemSessionProperties.getPartialResultsCompletionRatioThreshold;
import static com.facebook.presto.SystemSessionProperties.getPartialResultsMaxExecutionTimeMultiplier;
import static com.facebook.presto.SystemSessionProperties.isPartialResultsEnabled;
import static com.facebook.presto.SystemSessionProperties.isRuntimeOptimizerEnabled;
import static com.facebook.presto.execution.BasicStageExecutionStats.aggregateBasicStageStats;
import static com.facebook.presto.execution.SqlStageExecution.RECOVERABLE_ERROR_CODES;
import static com.facebook.presto.execution.StageExecutionInfo.unscheduledExecutionInfo;
import static com.facebook.presto.execution.StageExecutionState.CANCELED;
import static com.facebook.presto.execution.StageExecutionState.FAILED;
import static com.facebook.presto.execution.StageExecutionState.FINISHED;
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
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getLast;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Streams.stream;
import static com.google.common.graph.Traverser.forTree;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class SqlQueryScheduler
        implements SqlQuerySchedulerInterface
{
    private static final Logger log = Logger.get(SqlQueryScheduler.class);

    private final LocationFactory locationFactory;
    private final ExecutionPolicy executionPolicy;
    private final ExecutorService executor;
    private final SplitSchedulerStats schedulerStats;
    private final SectionExecutionFactory sectionExecutionFactory;
    private final RemoteTaskFactory remoteTaskFactory;
    private final SplitSourceFactory splitSourceFactory;
    private final InternalNodeManager nodeManager;

    private final Session session;
    private final QueryStateMachine queryStateMachine;
    private final AtomicReference<SubPlan> plan = new AtomicReference<>();

    // The following fields are required for adaptive optimization in runtime.
    private final FunctionAndTypeManager functionAndTypeManager;
    private final List<PlanOptimizer> runtimePlanOptimizers;
    private final WarningCollector warningCollector;
    private final PlanNodeIdAllocator idAllocator;
    private final PlanVariableAllocator variableAllocator;
    private final Set<StageId> runtimeOptimizedStages = Collections.synchronizedSet(new HashSet<>());
    private final PlanChecker planChecker;
    private final Metadata metadata;
    private final SqlParser sqlParser;

    private final StreamingPlanSection sectionedPlan;
    private final boolean summarizeTaskInfo;
    private final int maxConcurrentMaterializations;
    private final int maxStageRetries;

    private final Map<StageId, List<SectionExecution>> sectionExecutions = new ConcurrentHashMap<>();
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean scheduling = new AtomicBoolean();
    private final AtomicInteger retriedSections = new AtomicInteger();

    private final PartialResultQueryTaskTracker partialResultQueryTaskTracker;

    public static SqlQueryScheduler createSqlQueryScheduler(
            LocationFactory locationFactory,
            ExecutionPolicy executionPolicy,
            ExecutorService executor,
            SplitSchedulerStats schedulerStats,
            SectionExecutionFactory sectionExecutionFactory,
            RemoteTaskFactory remoteTaskFactory,
            SplitSourceFactory splitSourceFactory,
            InternalNodeManager nodeManager,
            Session session,
            QueryStateMachine queryStateMachine,
            SubPlan plan,
            boolean summarizeTaskInfo,
            FunctionAndTypeManager functionAndTypeManager,
            List<PlanOptimizer> runtimePlanOptimizers,
            WarningCollector warningCollector,
            PlanNodeIdAllocator idAllocator,
            PlanVariableAllocator variableAllocator,
            PlanChecker planChecker,
            Metadata metadata,
            SqlParser sqlParser,
            PartialResultQueryManager partialResultQueriesHandler)
    {
        SqlQueryScheduler sqlQueryScheduler = new SqlQueryScheduler(
                locationFactory,
                executionPolicy,
                executor,
                schedulerStats,
                sectionExecutionFactory,
                remoteTaskFactory,
                splitSourceFactory,
                nodeManager,
                session,
                queryStateMachine,
                plan,
                summarizeTaskInfo,
                functionAndTypeManager,
                runtimePlanOptimizers,
                warningCollector,
                idAllocator,
                variableAllocator,
                planChecker,
                metadata,
                sqlParser,
                partialResultQueriesHandler);
        sqlQueryScheduler.initialize();
        return sqlQueryScheduler;
    }

    private SqlQueryScheduler(
            LocationFactory locationFactory,
            ExecutionPolicy executionPolicy,
            ExecutorService executor,
            SplitSchedulerStats schedulerStats,
            SectionExecutionFactory sectionExecutionFactory,
            RemoteTaskFactory remoteTaskFactory,
            SplitSourceFactory splitSourceFactory,
            InternalNodeManager nodeManager,
            Session session,
            QueryStateMachine queryStateMachine,
            SubPlan plan,
            boolean summarizeTaskInfo,
            FunctionAndTypeManager functionAndTypeManager,
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
        this.executor = requireNonNull(executor, "executor is null");
        this.schedulerStats = requireNonNull(schedulerStats, "schedulerStats is null");
        this.sectionExecutionFactory = requireNonNull(sectionExecutionFactory, "sectionExecutionFactory is null");
        this.remoteTaskFactory = requireNonNull(remoteTaskFactory, "remoteTaskFactory is null");
        this.splitSourceFactory = requireNonNull(splitSourceFactory, "splitSourceFactory is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.session = requireNonNull(session, "session is null");
        this.queryStateMachine = requireNonNull(queryStateMachine, "queryStateMachine is null");
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionManager is null");
        this.runtimePlanOptimizers = requireNonNull(runtimePlanOptimizers, "runtimePlanOptimizers is null");
        this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
        this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
        this.variableAllocator = requireNonNull(variableAllocator, "variableAllocator is null");
        this.planChecker = requireNonNull(planChecker, "planChecker is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.plan.compareAndSet(null, requireNonNull(plan, "plan is null"));
        this.sectionedPlan = extractStreamingSections(plan);
        this.summarizeTaskInfo = summarizeTaskInfo;
        this.maxConcurrentMaterializations = getMaxConcurrentMaterializations(session);
        this.maxStageRetries = getMaxStageRetries(session);
        this.partialResultQueryTaskTracker = new PartialResultQueryTaskTracker(partialResultQueryManager, getPartialResultsCompletionRatioThreshold(session), getPartialResultsMaxExecutionTimeMultiplier(session), warningCollector);
    }

    // this is a separate method to ensure that the `this` reference is not leaked during construction
    private void initialize()
    {
        // when query is done or any time a stage completes, attempt to transition query to "final query info ready"
        queryStateMachine.addStateChangeListener(newState -> {
            if (newState.isDone()) {
                queryStateMachine.updateQueryInfo(Optional.of(getStageInfo()));
            }
        });
    }

    @Override
    public void start()
    {
        if (started.compareAndSet(false, true)) {
            startScheduling();
        }
    }

    private void startScheduling()
    {
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

            List<ExecutionSchedule> executionSchedules = new LinkedList<>();

            while (!Thread.currentThread().isInterrupted()) {
                // remove finished section
                executionSchedules.removeIf(ExecutionSchedule::isFinished);

                // try to pull more section that are ready to be run
                List<StreamingPlanSection> sectionsReadyForExecution = getSectionsReadyForExecution();

                // all finished
                if (sectionsReadyForExecution.isEmpty() && executionSchedules.isEmpty()) {
                    break;
                }

                // Apply runtime CBO on the ready sections before creating SectionExecutions.
                List<SectionExecution> sectionExecutions = createStageExecutions(sectionsReadyForExecution.stream()
                        .map(this::tryCostBasedOptimize)
                        .collect(toImmutableList()));
                if (queryStateMachine.isDone()) {
                    sectionExecutions.forEach(SectionExecution::abort);
                    break;
                }

                sectionExecutions.forEach(sectionExecution -> scheduledStageExecutions.addAll(sectionExecution.getSectionStages()));
                sectionExecutions.stream()
                        .map(SectionExecution::getSectionStages)
                        .map(executionPolicy::createExecutionSchedule)
                        .forEach(executionSchedules::add);

                while (!executionSchedules.isEmpty() && executionSchedules.stream().noneMatch(ExecutionSchedule::isFinished)) {
                    List<ListenableFuture<?>> blockedStages = new ArrayList<>();

                    List<StageExecutionAndScheduler> executionsToSchedule = executionSchedules.stream()
                            .flatMap(schedule -> schedule.getStagesToSchedule().stream())
                            .collect(toImmutableList());

                    for (StageExecutionAndScheduler executionAndScheduler : executionsToSchedule) {
                        executionAndScheduler.getStageExecution().beginScheduling();

                        // perform some scheduling work
                        ScheduleResult result = executionAndScheduler.getStageScheduler()
                                .schedule();

                        // Track leaf tasks if partial results are enabled
                        if (isPartialResultsEnabled(session) && executionAndScheduler.getStageExecution().getFragment().isLeaf()) {
                            for (RemoteTask task : result.getNewTasks()) {
                                partialResultQueryTaskTracker.trackTask(task);
                                task.addFinalTaskInfoListener(partialResultQueryTaskTracker::recordTaskFinish);
                            }
                        }

                        // modify parent and children based on the results of the scheduling
                        if (result.isFinished()) {
                            executionAndScheduler.getStageExecution().schedulingComplete();
                        }
                        else if (!result.getBlocked().isDone()) {
                            blockedStages.add(result.getBlocked());
                        }
                        executionAndScheduler.getStageLinkage()
                                .processScheduleResults(executionAndScheduler.getStageExecution().getState(), result.getNewTasks());
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
                    for (StageExecutionAndScheduler stageExecutionAndScheduler : scheduledStageExecutions) {
                        SqlStageExecution stageExecution = stageExecutionAndScheduler.getStageExecution();
                        StageId stageId = stageExecution.getStageExecutionId().getStageId();
                        if (!completedStages.contains(stageId) && stageExecution.getState().isDone()) {
                            stageExecutionAndScheduler.getStageLinkage()
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

            for (StageExecutionAndScheduler stageExecutionAndScheduler : scheduledStageExecutions) {
                StageExecutionState state = stageExecutionAndScheduler.getStageExecution().getState();
                if (state != SCHEDULED && state != RUNNING && !state.isDone()) {
                    throw new PrestoException(GENERIC_INTERNAL_ERROR, format("Scheduling is complete, but stage execution %s is in state %s", stageExecutionAndScheduler.getStageExecution().getStageExecutionId(), state));
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
            for (StageExecutionAndScheduler stageExecutionAndScheduler : scheduledStageExecutions) {
                try {
                    stageExecutionAndScheduler.getStageScheduler().close();
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

        // Apply runtime optimization on each StreamingSubPlan's fragment
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

        log.debug("Invoked CBO during runtime, optimized stage IDs: " + oldToNewFragment.keySet().stream()
                .map(PlanFragment::getId)
                .map(PlanFragmentId::toString)
                .collect(Collectors.joining(", ")));
        return new StreamingPlanSection(rewriteStreamingSubPlan(section.getPlan(), oldToNewFragment), section.getChildren());
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

    private List<StreamingPlanSection> getSectionsReadyForExecution()
    {
        long runningPlanSections =
                stream(forTree(StreamingPlanSection::getChildren).depthFirstPreOrder(sectionedPlan))
                        .map(section -> getLatestSectionExecution(getStageId(section.getPlan().getFragment().getId())))
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .filter(SectionExecution::isRunning)
                        .count();
        return stream(forTree(StreamingPlanSection::getChildren).depthFirstPreOrder(sectionedPlan))
                // get all sections ready for execution
                .filter(this::isReadyForExecution)
                .limit(maxConcurrentMaterializations - runningPlanSections)
                .collect(toImmutableList());
    }

    private boolean isReadyForExecution(StreamingPlanSection section)
    {
        Optional<SectionExecution> sectionExecution = getLatestSectionExecution(getStageId(section.getPlan().getFragment().getId()));
        if (sectionExecution.isPresent() && (sectionExecution.get().isRunning() || sectionExecution.get().isFinished())) {
            // already scheduled
            return false;
        }
        for (StreamingPlanSection child : section.getChildren()) {
            Optional<SectionExecution> childSectionExecution = getLatestSectionExecution(getStageId(child.getPlan().getFragment().getId()));
            if (!childSectionExecution.isPresent() || !childSectionExecution.get().isFinished()) {
                return false;
            }
        }
        return true;
    }

    private Optional<SectionExecution> getLatestSectionExecution(StageId stageId)
    {
        List<SectionExecution> sectionExecutions = this.sectionExecutions.get(stageId);
        if (sectionExecutions == null || sectionExecutions.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(getLast(sectionExecutions));
    }

    private StageId getStageId(PlanFragmentId fragmentId)
    {
        return new StageId(session.getQueryId(), fragmentId.getId());
    }

    private List<SectionExecution> createStageExecutions(List<StreamingPlanSection> sections)
    {
        ImmutableList.Builder<SectionExecution> result = ImmutableList.builder();
        for (StreamingPlanSection section : sections) {
            StageId sectionId = getStageId(section.getPlan().getFragment().getId());
            List<SectionExecution> attempts = sectionExecutions.computeIfAbsent(sectionId, (ignored) -> new CopyOnWriteArrayList<>());

            // sectionExecutions only get created when they are about to be scheduled, so there should
            // never be a non-failed SectionExecution for a section that's ready for execution
            verify(attempts.isEmpty() || getLast(attempts).isFailed(), "Non-failed sectionExecutions already exists");

            PlanFragment sectionRootFragment = section.getPlan().getFragment();

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

            int attemptId = attempts.size();
            SectionExecution sectionExecution = sectionExecutionFactory.createSectionExecutions(
                    session,
                    section,
                    locationsConsumer,
                    bucketToPartition,
                    outputBuffers,
                    summarizeTaskInfo,
                    remoteTaskFactory,
                    splitSourceFactory,
                    attemptId);

            addStateChangeListeners(sectionExecution);
            attempts.add(sectionExecution);
            result.add(sectionExecution);
        }
        return result.build();
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
                    ExecutionFailureInfo failureInfo = stageExecution.getStageExecutionInfo().getFailureCause()
                            .orElseThrow(() -> new VerifyException(format("stage execution failed, but the failure info is missing: %s", stageExecution.getStageExecutionId())));
                    Exception failureException = failureInfo.toException();

                    boolean isRootSection = isRootFragment(sectionExecution.getRootStage().getStageExecution().getFragment());
                    // root section directly streams the results to the user, cannot be retried
                    if (isRootSection) {
                        queryStateMachine.transitionToFailed(failureException);
                        return;
                    }

                    if (retriedSections.get() >= maxStageRetries) {
                        queryStateMachine.transitionToFailed(failureException);
                        return;
                    }

                    if (!RECOVERABLE_ERROR_CODES.contains(failureInfo.getErrorCode())) {
                        queryStateMachine.transitionToFailed(failureException);
                        return;
                    }

                    try {
                        if (sectionExecution.abort()) {
                            retriedSections.incrementAndGet();
                            nodeManager.refreshNodes();
                            startScheduling();
                        }
                    }
                    catch (Throwable t) {
                        if (failureException != t) {
                            failureException.addSuppressed(t);
                        }
                        queryStateMachine.transitionToFailed(failureException);
                    }
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

    private static boolean isRootFragment(PlanFragment fragment)
    {
        return fragment.getId().getId() == ROOT_FRAGMENT_ID;
    }

    @Override
    public long getUserMemoryReservation()
    {
        return getAllStagesExecutions().mapToLong(SqlStageExecution::getUserMemoryReservation).sum();
    }

    @Override
    public long getTotalMemoryReservation()
    {
        return getAllStagesExecutions().mapToLong(SqlStageExecution::getTotalMemoryReservation).sum();
    }

    @Override
    public Duration getTotalCpuTime()
    {
        long millis = getAllStagesExecutions()
                .map(SqlStageExecution::getTotalCpuTime)
                .mapToLong(Duration::toMillis)
                .sum();
        return new Duration(millis, MILLISECONDS);
    }

    @Override
    public DataSize getRawInputDataSize()
    {
        long rawInputDataSize = getAllStagesExecutions()
                .map(SqlStageExecution::getRawInputDataSize)
                .mapToLong(DataSize::toBytes)
                .sum();
        return DataSize.succinctBytes(rawInputDataSize);
    }

    @Override
    public DataSize getOutputDataSize()
    {
        return getStageInfo().getLatestAttemptExecutionInfo().getStats().getOutputDataSize();
    }

    @Override
    public BasicStageExecutionStats getBasicStageStats()
    {
        List<BasicStageExecutionStats> stageStats = getAllStagesExecutions()
                .map(SqlStageExecution::getBasicStageStats)
                .collect(toImmutableList());
        return aggregateBasicStageStats(stageStats);
    }

    private Stream<SqlStageExecution> getAllStagesExecutions()
    {
        return sectionExecutions.values().stream()
                .flatMap(Collection::stream)
                .flatMap(sectionExecution -> sectionExecution.getSectionStages().stream())
                .map(StageExecutionAndScheduler::getStageExecution);
    }

    @Override
    public StageInfo getStageInfo()
    {
        ListMultimap<StageId, SqlStageExecution> stageExecutions = getStageExecutions();
        return buildStageInfo(plan.get(), stageExecutions);
    }

    private StageInfo buildStageInfo(SubPlan subPlan, ListMultimap<StageId, SqlStageExecution> stageExecutions)
    {
        StageId stageId = getStageId(subPlan.getFragment().getId());
        List<SqlStageExecution> attempts = stageExecutions.get(stageId);

        StageExecutionInfo latestAttemptInfo = attempts.isEmpty() ?
                unscheduledExecutionInfo(stageId.getId(), queryStateMachine.isDone()) :
                getLast(attempts).getStageExecutionInfo();
        List<StageExecutionInfo> previousAttemptInfos = attempts.size() < 2 ?
                ImmutableList.of() :
                attempts.subList(0, attempts.size() - 1).stream()
                        .map(SqlStageExecution::getStageExecutionInfo)
                        .collect(toImmutableList());

        return new StageInfo(
                stageId,
                locationFactory.createStageLocation(stageId),
                Optional.of(subPlan.getFragment()),
                latestAttemptInfo,
                previousAttemptInfos,
                subPlan.getChildren().stream()
                        .map(plan -> buildStageInfo(plan, stageExecutions))
                        .collect(toImmutableList()),
                runtimeOptimizedStages.contains(stageId));
    }

    private ListMultimap<StageId, SqlStageExecution> getStageExecutions()
    {
        ImmutableListMultimap.Builder<StageId, SqlStageExecution> result = ImmutableListMultimap.builder();
        for (Collection<SectionExecution> sectionExecutionAttempts : sectionExecutions.values()) {
            for (SectionExecution sectionExecution : sectionExecutionAttempts) {
                for (StageExecutionAndScheduler stageExecution : sectionExecution.getSectionStages()) {
                    result.put(stageExecution.getStageExecution().getStageExecutionId().getStageId(), stageExecution.getStageExecution());
                }
            }
        }
        return result.build();
    }

    @Override
    public void cancelStage(StageId stageId)
    {
        try (SetThreadName ignored = new SetThreadName("Query-%s", queryStateMachine.getQueryId())) {
            getAllStagesExecutions()
                    .filter(execution -> execution.getStageExecutionId().getStageId().equals(stageId))
                    .forEach(SqlStageExecution::cancel);
        }
    }

    @Override
    public void abort()
    {
        try (SetThreadName ignored = new SetThreadName("Query-%s", queryStateMachine.getQueryId())) {
            checkState(queryStateMachine.isDone(), "query scheduler is expected to be aborted only if the query is finished: %s", queryStateMachine.getQueryState());
            getAllStagesExecutions().forEach(SqlStageExecution::abort);
        }
    }
}
