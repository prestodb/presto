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

import com.facebook.airlift.json.Codec;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.cost.FragmentStatsProvider;
import com.facebook.presto.cost.HistoryBasedPlanStatisticsTracker;
import com.facebook.presto.event.QueryMonitor;
import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.execution.QueryStateTimer;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.memory.NodeMemoryConfig;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spark.ErrorClassifier;
import com.facebook.presto.spark.PrestoSparkMetadataStorage;
import com.facebook.presto.spark.PrestoSparkQueryData;
import com.facebook.presto.spark.PrestoSparkQueryStatusInfo;
import com.facebook.presto.spark.PrestoSparkServiceWaitTimeMetrics;
import com.facebook.presto.spark.PrestoSparkTaskDescriptor;
import com.facebook.presto.spark.RddAndMore;
import com.facebook.presto.spark.classloader_interface.MutablePartitionId;
import com.facebook.presto.spark.classloader_interface.PrestoSparkSerializedPage;
import com.facebook.presto.spark.classloader_interface.PrestoSparkShuffleStats;
import com.facebook.presto.spark.classloader_interface.PrestoSparkTaskExecutorFactoryProvider;
import com.facebook.presto.spark.classloader_interface.SerializedTaskInfo;
import com.facebook.presto.spark.execution.task.PrestoSparkTaskExecutorFactory;
import com.facebook.presto.spark.node.PrestoSparkNodePartitioningManager;
import com.facebook.presto.spark.planner.IterativePlanFragmenter;
import com.facebook.presto.spark.planner.PrestoSparkPlanFragmenter;
import com.facebook.presto.spark.planner.PrestoSparkQueryPlanner.PlanAndMore;
import com.facebook.presto.spark.planner.PrestoSparkRddFactory;
import com.facebook.presto.spark.planner.optimizers.AdaptivePlanOptimizers;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.page.PagesSerde;
import com.facebook.presto.spi.plan.OutputNode;
import com.facebook.presto.spi.plan.PlanFragmentId;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.storage.TempStorage;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.planner.PartitioningProviderManager;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.SubPlan;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.facebook.presto.sql.planner.sanity.PlanChecker;
import com.facebook.presto.sql.planner.sanity.PlanCheckerProviderManager;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.units.Duration;
import org.apache.spark.MapOutputStatistics;
import org.apache.spark.SimpleFutureAction;
import org.apache.spark.SparkException;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.CollectionAccumulator;
import org.apache.spark.util.ThreadUtils;
import scala.Tuple2;
import scala.concurrent.ExecutionContextExecutorService;
import scala.concurrent.impl.ExecutionContextImpl;
import scala.runtime.AbstractFunction1;
import scala.util.Try;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.facebook.presto.execution.QueryState.PLANNING;
import static com.facebook.presto.spark.PrestoSparkQueryExecutionFactory.createQueryInfo;
import static com.facebook.presto.spark.PrestoSparkQueryExecutionFactory.createStageInfo;
import static com.facebook.presto.spark.execution.RuntimeStatistics.createRuntimeStats;
import static com.facebook.presto.spark.util.PrestoSparkUtils.computeNextTimeout;
import static com.facebook.presto.sql.planner.PlanFragmenterUtils.isCoordinatorOnlyDistribution;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.COORDINATOR_DISTRIBUTION;
import static com.facebook.presto.sql.planner.planPrinter.PlanPrinter.textLogicalPlan;
import static com.facebook.presto.sql.planner.planPrinter.PlanPrinter.textPlanFragment;
import static com.google.common.base.Throwables.propagateIfPossible;
import static com.google.common.base.Verify.verify;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * This class drives the adaptive query execution of a Presto-on-Spark query.
 * It iteratively generates fragments for the query, executes the fragments as they become ready (a fragment is ready when all
 * its dependencies have been executed), extracts statistics out of the executed fragments, attempts to re-optimize the part of
 * the query plan that has not yet been executed based on the new statistics before continuing the execution.
 */
public class PrestoSparkAdaptiveQueryExecution
        extends AbstractPrestoSparkQueryExecution
{
    private static final Logger log = Logger.get(PrestoSparkAdaptiveQueryExecution.class);

    private final IterativePlanFragmenter iterativePlanFragmenter;
    private final List<PlanOptimizer> adaptivePlanOptimizers;
    private final VariableAllocator variableAllocator;
    private final PlanNodeIdAllocator idAllocator;
    private final FragmentStatsProvider fragmentStatsProvider;

    /**
     * Set with the IDs of the fragments that have finished execution.
     */
    private final Set<PlanFragmentId> executedFragments = ConcurrentHashMap.newKeySet();

    /**
     * Queue with events related to the execution of plan fragments.
     */
    private final BlockingQueue<FragmentCompletionEvent> fragmentEventQueue = new LinkedBlockingQueue<>();

    // TODO: Bring over from the AbstractPrestoSparkQueryExecution the methods that are specific to adaptive execution.

    public PrestoSparkAdaptiveQueryExecution(
            JavaSparkContext sparkContext,
            Session session,
            QueryMonitor queryMonitor,
            CollectionAccumulator<SerializedTaskInfo> taskInfoCollector,
            CollectionAccumulator<PrestoSparkShuffleStats> shuffleStatsCollector,
            PrestoSparkTaskExecutorFactory taskExecutorFactory,
            PrestoSparkTaskExecutorFactoryProvider taskExecutorFactoryProvider,
            QueryStateTimer queryStateTimer,
            WarningCollector warningCollector,
            String query,
            PlanAndMore planAndMore,
            Optional<String> sparkQueueName,
            Codec<TaskInfo> taskInfoCodec,
            JsonCodec<PrestoSparkTaskDescriptor> sparkTaskDescriptorJsonCodec,
            JsonCodec<PrestoSparkQueryStatusInfo> queryStatusInfoJsonCodec,
            JsonCodec<PrestoSparkQueryData> queryDataJsonCodec,
            PrestoSparkRddFactory rddFactory,
            TransactionManager transactionManager,
            PagesSerde pagesSerde,
            PrestoSparkExecutionExceptionFactory executionExceptionFactory,
            Duration queryTimeout,
            long queryCompletionDeadline,
            PrestoSparkMetadataStorage metadataStorage,
            Optional<String> queryStatusInfoOutputLocation,
            Optional<String> queryDataOutputLocation,
            TempStorage tempStorage,
            NodeMemoryConfig nodeMemoryConfig,
            FeaturesConfig featuresConfig,
            QueryManagerConfig queryManagerConfig,
            Set<PrestoSparkServiceWaitTimeMetrics> waitTimeMetrics,
            Optional<ErrorClassifier> errorClassifier,
            PrestoSparkPlanFragmenter planFragmenter,
            Metadata metadata,
            PartitioningProviderManager partitioningProviderManager,
            HistoryBasedPlanStatisticsTracker historyBasedPlanStatisticsTracker,
            AdaptivePlanOptimizers adaptivePlanOptimizers,
            VariableAllocator variableAllocator,
            PlanNodeIdAllocator idAllocator,
            FragmentStatsProvider fragmentStatsProvider,
            Optional<CollectionAccumulator<Map<String, Long>>> bootstrapMetricsCollector,
            PlanCheckerProviderManager planCheckerProviderManager)
    {
        super(
                sparkContext,
                session,
                queryMonitor,
                taskInfoCollector,
                shuffleStatsCollector,
                taskExecutorFactory,
                taskExecutorFactoryProvider,
                queryStateTimer,
                warningCollector,
                query,
                planAndMore,
                sparkQueueName,
                taskInfoCodec,
                sparkTaskDescriptorJsonCodec,
                queryStatusInfoJsonCodec,
                queryDataJsonCodec,
                rddFactory,
                transactionManager,
                pagesSerde,
                executionExceptionFactory,
                queryTimeout,
                queryCompletionDeadline,
                metadataStorage,
                queryStatusInfoOutputLocation,
                queryDataOutputLocation,
                tempStorage,
                nodeMemoryConfig,
                featuresConfig,
                queryManagerConfig,
                waitTimeMetrics,
                errorClassifier,
                planFragmenter,
                metadata,
                partitioningProviderManager,
                historyBasedPlanStatisticsTracker,
                bootstrapMetricsCollector);

        this.fragmentStatsProvider = requireNonNull(fragmentStatsProvider, "fragmentStatsProvider is null");
        this.adaptivePlanOptimizers = requireNonNull(adaptivePlanOptimizers, "adaptivePlanOptimizers is null").getAdaptiveOptimizers();
        this.variableAllocator = requireNonNull(variableAllocator, "variableAllocator is null");
        this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
        this.iterativePlanFragmenter = createIterativePlanFragmenter(requireNonNull(planCheckerProviderManager, "planCheckerProviderManager is null"));
    }

    private IterativePlanFragmenter createIterativePlanFragmenter(PlanCheckerProviderManager planCheckerProviderManager)
    {
        boolean forceSingleNode = false;
        Function<PlanFragmentId, Boolean> isFragmentFinished = this.executedFragments::contains;

        // TODO Create the IterativePlanFragmenter by injection (it has to become stateless first--check PR 18811).
        return new IterativePlanFragmenter(
                this.planAndMore.getPlan(),
                isFragmentFinished,
                this.metadata,
                new PlanChecker(this.featuresConfig, forceSingleNode, planCheckerProviderManager),
                this.idAllocator,
                new PrestoSparkNodePartitioningManager(this.partitioningProviderManager),
                this.queryManagerConfig,
                this.session,
                this.warningCollector,
                forceSingleNode);
    }

    @Override
    protected List<Tuple2<MutablePartitionId, PrestoSparkSerializedPage>> doExecute()
            throws SparkException, TimeoutException
    {
        queryStateTimer.beginRunning();
        log.info("Using AdaptiveQueryExecutor");
        log.info(format("Logical plan : %s",
                textLogicalPlan(this.planAndMore.getPlan().getRoot(), this.planAndMore.getPlan().getTypes(), this.planAndMore.getPlan().getStatsAndCosts(), metadata.getFunctionAndTypeManager(), session, 0)));
        queryMonitor.queryUpdatedEvent(
                createQueryInfo(
                        session,
                        query,
                        PLANNING,
                        Optional.of(planAndMore),
                        sparkQueueName,
                        Optional.empty(),
                        queryStateTimer,
                        Optional.of(createStageInfo(session.getQueryId(), planFragmenter.fragmentQueryPlan(session, planAndMore.getPlan(), warningCollector), ImmutableList.of())),
                        warningCollector));

        IterativePlanFragmenter.PlanAndFragments planAndFragments = iterativePlanFragmenter.createReadySubPlans(this.planAndMore.getPlan().getRoot());

        ExecutionContextExecutorService executorService = !planAndFragments.hasRemainingPlan() ? null :
                (ExecutionContextExecutorService) ExecutionContextImpl.fromExecutorService(ThreadUtils.newDaemonCachedThreadPool("AdaptiveExecution", 16, 60), null);

        TableWriteInfo tableWriteInfo = getTableWriteInfo(session, this.planAndMore.getPlan().getRoot());

        while (planAndFragments.hasRemainingPlan()) {
            List<SubPlan> readyFragments = planAndFragments.getReadyFragments();
            Set<PlanFragmentId> rootChildren = getRootChildNodeFragmentIDs(planAndFragments.getRemainingPlan().get());
            for (SubPlan fragment : readyFragments) {
                log.info(format("Executing fragment : %s",
                        textPlanFragment(fragment.getFragment(), metadata.getFunctionAndTypeManager(), session, true)));
                Optional<Class<?>> outputType = Optional.empty();
                if (isCoordinatorOnly(this.planAndMore.getPlan()) && rootChildren.contains(fragment.getFragment().getId())) {
                    outputType = Optional.of(PrestoSparkSerializedPage.class);
                }

                SubPlan currentFragment = configureOutputPartitioning(session, fragment, planAndMore.getPhysicalResourceSettings().getHashPartitionCount());
                FragmentExecutionResult fragmentExecutionResult = executeFragment(currentFragment, tableWriteInfo, outputType);

                // Create the corresponding event when the fragment finishes execution (successfully or not) and place it in the event queue.
                // Note that these are Scala futures that we manipulate here in Java.
                Optional<SimpleFutureAction<MapOutputStatistics>> fragmentFuture = fragmentExecutionResult.getMapOutputStatisticsFutureAction();
                if (fragmentFuture.isPresent()) {
                    SimpleFutureAction<MapOutputStatistics> mapOutputStatsFuture = fragmentFuture.get();

                    mapOutputStatsFuture.onComplete(new AbstractFunction1<Try<MapOutputStatistics>, Void>()
                    {
                        @Override
                        public Void apply(Try<MapOutputStatistics> result)
                        {
                            if (result.isSuccess()) {
                                Optional<MapOutputStatistics> mapOutputStats = Optional.ofNullable(result.get());
                                publishFragmentCompletionEvent(new FragmentCompletionSuccessEvent(currentFragment.getFragment().getId(), mapOutputStats));
                            }
                            else {
                                Throwable throwable = result.failed().get();
                                publishFragmentCompletionEvent(new FragmentCompletionFailureEvent(currentFragment.getFragment().getId(), throwable));
                            }
                            return null;
                        }
                    }, executorService);
                }
                else {
                    log.info("Fragment %s will not get executed now either because there was no exchange involved (a broadcast is present) or because of an unknown issue.",
                            fragment.getFragment().getId());
                    // Mark this fragment/non-shuffle stage as completed to continue next stage plan generation
                    publishFragmentCompletionEvent(new FragmentCompletionSuccessEvent(currentFragment.getFragment().getId(), Optional.empty()));
                }
            }

            // Consume the next fragment execution completion event (block if no new fragment execution has finished) and re-optimize if possible.
            FragmentCompletionEvent fragmentEvent;
            try {
                fragmentEvent = fragmentEventQueue.poll(computeNextTimeout(queryCompletionDeadline), MILLISECONDS);
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            // In case poll() timed out without getting an event in the queue.
            if (fragmentEvent == null) {
                throw executionExceptionFactory.toPrestoSparkExecutionException(new RuntimeException("Adaptive query execution failed due to timeout."));
            }
            if (fragmentEvent instanceof FragmentCompletionFailureEvent) {
                FragmentCompletionFailureEvent failureEvent = (FragmentCompletionFailureEvent) fragmentEvent;
                propagateIfPossible(failureEvent.getExecutionError(), SparkException.class);
                propagateIfPossible(failureEvent.getExecutionError(), RuntimeException.class);
                throw new UncheckedExecutionException(failureEvent.getExecutionError());
            }

            verify(fragmentEvent instanceof FragmentCompletionSuccessEvent, String.format("Unexpected FragmentCompletionEvent type: %s", fragmentEvent.getClass().getSimpleName()));
            FragmentCompletionSuccessEvent successEvent = (FragmentCompletionSuccessEvent) fragmentEvent;
            executedFragments.add(successEvent.getFragmentId());

            // add runtime stats to the fragmentStatsProvider
            createRuntimeStats(successEvent.getMapOutputStats()).ifPresent(
                    stats -> fragmentStatsProvider.putStats(session.getQueryId(), successEvent.getFragmentId(), stats));

            // Re-optimize plan.
            PlanNode optimizedPlan = planAndFragments.getRemainingPlan().get();
            for (PlanOptimizer optimizer : adaptivePlanOptimizers) {
                optimizedPlan = optimizer.optimize(optimizedPlan, session, TypeProvider.viewOf(variableAllocator.getVariables()), variableAllocator, idAllocator, warningCollector).getPlanNode();
            }

            if (!optimizedPlan.equals(planAndFragments.getRemainingPlan().get())) {
                log.info("adaptive plan optimizations triggered");
            }

            // Call the iterative fragmenter on the remaining plan that has not yet been submitted for execution.
            planAndFragments = iterativePlanFragmenter.createReadySubPlans(optimizedPlan);
        }

        verify(planAndFragments.getReadyFragments().size() == 1, "The last step of the adaptive execution is expected to have a single fragment remaining.");
        SubPlan finalFragment = planAndFragments.getReadyFragments().get(0);

        setFinalFragmentedPlan(finalFragment);

        return executeFinalFragment(finalFragment, tableWriteInfo);
    }

    private static Set<PlanFragmentId> getRootChildNodeFragmentIDs(PlanNode rootPlanNode)
    {
        return PlanNodeSearcher.searchFrom(rootPlanNode)
                .recurseOnlyWhen(node -> !(node instanceof ExchangeNode && ((ExchangeNode) node).getScope() == ExchangeNode.Scope.REMOTE_STREAMING))
                .where(node1 -> node1 instanceof RemoteSourceNode)
                .findAll()
                .stream()
                .map(n -> ((RemoteSourceNode) n).getSourceFragmentIds())
                .flatMap(l -> l.stream())
                .collect(Collectors.toSet());
    }

    private boolean isCoordinatorOnly(Plan plan)
    {
        if (!(plan.getRoot() instanceof OutputNode)) {
            return false;
        }

        PlanNode outputSourceNode = ((OutputNode) plan.getRoot()).getSource();
        return isCoordinatorOnlyDistribution(outputSourceNode);
    }

    private void publishFragmentCompletionEvent(FragmentCompletionEvent fragmentCompletionEvent)
    {
        try {
            fragmentEventQueue.put(fragmentCompletionEvent);
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Execute the final fragment of the plan and collect the result.
     */
    private List<Tuple2<MutablePartitionId, PrestoSparkSerializedPage>> executeFinalFragment(SubPlan finalFragment, TableWriteInfo tableWriteInfo)
            throws SparkException, TimeoutException
    {
        if (finalFragment.getFragment().getPartitioning().equals(COORDINATOR_DISTRIBUTION)) {
            Map<PlanFragmentId, RddAndMore<PrestoSparkSerializedPage>> inputRdds = new HashMap<>();
            for (SubPlan child : finalFragment.getChildren()) {
                inputRdds.put(child.getFragment().getId(), getRdd(child.getFragment().getId()).get());
            }
            return collectPages(tableWriteInfo, finalFragment.getFragment(), inputRdds);
        }

        RddAndMore rddAndMore = createRddForSubPlan(finalFragment, tableWriteInfo, Optional.of(PrestoSparkSerializedPage.class));
        return rddAndMore.collectAndDestroyDependenciesWithTimeout(computeNextTimeout(queryCompletionDeadline), MILLISECONDS, waitTimeMetrics);
    }

    /**
     * Event for the completion of a fragment's execution.
     */
    private class FragmentCompletionEvent
    {
        protected final PlanFragmentId fragmentId;

        private FragmentCompletionEvent(PlanFragmentId fragmentId)
        {
            this.fragmentId = fragmentId;
        }

        public PlanFragmentId getFragmentId()
        {
            return fragmentId;
        }
    }

    private class FragmentCompletionSuccessEvent
            extends FragmentCompletionEvent
    {
        private Optional<MapOutputStatistics> mapOutputStats;

        private FragmentCompletionSuccessEvent(PlanFragmentId fragmentId, Optional<MapOutputStatistics> mapOutputStats)
        {
            super(fragmentId);
            this.mapOutputStats = mapOutputStats;
        }

        public Optional<MapOutputStatistics> getMapOutputStats()
        {
            return mapOutputStats;
        }
    }

    private class FragmentCompletionFailureEvent
            extends FragmentCompletionEvent
    {
        private Throwable executionError;

        private FragmentCompletionFailureEvent(PlanFragmentId fragmentId, Throwable executionError)
        {
            super(fragmentId);
            this.executionError = executionError;
        }

        public Throwable getExecutionError()
        {
            return executionError;
        }
    }
}
