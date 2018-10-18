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
import com.facebook.presto.OutputBuffers.OutputBufferId;
import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.cost.CostCalculator;
import com.facebook.presto.cost.StatsCalculator;
import com.facebook.presto.execution.QueryPreparer.PreparedQuery;
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.execution.scheduler.ExecutionPolicy;
import com.facebook.presto.execution.scheduler.NodeScheduler;
import com.facebook.presto.execution.scheduler.SplitSchedulerStats;
import com.facebook.presto.execution.scheduler.SqlQueryScheduler;
import com.facebook.presto.execution.warnings.WarningCollector;
import com.facebook.presto.failureDetector.FailureDetector;
import com.facebook.presto.memory.VersionedMemoryPoolId;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.operator.ForScheduler;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.split.SplitSource;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.Analyzer;
import com.facebook.presto.sql.analyzer.QueryExplainer;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.DistributedExecutionPlanner;
import com.facebook.presto.sql.planner.InputExtractor;
import com.facebook.presto.sql.planner.LogicalPlanner;
import com.facebook.presto.sql.planner.NodePartitioningManager;
import com.facebook.presto.sql.planner.OutputExtractor;
import com.facebook.presto.sql.planner.PartitioningHandle;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.PlanFragmenter;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.PlanOptimizers;
import com.facebook.presto.sql.planner.StageExecutionPlan;
import com.facebook.presto.sql.planner.SubPlan;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.tree.Explain;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.SetThreadName;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static com.facebook.presto.OutputBuffers.BROADCAST_PARTITION_ID;
import static com.facebook.presto.OutputBuffers.createInitialEmptyOutputBuffers;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static io.airlift.concurrent.MoreFutures.addExceptionCallback;
import static io.airlift.concurrent.MoreFutures.addSuccessCallback;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.succinctBytes;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

@ThreadSafe
public class SqlQueryExecution
        implements QueryExecution
{
    private static final Logger log = Logger.get(SqlQueryExecution.class);

    private static final OutputBufferId OUTPUT_BUFFER_ID = new OutputBufferId(0);

    private final QueryStateMachine stateMachine;

    private final ClusterSizeMonitor clusterSizeMonitor;
    private final Metadata metadata;
    private final SqlParser sqlParser;
    private final SplitManager splitManager;
    private final NodePartitioningManager nodePartitioningManager;
    private final NodeScheduler nodeScheduler;
    private final List<PlanOptimizer> planOptimizers;
    private final PlanFragmenter planFragmenter;
    private final RemoteTaskFactory remoteTaskFactory;
    private final LocationFactory locationFactory;
    private final int scheduleSplitBatchSize;
    private final ExecutorService queryExecutor;
    private final ScheduledExecutorService schedulerExecutor;
    private final FailureDetector failureDetector;

    private final AtomicReference<SqlQueryScheduler> queryScheduler = new AtomicReference<>();
    private final AtomicReference<Plan> queryPlan = new AtomicReference<>();
    private final NodeTaskMap nodeTaskMap;
    private final ExecutionPolicy executionPolicy;
    private final SplitSchedulerStats schedulerStats;
    private final Analysis analysis;
    private final StatsCalculator statsCalculator;
    private final CostCalculator costCalculator;

    private SqlQueryExecution(
            String query,
            Session session,
            URI self,
            ResourceGroupId resourceGroup,
            PreparedQuery preparedQuery,
            ClusterSizeMonitor clusterSizeMonitor,
            TransactionManager transactionManager,
            Metadata metadata,
            AccessControl accessControl,
            SqlParser sqlParser,
            SplitManager splitManager,
            NodePartitioningManager nodePartitioningManager,
            NodeScheduler nodeScheduler,
            List<PlanOptimizer> planOptimizers,
            PlanFragmenter planFragmenter,
            RemoteTaskFactory remoteTaskFactory,
            LocationFactory locationFactory,
            int scheduleSplitBatchSize,
            ExecutorService queryExecutor,
            ScheduledExecutorService schedulerExecutor,
            FailureDetector failureDetector,
            NodeTaskMap nodeTaskMap,
            QueryExplainer queryExplainer,
            ExecutionPolicy executionPolicy,
            SplitSchedulerStats schedulerStats,
            StatsCalculator statsCalculator,
            CostCalculator costCalculator,
            WarningCollector warningCollector)
    {
        try (SetThreadName ignored = new SetThreadName("Query-%s", session.getQueryId())) {
            this.clusterSizeMonitor = requireNonNull(clusterSizeMonitor, "clusterSizeMonitor is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
            this.splitManager = requireNonNull(splitManager, "splitManager is null");
            this.nodePartitioningManager = requireNonNull(nodePartitioningManager, "nodePartitioningManager is null");
            this.nodeScheduler = requireNonNull(nodeScheduler, "nodeScheduler is null");
            this.planOptimizers = requireNonNull(planOptimizers, "planOptimizers is null");
            this.planFragmenter = requireNonNull(planFragmenter, "planFragmenter is null");
            this.locationFactory = requireNonNull(locationFactory, "locationFactory is null");
            this.queryExecutor = requireNonNull(queryExecutor, "queryExecutor is null");
            this.schedulerExecutor = requireNonNull(schedulerExecutor, "schedulerExecutor is null");
            this.failureDetector = requireNonNull(failureDetector, "failureDetector is null");
            this.nodeTaskMap = requireNonNull(nodeTaskMap, "nodeTaskMap is null");
            this.executionPolicy = requireNonNull(executionPolicy, "executionPolicy is null");
            this.schedulerStats = requireNonNull(schedulerStats, "schedulerStats is null");
            this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
            this.costCalculator = requireNonNull(costCalculator, "costCalculator is null");

            checkArgument(scheduleSplitBatchSize > 0, "scheduleSplitBatchSize must be greater than 0");
            this.scheduleSplitBatchSize = scheduleSplitBatchSize;

            requireNonNull(query, "query is null");
            requireNonNull(session, "session is null");
            requireNonNull(self, "self is null");
            this.stateMachine = QueryStateMachine.begin(
                    query,
                    session,
                    self,
                    resourceGroup,
                    false,
                    transactionManager,
                    accessControl,
                    queryExecutor,
                    metadata,
                    warningCollector);

            // analyze query
            requireNonNull(preparedQuery, "preparedQuery is null");
            Analyzer analyzer = new Analyzer(
                    stateMachine.getSession(),
                    metadata,
                    sqlParser,
                    accessControl,
                    Optional.of(queryExplainer),
                    preparedQuery.getParameters(),
                    warningCollector);
            this.analysis = analyzer.analyze(preparedQuery.getStatement());

            stateMachine.setUpdateType(analysis.getUpdateType());

            // when the query finishes cache the final query info, and clear the reference to the output stage
            stateMachine.addStateChangeListener(state -> {
                if (!state.isDone()) {
                    return;
                }

                // query is now done, so abort any work that is still running
                SqlQueryScheduler scheduler = queryScheduler.get();
                if (scheduler != null) {
                    scheduler.abort();
                }
            });

            this.remoteTaskFactory = new MemoryTrackingRemoteTaskFactory(requireNonNull(remoteTaskFactory, "remoteTaskFactory is null"), stateMachine);
        }
    }

    @Override
    public VersionedMemoryPoolId getMemoryPool()
    {
        return stateMachine.getMemoryPool();
    }

    @Override
    public void setMemoryPool(VersionedMemoryPoolId poolId)
    {
        stateMachine.setMemoryPool(poolId);
    }

    @Override
    public DataSize getUserMemoryReservation()
    {
        // acquire reference to scheduler before checking finalQueryInfo, because
        // state change listener sets finalQueryInfo and then clears scheduler when
        // the query finishes.
        SqlQueryScheduler scheduler = queryScheduler.get();
        Optional<QueryInfo> finalQueryInfo = stateMachine.getFinalQueryInfo();
        if (finalQueryInfo.isPresent()) {
            return finalQueryInfo.get().getQueryStats().getUserMemoryReservation();
        }
        if (scheduler == null) {
            return new DataSize(0, BYTE);
        }
        return succinctBytes(scheduler.getUserMemoryReservation());
    }

    @Override
    public DataSize getTotalMemoryReservation()
    {
        // acquire reference to scheduler before checking finalQueryInfo, because
        // state change listener sets finalQueryInfo and then clears scheduler when
        // the query finishes.
        SqlQueryScheduler scheduler = queryScheduler.get();
        Optional<QueryInfo> finalQueryInfo = stateMachine.getFinalQueryInfo();
        if (finalQueryInfo.isPresent()) {
            return finalQueryInfo.get().getQueryStats().getTotalMemoryReservation();
        }
        if (scheduler == null) {
            return new DataSize(0, BYTE);
        }
        return succinctBytes(scheduler.getTotalMemoryReservation());
    }

    @Override
    public DateTime getCreateTime()
    {
        return stateMachine.getCreateTime();
    }

    @Override
    public Optional<DateTime> getExecutionStartTime()
    {
        return stateMachine.getExecutionStartTime();
    }

    @Override
    public DateTime getLastHeartbeat()
    {
        return stateMachine.getLastHeartbeat();
    }

    @Override
    public Optional<DateTime> getEndTime()
    {
        return stateMachine.getEndTime();
    }

    @Override
    public Duration getTotalCpuTime()
    {
        SqlQueryScheduler scheduler = queryScheduler.get();
        Optional<QueryInfo> finalQueryInfo = stateMachine.getFinalQueryInfo();
        if (finalQueryInfo.isPresent()) {
            return finalQueryInfo.get().getQueryStats().getTotalCpuTime();
        }
        if (scheduler == null) {
            return new Duration(0, SECONDS);
        }
        return scheduler.getTotalCpuTime();
    }

    @Override
    public BasicQueryInfo getBasicQueryInfo()
    {
        return stateMachine.getFinalQueryInfo()
                .map(BasicQueryInfo::new)
                .orElseGet(() -> stateMachine.getBasicQueryInfo(Optional.ofNullable(queryScheduler.get()).map(SqlQueryScheduler::getBasicStageStats)));
    }

    @Override
    public void start()
    {
        if (stateMachine.transitionToWaitingForResources()) {
            waitForMinimumWorkers();
        }
    }

    private void waitForMinimumWorkers()
    {
        ListenableFuture<?> minimumWorkerFuture = clusterSizeMonitor.waitForMinimumWorkers();
        addSuccessCallback(minimumWorkerFuture, this::startExecution);
        addExceptionCallback(minimumWorkerFuture, stateMachine::transitionToFailed);
    }

    private void startExecution()
    {
        try (SetThreadName ignored = new SetThreadName("Query-%s", stateMachine.getQueryId())) {
            try {
                // transition to planning
                if (!stateMachine.transitionToPlanning()) {
                    // query already started or finished
                    return;
                }

                // analyze query
                PlanRoot plan = analyzeQuery();

                metadata.beginQuery(getSession(), plan.getConnectors());

                // plan distribution of query
                planDistribution(plan);

                // transition to starting
                if (!stateMachine.transitionToStarting()) {
                    // query already started or finished
                    return;
                }

                // if query is not finished, start the scheduler, otherwise cancel it
                SqlQueryScheduler scheduler = queryScheduler.get();

                if (!stateMachine.isDone()) {
                    scheduler.start();
                }
            }
            catch (Throwable e) {
                fail(e);
                throwIfInstanceOf(e, Error.class);
            }
        }
    }

    @Override
    public void addStateChangeListener(StateChangeListener<QueryState> stateChangeListener)
    {
        try (SetThreadName ignored = new SetThreadName("Query-%s", stateMachine.getQueryId())) {
            stateMachine.addStateChangeListener(stateChangeListener);
        }
    }

    @Override
    public Session getSession()
    {
        return stateMachine.getSession();
    }

    @Override
    public Optional<ErrorCode> getErrorCode()
    {
        return stateMachine.getFailureInfo().map(ExecutionFailureInfo::getErrorCode);
    }

    @Override
    public void addFinalQueryInfoListener(StateChangeListener<QueryInfo> stateChangeListener)
    {
        stateMachine.addQueryInfoStateChangeListener(stateChangeListener);
    }

    private PlanRoot analyzeQuery()
    {
        try {
            return doAnalyzeQuery();
        }
        catch (StackOverflowError e) {
            throw new PrestoException(NOT_SUPPORTED, "statement is too large (stack overflow during analysis)", e);
        }
    }

    private PlanRoot doAnalyzeQuery()
    {
        // time analysis phase
        long analysisStart = System.nanoTime();

        // plan query
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        LogicalPlanner logicalPlanner = new LogicalPlanner(stateMachine.getSession(), planOptimizers, idAllocator, metadata, sqlParser, statsCalculator, costCalculator, stateMachine.getWarningCollector());
        Plan plan = logicalPlanner.plan(analysis);
        queryPlan.set(plan);

        // extract inputs
        List<Input> inputs = new InputExtractor(metadata, stateMachine.getSession()).extractInputs(plan.getRoot());
        stateMachine.setInputs(inputs);

        // extract output
        Optional<Output> output = new OutputExtractor().extractOutput(plan.getRoot());
        stateMachine.setOutput(output);

        // fragment the plan
        SubPlan fragmentedPlan = planFragmenter.createSubPlans(stateMachine.getSession(), metadata, nodePartitioningManager, plan, false);

        // record analysis time
        stateMachine.recordAnalysisTime(analysisStart);

        boolean explainAnalyze = analysis.getStatement() instanceof Explain && ((Explain) analysis.getStatement()).isAnalyze();
        return new PlanRoot(fragmentedPlan, !explainAnalyze, extractConnectors(analysis));
    }

    private static Set<ConnectorId> extractConnectors(Analysis analysis)
    {
        ImmutableSet.Builder<ConnectorId> connectors = ImmutableSet.builder();

        for (TableHandle tableHandle : analysis.getTables()) {
            connectors.add(tableHandle.getConnectorId());
        }

        if (analysis.getInsert().isPresent()) {
            TableHandle target = analysis.getInsert().get().getTarget();
            connectors.add(target.getConnectorId());
        }

        return connectors.build();
    }

    private void planDistribution(PlanRoot plan)
    {
        // time distribution planning
        long distributedPlanningStart = System.nanoTime();

        // plan the execution on the active nodes
        DistributedExecutionPlanner distributedPlanner = new DistributedExecutionPlanner(splitManager);
        StageExecutionPlan outputStageExecutionPlan = distributedPlanner.plan(plan.getRoot(), stateMachine.getSession());
        stateMachine.recordDistributedPlanningTime(distributedPlanningStart);

        // ensure split sources are closed
        stateMachine.addStateChangeListener(state -> {
            if (state.isDone()) {
                closeSplitSources(outputStageExecutionPlan);
            }
        });

        // if query was canceled, skip creating scheduler
        if (stateMachine.isDone()) {
            return;
        }

        // record output field
        stateMachine.setColumns(outputStageExecutionPlan.getFieldNames(), outputStageExecutionPlan.getFragment().getTypes());

        PartitioningHandle partitioningHandle = plan.getRoot().getFragment().getPartitioningScheme().getPartitioning().getHandle();
        OutputBuffers rootOutputBuffers = createInitialEmptyOutputBuffers(partitioningHandle)
                .withBuffer(OUTPUT_BUFFER_ID, BROADCAST_PARTITION_ID)
                .withNoMoreBufferIds();

        // build the stage execution objects (this doesn't schedule execution)
        SqlQueryScheduler scheduler = new SqlQueryScheduler(
                stateMachine,
                locationFactory,
                outputStageExecutionPlan,
                nodePartitioningManager,
                nodeScheduler,
                remoteTaskFactory,
                stateMachine.getSession(),
                plan.isSummarizeTaskInfos(),
                scheduleSplitBatchSize,
                queryExecutor,
                schedulerExecutor,
                failureDetector,
                rootOutputBuffers,
                nodeTaskMap,
                executionPolicy,
                schedulerStats);

        queryScheduler.set(scheduler);

        // if query was canceled during scheduler creation, abort the scheduler
        // directly since the callback may have already fired
        if (stateMachine.isDone()) {
            scheduler.abort();
            queryScheduler.set(null);
        }
    }

    private static void closeSplitSources(StageExecutionPlan plan)
    {
        for (SplitSource source : plan.getSplitSources().values()) {
            try {
                source.close();
            }
            catch (Throwable t) {
                log.warn(t, "Error closing split source");
            }
        }

        for (StageExecutionPlan stage : plan.getSubStages()) {
            closeSplitSources(stage);
        }
    }

    @Override
    public void cancelQuery()
    {
        stateMachine.transitionToCanceled();
    }

    @Override
    public void cancelStage(StageId stageId)
    {
        requireNonNull(stageId, "stageId is null");

        try (SetThreadName ignored = new SetThreadName("Query-%s", stateMachine.getQueryId())) {
            SqlQueryScheduler scheduler = queryScheduler.get();
            if (scheduler != null) {
                scheduler.cancelStage(stageId);
            }
        }
    }

    @Override
    public void fail(Throwable cause)
    {
        requireNonNull(cause, "cause is null");

        stateMachine.transitionToFailed(cause);
    }

    @Override
    public boolean isDone()
    {
        return getState().isDone();
    }

    @Override
    public void addOutputInfoListener(Consumer<QueryOutputInfo> listener)
    {
        stateMachine.addOutputInfoListener(listener);
    }

    @Override
    public ListenableFuture<QueryState> getStateChange(QueryState currentState)
    {
        return stateMachine.getStateChange(currentState);
    }

    @Override
    public void recordHeartbeat()
    {
        stateMachine.recordHeartbeat();
    }

    @Override
    public void pruneInfo()
    {
        stateMachine.pruneQueryInfo();
    }

    @Override
    public QueryId getQueryId()
    {
        return stateMachine.getQueryId();
    }

    @Override
    public QueryInfo getQueryInfo()
    {
        try (SetThreadName ignored = new SetThreadName("Query-%s", stateMachine.getQueryId())) {
            // acquire reference to scheduler before checking finalQueryInfo, because
            // state change listener sets finalQueryInfo and then clears scheduler when
            // the query finishes.
            SqlQueryScheduler scheduler = queryScheduler.get();

            return stateMachine.getFinalQueryInfo().orElseGet(() -> buildQueryInfo(scheduler));
        }
    }

    @Override
    public QueryState getState()
    {
        return stateMachine.getQueryState();
    }

    @Override
    public Plan getQueryPlan()
    {
        return queryPlan.get();
    }

    private QueryInfo buildQueryInfo(SqlQueryScheduler scheduler)
    {
        Optional<StageInfo> stageInfo = Optional.empty();
        if (scheduler != null) {
            stageInfo = Optional.ofNullable(scheduler.getStageInfo());
        }

        QueryInfo queryInfo = stateMachine.updateQueryInfo(stageInfo);
        if (queryInfo.isFinalQueryInfo()) {
            // capture the final query state and drop reference to the scheduler
            queryScheduler.set(null);
        }

        return queryInfo;
    }

    private static class PlanRoot
    {
        private final SubPlan root;
        private final boolean summarizeTaskInfos;
        private final Set<ConnectorId> connectors;

        public PlanRoot(SubPlan root, boolean summarizeTaskInfos, Set<ConnectorId> connectors)
        {
            this.root = requireNonNull(root, "root is null");
            this.summarizeTaskInfos = summarizeTaskInfos;
            this.connectors = ImmutableSet.copyOf(connectors);
        }

        public SubPlan getRoot()
        {
            return root;
        }

        public boolean isSummarizeTaskInfos()
        {
            return summarizeTaskInfos;
        }

        public Set<ConnectorId> getConnectors()
        {
            return connectors;
        }
    }

    public static class SqlQueryExecutionFactory
            implements QueryExecutionFactory<QueryExecution>
    {
        private final SplitSchedulerStats schedulerStats;
        private final int scheduleSplitBatchSize;
        private final Metadata metadata;
        private final AccessControl accessControl;
        private final SqlParser sqlParser;
        private final SplitManager splitManager;
        private final NodePartitioningManager nodePartitioningManager;
        private final NodeScheduler nodeScheduler;
        private final List<PlanOptimizer> planOptimizers;
        private final PlanFragmenter planFragmenter;
        private final RemoteTaskFactory remoteTaskFactory;
        private final TransactionManager transactionManager;
        private final QueryExplainer queryExplainer;
        private final LocationFactory locationFactory;
        private final ExecutorService queryExecutor;
        private final ScheduledExecutorService schedulerExecutor;
        private final FailureDetector failureDetector;
        private final NodeTaskMap nodeTaskMap;
        private final Map<String, ExecutionPolicy> executionPolicies;
        private final ClusterSizeMonitor clusterSizeMonitor;
        private final StatsCalculator statsCalculator;
        private final CostCalculator costCalculator;

        @Inject
        SqlQueryExecutionFactory(QueryManagerConfig config,
                Metadata metadata,
                AccessControl accessControl,
                SqlParser sqlParser,
                LocationFactory locationFactory,
                SplitManager splitManager,
                NodePartitioningManager nodePartitioningManager,
                NodeScheduler nodeScheduler,
                PlanOptimizers planOptimizers,
                PlanFragmenter planFragmenter,
                RemoteTaskFactory remoteTaskFactory,
                TransactionManager transactionManager,
                @ForQueryExecution ExecutorService queryExecutor,
                @ForScheduler ScheduledExecutorService schedulerExecutor,
                FailureDetector failureDetector,
                NodeTaskMap nodeTaskMap,
                QueryExplainer queryExplainer,
                Map<String, ExecutionPolicy> executionPolicies,
                SplitSchedulerStats schedulerStats,
                ClusterSizeMonitor clusterSizeMonitor,
                StatsCalculator statsCalculator,
                CostCalculator costCalculator)
        {
            requireNonNull(config, "config is null");
            this.schedulerStats = requireNonNull(schedulerStats, "schedulerStats is null");
            this.scheduleSplitBatchSize = config.getScheduleSplitBatchSize();
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.accessControl = requireNonNull(accessControl, "accessControl is null");
            this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
            this.locationFactory = requireNonNull(locationFactory, "locationFactory is null");
            this.splitManager = requireNonNull(splitManager, "splitManager is null");
            this.nodePartitioningManager = requireNonNull(nodePartitioningManager, "nodePartitioningManager is null");
            this.nodeScheduler = requireNonNull(nodeScheduler, "nodeScheduler is null");
            requireNonNull(planOptimizers, "planOptimizers is null");
            this.planFragmenter = requireNonNull(planFragmenter, "planFragmenter is null");
            this.remoteTaskFactory = requireNonNull(remoteTaskFactory, "remoteTaskFactory is null");
            this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
            this.queryExecutor = requireNonNull(queryExecutor, "queryExecutor is null");
            this.schedulerExecutor = requireNonNull(schedulerExecutor, "schedulerExecutor is null");
            this.failureDetector = requireNonNull(failureDetector, "failureDetector is null");
            this.nodeTaskMap = requireNonNull(nodeTaskMap, "nodeTaskMap is null");
            this.queryExplainer = requireNonNull(queryExplainer, "queryExplainer is null");
            this.executionPolicies = requireNonNull(executionPolicies, "schedulerPolicies is null");
            this.clusterSizeMonitor = requireNonNull(clusterSizeMonitor, "clusterSizeMonitor is null");
            this.planOptimizers = planOptimizers.get();
            this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
            this.costCalculator = requireNonNull(costCalculator, "costCalculator is null");
        }

        @Override
        public QueryExecution createQueryExecution(
                String query,
                Session session,
                PreparedQuery preparedQuery,
                ResourceGroupId resourceGroup,
                WarningCollector warningCollector)
        {
            String executionPolicyName = SystemSessionProperties.getExecutionPolicy(session);
            ExecutionPolicy executionPolicy = executionPolicies.get(executionPolicyName);
            checkArgument(executionPolicy != null, "No execution policy %s", executionPolicy);

            SqlQueryExecution execution = new SqlQueryExecution(
                    query,
                    session,
                    locationFactory.createQueryLocation(session.getQueryId()),
                    resourceGroup,
                    preparedQuery,
                    clusterSizeMonitor,
                    transactionManager,
                    metadata,
                    accessControl,
                    sqlParser,
                    splitManager,
                    nodePartitioningManager,
                    nodeScheduler,
                    planOptimizers,
                    planFragmenter,
                    remoteTaskFactory,
                    locationFactory,
                    scheduleSplitBatchSize,
                    queryExecutor,
                    schedulerExecutor,
                    failureDetector,
                    nodeTaskMap,
                    queryExplainer,
                    executionPolicy,
                    schedulerStats,
                    statsCalculator,
                    costCalculator,
                    warningCollector);

            return execution;
        }
    }
}
