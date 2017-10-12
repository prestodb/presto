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
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.execution.scheduler.ExecutionPolicy;
import com.facebook.presto.execution.scheduler.NodeScheduler;
import com.facebook.presto.execution.scheduler.SplitSchedulerStats;
import com.facebook.presto.execution.scheduler.SqlQueryScheduler;
import com.facebook.presto.failureDetector.FailureDetector;
import com.facebook.presto.memory.VersionedMemoryPoolId;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.resourceGroups.QueryType;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.split.SplitSource;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.Analyzer;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
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
import com.facebook.presto.sql.tree.CreateTableAsSelect;
import com.facebook.presto.sql.tree.Delete;
import com.facebook.presto.sql.tree.DescribeInput;
import com.facebook.presto.sql.tree.DescribeOutput;
import com.facebook.presto.sql.tree.Explain;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Insert;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.ShowCatalogs;
import com.facebook.presto.sql.tree.ShowColumns;
import com.facebook.presto.sql.tree.ShowCreate;
import com.facebook.presto.sql.tree.ShowFunctions;
import com.facebook.presto.sql.tree.ShowGrants;
import com.facebook.presto.sql.tree.ShowPartitions;
import com.facebook.presto.sql.tree.ShowSchemas;
import com.facebook.presto.sql.tree.ShowSession;
import com.facebook.presto.sql.tree.ShowStats;
import com.facebook.presto.sql.tree.ShowTables;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.SetThreadName;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static com.facebook.presto.OutputBuffers.BROADCAST_PARTITION_ID;
import static com.facebook.presto.OutputBuffers.createInitialEmptyOutputBuffers;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.resourceGroups.QueryType.DELETE;
import static com.facebook.presto.spi.resourceGroups.QueryType.DESCRIBE;
import static com.facebook.presto.spi.resourceGroups.QueryType.EXPLAIN;
import static com.facebook.presto.spi.resourceGroups.QueryType.INSERT;
import static com.facebook.presto.spi.resourceGroups.QueryType.SELECT;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

@ThreadSafe
public final class SqlQueryExecution
        implements QueryExecution
{
    private static final Logger log = Logger.get(SqlQueryExecution.class);

    private static final OutputBufferId OUTPUT_BUFFER_ID = new OutputBufferId(0);

    private final QueryStateMachine stateMachine;

    private final Statement statement;
    private final Metadata metadata;
    private final AccessControl accessControl;
    private final SqlParser sqlParser;
    private final SplitManager splitManager;
    private final NodePartitioningManager nodePartitioningManager;
    private final NodeScheduler nodeScheduler;
    private final List<PlanOptimizer> planOptimizers;
    private final RemoteTaskFactory remoteTaskFactory;
    private final LocationFactory locationFactory;
    private final int scheduleSplitBatchSize;
    private final ExecutorService queryExecutor;
    private final FailureDetector failureDetector;

    private final QueryExplainer queryExplainer;
    private final PlanFlattener planFlattener;
    private final CostCalculator costCalculator;
    private final AtomicReference<SqlQueryScheduler> queryScheduler = new AtomicReference<>();
    private final AtomicReference<Plan> queryPlan = new AtomicReference<>();
    private final NodeTaskMap nodeTaskMap;
    private final ExecutionPolicy executionPolicy;
    private final List<Expression> parameters;
    private final SplitSchedulerStats schedulerStats;

    public SqlQueryExecution(QueryId queryId,
            String query,
            Session session,
            URI self,
            Statement statement,
            TransactionManager transactionManager,
            Metadata metadata,
            AccessControl accessControl,
            SqlParser sqlParser,
            SplitManager splitManager,
            NodePartitioningManager nodePartitioningManager,
            NodeScheduler nodeScheduler,
            CostCalculator costCalculator,
            List<PlanOptimizer> planOptimizers,
            RemoteTaskFactory remoteTaskFactory,
            LocationFactory locationFactory,
            int scheduleSplitBatchSize,
            ExecutorService queryExecutor,
            FailureDetector failureDetector,
            NodeTaskMap nodeTaskMap,
            QueryExplainer queryExplainer,
            PlanFlattener planFlattener,
            ExecutionPolicy executionPolicy,
            List<Expression> parameters,
            SplitSchedulerStats schedulerStats)
    {
        try (SetThreadName ignored = new SetThreadName("Query-%s", queryId)) {
            this.statement = requireNonNull(statement, "statement is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.accessControl = requireNonNull(accessControl, "accessControl is null");
            this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
            this.splitManager = requireNonNull(splitManager, "splitManager is null");
            this.nodePartitioningManager = requireNonNull(nodePartitioningManager, "nodePartitioningManager is null");
            this.nodeScheduler = requireNonNull(nodeScheduler, "nodeScheduler is null");
            this.costCalculator = requireNonNull(costCalculator, "costCalculator is null");
            this.planOptimizers = requireNonNull(planOptimizers, "planOptimizers is null");
            this.locationFactory = requireNonNull(locationFactory, "locationFactory is null");
            this.queryExecutor = requireNonNull(queryExecutor, "queryExecutor is null");
            this.failureDetector = requireNonNull(failureDetector, "failureDetector is null");
            this.nodeTaskMap = requireNonNull(nodeTaskMap, "nodeTaskMap is null");
            this.executionPolicy = requireNonNull(executionPolicy, "executionPolicy is null");
            this.queryExplainer = requireNonNull(queryExplainer, "queryExplainer is null");
            this.planFlattener = requireNonNull(planFlattener, "planFlattener is null");
            this.parameters = requireNonNull(parameters);
            this.schedulerStats = requireNonNull(schedulerStats, "schedulerStats is null");

            checkArgument(scheduleSplitBatchSize > 0, "scheduleSplitBatchSize must be greater than 0");
            this.scheduleSplitBatchSize = scheduleSplitBatchSize;

            requireNonNull(queryId, "queryId is null");
            requireNonNull(query, "query is null");
            requireNonNull(session, "session is null");
            requireNonNull(self, "self is null");
            this.stateMachine = QueryStateMachine.begin(queryId, query, session, self, false, transactionManager, accessControl, queryExecutor, metadata);

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
    public long getTotalMemoryReservation()
    {
        // acquire reference to outputStage before checking finalQueryInfo, because
        // state change listener sets finalQueryInfo and then clears outputStage when
        // the query finishes.
        SqlQueryScheduler scheduler = queryScheduler.get();
        Optional<QueryInfo> finalQueryInfo = stateMachine.getFinalQueryInfo();
        if (finalQueryInfo.isPresent()) {
            return finalQueryInfo.get().getQueryStats().getTotalMemoryReservation().toBytes();
        }
        if (scheduler == null) {
            return 0;
        }
        return scheduler.getTotalMemoryReservation();
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
    public Session getSession()
    {
        return stateMachine.getSession();
    }

    @Override
    public void start()
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
    public void addFinalQueryInfoListener(StateChangeListener<QueryInfo> stateChangeListener)
    {
        stateMachine.addQueryInfoStateChangeListener(stateChangeListener);
    }

    @Override
    public Optional<QueryType> getQueryType()
    {
        if (statement instanceof Query) {
            return Optional.of(SELECT);
        }
        else if (statement instanceof Explain) {
            return Optional.of(EXPLAIN);
        }
        else if (statement instanceof ShowCatalogs || statement instanceof ShowCreate || statement instanceof ShowFunctions ||
                statement instanceof ShowGrants || statement instanceof ShowPartitions || statement instanceof ShowSchemas ||
                statement instanceof ShowSession || statement instanceof ShowStats || statement instanceof ShowTables ||
                statement instanceof ShowColumns || statement instanceof DescribeInput || statement instanceof DescribeOutput) {
            return Optional.of(DESCRIBE);
        }
        else if (statement instanceof CreateTableAsSelect || statement instanceof Insert) {
            return Optional.of(INSERT);
        }
        else if (statement instanceof Delete) {
            return Optional.of(DELETE);
        }
        return Optional.empty();
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

        // analyze query
        Analyzer analyzer = new Analyzer(stateMachine.getSession(), metadata, sqlParser, accessControl, Optional.of(queryExplainer), parameters);
        Analysis analysis = analyzer.analyze(statement);

        stateMachine.setUpdateType(analysis.getUpdateType());

        // plan query
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        LogicalPlanner logicalPlanner = new LogicalPlanner(stateMachine.getSession(), planOptimizers, idAllocator, metadata, sqlParser, costCalculator);
        Plan plan = logicalPlanner.plan(analysis);
        queryPlan.set(plan);

        // extract inputs
        List<Input> inputs = new InputExtractor(metadata, stateMachine.getSession()).extractInputs(plan.getRoot());
        stateMachine.setInputs(inputs);

        // extract output
        Optional<Output> output = new OutputExtractor().extractOutput(plan.getRoot());
        stateMachine.setOutput(output);

        // fragment the plan
        SubPlan fragmentedPlan = PlanFragmenter.createSubPlans(stateMachine.getSession(), metadata, plan);
        stateMachine.setPlan(planFlattener.flatten(fragmentedPlan, stateMachine.getSession()));

        // record analysis time
        stateMachine.recordAnalysisTime(analysisStart);

        boolean explainAnalyze = analysis.getStatement() instanceof Explain && ((Explain) analysis.getStatement()).isAnalyze();
        return new PlanRoot(fragmentedPlan, !explainAnalyze, extractConnectors(analysis));
    }

    private Set<ConnectorId> extractConnectors(Analysis analysis)
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

            Optional<QueryInfo> finalQueryInfo = stateMachine.getFinalQueryInfo();
            if (finalQueryInfo.isPresent()) {
                return finalQueryInfo.get();
            }

            return buildQueryInfo(scheduler);
        }
    }

    @Override
    public QueryState getState()
    {
        return stateMachine.getQueryState();
    }

    @Override
    public Optional<ResourceGroupId> getResourceGroup()
    {
        return stateMachine.getResourceGroup();
    }

    @Override
    public void setResourceGroup(ResourceGroupId resourceGroupId)
    {
        stateMachine.setResourceGroup(resourceGroupId);
    }

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
            implements QueryExecutionFactory<SqlQueryExecution>
    {
        private final SplitSchedulerStats schedulerStats;
        private final int scheduleSplitBatchSize;
        private final Metadata metadata;
        private final AccessControl accessControl;
        private final SqlParser sqlParser;
        private final SplitManager splitManager;
        private final NodePartitioningManager nodePartitioningManager;
        private final NodeScheduler nodeScheduler;
        private final CostCalculator costCalculator;
        private final List<PlanOptimizer> planOptimizers;
        private final RemoteTaskFactory remoteTaskFactory;
        private final TransactionManager transactionManager;
        private final QueryExplainer queryExplainer;
        private final PlanFlattener planFlattener;
        private final LocationFactory locationFactory;
        private final ExecutorService executor;
        private final FailureDetector failureDetector;
        private final NodeTaskMap nodeTaskMap;
        private final Map<String, ExecutionPolicy> executionPolicies;

        @Inject
        SqlQueryExecutionFactory(QueryManagerConfig config,
                FeaturesConfig featuresConfig,
                Metadata metadata,
                AccessControl accessControl,
                SqlParser sqlParser,
                LocationFactory locationFactory,
                SplitManager splitManager,
                NodePartitioningManager nodePartitioningManager,
                NodeScheduler nodeScheduler,
                CostCalculator costCalculator,
                PlanOptimizers planOptimizers,
                RemoteTaskFactory remoteTaskFactory,
                TransactionManager transactionManager,
                @ForQueryExecution ExecutorService executor,
                FailureDetector failureDetector,
                NodeTaskMap nodeTaskMap,
                QueryExplainer queryExplainer,
                PlanFlattener planFlattener,
                Map<String, ExecutionPolicy> executionPolicies,
                SplitSchedulerStats schedulerStats)
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
            this.remoteTaskFactory = requireNonNull(remoteTaskFactory, "remoteTaskFactory is null");
            this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
            requireNonNull(featuresConfig, "featuresConfig is null");
            this.executor = requireNonNull(executor, "executor is null");
            this.failureDetector = requireNonNull(failureDetector, "failureDetector is null");
            this.nodeTaskMap = requireNonNull(nodeTaskMap, "nodeTaskMap is null");
            this.queryExplainer = requireNonNull(queryExplainer, "queryExplainer is null");
            this.planFlattener = requireNonNull(planFlattener, "planFlattener is null");

            this.executionPolicies = requireNonNull(executionPolicies, "schedulerPolicies is null");
            this.costCalculator = requireNonNull(costCalculator, "cost calculator is null");
            this.planOptimizers = planOptimizers.get();
        }

        @Override
        public SqlQueryExecution createQueryExecution(QueryId queryId, String query, Session session, Statement statement, List<Expression> parameters)
        {
            String executionPolicyName = SystemSessionProperties.getExecutionPolicy(session);
            ExecutionPolicy executionPolicy = executionPolicies.get(executionPolicyName);
            checkArgument(executionPolicy != null, "No execution policy %s", executionPolicy);

            return new SqlQueryExecution(
                    queryId,
                    query,
                    session,
                    locationFactory.createQueryLocation(queryId),
                    statement,
                    transactionManager,
                    metadata,
                    accessControl,
                    sqlParser,
                    splitManager,
                    nodePartitioningManager,
                    nodeScheduler,
                    costCalculator,
                    planOptimizers,
                    remoteTaskFactory,
                    locationFactory,
                    scheduleSplitBatchSize,
                    executor,
                    failureDetector,
                    nodeTaskMap,
                    queryExplainer,
                    planFlattener,
                    executionPolicy,
                    parameters,
                    schedulerStats);
        }
    }
}
