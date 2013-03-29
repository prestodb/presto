/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.facebook.presto.event.query.QueryMonitor;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.NodeManager;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.sql.analyzer.AnalysisResult;
import com.facebook.presto.sql.analyzer.Analyzer;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.planner.DistributedExecutionPlanner;
import com.facebook.presto.sql.planner.DistributedLogicalPlanner;
import com.facebook.presto.sql.planner.LogicalPlanner;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.StageExecutionPlan;
import com.facebook.presto.sql.planner.SubPlan;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.tree.Statement;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.net.URI;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.execution.StageInfo.getAllStages;
import static com.facebook.presto.execution.StageInfo.stageStateGetter;
import static com.facebook.presto.util.FutureUtils.waitForFuture;
import static com.facebook.presto.util.Threads.threadsNamed;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.any;
import static com.google.common.collect.Iterables.transform;

@ThreadSafe
public class SqlQueryExecution
        implements QueryExecution
{
    private static final String ROOT_OUTPUT_BUFFER_NAME = "out";

    private final QueryStateMachine stateMachine;

    private final Statement statement;
    private final Metadata metadata;
    private final SplitManager splitManager;
    private final NodeManager nodeManager;
    private final List<PlanOptimizer> planOptimizers;
    private final RemoteTaskFactory remoteTaskFactory;
    private final LocationFactory locationFactory;
    private final int maxPendingSplitsPerNode;
    private final ExecutorService queryExecutor;

    private final AtomicReference<SqlStageExecution> outputStage = new AtomicReference<>();

    public SqlQueryExecution(String queryId,
            String query,
            Session session,
            URI self,
            Statement statement,
            Metadata metadata,
            SplitManager splitManager,
            NodeManager nodeManager,
            List<PlanOptimizer> planOptimizers,
            RemoteTaskFactory remoteTaskFactory,
            LocationFactory locationFactory,
            QueryMonitor queryMonitor,
            int maxPendingSplitsPerNode,
            ExecutorService queryExecutor)
    {
        this.statement = checkNotNull(statement, "statement is null");
        this.metadata = checkNotNull(metadata, "metadata is null");
        this.splitManager = checkNotNull(splitManager, "splitManager is null");
        this.nodeManager = checkNotNull(nodeManager, "nodeManager is null");
        this.planOptimizers = checkNotNull(planOptimizers, "planOptimizers is null");
        this.remoteTaskFactory = checkNotNull(remoteTaskFactory, "remoteTaskFactory is null");
        this.locationFactory = checkNotNull(locationFactory, "locationFactory is null");
        this.queryExecutor = checkNotNull(queryExecutor, "queryExecutor is null");

        checkArgument(maxPendingSplitsPerNode > 0, "maxPendingSplitsPerNode must be greater than 0");
        this.maxPendingSplitsPerNode = maxPendingSplitsPerNode;

        checkNotNull(queryId, "queryId is null");
        checkNotNull(query, "query is null");
        checkNotNull(session, "session is null");
        checkNotNull(self, "self is null");
        checkNotNull(queryMonitor, "queryMonitor is null");
        this.stateMachine = new QueryStateMachine(queryId, query, session, self, queryMonitor);
    }

    @Override
    public void start()
    {
        try {
            // transition to planning
            if (!stateMachine.beginPlanning()) {
                // query already started or finished
                return;
            }

            // analyze query
            SubPlan subplan = analyzeQuery();

            // plan distribution of query
            planDistribution(subplan);

            // transition to starting
            if (!stateMachine.starting()) {
                // query already started or finished
                return;
            }

            // if query is not finished, start the stage, otherwise cancel it
            SqlStageExecution stage = outputStage.get();

            if (!stateMachine.isDone()) {
                stage.addOutputBuffer(ROOT_OUTPUT_BUFFER_NAME);
                stage.noMoreOutputBuffers();
                stage.start();
            }
            else {
                stage.cancel();
            }
        }
        catch (Exception e) {
            fail(e);
        }
    }

    private SubPlan analyzeQuery()
    {
        // time analysis phase
        long analysisStart = System.nanoTime();

        // analyze query
        Analyzer analyzer = new Analyzer(stateMachine.getSession(), metadata);
        AnalysisResult analysis = analyzer.analyze(statement);

        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        // plan query
        LogicalPlanner logicalPlanner = new LogicalPlanner(stateMachine.getSession(), planOptimizers, idAllocator);
        PlanNode plan = logicalPlanner.plan(analysis);

        // fragment the plan
        SubPlan subplan = new DistributedLogicalPlanner(metadata, idAllocator).createSubplans(plan, analysis.getSymbolAllocator(), false);

        stateMachine.getStats().recordAnalysisTime(analysisStart);
        return subplan;
    }

    private void planDistribution(SubPlan subplan)
    {
        // time distribution planning
        long distributedPlanningStart = System.nanoTime();

        // plan the execution on the active nodes
        DistributedExecutionPlanner distributedPlanner = new DistributedExecutionPlanner(splitManager, stateMachine.getSession());
        StageExecutionPlan outputStageExecutionPlan = distributedPlanner.plan(subplan);

        if (stateMachine.isDone()) {
            return;
        }

        // record field names
        stateMachine.setOutputFieldNames(outputStageExecutionPlan.getFieldNames());

        // build the stage execution objects (this doesn't schedule execution)
        SqlStageExecution outputStage = new SqlStageExecution(stateMachine.getQueryId(),
                locationFactory,
                outputStageExecutionPlan,
                nodeManager,
                remoteTaskFactory,
                stateMachine.getSession(),
                maxPendingSplitsPerNode,
                queryExecutor);
        this.outputStage.set(outputStage);

        // record planning time
        stateMachine.getStats().recordDistributedPlanningTime(distributedPlanningStart);
    }

    @Override
    public void cancel()
    {
        stateMachine.cancel();
        cancelOutputStage();
    }

    private void cancelOutputStage()
    {
        SqlStageExecution stageExecution = outputStage.get();
        if (stageExecution != null) {
            stageExecution.cancel();
        }
    }

    @Override
    public void cancelStage(String stageId)
    {
        Preconditions.checkNotNull(stageId, "stageId is null");

        SqlStageExecution stageExecution = outputStage.get();
        if (stageExecution != null) {
            stageExecution.cancelStage(stageId);
        }
    }

    @Override
    public void fail(Throwable cause)
    {
        // transition to failed state, only if not already finished
        stateMachine.fail(cause);
        cancelOutputStage();
    }

    @Override
    public QueryInfo getQueryInfo()
    {
        SqlStageExecution outputStage = this.outputStage.get();
        StageInfo stageInfo = null;
        if (outputStage != null) {
            stageInfo = outputStage.getStageInfo();
        }
        return stateMachine.getQueryInfo(stageInfo);
    }

    @Override
    public void updateState(boolean forceRefresh)
    {
        if (!forceRefresh) {
            if (stateMachine.isDone()) {
                return;
            }
        }

        SqlStageExecution outputStage = this.outputStage.get();
        if (outputStage == null) {
            return;
        }
        waitForFuture(outputStage.updateState(forceRefresh));

        outputStage.updateState(forceRefresh);

        if (!stateMachine.isDone()) {
            // if output stage is done, transition to done
            StageInfo outputStageInfo = outputStage.getStageInfo();
            StageState outputStageState = outputStageInfo.getState();
            if (outputStageState.isDone()) {
                if (outputStageState == StageState.FAILED) {
                    stateMachine.fail(failureCause(outputStageInfo));
                }
                else if (outputStageState == StageState.CANCELED) {
                    stateMachine.cancel();
                }
                else {
                    stateMachine.finished();
                }
            }
            else if (stateMachine.getQueryState() == QueryState.STARTING) {
                // if output stage is running,  transition query to running...
                if (outputStageState == StageState.RUNNING) {
                    stateMachine.running();
                }

                // if any stage is running, record execution start time
                if (any(transform(getAllStages(outputStage.getStageInfo()), stageStateGetter()), isStageRunningOrDone())) {
                    stateMachine.getStats().recordExecutionStart();
                }
            }
        }
    }

    private static Throwable failureCause(StageInfo stageInfo)
    {
        if (!stageInfo.getFailures().isEmpty()) {
            return stageInfo.getFailures().get(0).toException();
        }

        for (TaskInfo taskInfo : stageInfo.getTasks()) {
            if (!taskInfo.getFailures().isEmpty()) {
                return taskInfo.getFailures().get(0).toException();
            }
        }

        for (StageInfo subStageInfo : stageInfo.getSubStages()) {
            Throwable cause = failureCause(subStageInfo);
            if (cause != null) {
                return cause;
            }
        }

        return null;
    }

    private static Predicate<StageState> isStageRunningOrDone()
    {
        return new Predicate<StageState>()
        {
            @Override
            public boolean apply(StageState stageState)
            {
                return stageState == StageState.RUNNING || stageState.isDone();
            }
        };
    }

    public static class SqlQueryExecutionFactory
            implements QueryExecutionFactory<SqlQueryExecution>
    {
        private final int maxPendingSplitsPerNode;
        private final Metadata metadata;
        private final SplitManager splitManager;
        private final NodeManager nodeManager;
        private final List<PlanOptimizer> planOptimizers;
        private final RemoteTaskFactory remoteTaskFactory;
        private final LocationFactory locationFactory;
        private final QueryMonitor queryMonitor;

        private final ExecutorService queryExecutor;

        @Inject
        SqlQueryExecutionFactory(QueryManagerConfig config,
                Metadata metadata,
                QueryMonitor queryMonitor,
                LocationFactory locationFactory,
                SplitManager splitManager,
                NodeManager nodeManager,
                List<PlanOptimizer> planOptimizers,
                RemoteTaskFactory remoteTaskFactory)
        {
            Preconditions.checkNotNull(config, "config is null");
            this.maxPendingSplitsPerNode = config.getMaxPendingSplitsPerNode();
            this.metadata = checkNotNull(metadata, "metadata is null");
            this.queryMonitor = checkNotNull(queryMonitor, "queryMonitor is null");
            this.locationFactory = checkNotNull(locationFactory, "locationFactory is null");
            this.splitManager = checkNotNull(splitManager, "splitManager is null");
            this.nodeManager = checkNotNull(nodeManager, "nodeManager is null");
            this.planOptimizers = checkNotNull(planOptimizers, "planOptimizers is null");
            this.remoteTaskFactory = checkNotNull(remoteTaskFactory, "remoteTaskFactory is null");

            this.queryExecutor = Executors.newCachedThreadPool(threadsNamed("query-scheduler-%d"));
        }

        @Override
        public SqlQueryExecution createQueryExecution(String queryId, String query, Session session, Statement statement)
        {
            SqlQueryExecution queryExecution = new SqlQueryExecution(queryId,
                    query,
                    session,
                    locationFactory.createQueryLocation(queryId),
                    statement,
                    metadata,
                    splitManager,
                    nodeManager,
                    planOptimizers,
                    remoteTaskFactory,
                    locationFactory,
                    queryMonitor,
                    maxPendingSplitsPerNode,
                    queryExecutor);

            return queryExecution;
        }
    }
}
