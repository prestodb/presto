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
import com.facebook.presto.sql.planner.DistributedExecutionPlanner;
import com.facebook.presto.sql.planner.DistributedLogicalPlanner;
import com.facebook.presto.sql.planner.LogicalPlanner;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.StageExecutionPlan;
import com.facebook.presto.sql.planner.SubPlan;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.tree.Statement;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.execution.QueryInfo.addFailure;
import static com.facebook.presto.execution.QueryInfo.getQueryId;
import static com.facebook.presto.execution.QueryInfo.getQueryStats;
import static com.facebook.presto.execution.QueryInfo.getSession;
import static com.facebook.presto.execution.QueryInfo.updateFieldNames;
import static com.facebook.presto.execution.StageInfo.getAllStages;
import static com.facebook.presto.execution.StageInfo.stageStateGetter;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.any;
import static com.google.common.collect.Iterables.transform;

@ThreadSafe
public class SqlQueryExecution
        implements QueryExecution
{
    private static final Logger log = Logger.get(SqlQueryExecution.class);
    private static final String ROOT_OUTPUT_BUFFER_NAME = "out";

    private final Statement statement;
    private final Metadata metadata;
    private final SplitManager splitManager;
    private final NodeManager nodeManager;
    private final RemoteTaskFactory remoteTaskFactory;
    private final LocationFactory locationFactory;
    private final QueryMonitor queryMonitor;
    private final int maxPendingSplitsPerNode;
    private final ExecutorService queryExecutor;

    private final QueryExecutionState queryExecutionState = new QueryExecutionState();
    private final AtomicReference<SqlStageExecution> outputStage = new AtomicReference<>();
    private final AtomicReference<QueryInfo> queryInfo = new AtomicReference<>();

    public SqlQueryExecution(Statement statement,
            QueryInfo queryInfo,
            Metadata metadata,
            SplitManager splitManager,
            NodeManager nodeManager,
            RemoteTaskFactory remoteTaskFactory,
            LocationFactory locationFactory,
            QueryMonitor queryMonitor,
            int maxPendingSplitsPerNode,
            ExecutorService queryExecutor)
    {
        checkNotNull(statement, "statement is null");
        checkNotNull(metadata, "metadata is null");
        checkNotNull(splitManager, "splitManager is null");
        checkNotNull(nodeManager, "nodeManager is null");
        checkNotNull(remoteTaskFactory, "remoteTaskFactory is null");
        checkNotNull(locationFactory, "locationFactory is null");
        checkNotNull(queryMonitor, "queryMonitor is null");
        checkArgument(maxPendingSplitsPerNode > 0, "maxPendingSplitsPerNode must be greater than 0");
        checkNotNull(queryExecutor, "queryExecutor is null");

        this.queryInfo.set(checkNotNull(queryInfo, "queryInfo is null"));

        this.statement = statement;

        this.metadata = metadata;
        this.splitManager = splitManager;
        this.nodeManager = nodeManager;
        this.remoteTaskFactory = remoteTaskFactory;
        this.locationFactory = locationFactory;
        this.queryMonitor = queryMonitor;
        this.maxPendingSplitsPerNode = maxPendingSplitsPerNode;
        this.queryExecutor = queryExecutor;
    }

    @Override
    public void start()
    {
        try {
            // transition to planning
            queryExecutionState.transitionToPlanningState();

            // query is now started
            getQueryStats(queryInfo).recordAnalysisStart();

            // analyze query
            SubPlan subplan = analyzeQuery();

            // plan distribution of query
            planDistribution(subplan);

            // transition to starting
            queryExecutionState.transitionToStartingState();

            // if query is not finished, start the stage, otherwise cancel it
            SqlStageExecution stage = outputStage.get();

            if (!queryExecutionState.isDone()) {
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
        Analyzer analyzer = new Analyzer(getSession(queryInfo), metadata);
        AnalysisResult analysis = analyzer.analyze(statement);

        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        // plan query
        LogicalPlanner logicalPlanner = new LogicalPlanner(getSession(queryInfo), metadata, idAllocator);
        PlanNode plan = logicalPlanner.plan(analysis);

        // fragment the plan
        SubPlan subplan = new DistributedLogicalPlanner(metadata, idAllocator).createSubplans(plan, analysis.getSymbolAllocator(), false);

        getQueryStats(queryInfo).recordAnalysisTime(analysisStart);
        return subplan;
    }

    private void planDistribution(SubPlan subplan)
    {
        // time distribution planning
        long distributedPlanningStart = System.nanoTime();

        // plan the execution on the active nodes
        DistributedExecutionPlanner distributedPlanner = new DistributedExecutionPlanner(splitManager, getSession(queryInfo));
        StageExecutionPlan outputStageExecutionPlan = distributedPlanner.plan(subplan);

        queryExecutionState.checkQueryState(QueryState.PLANNING);

        // record field names
        updateFieldNames(queryInfo, ImmutableList.copyOf(outputStageExecutionPlan.getFieldNames()));

        // build the stage execution objects (this doesn't schedule execution)
        SqlStageExecution outputStage = new SqlStageExecution(getQueryId(queryInfo),
                locationFactory,
                outputStageExecutionPlan,
                nodeManager,
                remoteTaskFactory,
                getSession(queryInfo),
                maxPendingSplitsPerNode,
                queryExecutor);
        SqlQueryExecution.this.outputStage.set(outputStage);

        // record planning time
        getQueryStats(queryInfo).recordDistributedPlanningTime(distributedPlanningStart);
    }

    @Override
    public void cancel()
    {
        // transition to canceled state, only if not already finished
        if (!queryExecutionState.isDone()) {
            log.debug("Cancelling query %s", getQueryId(queryInfo));
            queryExecutionState.toState(QueryState.CANCELED);
            getQueryStats(queryInfo).recordEnd();
            queryMonitor.completionEvent(getQueryInfo());
        }

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
    public void fail(Throwable cause)
    {
        // transition to failed state, only if not already finished
        if (!queryExecutionState.isDone()) {
            log.debug("Failing query %s", getQueryId(queryInfo));
            queryExecutionState.toState(QueryState.FAILED);
            addFailure(queryInfo, cause);
            getQueryStats(queryInfo).recordEnd();
            queryMonitor.completionEvent(getQueryInfo());
        }

        cancelOutputStage();
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
    public QueryInfo getQueryInfo()
    {
        while (true) {
            SqlStageExecution outputStage = this.outputStage.get();
            StageInfo stageInfo = null;
            if (outputStage != null) {
                stageInfo = outputStage.getStageInfo();
            }

            QueryInfo currentQueryInfo = queryInfo.get();

            QueryInfo newQueryInfo = currentQueryInfo
                    .queryState(queryExecutionState.getQueryState())
                    .stageInfo(stageInfo);

            if (queryInfo.compareAndSet(currentQueryInfo, newQueryInfo)) {
                return newQueryInfo;
            }
        }
    }

    @Override
    public void updateState(boolean forceRefresh)
    {
        if (!forceRefresh) {
            synchronized (this) {
                if (queryExecutionState.isDone()) {
                    return;
                }
            }
        }

        SqlStageExecution outputStage = this.outputStage.get();
        if (outputStage == null) {
            return;
        }

        outputStage.updateState();

        if (!queryExecutionState.isDone()) {
            // if output stage is done, transition to done
            StageInfo outputStageInfo = outputStage.getStageInfo();
            StageState outputStageState = outputStageInfo.getState();
            if (outputStageState.isDone()) {
                if (outputStageState == StageState.FAILED) {
                    log.debug("Transition query %s to FAILED: output stage FAILED", getQueryId(queryInfo));
                    queryExecutionState.toState(QueryState.FAILED);
                } else if (outputStageState == StageState.CANCELED) {
                    log.debug("Transition query %s to CANCELED: output stage CANCELED", getQueryId(queryInfo));
                    queryExecutionState.toState(QueryState.CANCELED);
                } else {
                    log.debug("Transition query %s to FINISHED: output stage FINISHED", getQueryId(queryInfo));
                    queryExecutionState.toState(QueryState.FINISHED);
                }
                getQueryStats(queryInfo).recordEnd();
                queryMonitor.completionEvent(getQueryInfo());
            }
            else if (queryExecutionState.getQueryState() == QueryState.STARTING) {
                // if output stage is running,  transition query to running...
                if (outputStageState == StageState.RUNNING) {
                    queryExecutionState.transitionToRunningState();
                }

                // if any stage is running, record execution start time
                if (any(transform(getAllStages(outputStage.getStageInfo()), stageStateGetter()), isStageRunningOrDone())) {
                    getQueryStats(queryInfo).recordExecutionStart();
                }
            }
        }
    }

    private static Predicate<StageState> isStageRunningOrDone()
    {
        return new Predicate<StageState>() {
            @Override
            public boolean apply(StageState stageState)
            {
                return stageState == StageState.RUNNING || stageState.isDone();
            }
        };
    }

    public static class SqlQueryExecutionFactory
    {
        private final Metadata metadata;
        private final SplitManager splitManager;
        private final NodeManager nodeManager;
        private final RemoteTaskFactory remoteTaskFactory;
        private final LocationFactory locationFactory;
        private final QueryMonitor queryMonitor;

        @Inject
        SqlQueryExecutionFactory(
                Metadata metadata,
                QueryMonitor queryMonitor,
                LocationFactory locationFactory,
                SplitManager splitManager,
                NodeManager nodeManager,
                RemoteTaskFactory remoteTaskFactory)
        {
            this.metadata = checkNotNull(metadata, "metadata is null");
            this.queryMonitor = checkNotNull(queryMonitor, "queryMonitor is null");
            this.locationFactory = checkNotNull(locationFactory, "locationFactory is null");
            this.splitManager = checkNotNull(splitManager, "splitManager is null");
            this.nodeManager = checkNotNull(nodeManager, "nodeManager is null");
            this.remoteTaskFactory = checkNotNull(remoteTaskFactory, "remoteTaskFactory is null");
        }

        public SqlQueryExecution createSqlQueryExecution(Statement statement,
                QueryInfo queryInfo,
                int maxPendingSplitsPerNode,
                ExecutorService queryExecutor)
        {
            SqlQueryExecution queryExecution = new SqlQueryExecution(statement,
                    queryInfo,
                    metadata,
                    splitManager,
                    nodeManager,
                    remoteTaskFactory,
                    locationFactory,
                    queryMonitor,
                    maxPendingSplitsPerNode,
                    queryExecutor);

            queryMonitor.createdEvent(queryExecution.getQueryInfo());
            return queryExecution;
        }
    }
}
