/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.facebook.presto.execution.StageExecutionNode.StageStateChangeListener;
import com.facebook.presto.importer.PeriodicImportManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.ShardManager;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.Analyzer;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.planner.DistributedExecutionPlanner;
import com.facebook.presto.sql.planner.DistributedLogicalPlanner;
import com.facebook.presto.sql.planner.LogicalPlanner;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.StageExecutionPlan;
import com.facebook.presto.sql.planner.SubPlan;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.storage.StorageManager;
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
    private final NodeScheduler nodeScheduler;
    private final List<PlanOptimizer> planOptimizers;
    private final RemoteTaskFactory remoteTaskFactory;
    private final LocationFactory locationFactory;
    private final int maxPendingSplitsPerNode;
    private final ExecutorService queryExecutor;
    private final ShardManager shardManager;
    private final StorageManager storageManager;
    private final  PeriodicImportManager periodicImportManager;

    private final AtomicReference<SqlStageExecution> outputStage = new AtomicReference<>();

    public SqlQueryExecution(QueryId queryId,
            String query,
            Session session,
            URI self,
            Statement statement,
            Metadata metadata,
            SplitManager splitManager,
            NodeScheduler nodeScheduler,
            List<PlanOptimizer> planOptimizers,
            RemoteTaskFactory remoteTaskFactory,
            LocationFactory locationFactory,
            int maxPendingSplitsPerNode,
            ExecutorService queryExecutor,
            ShardManager shardManager,
            StorageManager storageManager,
            PeriodicImportManager periodicImportManager)
    {
        this.statement = checkNotNull(statement, "statement is null");
        this.metadata = checkNotNull(metadata, "metadata is null");
        this.splitManager = checkNotNull(splitManager, "splitManager is null");
        this.nodeScheduler = checkNotNull(nodeScheduler, "nodeScheduler is null");
        this.planOptimizers = checkNotNull(planOptimizers, "planOptimizers is null");
        this.remoteTaskFactory = checkNotNull(remoteTaskFactory, "remoteTaskFactory is null");
        this.locationFactory = checkNotNull(locationFactory, "locationFactory is null");
        this.queryExecutor = checkNotNull(queryExecutor, "queryExecutor is null");
        this.shardManager = checkNotNull(shardManager, "shardManager is null");
        this.storageManager = checkNotNull(storageManager, "storageManager is null");
        this.periodicImportManager = checkNotNull(periodicImportManager, "periodicImportManager is null");

        checkArgument(maxPendingSplitsPerNode > 0, "maxPendingSplitsPerNode must be greater than 0");
        this.maxPendingSplitsPerNode = maxPendingSplitsPerNode;

        checkNotNull(queryId, "queryId is null");
        checkNotNull(query, "query is null");
        checkNotNull(session, "session is null");
        checkNotNull(self, "self is null");
        this.stateMachine = new QueryStateMachine(queryId, query, session, self);
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

    @Override
    public void addListener(Runnable listener)
    {
        stateMachine.addListener(listener);
    }

    private SubPlan analyzeQuery()
    {
        // time analysis phase
        long analysisStart = System.nanoTime();

        // analyze query
        Analyzer analyzer = new Analyzer(stateMachine.getSession(), metadata);

        Analysis analysis = analyzer.analyze(statement);
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        // plan query
        LogicalPlanner logicalPlanner = new LogicalPlanner(stateMachine.getSession(), planOptimizers, idAllocator, metadata, periodicImportManager, storageManager);
        Plan plan = logicalPlanner.plan(analysis);

        // fragment the plan
        SubPlan subplan = new DistributedLogicalPlanner(metadata, idAllocator).createSubplans(plan, false);

        stateMachine.getStats().recordAnalysisTime(analysisStart);
        return subplan;
    }

    private void planDistribution(SubPlan subplan)
    {
        // time distribution planning
        long distributedPlanningStart = System.nanoTime();

        // plan the execution on the active nodes
        DistributedExecutionPlanner distributedPlanner = new DistributedExecutionPlanner(splitManager, stateMachine.getSession(), shardManager);
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
                nodeScheduler,
                remoteTaskFactory,
                stateMachine.getSession(),
                maxPendingSplitsPerNode,
                queryExecutor);
        this.outputStage.set(outputStage);
        outputStage.addStateChangeListener(new StageStateChangeListener() {
            @Override
            public void stateChanged(StageInfo stageInfo)
            {
                doUpdateState(stageInfo);
            }
        });

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
    public void cancelStage(StageId stageId)
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

        doUpdateState(outputStage.getStageInfo());
    }

    private void doUpdateState(StageInfo stageInfo)
    {
        if (!stateMachine.isDone()) {
            // if output stage is done, transition to done
            StageState outputStageState = stageInfo.getState();
            if (outputStageState.isDone()) {
                if (outputStageState == StageState.FAILED) {
                    stateMachine.fail(failureCause(stageInfo));
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
                if (any(transform(getAllStages(stageInfo), stageStateGetter()), isStageRunningOrDone())) {
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
        private final NodeScheduler nodeScheduler;
        private final List<PlanOptimizer> planOptimizers;
        private final RemoteTaskFactory remoteTaskFactory;
        private final LocationFactory locationFactory;
        private final ShardManager shardManager;
        private final StorageManager storageManager;
        private final PeriodicImportManager periodicImportManager;

        private final ExecutorService queryExecutor;

        @Inject
        SqlQueryExecutionFactory(QueryManagerConfig config,
                Metadata metadata,
                LocationFactory locationFactory,
                SplitManager splitManager,
                NodeScheduler nodeScheduler,
                List<PlanOptimizer> planOptimizers,
                RemoteTaskFactory remoteTaskFactory,
                ShardManager shardManager,
                StorageManager storageManager,
                PeriodicImportManager periodicImportManager)
        {
            Preconditions.checkNotNull(config, "config is null");
            this.maxPendingSplitsPerNode = config.getMaxPendingSplitsPerNode();
            this.metadata = checkNotNull(metadata, "metadata is null");
            this.locationFactory = checkNotNull(locationFactory, "locationFactory is null");
            this.splitManager = checkNotNull(splitManager, "splitManager is null");
            this.nodeScheduler = checkNotNull(nodeScheduler, "nodeScheduler is null");
            this.planOptimizers = checkNotNull(planOptimizers, "planOptimizers is null");
            this.remoteTaskFactory = checkNotNull(remoteTaskFactory, "remoteTaskFactory is null");
            this.shardManager = checkNotNull(shardManager, "shardManager is null");
            this.storageManager = checkNotNull(storageManager, "storageManager is null");
            this.periodicImportManager = checkNotNull(periodicImportManager, "periodicImportManager is null");

            this.queryExecutor = Executors.newCachedThreadPool(threadsNamed("query-scheduler-%d"));
        }

        @Override
        public SqlQueryExecution createQueryExecution(QueryId queryId, String query, Session session, Statement statement)
        {
            SqlQueryExecution queryExecution = new SqlQueryExecution(queryId,
                    query,
                    session,
                    locationFactory.createQueryLocation(queryId),
                    statement,
                    metadata,
                    splitManager,
                    nodeScheduler,
                    planOptimizers,
                    remoteTaskFactory,
                    locationFactory,
                    maxPendingSplitsPerNode,
                    queryExecutor,
                    shardManager,
                    storageManager,
                    periodicImportManager);

            return queryExecution;
        }
    }
}
