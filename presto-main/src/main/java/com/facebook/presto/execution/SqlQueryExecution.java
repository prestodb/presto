/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.facebook.presto.event.query.QueryMonitor;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.sql.analyzer.AnalysisResult;
import com.facebook.presto.sql.analyzer.Analyzer;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.DistributedExecutionPlanner;
import com.facebook.presto.sql.planner.DistributedLogicalPlanner;
import com.facebook.presto.sql.planner.LogicalPlanner;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.StageExecutionPlan;
import com.facebook.presto.sql.planner.SubPlan;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.util.IterableTransformer;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.execution.FailureInfo.toFailures;
import static com.facebook.presto.execution.StageInfo.getAllStages;
import static com.facebook.presto.execution.StageInfo.stageStateGetter;
import static com.facebook.presto.execution.TaskInfo.taskIdGetter;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.any;
import static com.google.common.collect.Iterables.transform;

@ThreadSafe
public class SqlQueryExecution
        implements QueryExecution
{
    private static final Logger log = Logger.get(SqlQueryExecution.class);
    private static final String ROOT_OUTPUT_BUFFER_NAME = "out";

    private final String queryId;
    private final String sql;
    private final Session session;
    private final Metadata metadata;
    private final SplitManager splitManager;
    private final StageManager stageManager;
    private final LocationFactory locationFactory;
    private final QueryMonitor queryMonitor;

    private final QueryStats queryStats = new QueryStats();

    // All state changes must occur within a synchronized lock.
    // The only reason we use an atomic reference here is so read-only threads don't have to block.
    @GuardedBy("this")
    private final AtomicReference<QueryState> queryState = new AtomicReference<>(QueryState.QUEUED);
    @GuardedBy("this")
    private final AtomicReference<StageExecution> outputStage = new AtomicReference<>();
    @GuardedBy("this")
    private final AtomicReference<ImmutableList<String>> fieldNames = new AtomicReference<>(ImmutableList.<String>of());

    private final LinkedBlockingQueue<Throwable> failureCauses = new LinkedBlockingQueue<>();

    public SqlQueryExecution(String queryId,
            String sql,
            Session session,
            Metadata metadata,
            SplitManager splitManager,
            StageManager stageManager,
            LocationFactory locationFactory,
            QueryMonitor queryMonitor)
    {
        checkNotNull(queryId, "queryId is null");
        checkNotNull(sql, "sql is null");
        checkNotNull(session, "session is null");
        checkNotNull(metadata, "metadata is null");
        checkNotNull(splitManager, "splitManager is null");
        checkNotNull(stageManager, "stageManager is null");
        checkNotNull(locationFactory, "locationFactory is null");
        checkNotNull(queryMonitor, "queryMonitor is null");

        this.queryId = queryId;
        this.sql = sql;

        this.session = session;
        this.metadata = metadata;
        this.splitManager = splitManager;
        this.stageManager = stageManager;
        this.locationFactory = locationFactory;
        this.queryMonitor = queryMonitor;
    }

    @Override
    public String getQueryId()
    {
        return queryId;
    }

    @Override
    public QueryInfo getQueryInfo()
    {
        StageExecution outputStage = this.outputStage.get();
        StageInfo stageInfo = null;
        if (outputStage != null) {
            stageInfo = outputStage.getStageInfo();
        }

        return new QueryInfo(queryId,
                session,
                queryState.get(),
                locationFactory.createQueryLocation(queryId),
                fieldNames.get(),
                sql,
                queryStats,
                stageInfo,
                toFailures(failureCauses));
    }

    @Override
    public void start()
    {
        Preconditions.checkState(!Thread.holdsLock(this), "Can not start while holding a lock on this");

        // transition to scheduling
        synchronized (this) {
            Preconditions.checkState(queryState.compareAndSet(QueryState.QUEUED, QueryState.PLANNING), "Stage has already been started");
        }

        try {
            // query is now started
            queryStats.recordAnalysisStart();

            // analyze query
            SubPlan subplan = analyzeQuery();

            // plan distribution of query
            planDistribution(subplan);

            // transition to starting
            synchronized (this) {
                Preconditions.checkState(queryState.compareAndSet(QueryState.PLANNING, QueryState.STARTING),
                        "Expected query to be in state %s",
                        QueryState.PLANNING);
            }

            // start the query execution
            startStage(outputStage.get(), ImmutableList.of(ROOT_OUTPUT_BUFFER_NAME));
        }
        catch (Exception e) {
            synchronized (this) {
                failureCauses.add(e);
                queryStats.recordEnd();
                queryState.set(QueryState.FAILED);
                queryMonitor.completionEvent(getQueryInfo());
            }
            cancel();
        }
    }

    private SubPlan analyzeQuery()
    {
        Preconditions.checkState(!Thread.holdsLock(this), "Can not analyse while holding a lock on this");

        // time analysis phase
        long analysisStart = System.nanoTime();

        // parse query
        Statement statement = SqlParser.createStatement(sql);

        // analyze query
        Analyzer analyzer = new Analyzer(session, metadata);
        AnalysisResult analysis = analyzer.analyze(statement);

        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        // plan query
        LogicalPlanner logicalPlanner = new LogicalPlanner(session, metadata, idAllocator);
        PlanNode plan = logicalPlanner.plan(analysis);

        // fragment the plan
        SubPlan subplan = new DistributedLogicalPlanner(metadata, idAllocator).createSubplans(plan, analysis.getSymbolAllocator(), false);

        queryStats.recordAnalysisTime(analysisStart);
        return subplan;
    }

    private void planDistribution(SubPlan subplan)
    {
        Preconditions.checkState(!Thread.holdsLock(this), "Can not perform distributed planning while holding a lock on this");

        // time distribution planning
        long distributedPlanningStart = System.nanoTime();

        // plan the execution on the active nodes
        DistributedExecutionPlanner distributedPlanner = new DistributedExecutionPlanner(splitManager, session);
        StageExecutionPlan outputStageExecutionPlan = distributedPlanner.plan(subplan);

        synchronized (this) {
            QueryState queryState = this.queryState.get();
            Preconditions.checkState(queryState == QueryState.PLANNING,
                    "Expected query to be in state %s but was in state %s",
                    QueryState.PLANNING,
                    queryState);

            // record field names
            fieldNames.set(ImmutableList.copyOf(outputStageExecutionPlan.getFieldNames()));

            // build the stage execution objects (this doesn't schedule execution)
            StageExecution outputStage = stageManager.createStage(session,
                    queryId,
                    this.queryState,
                    outputStageExecutionPlan);

            this.outputStage.set(outputStage);
        }

        // record planning time
        queryStats.recordDistributedPlanningTime(distributedPlanningStart);
    }

    private void startStage(StageExecution stage, List<String> outputIds)
    {
        Preconditions.checkState(!Thread.holdsLock(this), "Can not start while holding a lock on this");

        // if query is not finished, start the stage, otherwise cancel it
        if (!queryState.get().isDone()) {
            stage.startTasks(outputIds);
        } else {
            stage.cancel();
        }

        // for each partition in stage create an outputBuffer
        List<String> stageTaskIds = IterableTransformer.on(stage.getStageInfo().getTasks()).transform(taskIdGetter()).list();

        // start all sub stages before starting the main stage
        for (StageExecution subStage : stage.getSubStages()) {
            startStage(subStage, stageTaskIds);
        }
    }

    @Override
    public void cancel()
    {
        Preconditions.checkState(!Thread.holdsLock(this), "Can not cancel while holding a lock on this");

        // transition to canceled state, only if not already finished
        synchronized (this) {
            if (!queryState.get().isDone()) {
                queryStats.recordEnd();
                queryState.set(QueryState.CANCELED);
                queryMonitor.completionEvent(getQueryInfo());
                log.debug("Cancelling query %s", queryId);
            }
        }

        StageExecution stageExecution = outputStage.get();
        if (stageExecution != null) {
            stageExecution.cancel();
        }
    }

    @Override
    public void fail(Throwable cause)
    {
        Preconditions.checkState(!Thread.holdsLock(this), "Can not cancel while holding a lock on this");

        // transition to canceled state, only if not already finished
        synchronized (this) {
            if (!queryState.get().isDone()) {
                failureCauses.add(cause);
                queryStats.recordEnd();
                queryState.set(QueryState.FAILED);
                queryMonitor.completionEvent(getQueryInfo());
                log.debug("Failing query %s", queryId);
            }
        }

        StageExecution stageExecution = outputStage.get();
        if (stageExecution != null) {
            stageExecution.cancel();
        }
    }

    public void updateState()
    {
        Preconditions.checkState(!Thread.holdsLock(this), "Can not update while holding a lock on this");

        StageExecution outputStage = this.outputStage.get();
        if (outputStage == null) {
            return;
        }
        outputStage.updateState();

        synchronized (this) {
            if (queryState.get().isDone()) {
                return;
            }

            // if output stage is done, transition to done
            StageInfo outputStageInfo = outputStage.getStageInfo();
            StageState outputStageState = outputStageInfo.getState();
            if (outputStageState.isDone()) {
                if (outputStageState == StageState.FAILED) {
                    this.queryState.set(QueryState.FAILED);
                } else {
                    this.queryState.set(QueryState.FINISHED);
                }
                queryStats.recordEnd();
                queryMonitor.completionEvent(getQueryInfo());
            } else if (queryState.get() == QueryState.STARTING) {
                // if output stage is running transition to running
                if (outputStageState == StageState.RUNNING) {
                    this.queryState.set(QueryState.RUNNING);
                }

                // if any stage is running, record execution start time
                if (any(transform(getAllStages(outputStage.getStageInfo()), stageStateGetter()), isStageRunningOrDone())) {
                    queryStats.recordExecutionStart();
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
}
