/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.facebook.presto.event.query.QueryMonitor;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.NodeManager;
import com.facebook.presto.server.HttpTaskClient;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.sql.analyzer.AnalysisResult;
import com.facebook.presto.sql.analyzer.Analyzer;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.DistributedExecutionPlanner;
import com.facebook.presto.sql.planner.DistributedLogicalPlanner;
import com.facebook.presto.sql.planner.LogicalPlanner;
import com.facebook.presto.sql.planner.Partition;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.StageExecutionPlan;
import com.facebook.presto.sql.planner.SubPlan;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.util.IterableTransformer;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.execution.FailureInfo.toFailures;
import static com.facebook.presto.execution.StageInfo.getAllStages;
import static com.facebook.presto.execution.StageInfo.stageStateGetter;
import static com.facebook.presto.sql.planner.Partition.nodeIdentifierGetter;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.any;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;

@ThreadSafe
public class SqlQueryExecution
        implements QueryExecution
{
    private static final String ROOT_OUTPUT_BUFFER_NAME = "out";

    private final String queryId;
    private final String sql;
    private final Session session;
    private final Metadata metadata;
    private final NodeManager nodeManager;
    private final SplitManager splitManager;
    private final StageManager stageManager;
    private final RemoteTaskFactory remoteTaskFactory;
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
            NodeManager nodeManager,
            SplitManager splitManager,
            StageManager stageManager,
            RemoteTaskFactory remoteTaskFactory,
            LocationFactory locationFactory,
            QueryMonitor queryMonitor)
    {
        checkNotNull(queryId, "queryId is null");
        checkNotNull(sql, "sql is null");
        checkNotNull(session, "session is null");
        checkNotNull(metadata, "metadata is null");
        checkNotNull(nodeManager, "nodeManager is null");
        checkNotNull(splitManager, "splitManager is null");
        checkNotNull(stageManager, "stageManager is null");
        checkNotNull(remoteTaskFactory, "remoteTaskFactory is null");
        checkNotNull(locationFactory, "locationFactory is null");
        checkNotNull(queryMonitor, "queryMonitor is null");

        this.queryId = queryId;
        this.sql = sql;

        this.session = session;
        this.metadata = metadata;
        this.nodeManager = nodeManager;
        this.splitManager = splitManager;
        this.stageManager = stageManager;
        this.remoteTaskFactory = remoteTaskFactory;
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
            startStage(outputStage.get());
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
        DistributedExecutionPlanner distributedPlanner = new DistributedExecutionPlanner(nodeManager, splitManager, queryState, session);
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
            StageExecution outputStage = createStage(new AtomicInteger(), outputStageExecutionPlan, ImmutableList.of(ROOT_OUTPUT_BUFFER_NAME));
            this.outputStage.set(outputStage);
        }

        // record planning time
        queryStats.recordDistributedPlanningTime(distributedPlanningStart);
    }

    private void startStage(StageExecution stage)
    {
        Preconditions.checkState(!Thread.holdsLock(this), "Can not start while holding a lock on this");

        // start all sub stages before starting the main stage
        for (StageExecution subStage : stage.getSubStages()) {
            startStage(subStage);
        }
        // if query is not finished, start the stage, otherwise cancel it
        if (!queryState.get().isDone()) {
            stage.startTasks();
        } else {
            stage.cancel();
        }
    }

    private StageExecution createStage(AtomicInteger nextStageId, StageExecutionPlan stageExecutionPlan, List<String> outputIds)
    {
        String stageId = queryId + "." + nextStageId.getAndIncrement();

        Set<ExchangeNode> exchanges = IterableTransformer.on(stageExecutionPlan.getFragment().getSources())
                .select(Predicates.instanceOf(ExchangeNode.class))
                .cast(ExchangeNode.class)
                .set();

        Map<String, StageExecution> subStages = IterableTransformer.on(stageExecutionPlan.getSubStages())
                .uniqueIndex(fragmentIdGetter())
                .transformValues(stageCreator(nextStageId, stageExecutionPlan.getPartitions()))
                .immutableMap();

        URI stageLocation = locationFactory.createStageLocation(stageId);
        int taskId = 0;
        ImmutableList.Builder<RemoteTask> tasks = ImmutableList.builder();
        for (Partition partition : stageExecutionPlan.getPartitions()) {
            String nodeIdentifier = partition.getNode().getNodeIdentifier();

            ImmutableMap.Builder<PlanNodeId, ExchangePlanFragmentSource> exchangeSources = ImmutableMap.builder();
            for (ExchangeNode exchange : exchanges) {
                StageExecution childStage = subStages.get(String.valueOf(exchange.getSourceFragmentId()));
                ExchangePlanFragmentSource source = childStage.getExchangeSourceFor(nodeIdentifier);

                exchangeSources.put(exchange.getId(), source);
            }

            tasks.add(remoteTaskFactory.createRemoteTask(session,
                    queryId,
                    stageId,
                    stageId + '.' + taskId++,
                    partition.getNode(),
                    stageExecutionPlan.getFragment(),
                    partition.getSplits(),
                    exchangeSources.build(),
                    outputIds));

            queryStats.addSplits(partition.getSplits().size());
        }

        return stageManager.createStage(queryId, stageId, stageLocation, stageExecutionPlan.getFragment(), tasks.build(), subStages.values());
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
                // if any stage is running transition to running
                if (any(transform(getAllStages(outputStage.getStageInfo()), stageStateGetter()), isStageRunningOrDone())) {
                    queryStats.recordExecutionStart();
                    this.queryState.set(QueryState.RUNNING);
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

    private Function<StageExecutionPlan, StageExecution> stageCreator(final AtomicInteger nextStageId, List<Partition> partitions)
    {
        // for each partition in stage create an outputBuffer
        final List<String> outputIds = IterableTransformer.on(partitions).transform(nodeIdentifierGetter()).list();
        return new Function<StageExecutionPlan, StageExecution>()
        {
            @Override
            public StageExecution apply(@Nullable StageExecutionPlan subStage)
            {
                return createStage(nextStageId, subStage, outputIds);
            }
        };
    }

    private Function<StageExecutionPlan, String> fragmentIdGetter()
    {
        return new Function<StageExecutionPlan, String>()
        {
            @Override
            public String apply(@Nullable StageExecutionPlan input)
            {
                return String.valueOf(input.getFragment().getId());
            }
        };
    }

    private static void waitForRunning(List<HttpTaskClient> taskClients)
    {
        while (true) {
            long start = System.nanoTime();

            // remove tasks that have started running
            taskClients = ImmutableList.copyOf(filter(taskClients, new Predicate<HttpTaskClient>()
            {
                @Override
                public boolean apply(HttpTaskClient taskClient)
                {
                    TaskInfo taskInfo = taskClient.getTaskInfo();
                    if (taskInfo == null) {
                        return false;
                    }
                    TaskState state = taskInfo.getState();
                    return state == TaskState.PLANNED || state == TaskState.QUEUED;
                }
            }));

            // if no tasks are left, the stage has started
            if (taskClients.isEmpty()) {
                return;
            }

            // sleep for 100ms (minus the time we spent fetching the task states)
            Duration duration = Duration.nanosSince(start);
            long waitTime = (long) (100 - duration.toMillis());
            if (waitTime > 0) {
                try {
                    Thread.sleep(waitTime);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw Throwables.propagate(e);
                }
            }
        }
    }

}
