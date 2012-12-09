/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

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
import com.facebook.presto.sql.planner.StageExecutionPlan;
import com.facebook.presto.sql.planner.SubPlan;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.util.IterableTransformer;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.sql.planner.Partition.nodeIdentifierGetter;
import static com.google.common.collect.Iterables.filter;

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
    private final AtomicReference<QueryState> queryState = new AtomicReference<>(QueryState.QUEUED);
    private final StageManager stageManager;
    private final RemoteTaskFactory remoteTaskFactory;
    private final LocationFactory locationFactory;

    private final AtomicReference<StageExecution> outputStage = new AtomicReference<>();
    private List<String> fieldNames = ImmutableList.of();
    private List<TupleInfo> tupleInfos = ImmutableList.of();

    public SqlQueryExecution(String queryId,
            String sql,
            Session session,
            Metadata metadata,
            NodeManager nodeManager,
            SplitManager splitManager,
            StageManager stageManager,
            RemoteTaskFactory remoteTaskFactory,
            LocationFactory locationFactory)
    {
        Preconditions.checkNotNull(queryId, "queryId is null");
        Preconditions.checkNotNull(sql, "sql is null");
        Preconditions.checkNotNull(session, "session is null");
        Preconditions.checkNotNull(metadata, "metadata is null");
        Preconditions.checkNotNull(nodeManager, "nodeManager is null");
        Preconditions.checkNotNull(splitManager, "splitManager is null");
        Preconditions.checkNotNull(stageManager, "stageManager is null");
        Preconditions.checkNotNull(remoteTaskFactory, "remoteTaskFactory is null");
        Preconditions.checkNotNull(locationFactory, "locationFactory is null");

        this.queryId = queryId;
        this.sql = sql;

        this.session = session;
        this.metadata = metadata;
        this.nodeManager = nodeManager;
        this.splitManager = splitManager;
        this.stageManager = stageManager;
        this.remoteTaskFactory = remoteTaskFactory;
        this.locationFactory = locationFactory;
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
                queryState.get(),
                locationFactory.createQueryLocation(queryId),
                fieldNames,
                tupleInfos,
                stageInfo);
    }

    @Override
    public void start()
    {
        try {
            // parse query
            Statement statement = SqlParser.createStatement(sql);

            // analyze query
            Analyzer analyzer = new Analyzer(session, metadata);
            AnalysisResult analysis = analyzer.analyze(statement);

            // plan query
            LogicalPlanner logicalPlanner = new LogicalPlanner();
            PlanNode plan = logicalPlanner.plan((Query) statement, analysis);

            // fragment the plan
            SubPlan subplan = new DistributedLogicalPlanner(metadata).createSubplans(plan, analysis.getSymbolAllocator(), false);

            // plan the execution on the active nodes
            DistributedExecutionPlanner distributedPlanner = new DistributedExecutionPlanner(nodeManager, splitManager);
            StageExecutionPlan outputStageExecutionPlan = distributedPlanner.plan(subplan);

            fieldNames = outputStageExecutionPlan.getFieldNames();
            tupleInfos = outputStageExecutionPlan.getTupleInfos();

            StageExecution outputStage = createStage(new AtomicInteger(), outputStageExecutionPlan, ImmutableList.of(ROOT_OUTPUT_BUFFER_NAME));
            this.outputStage.set(outputStage);
            startStage(outputStage);
        }
        catch (Exception e) {
            e.printStackTrace();
            throw Throwables.propagate(e);
        }
    }

    private void startStage(StageExecution stage)
    {
        for (StageExecution subStage : stage.getSubStages()) {
            startStage(subStage);
        }
        stage.start();
    }

    private StageExecution createStage(AtomicInteger nextStageId, StageExecutionPlan stageExecutionPlan, List<String> outputIds)
    {
        String stageId = queryId + "." + nextStageId.getAndIncrement();

        Map<String, StageExecution> subStages = IterableTransformer.on(stageExecutionPlan.getSubStages())
                .uniqueIndex(fragmentIdGetter())
                .transformValues(stageCreator(nextStageId, stageExecutionPlan.getPartitions()))
                .immutableMap();

        URI stageLocation = locationFactory.createStageLocation(stageId);
        int taskId = 0;
        ImmutableList.Builder<RemoteTask> tasks = ImmutableList.builder();
        for (Partition partition : stageExecutionPlan.getPartitions()) {
            String nodeIdentifier = partition.getNode().getNodeIdentifier();

            ImmutableMap.Builder<String, ExchangePlanFragmentSource> exchangeSources = ImmutableMap.builder();
            for (Entry<String, StageExecution> entry : subStages.entrySet()) {
                exchangeSources.put(entry.getKey(), entry.getValue().getExchangeSourceFor(nodeIdentifier));
            }

            tasks.add(remoteTaskFactory.createRemoteTask(queryId,
                    stageId,
                    stageId + '.' + taskId++,
                    partition.getNode(),
                    stageExecutionPlan.getFragment(),
                    partition.getSplits(),
                    exchangeSources.build(),
                    outputIds));
        }

        return stageManager.createStage(queryId, stageId, stageLocation, tasks.build(), subStages.values());
    }

    @Override
    public void cancel()
    {
        while (true) {
            QueryState queryState = this.queryState.get();
            if (queryState.isDone()) {
                return;
            }
            if (this.queryState.compareAndSet(queryState, QueryState.CANCELED)) {
                break;
            }
        }

        StageExecution stageExecution = outputStage.get();
        if (stageExecution != null) {
            stageExecution.cancel();
        }
    }

    public void updateState()
    {
        StageExecution outputStage = this.outputStage.get();
        if (outputStage == null) {
            return;
        }
        outputStage.updateState();

        StageInfo outputStageInfo = outputStage.getStageInfo();
        if (queryState.get().isDone()) {
            return;
        }

        switch (outputStageInfo.getState()) {
            case PLANNED:
            case CANCELED:
                // leave state unchanged
                break;
            case SCHEDULED:
                this.queryState.set(QueryState.QUEUED);
                break;
            case RUNNING:
                this.queryState.set(QueryState.RUNNING);
                break;
            case FINISHED:
                this.queryState.set(QueryState.FINISHED);
                break;
            case FAILED:
                this.queryState.set(QueryState.FAILED);
                break;
        }
    }

    private Function<StageExecutionPlan, StageExecution> stageCreator(final AtomicInteger nextStageId, List<Partition> partitions)
    {
        // for each partition in stage create an outputBuffer
        final List<String> outputIds = IterableTransformer.on(partitions).transform(nodeIdentifierGetter()).list();
        return new Function<StageExecutionPlan, StageExecution>() {
            @Override
            public StageExecution apply(@Nullable StageExecutionPlan subStage)
            {
                return createStage(nextStageId, subStage, outputIds);
            }
        };
    }

    private Function<StageExecutionPlan, String> fragmentIdGetter()
    {
        return new Function<StageExecutionPlan, String>() {
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
