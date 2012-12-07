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
import com.facebook.presto.sql.planner.StageExecutionPlan;
import com.facebook.presto.sql.planner.SubPlan;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Statement;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.airlift.log.Logger;

import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.execution.TaskInfo.taskStateGetter;
import static com.google.common.base.Predicates.equalTo;
import static com.google.common.collect.Iterables.all;
import static com.google.common.collect.Iterables.any;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.transform;

public class SqlQueryExecution
        implements QueryExecution
{
    private static final Logger log = Logger.get(SqlQueryExecution.class);

    private final String queryId;
    private final TaskScheduler taskScheduler;
    private final ConcurrentHashMap<String, List<HttpTaskClient>> stages = new ConcurrentHashMap<>();
    private final AtomicReference<QueryState> queryState = new AtomicReference<>(QueryState.QUEUED);
    private final StageExecutionPlan outputStageExecutionPlan;

    public SqlQueryExecution(String queryId, String sql, TaskScheduler taskScheduler, Session session, Metadata metadata, NodeManager nodeManager, SplitManager splitManager)
    {
        this.queryId = queryId;
        this.taskScheduler = taskScheduler;

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
            outputStageExecutionPlan = distributedPlanner.plan(subplan);

        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }

    }

    @Override
    public String getQueryId()
    {
        return queryId;
    }

    @Override
    public void cancel()
    {
        while (true) {
            QueryState queryState = this.queryState.get();
            if (queryState.isDone()) {
                break;
            }
            if (this.queryState.compareAndSet(queryState, QueryState.CANCELED)) {
                break;
            }
        }

        for (HttpTaskClient taskClient : Iterables.concat(stages.values())) {
            taskClient.cancel();
        }
    }

    @Override
    public QueryInfo getQueryInfo()
    {
        ImmutableMap.Builder<String, List<TaskInfo>> map = ImmutableMap.builder();
        for (Entry<String, List<HttpTaskClient>> stage : stages.entrySet()) {
            map.put(String.valueOf(stage.getKey()), ImmutableList.copyOf(Iterables.transform(stage.getValue(), new Function<HttpTaskClient, TaskInfo>()
            {
                @Override
                public TaskInfo apply(HttpTaskClient taskClient)
                {
                    TaskInfo taskInfo = taskClient.getTaskInfo();
                    if (taskInfo == null) {
                        // task was not found, so we mark the master as failed
                        RuntimeException exception = new RuntimeException(String.format("Query %s task %s has been deleted", queryId, taskClient.getTaskId()));
                        cancel();
                        throw exception;
                    }
                    return taskInfo;
                }
            })));
        }
        ImmutableMap<String, List<TaskInfo>> stages = map.build();

        QueryState queryState = this.queryState.get();
        if (!stages.isEmpty()) {
            if (queryState == QueryState.QUEUED) {
                queryState = QueryState.RUNNING;
                this.queryState.set(queryState);
            }

            if (queryState == QueryState.RUNNING) {
                // if all output tasks are finished, the query is finished
                // todo this is no longer correct
                List<TaskInfo> outputTasks = stages.get(outputStageExecutionPlan.getStageId());
                if (outputTasks != null && !outputTasks.isEmpty() && all(transform(outputTasks, taskStateGetter()), TaskState.inDoneState())) {
                    queryState = QueryState.FINISHED;
                    this.queryState.set(queryState);
                } else if (any(transform(concat(stages.values()), taskStateGetter()), equalTo(TaskState.FAILED))) {
                    // if any task is failed, the query has failed
                    queryState = QueryState.FAILED;
                    this.queryState.set(queryState);
                    log.debug("A task for query %s failed, canceling all tasks: stages %s", queryId, this.stages);
                    cancel();
                }
            }
        }

        return new QueryInfo(queryId, outputStageExecutionPlan.getTupleInfos(), outputStageExecutionPlan.getFieldNames(), queryState, outputStageExecutionPlan.getStageId(), stages);
    }

    @Override
    public void start()
    {
        try {
            taskScheduler.schedule(outputStageExecutionPlan, stages);

            // mark it as finished if there will never be any output TODO: think about this more -- shouldn't have stages with no tasks?
            if (stages.get(outputStageExecutionPlan.getStageId()).isEmpty()) {
                queryState.set(QueryState.FINISHED);
            }
        }
        catch (Exception e) {
            queryState.set(QueryState.FAILED);
            log.debug(e, "Query %s failed to start", queryId);
            cancel();
            throw Throwables.propagate(e);
        }
    }
}
