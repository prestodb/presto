/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.importer.ImportField;
import com.facebook.presto.importer.ImportManager;
import com.facebook.presto.ingest.ImportSchemaUtil;
import com.facebook.presto.metadata.ColumnMetadata;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.NativeColumnHandle;
import com.facebook.presto.metadata.NativeTableHandle;
import com.facebook.presto.metadata.NodeManager;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.spi.ImportClient;
import com.facebook.presto.spi.SchemaField;
import com.facebook.presto.split.ImportClientFactory;
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
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.airlift.log.Logger;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.List;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.server.TaskInfo.taskStateGetter;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_LONG;
import static com.facebook.presto.util.Threads.threadsNamed;
import static com.google.common.base.Predicates.equalTo;
import static com.google.common.collect.Iterables.all;
import static com.google.common.collect.Iterables.any;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;

@ThreadSafe
public class StaticQueryManager
        implements QueryManager
{
    private static final Logger log = Logger.get(StaticQueryManager.class);

    private final TaskScheduler taskScheduler;
    private final ExecutorService queryExecutor;
    private final ImportClientFactory importClientFactory;
    private final ImportManager importManager;
    private final Metadata metadata;
    private final NodeManager nodeManager;
    private final SplitManager splitManager;

    private final AtomicInteger nextQueryId = new AtomicInteger();
    private final ConcurrentMap<String, QueryWorker> queries = new ConcurrentHashMap<>();

    @Inject
    public StaticQueryManager(
            TaskScheduler taskScheduler,
            ImportClientFactory importClientFactory,
            ImportManager importManager,
            Metadata metadata,
            NodeManager nodeManager,
            SplitManager splitManager)
    {
        Preconditions.checkNotNull(taskScheduler, "taskScheduler is null");
        Preconditions.checkNotNull(importClientFactory, "importClientFactory is null");
        Preconditions.checkNotNull(importManager, "importManager is null");
        Preconditions.checkNotNull(metadata, "metadata is null");

        this.taskScheduler = taskScheduler;
        this.queryExecutor = new ThreadPoolExecutor(1000,
                1000,
                1, TimeUnit.MINUTES,
                new LinkedBlockingQueue<Runnable>(),
                threadsNamed("query-processor-%d"));

        this.importClientFactory = importClientFactory;
        this.importManager = importManager;
        this.metadata = metadata;
        this.nodeManager = nodeManager;
        this.splitManager = splitManager;
    }

    @Override
    public List<QueryInfo> getAllQueryInfo()
    {
        return ImmutableList.copyOf(filter(transform(queries.values(), new Function<QueryWorker, QueryInfo>()
        {
            @Override
            public QueryInfo apply(QueryWorker queryWorker)
            {
                try {
                    return queryWorker.getQueryInfo();
                }
                catch (Exception ignored) {
                    return null;
                }
            }
        }), Predicates.notNull()));
    }

    @Override
    public QueryInfo getQueryInfo(String queryId)
    {
        Preconditions.checkNotNull(queryId, "queryId is null");

        QueryWorker query = queries.get(queryId);
        if (query == null) {
            throw new NoSuchElementException();
        }
        try {
            return query.getQueryInfo();
        }
        catch (RuntimeException e) {
            // todo need better signal for a failed task
            queries.remove(queryId);
            throw e;
        }
    }

    @Override
    public QueryInfo createQuery(String query)
    {
        Preconditions.checkNotNull(query, "query is null");
        Preconditions.checkArgument(query.length() > 0, "query must not be empty string");

        QueryWorker queryWorker;
        if (query.startsWith("sql:")) {
            // e.g.: sql:select count(*) from hivedba_query_stats
            String sql = query.substring("sql:".length());
            queryWorker = new SqlQueryWorker(String.valueOf(nextQueryId.getAndIncrement()),
                    sql,
                    taskScheduler,
                    new Session(),
                    metadata,
                    nodeManager,
                    splitManager);
        }
        else {
            // todo this is a hack until we have language support for import or create table as select
            ImmutableList<String> strings = ImmutableList.copyOf(Splitter.on(":").split(query));
            String queryBase = strings.get(0);

            switch (queryBase) {
                // e.g.: import-table:hive:default:hivedba_query_stats
                case "import-table":
                    queryWorker = new ImportTableWorker(String.valueOf(nextQueryId.getAndIncrement()), importClientFactory, importManager, metadata, strings.get(1), strings.get(2), strings.get(3));
                    break;

                default:
                    throw new IllegalArgumentException("Unsupported query " + query);
            }
        }

        queries.put(queryWorker.getQueryId(), queryWorker);
        queryExecutor.submit(queryWorker);

        return queryWorker.getQueryInfo();
    }

    @Override
    public void cancelQuery(String queryId)
    {
        Preconditions.checkNotNull(queryId, "queryId is null");

        log.debug("Cancel query %s", queryId);

        QueryWorker query = queries.remove(queryId);
        if (query != null) {
            query.cancel();
        }
    }

    private static interface QueryWorker
            extends Runnable
    {
        String getQueryId();

        QueryInfo getQueryInfo();

        void cancel();
    }

    private static class SqlQueryWorker
            implements QueryWorker
    {
        private final String queryId;
        private final TaskScheduler taskScheduler;
        private final ConcurrentHashMap<String, List<HttpTaskClient>> stages = new ConcurrentHashMap<>();
        private final AtomicReference<QueryState> queryState = new AtomicReference<>(QueryState.QUEUED);
        private final StageExecutionPlan outputStageExecutionPlan;

        public SqlQueryWorker(String queryId, String sql, TaskScheduler taskScheduler, Session session, Metadata metadata, NodeManager nodeManager, SplitManager splitManager)
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
                LogicalPlanner planner = new LogicalPlanner();
                PlanNode plan = planner.plan((Query) statement, analysis);

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
        public void run()
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

    private static class ImportTableWorker
            implements QueryWorker
    {
        private static final List<TupleInfo> TUPLE_INFOS = ImmutableList.of(SINGLE_LONG);
        private static final List<String> FIELD_NAMES = ImmutableList.of("dummy");

        private final String queryId;
        private final ImportClientFactory importClientFactory;
        private final ImportManager importManager;
        private final Metadata metadata;
        private final String sourceName;
        private final String databaseName;
        private final String tableName;

        private ImportTableWorker(
                String queryId,
                ImportClientFactory importClientFactory,
                ImportManager importManager,
                Metadata metadata,
                String sourceName,
                String databaseName,
                String tableName)
        {
            this.queryId = queryId;
            this.importClientFactory = importClientFactory;
            this.importManager = importManager;
            this.metadata = metadata;
            this.sourceName = sourceName;
            this.databaseName = databaseName;
            this.tableName = tableName;
        }

        @Override
        public String getQueryId()
        {
            return queryId;
        }

        @Override
        public QueryInfo getQueryInfo()
        {
            return new QueryInfo(queryId, TUPLE_INFOS, FIELD_NAMES, QueryState.FINISHED, null, ImmutableMap.<String, List<TaskInfo>>of());
        }

        @Override
        public void run()
        {
            String catalogName = "default";
            String schemaName = "default";

            ImportClient importClient = importClientFactory.getClient(sourceName);
            List<SchemaField> schema = importClient.getTableSchema(databaseName, tableName);
            List<ColumnMetadata> columns = ImportSchemaUtil.createColumnMetadata(schema);
            TableMetadata table = new TableMetadata(catalogName, schemaName, tableName, columns);
            metadata.createTable(table);

            table = metadata.getTable(catalogName, schemaName, tableName);
            long tableId = ((NativeTableHandle) table.getTableHandle().get()).getTableId();
            List<ImportField> fields = getImportFields(table.getColumns());

            importManager.importTable(tableId, sourceName, databaseName, tableName, fields);
        }

        @Override
        public void cancel()
        {
            // imports are global background tasks, so canceling this "scheduling" query doesn't mean anything
        }

        private static List<ImportField> getImportFields(List<ColumnMetadata> columns)
        {
            ImmutableList.Builder<ImportField> fields = ImmutableList.builder();
            for (ColumnMetadata column : columns) {
                long columnId = ((NativeColumnHandle) column.getColumnHandle().get()).getColumnId();
                fields.add(new ImportField(columnId, column.getType(), column.getName()));
            }
            return fields.build();
        }
    }
}
