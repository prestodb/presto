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
import com.facebook.presto.metadata.Node;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.server.QueryState.State;
import com.facebook.presto.spi.ImportClient;
import com.facebook.presto.spi.SchemaField;
import com.facebook.presto.split.ImportClientFactory;
import com.facebook.presto.split.Split;
import com.facebook.presto.split.SplitAssignments;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.sql.compiler.AnalysisResult;
import com.facebook.presto.sql.compiler.Analyzer;
import com.facebook.presto.sql.compiler.SessionMetadata;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.FragmentPlanner;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.PlanFragmentSource;
import com.facebook.presto.sql.planner.PlanNode;
import com.facebook.presto.sql.planner.Planner;
import com.facebook.presto.sql.planner.TableScan;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleInfo.Type;
import com.facebook.presto.util.IterableTransformer;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import io.airlift.http.client.ApacheHttpClient;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.tuple.TupleInfo.SINGLE_LONG;
import static com.facebook.presto.util.Threads.threadsNamed;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static io.airlift.json.JsonCodec.jsonCodec;

@ThreadSafe
public class StaticQueryManager
        implements QueryManager
{
    private final QueryTaskManager queryTaskManager;
    private final ExecutorService queryExecutor;
    private final ImportClientFactory importClientFactory;
    private final ImportManager importManager;
    private final Metadata metadata;
    private final SplitManager splitManager;
    private final JsonCodec<QueryFragmentRequest> queryFragmentRequestJsonCodec;

    private final AtomicInteger nextQueryId = new AtomicInteger();
    private final ConcurrentMap<String, QueryWorker> queries = new ConcurrentHashMap<>();

    @Inject
    public StaticQueryManager(
            QueryTaskManager queryTaskManager,
            ImportClientFactory importClientFactory,
            ImportManager importManager,
            Metadata metadata,
            SplitManager splitManager,
            JsonCodec<QueryFragmentRequest> queryFragmentRequestJsonCodec)
    {
        Preconditions.checkNotNull(queryTaskManager, "queryTaskManager is null");
        Preconditions.checkNotNull(importClientFactory, "importClientFactory is null");
        Preconditions.checkNotNull(importManager, "importManager is null");
        Preconditions.checkNotNull(metadata, "metadata is null");
        Preconditions.checkNotNull(splitManager, "splitManager is null");
        Preconditions.checkNotNull(queryFragmentRequestJsonCodec, "queryFragmentRequestJsonCodec is null");

        this.queryTaskManager = queryTaskManager;
        this.queryExecutor = new ThreadPoolExecutor(1000,
                1000,
                1, TimeUnit.MINUTES,
                new LinkedBlockingQueue<Runnable>(),
                threadsNamed("query-processor-%d"));

        this.importClientFactory = importClientFactory;
        this.importManager = importManager;
        this.metadata = metadata;
        this.splitManager = splitManager;
        this.queryFragmentRequestJsonCodec = queryFragmentRequestJsonCodec;
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
        return query.getQueryInfo();
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
                    queryTaskManager,
                    metadata,
                    splitManager,
                    queryExecutor,
                    queryFragmentRequestJsonCodec,
                    jsonCodec(QueryTaskInfo.class));
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
        private final QueryTaskManager queryTaskManager;
        private final ExecutorService executor;
        private final ApacheHttpClient httpClient;
        private final JsonCodec<QueryFragmentRequest> queryFragmentRequestCodec;
        private final JsonCodec<QueryTaskInfo> queryTaskInfoCodec;
        private final SplitManager splitManager;
        private final SessionMetadata sessionMetadata;
        private final List<PlanFragment> fragments;
        private final ConcurrentHashMap<String, List<QueryTask>> stages = new ConcurrentHashMap<>();
        private final List<TupleInfo> tupleInfos;
        private final AtomicReference<State> queryState = new AtomicReference<>(State.PREPARING);


        public SqlQueryWorker(String queryId,
                String sql,
                QueryTaskManager queryTaskManager,
                Metadata metadata,
                SplitManager splitManager,
                ExecutorService executor,
                JsonCodec<QueryFragmentRequest> queryFragmentRequestCodec,
                JsonCodec<QueryTaskInfo> queryTaskInfoCodec)
        {
            this.queryId = queryId;
            this.queryTaskManager = queryTaskManager;
            this.executor = executor;
            this.queryFragmentRequestCodec = queryFragmentRequestCodec;
            httpClient = new ApacheHttpClient(new HttpClientConfig()
                    .setConnectTimeout(new Duration(5, TimeUnit.SECONDS))
                    .setReadTimeout(new Duration(5, TimeUnit.SECONDS)));
            this.splitManager = splitManager;
            this.queryTaskInfoCodec = queryTaskInfoCodec;

            try {
                // parse query
                Statement statement = SqlParser.createStatement(sql);

                // analyse query
                sessionMetadata = new SessionMetadata(metadata);
                Analyzer analyzer = new Analyzer(sessionMetadata);
                AnalysisResult analysis = analyzer.analyze(statement);

                // plan query
                Planner planner = new Planner();
                PlanNode plan = planner.plan((Query) statement, analysis);

                // fragment the plan
                FragmentPlanner fragmentPlanner = new FragmentPlanner(sessionMetadata);
                fragments = fragmentPlanner.createFragments(plan, analysis.getSymbolAllocator(), false);

                Preconditions.checkArgument(fragments.size() == 2, "Distributed plans with more than 2 levels not yet supported");
            }
            catch (Exception e) {
                throw Throwables.propagate(e);
            }

            PlanFragment top = fragments.get(1);
            tupleInfos = ImmutableList.copyOf(IterableTransformer.on(top.getRoot().getOutputSymbols())
                    .transform(Functions.forMap(top.getSymbols()))
                    .transform(com.facebook.presto.sql.compiler.Type.toRaw())
                    .transform(new Function<Type, TupleInfo>()
                    {
                        @Override
                        public TupleInfo apply(Type input)
                        {
                            return new TupleInfo(input);
                        }
                    })
                    .list());
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
                State state = queryState.get();
                if (state != State.PREPARING && state != State.RUNNING) {
                    break;
                }
                if (queryState.compareAndSet(state, State.CANCELED)) {
                    break;
                }
            }

            for (QueryTask taskClient : Iterables.concat(stages.values())) {
                taskClient.cancel();
            }
        }

        @Override
        public QueryInfo getQueryInfo()
        {
            ImmutableMap.Builder<String, List<QueryTaskInfo>> map = ImmutableMap.builder();
            for (Entry<String, List<QueryTask>> stage : stages.entrySet()) {
                map.put(stage.getKey(), ImmutableList.copyOf(Iterables.transform(stage.getValue(), new Function<QueryTask, QueryTaskInfo>()
                {
                    @Override
                    public QueryTaskInfo apply(QueryTask queryTask)
                    {
                        QueryTaskInfo taskInfo = queryTask.getQueryTaskInfo();
                        if (taskInfo == null) {
                            // task was not found, so we mark the master as failed
                            RuntimeException exception = new RuntimeException(String.format("Query %s task %s has been deleted", queryId, queryTask.getTaskId()));
                            cancel();
                            throw exception;
                        }
                        return taskInfo;
                    }
                })));
            }
            ImmutableMap<String, List<QueryTaskInfo>> stages = map.build();

            State overallState = queryState.get();
            if (!stages.isEmpty()) {
                Iterable<State> taskStates = transform(concat(stages.values()), new Function<QueryTaskInfo, State>() {
                    @Override
                    public State apply(QueryTaskInfo queryTaskInfo)
                    {
                        return queryTaskInfo.getState();
                    }
                });

                if (overallState == State.PREPARING || overallState == State.RUNNING) {
                    if (Iterables.any(taskStates, Predicates.equalTo(State.FAILED))) {
                        overallState = State.FAILED;
                        queryState.set(overallState);
                        cancel();
                    }
                    else if (Iterables.any(taskStates, Predicates.equalTo(State.CANCELED))) {
                        overallState = State.CANCELED;
                        queryState.set(overallState);
                        cancel();
                    }
                    else if (Iterables.all(taskStates, Predicates.equalTo(State.FINISHED))) {
                        overallState = State.FINISHED;
                        queryState.set(overallState);
                    }
                }
            }

            return new QueryInfo(queryId, tupleInfos, overallState, stages);
        }

        @Override
        public void run()
        {
            queryState.set(State.RUNNING);
            try {
                PlanFragment top = fragments.get(1);
                final PlanFragment bottom = fragments.get(0);

                // divide bottom fragment file scan splits amongst the nodes
                TableScan tableScan = (TableScan) Iterables.getOnlyElement(bottom.getSources());
                final String tableScanSourceId = tableScan.getTable().getHandleId();
                Iterable<SplitAssignments> splitAssignments = splitManager.getSplitAssignments(tableScan.getTable());
                Multimap<Node, Split> nodeSplits = SplitAssignments.randomNodeAssignment(new Random(), splitAssignments);

                // create a task for each file scan split
                List<HttpTaskClient> providers = ImmutableList.copyOf(transform(nodeSplits.asMap().entrySet(), new Function<Entry<Node, Collection<Split>>, HttpTaskClient>()
                {
                    @Override
                    public HttpTaskClient apply(Entry<Node, Collection<Split>> splits)
                    {
                        // convert splits to table scan sources
                        List<PlanFragmentSource> tableScanSources = ImmutableList.copyOf(transform(splits.getValue(), new Function<Split, PlanFragmentSource>()
                        {
                            @Override
                            public PlanFragmentSource apply(Split split)
                            {
                                return new TableScanPlanFragmentSource(split);
                            }
                        }));
                        ImmutableMap<String, List<PlanFragmentSource>> sourceSplits = ImmutableMap.of(tableScanSourceId, tableScanSources);

                        // schedule table scan task on remote node
                        return new HttpTaskClient(bottom,
                                sourceSplits,
                                httpClient,
                                executor,
                                queryFragmentRequestCodec,
                                queryTaskInfoCodec,
                                splits.getKey().getHttpUri().resolve("/v1/presto/task"), "out"
                        );
                    }
                }));
                List<TupleInfo> exchangeTupleInfos = providers.get(0).getTupleInfos();
                addStage(String.valueOf(bottom.getId()), providers);

                // create an exchange source for table scan jobs
                ImmutableMap.Builder<String, URI> exchangeSources = ImmutableMap.builder();
                for (HttpTaskClient provider : providers) {
                    exchangeSources.put(provider.getTaskId(), provider.getLocation());
                }
                PlanFragmentSource exchangeSource = new ExchangePlanFragmentSource(exchangeSources.build(), "out", exchangeTupleInfos);

                // schedule top fragment on this node
                Map<String, List<PlanFragmentSource>> topSources = ImmutableMap.<String, List<PlanFragmentSource>>of(String.valueOf(bottom.getId()), ImmutableList.of(exchangeSource));
                QueryTask queryTask = queryTaskManager.createQueryTask(top, topSources);
                stages.put("out", ImmutableList.of(queryTask));
            }
            catch (Exception e) {
                queryState.set(State.FAILED);
                cancel();
                throw Throwables.propagate(e);
            }
        }

        public void addStage(String stageId, Iterable<? extends QueryTask> tasks)
        {
            stages.put(stageId, ImmutableList.copyOf(tasks));
        }
    }

    private static class ImportTableWorker
            implements QueryWorker
    {
        private static final ImmutableList<TupleInfo> TUPLE_INFOS = ImmutableList.of(SINGLE_LONG);

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
            return new QueryInfo(queryId, TUPLE_INFOS, State.FINISHED, ImmutableMap.<String, List<QueryTaskInfo>>of());
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

    private static void waitForRunning(List<HttpTaskClient> taskClients)
    {
        while (true) {
            long start = System.nanoTime();

            taskClients = ImmutableList.copyOf(filter(taskClients, new Predicate<HttpTaskClient>()
            {
                @Override
                public boolean apply(HttpTaskClient taskClient)
                {
                    QueryTaskInfo queryInfo = taskClient.getQueryTaskInfo();
                    return queryInfo.getState() == State.PREPARING;
                }
            }));
            if (taskClients.isEmpty()) {
                return;
            }

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
