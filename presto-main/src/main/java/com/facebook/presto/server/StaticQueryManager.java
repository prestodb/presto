/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.importer.ImportField;
import com.facebook.presto.importer.ImportManager;
import com.facebook.presto.ingest.DelimitedRecordIterable;
import com.facebook.presto.ingest.ImportSchemaUtil;
import com.facebook.presto.ingest.RecordProjectOperator;
import com.facebook.presto.ingest.RecordProjection;
import com.facebook.presto.ingest.RecordProjections;
import com.facebook.presto.metadata.ColumnMetadata;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.NativeColumnHandle;
import com.facebook.presto.metadata.NativeTableHandle;
import com.facebook.presto.metadata.Node;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.Page;
import com.facebook.presto.server.QueryState.State;
import com.facebook.presto.spi.ImportClient;
import com.facebook.presto.spi.SchemaField;
import com.facebook.presto.split.DataStreamProvider;
import com.facebook.presto.split.ImportClientFactory;
import com.facebook.presto.split.Split;
import com.facebook.presto.split.SplitAssignments;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.sql.compiler.AnalysisResult;
import com.facebook.presto.sql.compiler.Analyzer;
import com.facebook.presto.sql.compiler.SessionMetadata;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.ExecutionPlanner;
import com.facebook.presto.sql.planner.FragmentPlanner;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.PlanNode;
import com.facebook.presto.sql.planner.Planner;
import com.facebook.presto.sql.planner.TableScan;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleInfo.Type;
import com.facebook.presto.util.IterableTransformer;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;
import com.google.common.util.concurrent.Uninterruptibles;
import io.airlift.http.client.ApacheHttpClient;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;

import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;
import javax.ws.rs.core.MediaType;
import java.io.File;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.tuple.TupleInfo.SINGLE_LONG;
import static com.facebook.presto.util.Threads.threadsNamed;
import static com.google.common.collect.Iterables.transform;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;

public class StaticQueryManager
        implements QueryManager
{
    private final int pageBufferMax;
    private final ExecutorService queryExecutor;
    private final ThreadPoolExecutor shardExecutor;
    private final ImportClientFactory importClientFactory;
    private final ImportManager importManager;
    private final Metadata metadata;
    private final DataStreamProvider dataStreamProvider;
    private final SplitManager splitManager;
    private final JsonCodec<QueryFragmentRequest> queryFragmentRequestJsonCodec;

    @GuardedBy("this")
    private int nextQueryId;

    // todo this is a potential memory leak if the query objects are not removed
    @GuardedBy("this")
    private final Map<String, MasterQueryState> masterQueries = new HashMap<>();

    @GuardedBy("this")
    private final Map<String, QueryState> queries = new HashMap<>();

    @Inject
    public StaticQueryManager(
            ImportClientFactory importClientFactory,
            ImportManager importManager,
            Metadata metadata,
            DataStreamProvider dataStreamProvider,
            SplitManager splitManager,
            JsonCodec<QueryFragmentRequest> queryFragmentRequestJsonCodec)
    {
        this(20, importClientFactory, importManager, metadata, dataStreamProvider, splitManager, queryFragmentRequestJsonCodec);
    }

    public StaticQueryManager(
            int pageBufferMax,
            ImportClientFactory importClientFactory,
            ImportManager importManager,
            Metadata metadata,
            DataStreamProvider dataStreamProvider,
            SplitManager splitManager,
            JsonCodec<QueryFragmentRequest> queryFragmentRequestJsonCodec)
    {
        Preconditions.checkArgument(pageBufferMax > 0, "blockBufferMax must be at least 1");
        this.pageBufferMax = pageBufferMax;

        int processors = Runtime.getRuntime().availableProcessors();
        queryExecutor = new ThreadPoolExecutor(1000,
                1000,
                1, TimeUnit.MINUTES,
                new LinkedBlockingQueue<Runnable>(),
                threadsNamed("query-processor-%d"));
        shardExecutor = new ThreadPoolExecutor(processors,
                processors,
                1, TimeUnit.MINUTES,
                new SynchronousQueue<Runnable>(),
                threadsNamed("shard-query-processor-%d"),
                new ThreadPoolExecutor.CallerRunsPolicy());

        this.importClientFactory = importClientFactory;
        this.importManager = importManager;
        this.metadata = metadata;
        this.dataStreamProvider = dataStreamProvider;
        this.splitManager = splitManager;
        this.queryFragmentRequestJsonCodec = queryFragmentRequestJsonCodec;
    }

    @Override
    public List<QueryInfo> getAllQueryInfo()
    {
        return ImmutableList.copyOf(transform(masterQueries.values(), new Function<MasterQueryState, QueryInfo>()
        {
            @Override
            public QueryInfo apply(MasterQueryState masterQueryState)
            {
                return masterQueryState.toQueryInfo();
            }
        }));
    }

    @Override
    public synchronized QueryInfo createQuery(String query)
    {
        Preconditions.checkNotNull(query, "query is null");
        Preconditions.checkArgument(query.length() > 0, "query must not be empty string");

        String queryId = String.valueOf(nextQueryId++);

        // TODO: fix how we get our query parameters. typically we will have a query parser/planner to drive executions, but in the meantime, just use this ghetto-ness
        ImmutableList<String> strings = ImmutableList.copyOf(Splitter.on(":").split(query));
        String queryBase = strings.get(0);

        MasterQueryTask queryTask;

        if (query.startsWith("sql:")) {
            // e.g.: sql:select count(*) from hivedba_query_stats
            String sql = query.substring("sql:".length());
            queryTask = new SqlFragmentMaster(queryId, pageBufferMax, queryExecutor, metadata, splitManager, queryFragmentRequestJsonCodec, sql);
        }
        else {
            switch (queryBase) {
                // e.g.: import-delimited:default:hivedba_query_stats:string,long,string,long:/tmp/myfile.csv:,
                case "import-delimited":
                    List<Type> types = ImmutableList.copyOf(transform(Splitter.on(",").split(strings.get(2)), new Function<String, Type>()
                    {
                        @Override
                        public Type apply(String input)
                        {
                            return Type.fromName(input);
                        }
                    }));
                    queryTask = new ImportDelimited(queryId,
                            pageBufferMax,
                            strings.get(1),
                            strings.get(2),
                            new TupleInfo(types),
                            new File(strings.get(3)),
                            Splitter.on(strings.get(4)));
                    break;

                // e.g.: import-table:hive:default:hivedba_query_stats
                case "import-table":
                    queryTask = new ImportTableQuery(queryId, pageBufferMax, importClientFactory, importManager, metadata, strings.get(1), strings.get(2), strings.get(3));
                    break;

                default:
                    throw new IllegalArgumentException("Unsupported query " + query);
            }
        }

        MasterQueryState masterQueryState = queryTask.getMasterQueryState();
        masterQueries.put(queryId, masterQueryState);
        queries.put(queryId, masterQueryState.getOutputQueryState());
        queryExecutor.submit(queryTask);

        return new QueryInfo(queryId, queryTask.getTupleInfos(), State.PREPARING, 0, 0, 0);
    }

    @Override
    public synchronized QueryInfo createQueryFragment(Map<String, List<Split>> sourceSplits, PlanFragment planFragment)
    {
        String queryId = String.valueOf(nextQueryId++);
        QueryTask queryTask = new FragmentWorker(pageBufferMax, planFragment, sourceSplits, dataStreamProvider, metadata, shardExecutor);
        queries.put(queryId, queryTask.getQueryState());
        queryExecutor.submit(queryTask);

        return new QueryInfo(queryId, queryTask.getTupleInfos(), State.PREPARING, 0, 0, 0);
    }

    @Override
    public QueryInfo getQueryInfo(String queryId)
    {
        try {
            return getMasterQuery(queryId).toQueryInfo();
        }
        catch (NoSuchElementException e) {
            QueryState queryState = getQuery(queryId);
            return new QueryInfo(queryId,
                    queryState.getTupleInfos(),
                    queryState.getState(),
                    queryState.getBufferedPageCount(),
                    queryState.getSplits(),
                    queryState.getCompletedSplits());
        }
    }

    @Override
    public State getQueryStatus(String queryId)
    {
        return getQuery(queryId).getState();
    }

    @Override
    public List<Page> getQueryResults(String queryId, int maxPageCount)
            throws InterruptedException
    {
        Preconditions.checkNotNull(queryId, "queryId is null");
        Preconditions.checkArgument(maxPageCount > 0, "maxPageCount must be at least 1");

        return getQuery(queryId).getNextPages(maxPageCount);
    }

    @Override
    public void destroyQuery(String queryId)
    {
        Preconditions.checkNotNull(queryId, "queryId is null");

        QueryState query;
        synchronized (this) {
            masterQueries.remove(queryId);
            query = queries.remove(queryId);
        }
        if (query != null && !query.isDone()) {
            query.cancel();
        }
    }

    public synchronized MasterQueryState getMasterQuery(String queryId)
            throws NoSuchElementException
    {
        MasterQueryState masterQueryState = masterQueries.get(queryId);
        if (masterQueryState == null) {
            throw new NoSuchElementException();
        }
        return masterQueryState;
    }

    public synchronized QueryState getQuery(String queryId)
            throws NoSuchElementException
    {
        QueryState queryState = queries.get(queryId);
        if (queryState == null) {
            throw new NoSuchElementException();
        }
        return queryState;
    }

    private static interface QueryTask
            extends Runnable
    {
        List<TupleInfo> getTupleInfos();

        QueryState getQueryState();
    }

    private static interface MasterQueryTask
            extends QueryTask
    {
        MasterQueryState getMasterQueryState();
    }

    private static class SqlFragmentMaster
            implements MasterQueryTask
    {
        private final MasterQueryState masterQueryState;
        private final ExecutorService executor;
        private final ApacheHttpClient httpClient;
        private final JsonCodec<QueryFragmentRequest> codec;
        private final SplitManager splitManager;
        private final QueryState queryState;
        private final SessionMetadata sessionMetadata;
        private final List<PlanFragment> fragments;

        public SqlFragmentMaster(String queryId,
                int pageBufferMax,
                ExecutorService executor,
                Metadata metadata,
                SplitManager splitManager,
                JsonCodec<QueryFragmentRequest> codec,
                String sql)
        {
            this.executor = executor;
            this.codec = codec;
            httpClient = new ApacheHttpClient(new HttpClientConfig()
                    .setConnectTimeout(new Duration(5, TimeUnit.MINUTES))
                    .setReadTimeout(new Duration(5, TimeUnit.MINUTES)));
            this.splitManager = splitManager;

            try {
                Statement statement = SqlParser.createStatement(sql);

                sessionMetadata = new SessionMetadata(metadata);

                Analyzer analyzer = new Analyzer(sessionMetadata);
                final AnalysisResult analysis = analyzer.analyze(statement);

                Planner planner = new Planner();
                PlanNode plan = planner.plan((Query) statement, analysis);

                fragments = new FragmentPlanner(sessionMetadata).createFragments(plan, analysis.getSymbolAllocator(), false);

                Preconditions.checkArgument(fragments.size() == 2, "Distributed plans with more than 2 levels not yet supported");
            }
            catch (Exception e) {
                throw Throwables.propagate(e);
            }

            PlanFragment top = fragments.get(1);

            List<TupleInfo> tupleInfos = ImmutableList.copyOf(IterableTransformer.on(top.getRoot().getOutputSymbols())
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

            this.masterQueryState = new MasterQueryState(queryId, new QueryState(tupleInfos, 1, pageBufferMax));
            queryState = masterQueryState.getOutputQueryState();
        }

        @Override
        public MasterQueryState getMasterQueryState()
        {
            return masterQueryState;
        }

        @Override
        public QueryState getQueryState()
        {
            return masterQueryState.getOutputQueryState();
        }

        @Override
        public List<TupleInfo> getTupleInfos()
        {
            return masterQueryState.getOutputQueryState().getTupleInfos();
        }

        @Override
        public void run()
        {
            try {
                PlanFragment top = fragments.get(1);
                final PlanFragment bottom = fragments.get(0);

                TableScan tableScan = (TableScan) Iterables.getOnlyElement(bottom.getSources());

                Iterable<SplitAssignments> splitAssignments = splitManager.getSplitAssignments(tableScan.getTable());

                Multimap<Node, Split> nodeSplits = SplitAssignments.randomNodeAssignment(new Random(), splitAssignments);
                List<HttpQueryProvider> providers = ImmutableList.copyOf(transform(nodeSplits.asMap().entrySet(),
                        new Function<Entry<Node, Collection<Split>>, HttpQueryProvider>()
                        {
                            @Override
                            public HttpQueryProvider apply(Entry<Node, Collection<Split>> splits)
                            {
                                QueryFragmentRequest queryFragmentRequest = new QueryFragmentRequest(
                                        ImmutableMap.<String, List<Split>>of("source", ImmutableList.copyOf(splits.getValue())),
                                        bottom // bottom fragment
                                );
                                return new HttpQueryProvider(
                                        jsonBodyGenerator(codec, queryFragmentRequest),
                                        Optional.of(MediaType.APPLICATION_JSON),
                                        httpClient,
                                        executor,
                                        splits.getKey().getHttpUri().resolve("/v1/presto/query")
                                );

                            }
                        }));

                masterQueryState.addStage("sql-frag-worker", providers);

                QueryDriversOperator operator = new QueryDriversOperator(10, providers);

                ExecutionPlanner executionPlanner = new ExecutionPlanner(sessionMetadata,
                        null, // no data provider for non-leaf plan TODO: unify exchanges with data providers
                        top.getSymbols(),
                        ImmutableMap.<Integer, Operator>of(bottom.getId(), operator),
                        null // no split for non-leaf plan TODO: unify exchanges with data providers
                );

                Uninterruptibles.sleepUninterruptibly(3, TimeUnit.SECONDS);
                Operator aggregation = executionPlanner.plan(top.getRoot());

                for (Page page : aggregation) {
                    queryState.addPage(page);
                }
                queryState.sourceFinished();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                queryState.queryFailed(e);
                throw Throwables.propagate(e);
            }
            catch (Exception e) {
                queryState.queryFailed(e);
                throw Throwables.propagate(e);
            }
        }
    }

    private static class FragmentWorker
            implements QueryTask
    {
        private final QueryState queryState;

        private final List<Split> splits;
        private final DataStreamProvider dataStreamProvider;
        private final ThreadPoolExecutor shardExecutor;
        private final PlanFragment fragment;
        private final List<TupleInfo> tupleInfos;
        private final Metadata metadata;

        private FragmentWorker(int pageBufferMax,
                PlanFragment fragment,
                Map<String, List<Split>> sourceSplits,
                DataStreamProvider dataStreamProvider,
                Metadata metadata,
                ThreadPoolExecutor shardExecutor)
        {
            this.fragment = fragment;
            this.splits = sourceSplits.get("source");
            this.dataStreamProvider = dataStreamProvider;
            this.shardExecutor = shardExecutor;
            this.metadata = metadata;

            this.tupleInfos = ImmutableList.copyOf(IterableTransformer.on(fragment.getRoot().getOutputSymbols())
                    .transform(Functions.forMap(fragment.getSymbols()))
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

            this.queryState = new QueryState(tupleInfos, 1, pageBufferMax, splits.size());
        }

        @Override
        public QueryState getQueryState()
        {
            return queryState;
        }

        @Override
        public List<TupleInfo> getTupleInfos()
        {
            return tupleInfos;
        }

        @Override
        public void run()
        {
            try {
                List<Future<Void>> results = shardExecutor.invokeAll(Lists.transform(splits, new Function<Split, Callable<Void>>()
                {
                    @Override
                    public Callable<Void> apply(Split split)
                    {
                        return new SplitSumFragmentWorker(queryState, split, fragment, dataStreamProvider, metadata);
                    }
                }));

                checkQueryResults(results);

                queryState.sourceFinished();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                queryState.queryFailed(e);
                throw Throwables.propagate(e);
            }
            catch (Exception e) {
                queryState.queryFailed(e);
                throw Throwables.propagate(e);
            }
        }

        private static class SplitSumFragmentWorker
                implements Callable<Void>
        {
            private final QueryState queryState;
            private final Operator operator;

            private SplitSumFragmentWorker(QueryState queryState, Split split, PlanFragment fragment, DataStreamProvider dataStreamProvider, Metadata metadata)
            {
                this.queryState = queryState;

                ExecutionPlanner planner = new ExecutionPlanner(new SessionMetadata(metadata), dataStreamProvider, fragment.getSymbols(), ImmutableMap.<Integer, Operator>of(), split);
                operator = planner.plan(fragment.getRoot());
            }

            @Override
            public Void call()
                    throws Exception
            {
                try {
                    for (Page page : operator) {
                        queryState.addPage(page);
                    }
                    return null;
                }
                catch (Exception e) {
                    queryState.queryFailed(e);
                    throw Throwables.propagate(e);
                } finally {
                    queryState.splitCompleted();
                }
            }
        }
    }

    private static class ImportTableQuery
            implements MasterQueryTask
    {
        private static final ImmutableList<TupleInfo> TUPLE_INFOS = ImmutableList.of(SINGLE_LONG);

        private final MasterQueryState masterQueryState;
        private final ImportClientFactory importClientFactory;
        private final ImportManager importManager;
        private final Metadata metadata;
        private final String sourceName;
        private final String databaseName;
        private final String tableName;

        private ImportTableQuery(
                String queryId,
                int pageBufferMax,
                ImportClientFactory importClientFactory,
                ImportManager importManager,
                Metadata metadata,
                String sourceName,
                String databaseName,
                String tableName)
        {
            this.masterQueryState = new MasterQueryState(queryId, new QueryState(TUPLE_INFOS, 1, pageBufferMax));
            this.importClientFactory = importClientFactory;
            this.importManager = importManager;
            this.metadata = metadata;
            this.sourceName = sourceName;
            this.databaseName = databaseName;
            this.tableName = tableName;
        }

        @Override
        public MasterQueryState getMasterQueryState()
        {
            return masterQueryState;
        }

        @Override
        public QueryState getQueryState()
        {
            return masterQueryState.getOutputQueryState();
        }

        @Override
        public List<TupleInfo> getTupleInfos()
        {
            return TUPLE_INFOS;
        }

        @Override
        public void run()
        {
            QueryState queryState = masterQueryState.getOutputQueryState();
            try {
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

                queryState.addPage(new Page(new BlockBuilder(TupleInfo.SINGLE_LONG).append(0).build()));
                queryState.sourceFinished();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                queryState.queryFailed(e);
                throw Throwables.propagate(e);
            }
            catch (Exception e) {
                queryState.queryFailed(e);
                throw Throwables.propagate(e);
            }
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

    private static class ImportDelimited
            implements MasterQueryTask
    {
        private static final ImmutableList<TupleInfo> TUPLE_INFOS = ImmutableList.of(SINGLE_LONG);

        private final MasterQueryState masterQueryState;
        private final String databaseName;
        private final String tableName;
        private final RecordProjectOperator source;

        public ImportDelimited(
                String queryId,
                int pageBufferMax,
                String databaseName,
                String tableName,
                TupleInfo tupleInfo,
                File delimitedFile,
                Splitter splitter)
        {
            this.masterQueryState = new MasterQueryState(queryId, new QueryState(TUPLE_INFOS, 1, pageBufferMax));
            this.databaseName = databaseName;
            this.tableName = tableName;

            ImmutableList.Builder<RecordProjection> builder = ImmutableList.builder();
            for (int i = 0; i < tupleInfo.getFieldCount(); i++) {
                builder.add(RecordProjections.createProjection(i, tupleInfo.getTypes().get(i)));
            }
            List<RecordProjection> recordProjections = builder.build();

            InputSupplier<InputStreamReader> readerSupplier = Files.newReaderSupplier(delimitedFile, Charsets.UTF_8);
            DelimitedRecordIterable records = new DelimitedRecordIterable(readerSupplier, splitter);
            source = new RecordProjectOperator(records, recordProjections);
        }

        @Override
        public MasterQueryState getMasterQueryState()
        {
            return masterQueryState;
        }

        @Override
        public QueryState getQueryState()
        {
            return masterQueryState.getOutputQueryState();
        }

        @Override
        public List<TupleInfo> getTupleInfos()
        {
            return TUPLE_INFOS;
        }

        @Override
        public void run()
        {
            QueryState queryState = masterQueryState.getOutputQueryState();
            try {
                // TODO: fix this
                //                long rowCount = storageManager.importShard(source, databaseName, tableName);
                long rowCount = 0;
                queryState.addPage(new Page(new BlockBuilder(SINGLE_LONG).append(rowCount).build()));
                queryState.sourceFinished();
                throw new UnsupportedOperationException();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                queryState.queryFailed(e);
                throw Throwables.propagate(e);
            }
            catch (Exception e) {
                queryState.queryFailed(e);
                throw Throwables.propagate(e);
            }
        }
    }

    private static void checkQueryResults(Iterable<Future<Void>> results)
            throws InterruptedException
    {
        RuntimeException queryFailedException = null;
        for (Future<Void> result : results) {
            try {
                result.get();
            }
            catch (ExecutionException e) {
                if (queryFailedException == null) {
                    queryFailedException = new RuntimeException("Query failed");
                }
                Throwable cause = e.getCause();
                queryFailedException.addSuppressed(cause);
            }
        }
        if (queryFailedException != null) {
            throw queryFailedException;
        }
    }

    private static void waitForRunning(List<HttpQueryProvider> queryProviders)
    {
        while (true) {
            long start = System.nanoTime();

            queryProviders = ImmutableList.copyOf(Iterables.filter(queryProviders, new Predicate<HttpQueryProvider>()
            {
                @Override
                public boolean apply(HttpQueryProvider queryProvider)
                {
                    QueryInfo queryInfo = queryProvider.getQueryInfo();
                    return queryInfo.getState() == State.PREPARING;
                }
            }));
            if (queryProviders.isEmpty()) {
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
