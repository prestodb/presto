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
import com.facebook.presto.metadata.ColumnHandle;
import com.facebook.presto.metadata.ColumnMetadata;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.NativeColumnHandle;
import com.facebook.presto.metadata.NativeTableHandle;
import com.facebook.presto.metadata.Node;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.operator.AggregationOperator;
import com.facebook.presto.operator.HashAggregationOperator;
import com.facebook.presto.operator.HashJoinOperator;
import com.facebook.presto.operator.LimitOperator;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.SourceHashManager;
import com.facebook.presto.operator.SourceHashProvider;
import com.facebook.presto.server.QueryState.State;
import com.facebook.presto.spi.ImportClient;
import com.facebook.presto.spi.SchemaField;
import com.facebook.presto.split.DataStreamProvider;
import com.facebook.presto.split.ExchangeSplit;
import com.facebook.presto.split.ImportClientFactory;
import com.facebook.presto.split.PlanFragment;
import com.facebook.presto.split.Split;
import com.facebook.presto.split.SplitAssignments;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleInfo.Type;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;
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

import static com.facebook.presto.operator.ProjectionFunctions.concat;
import static com.facebook.presto.operator.ProjectionFunctions.singleColumn;
import static com.facebook.presto.operator.aggregation.AggregationFunctions.finalAggregation;
import static com.facebook.presto.operator.aggregation.AggregationFunctions.partialAggregation;
import static com.facebook.presto.operator.aggregation.CountAggregation.countAggregation;
import static com.facebook.presto.operator.aggregation.LongSumAggregation.longSumAggregation;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_LONG;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_VARBINARY;
import static com.facebook.presto.tuple.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.tuple.TupleInfo.Type.VARIABLE_BINARY;
import static com.facebook.presto.util.Threads.threadsNamed;
import static com.google.common.collect.Iterables.filter;
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

    private final SourceHashManager sourceHashManager;

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
        this.sourceHashManager = new SourceHashManager(queryExecutor);
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

            // e.g.: sum-frag:catalog
            case "sum-frag":
                queryTask = new SumFragmentMaster(queryId, pageBufferMax, queryExecutor, metadata, splitManager, queryFragmentRequestJsonCodec, strings.get(1));
                break;

            case "join-frag":
                queryTask = new JoinFragmentMaster(queryId, pageBufferMax, queryExecutor, metadata, splitManager, queryFragmentRequestJsonCodec);
                break;

            default:
                throw new IllegalArgumentException("Unsupported query " + query);
        }

        MasterQueryState masterQueryState = queryTask.getMasterQueryState();
        masterQueries.put(queryId, masterQueryState);
        queries.put(queryId, masterQueryState.getOutputQueryState());
        queryExecutor.submit(queryTask);

        return new QueryInfo(queryId, queryTask.getTupleInfos(), State.PREPARING, 0);
    }

    @Override
    public synchronized QueryInfo createQueryFragment(Map<String, List<Split>> sourceSplits, PlanFragment planFragment)
    {
        String queryId = String.valueOf(nextQueryId++);


        QueryTask queryTask;
        switch (planFragment.getQuery()) {
            case "sum-frag-worker":
                queryTask = new SumFragmentWorker(pageBufferMax, sourceSplits, planFragment.getColumnHandles(), dataStreamProvider, shardExecutor);
                break;
            case "join-worker":
                queryTask = new JoinWorker(queryId, pageBufferMax, sourceSplits, planFragment.getColumnHandles(), dataStreamProvider, sourceHashManager, shardExecutor);
                break;
            case "join-build-worker":
                queryTask = new JoinBuildTableWorker(pageBufferMax, sourceSplits, planFragment.getColumnHandles(), dataStreamProvider, shardExecutor);
                break;
            default:
                throw new IllegalArgumentException("Unsupported fragment query: " + planFragment.getQuery());
        }

        queries.put(queryId, queryTask.getQueryState());
        queryExecutor.submit(queryTask);

        return new QueryInfo(queryId, queryTask.getTupleInfos(), State.PREPARING, 0);
    }

    @Override
    public QueryInfo getQueryInfo(String queryId)
    {
        try {
            return getMasterQuery(queryId).toQueryInfo();
        }
        catch (NoSuchElementException e) {
            QueryState queryState = getQuery(queryId);
            return new QueryInfo(queryId, queryState.getTupleInfos(), queryState.getState(), queryState.getBufferedPageCount());
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

    private static class SumFragmentMaster
            implements MasterQueryTask
    {
        private final MasterQueryState masterQueryState;
        private final ExecutorService executor;
        private final ApacheHttpClient httpClient;
        private final JsonCodec<QueryFragmentRequest> codec;
        private final Metadata metadata;
        private final SplitManager splitManager;
        private final String catalogName;

        public SumFragmentMaster(String queryId,
                int pageBufferMax,
                ExecutorService executor,
                Metadata metadata,
                SplitManager splitManager,
                JsonCodec<QueryFragmentRequest> codec,
                String catalogName)
        {
            this.masterQueryState = new MasterQueryState(queryId, new QueryState(ImmutableList.of(new TupleInfo(VARIABLE_BINARY, FIXED_INT_64)), 1, pageBufferMax));
            this.executor = executor;
            this.codec = codec;
            httpClient = new ApacheHttpClient(new HttpClientConfig()
                    .setConnectTimeout(new Duration(5, TimeUnit.MINUTES))
                    .setReadTimeout(new Duration(5, TimeUnit.MINUTES)));
            this.metadata = metadata;
            this.splitManager = splitManager;
            this.catalogName = catalogName;
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
            QueryState queryState = masterQueryState.getOutputQueryState();
            try {
                final TableMetadata table = metadata.getTable(catalogName, "default", "hivedba_query_stats");
                Iterable<SplitAssignments> splitAssignments = splitManager.getSplitAssignments(table.getTableHandle().get());

                Multimap<Node, Split> nodeSplits = SplitAssignments.randomNodeAssignment(new Random(), splitAssignments);
                List<HttpQueryProvider> providers = ImmutableList.copyOf(transform(nodeSplits.asMap().entrySet(),
                        new Function<Entry<Node, Collection<Split>>, HttpQueryProvider>()
                        {
                            @Override
                            public HttpQueryProvider apply(Entry<Node, Collection<Split>> splits)
                            {
                                QueryFragmentRequest queryFragmentRequest = new QueryFragmentRequest(
                                        ImmutableMap.<String, List<Split>>of("source", ImmutableList.copyOf(splits.getValue())),
                                        new PlanFragment("sum-frag-worker",
                                                ImmutableList.of(
                                                        table.getColumns().get(2).getColumnHandle().get(),
                                                        table.getColumns().get(6).getColumnHandle().get()
                                                )
                                        )
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

                masterQueryState.addStage("sum-frag-worker", providers);

                // wait for providers to start
                waitForRunning(providers);

                QueryDriversOperator operator = new QueryDriversOperator(10, providers);

                HashAggregationOperator aggregation = new HashAggregationOperator(operator,
                        0,
                        ImmutableList.of(finalAggregation(longSumAggregation(1, 0))),
                        ImmutableList.of(concat(singleColumn(VARIABLE_BINARY, 0, 0), singleColumn(FIXED_INT_64, 1, 0))));

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

    private static class SumFragmentWorker
            implements QueryTask
    {
        private static final ImmutableList<TupleInfo> TUPLE_INFOS = ImmutableList.of(SINGLE_VARBINARY, SINGLE_LONG);
        private final QueryState queryState;
        private final List<Split> splits;
        private final List<ColumnHandle> columnHandles;
        private final DataStreamProvider dataStreamProvider;
        private final ThreadPoolExecutor shardExecutor;

        private SumFragmentWorker(int pageBufferMax,
                Map<String, List<Split>> sourceSplits,
                List<ColumnHandle> columnHandles,
                DataStreamProvider dataStreamProvider,
                ThreadPoolExecutor shardExecutor)
        {
            this.queryState = new QueryState(TUPLE_INFOS, 1, pageBufferMax);
            this.splits = sourceSplits.get("source");
            this.columnHandles = ImmutableList.copyOf(columnHandles);
            this.dataStreamProvider = dataStreamProvider;
            this.shardExecutor = shardExecutor;
        }

        @Override
        public QueryState getQueryState()
        {
            return queryState;
        }

        @Override
        public List<TupleInfo> getTupleInfos()
        {
            return TUPLE_INFOS;
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
                        return new SplitSumFragmentWorker(queryState, split, columnHandles, dataStreamProvider);
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
            private final Split split;
            private final List<ColumnHandle> columnHandles;
            private final DataStreamProvider dataStreamProvider;

            private SplitSumFragmentWorker(QueryState queryState, Split split, List<ColumnHandle> columnHandles, DataStreamProvider dataStreamProvider)
            {
                this.queryState = queryState;
                this.split = split;
                this.columnHandles = ImmutableList.copyOf(columnHandles);
                this.dataStreamProvider = dataStreamProvider;
            }

            @Override
            public Void call()
                    throws Exception
            {
                try {
                    Operator dataStream = dataStreamProvider.createDataStream(split, columnHandles);
                    HashAggregationOperator sumOperator = new HashAggregationOperator(
                            dataStream,
                            0,
                            ImmutableList.of(partialAggregation(longSumAggregation(1, 0))),
                            ImmutableList.of(singleColumn(VARIABLE_BINARY, 0, 0), singleColumn(FIXED_INT_64, 1, 0)));

                    for (Page page : sumOperator) {
                        queryState.addPage(page);
                    }
                    return null;
                }
                catch (Exception e) {
                    queryState.queryFailed(e);
                    throw Throwables.propagate(e);
                }
            }
        }
    }

    private static class JoinFragmentMaster
            implements MasterQueryTask
    {
        private final MasterQueryState masterQueryState;
        private final ExecutorService executor;
        private final ApacheHttpClient httpClient;
        private final JsonCodec<QueryFragmentRequest> codec;
        private final Metadata metadata;
        private final SplitManager splitManager;

        public JoinFragmentMaster(String queryId, int pageBufferMax, ExecutorService executor, Metadata metadata, SplitManager splitManager, JsonCodec<QueryFragmentRequest> codec)
        {
            this.masterQueryState = new MasterQueryState(queryId, new QueryState(ImmutableList.of(new TupleInfo(FIXED_INT_64, FIXED_INT_64, FIXED_INT_64)), 1, pageBufferMax));
            this.executor = executor;
            this.codec = codec;
            httpClient = new ApacheHttpClient(new HttpClientConfig()
                    .setConnectTimeout(new Duration(5, TimeUnit.MINUTES))
                    .setReadTimeout(new Duration(5, TimeUnit.MINUTES)));
            this.metadata = metadata;
            this.splitManager = splitManager;
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
            QueryState queryState = masterQueryState.getOutputQueryState();
            try {
                final List<Split> buildSplits = scheduleBuildTableScan();

                final TableMetadata table = metadata.getTable("default", "default", "hivedba_query_stats");
                Iterable<SplitAssignments> splitAssignments = splitManager.getSplitAssignments(table.getTableHandle().get());

                final PlanFragment planFragment = new PlanFragment("join-worker",
                        ImmutableList.of(
                                table.getColumns().get(2).getColumnHandle().get(),
                                table.getColumns().get(6).getColumnHandle().get()
                        )
                );

                Multimap<Node, Split> nodeSplits = SplitAssignments.randomNodeAssignment(new Random(), splitAssignments);
                List<HttpQueryProvider> driverProviders = ImmutableList.copyOf(
                        transform(nodeSplits.asMap().entrySet(), new Function<Entry<Node, Collection<Split>>, HttpQueryProvider>()
                        {
                            @Override
                            public HttpQueryProvider apply(Entry<Node, Collection<Split>> splits)
                            {
                                ImmutableMap<String, List<Split>> sources = ImmutableMap.<String, List<Split>>of(
                                        "probe", ImmutableList.copyOf(splits.getValue()),
                                        "build", ImmutableList.copyOf(buildSplits)
                                );
                                QueryFragmentRequest queryFragmentRequest = new QueryFragmentRequest(sources, planFragment);

                                return new HttpQueryProvider(
                                        jsonBodyGenerator(codec, queryFragmentRequest),
                                        Optional.of(MediaType.APPLICATION_JSON),
                                        httpClient,
                                        executor,
                                        splits.getKey().getHttpUri().resolve("/v1/presto/query")
                                );

                            }
                        }));

                masterQueryState.addStage("join-worker", driverProviders);

                // wait for all queries to start
                waitForRunning(driverProviders);

                QueryDriversOperator operator = new QueryDriversOperator(10, driverProviders);

                AggregationOperator aggregation = new AggregationOperator(operator,
                        ImmutableList.of(finalAggregation(countAggregation(0, 0)), finalAggregation(longSumAggregation(1, 0)), finalAggregation(longSumAggregation(2, 0))),
                        ImmutableList.of(concat(singleColumn(FIXED_INT_64, 0, 0), singleColumn(FIXED_INT_64, 1, 0), singleColumn(FIXED_INT_64, 2, 0))));

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

        private List<Split> scheduleBuildTableScan()
        {
            TableMetadata table = metadata.getTable("default", "default", "hivedba_query_stats");
            Iterable<SplitAssignments> splitAssignments = splitManager.getSplitAssignments(table.getTableHandle().get());

            final PlanFragment buildPlanFragment = new PlanFragment("join-build-worker",
                    ImmutableList.of(
                            table.getColumns().get(2).getColumnHandle().get(),
                            table.getColumns().get(6).getColumnHandle().get()
                    )
            );

            Multimap<Node, Split> nodeSplits = SplitAssignments.randomNodeAssignment(new Random(), splitAssignments);
            List<HttpQueryProvider> queryProviders = ImmutableList.copyOf(transform(nodeSplits.asMap().entrySet(), new Function<Entry<Node, Collection<Split>>, HttpQueryProvider>()
            {
                @Override
                public HttpQueryProvider apply(Entry<Node, Collection<Split>> splits)
                {
                    ImmutableMap<String, List<Split>> sources = ImmutableMap.<String, List<Split>>of("source", ImmutableList.copyOf(splits.getValue()));
                    QueryFragmentRequest queryFragmentRequest = new QueryFragmentRequest(sources, buildPlanFragment);

                    return new HttpQueryProvider(
                            jsonBodyGenerator(codec, queryFragmentRequest),
                            Optional.of(MediaType.APPLICATION_JSON),
                            httpClient,
                            executor,
                            splits.getKey().getHttpUri().resolve("/v1/presto/query")
                    );
                }
            }));

            masterQueryState.addStage("join-build-worker", queryProviders);

            // wait for all queries to start
            waitForRunning(queryProviders);

            // create a split for each query
            List<Split> splits = ImmutableList.copyOf(Lists.transform(queryProviders, new Function<HttpQueryProvider, Split>()
            {
                @Override
                public Split apply(HttpQueryProvider queryProvider)
                {
                    return new ExchangeSplit(queryProvider.getLocation());
                }
            }));
            return splits;
        }
    }

    private static class JoinWorker
            implements QueryTask
    {
        private static final ImmutableList<TupleInfo> TUPLE_INFOS = ImmutableList.of(SINGLE_LONG, SINGLE_LONG, SINGLE_LONG);
        private final String queryId;
        private final QueryState queryState;
        private final List<Split> probeSplits;
        private final List<Split> buildSplits;
        private final List<ColumnHandle> columnHandles;

        private final DataStreamProvider dataStreamProvider;

        private final SourceHashManager sourceHashManager;
        private final ThreadPoolExecutor shardExecutor;

        public JoinWorker(String queryId,
                int pageBufferMax,
                Map<String, List<Split>> sourceSplits,
                List<ColumnHandle> columnHandles,
                DataStreamProvider dataStreamProvider,
                SourceHashManager sourceHashManager, ThreadPoolExecutor shardExecutor)
        {
            this.queryId = queryId;
            this.queryState = new QueryState(TUPLE_INFOS, 1, pageBufferMax);

            this.probeSplits = sourceSplits.get("probe");
            this.buildSplits = sourceSplits.get("build");
            this.shardExecutor = shardExecutor;
            this.columnHandles = ImmutableList.copyOf(columnHandles);
            this.dataStreamProvider = dataStreamProvider;

            this.sourceHashManager = sourceHashManager;
        }

        @Override
        public QueryState getQueryState()
        {
            return queryState;
        }

        @Override
        public List<TupleInfo> getTupleInfos()
        {
            return TUPLE_INFOS;
        }

        @Override
        public void run()
        {
            try {
                final SourceHashProvider hashProvider = sourceHashManager.getSourceHashProvider(queryId,
                        0,
                        1_000_000,
                        buildSplits,
                        ImmutableList.of(SINGLE_VARBINARY, SINGLE_LONG)
                );

                List<Future<Void>> results = shardExecutor.invokeAll(Lists.transform(probeSplits, new Function<Split, Callable<Void>>()
                {
                    @Override
                    public Callable<Void> apply(Split split)
                    {
                        return new SplitJoinWorker(queryState, split, columnHandles, dataStreamProvider, hashProvider);
                    }
                }));

                checkQueryResults(results);

                queryState.sourceFinished();
                sourceHashManager.dropSourceHashProvider(queryId);
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

        private static class SplitJoinWorker
                implements Callable<Void>
        {
            private final QueryState queryState;
            private final Split split;
            private final List<ColumnHandle> columnHandles;
            private final DataStreamProvider dataStreamProvider;
            private final SourceHashProvider hashProvider;

            private SplitJoinWorker(QueryState queryState, Split split, List<ColumnHandle> columnHandles, DataStreamProvider dataStreamProvider, SourceHashProvider hashProvider)
            {
                this.queryState = queryState;
                this.split = split;
                this.columnHandles = columnHandles;
                this.dataStreamProvider = dataStreamProvider;
                this.hashProvider = hashProvider;
            }

            @Override
            public Void call()
                    throws Exception
            {
                try {
                    Operator dataStream = dataStreamProvider.createDataStream(split, columnHandles);
                    HashJoinOperator hashJoinOperator = new HashJoinOperator(hashProvider, dataStream, 0);
                    AggregationOperator sumOperator = new AggregationOperator(
                            hashJoinOperator,
                            ImmutableList.of(partialAggregation(countAggregation(1, 0)),
                                    partialAggregation(longSumAggregation(1, 0)),
                                    partialAggregation(longSumAggregation(2, 0))),
                            ImmutableList.of(singleColumn(FIXED_INT_64, 0, 0), singleColumn(FIXED_INT_64, 1, 0), singleColumn(FIXED_INT_64, 2, 0)));

                    for (Page page : sumOperator) {
                        queryState.addPage(page);
                    }
                    return null;

                }
                catch (Exception e) {
                    queryState.queryFailed(e);
                    throw e;
                }
            }
        }
    }


    private static class JoinBuildTableWorker
            implements QueryTask
    {
        private static final ImmutableList<TupleInfo> TUPLE_INFOS = ImmutableList.of(SINGLE_VARBINARY, SINGLE_LONG);
        private final QueryState queryState;
        private final List<Split> splits;
        private final List<ColumnHandle> columnHandles;
        private final DataStreamProvider dataStreamProvider;
        private final ThreadPoolExecutor shardExecutor;

        private JoinBuildTableWorker(int pageBufferMax,
                Map<String, List<Split>> sourceSplits,
                List<ColumnHandle> columnHandles,
                DataStreamProvider dataStreamProvider,
                ThreadPoolExecutor shardExecutor)
        {
            this.queryState = new QueryState(TUPLE_INFOS, 1, pageBufferMax);
            this.splits = sourceSplits.get("source");
            this.columnHandles = ImmutableList.copyOf(columnHandles);
            this.dataStreamProvider = dataStreamProvider;
            this.shardExecutor = shardExecutor;
        }

        @Override
        public QueryState getQueryState()
        {
            return queryState;
        }

        @Override
        public List<TupleInfo> getTupleInfos()
        {
            return TUPLE_INFOS;
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
                        return new SplitJoinBuildTableWorker(queryState, split, columnHandles, dataStreamProvider);
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

        private static class SplitJoinBuildTableWorker
                implements Callable<Void>
        {
            private final QueryState queryState;
            private final Split split;
            private final List<ColumnHandle> columnHandles;
            private final DataStreamProvider dataStreamProvider;

            private SplitJoinBuildTableWorker(QueryState queryState, Split split, List<ColumnHandle> columnHandles, DataStreamProvider dataStreamProvider)
            {
                this.queryState = queryState;
                this.split = split;
                this.columnHandles = columnHandles;
                this.dataStreamProvider = dataStreamProvider;
            }

            @Override
            public Void call()
                    throws Exception
            {
                try {
                    Operator dataStream = dataStreamProvider.createDataStream(split, columnHandles);
                    LimitOperator limitOperator = new LimitOperator(dataStream, 2);

                    for (Page page : limitOperator) {
                        queryState.addPage(page);
                    }
                    return null;
                }
                catch (Exception e) {
                    queryState.queryFailed(e);
                    throw e;
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

            queryProviders = ImmutableList.copyOf(filter(queryProviders, new Predicate<HttpQueryProvider>()
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
