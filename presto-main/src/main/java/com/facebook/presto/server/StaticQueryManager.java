/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleInfo.Type;
import com.facebook.presto.hive.HiveClient;
import com.facebook.presto.ingest.DelimitedRecordIterable;
import com.facebook.presto.ingest.RecordProjectOperator;
import com.facebook.presto.ingest.RecordProjection;
import com.facebook.presto.ingest.RecordProjections;
import com.facebook.presto.metadata.ColumnMetadata;
import com.facebook.presto.metadata.HiveImportManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.StorageManager;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.operator.AggregationOperator;
import com.facebook.presto.operator.AlignmentOperator;
import com.facebook.presto.operator.HashAggregationOperator;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.aggregation.LongSumAggregation;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.airlift.http.client.ApacheHttpClient;
import io.airlift.http.client.AsyncHttpClient;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.units.Duration;

import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;
import java.io.File;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.util.RetryDriver.runWithRetry;
import static com.facebook.presto.operator.ProjectionFunctions.concat;
import static com.facebook.presto.operator.ProjectionFunctions.singleColumn;

public class StaticQueryManager implements QueryManager
{
    private final int pageBufferMax;
    private final ExecutorService executor;
    private final HiveClient hiveClient;
    private final Metadata metadata;
    private final StorageManager storageManager;
    private final HiveImportManager hiveImportManager;

    @GuardedBy("this")
    private int nextQueryId;

    // todo this is a potential memory leak if the query objects are not removed
    @GuardedBy("this")
    private final Map<String, QueryState> queries = new HashMap<>();

    @Inject
    public StaticQueryManager(HiveClient hiveClient, Metadata metadata, StorageManager storageManager, HiveImportManager hiveImportManager)
    {
        this(20, hiveClient, metadata, storageManager, hiveImportManager);
    }

    public StaticQueryManager(int pageBufferMax, HiveClient hiveClient, Metadata metadata, StorageManager storageManager, HiveImportManager hiveImportManager)
    {
        Preconditions.checkArgument(pageBufferMax > 0, "blockBufferMax must be at least 1");
        this.pageBufferMax = pageBufferMax;
        executor = Executors.newFixedThreadPool(50, new ThreadFactoryBuilder().setNameFormat("http-query-processor-%d").build()) ;
        this.hiveClient = hiveClient;
        this.metadata = metadata;
        this.storageManager = storageManager;
        this.hiveImportManager = hiveImportManager;
    }

    @Override
    public synchronized String createQuery(String query)
    {
        Preconditions.checkNotNull(query, "query is null");
        Preconditions.checkArgument(query.length() > 0, "query must not be empty string");

        String queryId = String.valueOf(nextQueryId++);
        final QueryState queryState = new QueryState(1, pageBufferMax);

        // TODO: fix how we get our query parameters. typically we will have a query parser/planner to drive executions, but in the meantime, just use this ghetto-ness
        ImmutableList<String> strings = ImmutableList.copyOf(Splitter.on(":").split(query));
        String queryBase = strings.get(0);

        Runnable queryTask;
        switch (queryBase) {
            case "sum":
                queryTask = new SumQuery(queryState, executor,
                        URI.create("http://localhost:8080/v1/presto/query"),
                        URI.create("http://localhost:8080/v1/presto/query"),
                        URI.create("http://localhost:8080/v1/presto/query"),
                        URI.create("http://localhost:8080/v1/presto/query"),

                        URI.create("http://localhost:8080/v1/presto/query"),
                        URI.create("http://localhost:8080/v1/presto/query"),
                        URI.create("http://localhost:8080/v1/presto/query"),
                        URI.create("http://localhost:8080/v1/presto/query"),

                        URI.create("http://localhost:8080/v1/presto/query"),
                        URI.create("http://localhost:8080/v1/presto/query"),
                        URI.create("http://localhost:8080/v1/presto/query"),
                        URI.create("http://localhost:8080/v1/presto/query"),

                        URI.create("http://localhost:8080/v1/presto/query"),
                        URI.create("http://localhost:8080/v1/presto/query"),
                        URI.create("http://localhost:8080/v1/presto/query"),
                        URI.create("http://localhost:8080/v1/presto/query"));
                break;
            case "sum-partial":
                queryTask = new PartialSumQuery(metadata, storageManager, queryState);
                break;

            // e.g.: import-delimited:default:hivedba_query_stats:string,long,string,long:/tmp/myfile.csv:,
            case "import-delimited":
                List<Type> types = ImmutableList.copyOf(Iterables.transform(Splitter.on(",").split(strings.get(2)), new Function<String, Type>()
                {
                    @Override
                    public Type apply(String input)
                    {
                        return Type.fromName(input);
                    }
                }));
                TupleInfo tupleInfo = new TupleInfo(types);
                queryTask = new ImportDelimited(queryState, storageManager, strings.get(1), strings.get(2), tupleInfo, new File(strings.get(3)), Splitter.on(strings.get(4)));
                break;

            // e.g.: import-hive-table:default:hivedba_query_stats
            case "import-hive-table":
                queryTask = new ImportHiveTableQuery(queryState, hiveClient, executor, strings.get(1), strings.get(2));
                break;

            // e.g.: import-hive-table-partition:default:hivedba_query_stats:ds=2012-07-11/cluster_name=silver
            case "import-hive-table-partition":
                queryTask = new ImportHiveTablePartitionQuery(queryState, hiveImportManager, strings.get(1), strings.get(2), strings.get(3));
                break;

            default:
                throw new IllegalArgumentException("Unsupported query " + query);
        }

        queries.put(queryId, queryState);
        executor.submit(queryTask);

        return queryId;
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
            query = queries.remove(queryId);
        }
        if (query != null && !query.isDone()) {
            query.cancel();
        }
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

    private static class SumQuery implements Runnable
    {
        private final QueryState queryState;
        private final List<URI> servers;
        private final AsyncHttpClient asyncHttpClient;

        public SumQuery(QueryState queryState, ExecutorService executor, URI... servers)
        {
            this(queryState, executor, ImmutableList.copyOf(servers));
        }

        public SumQuery(QueryState queryState, ExecutorService executor, Iterable<URI> servers)
        {
            this.queryState = queryState;
            this.servers = ImmutableList.copyOf(servers);
            ApacheHttpClient httpClient = new ApacheHttpClient(new HttpClientConfig()
                    .setConnectTimeout(new Duration(1, TimeUnit.MINUTES))
                    .setReadTimeout(new Duration(1, TimeUnit.MINUTES)));
            asyncHttpClient = new AsyncHttpClient(httpClient, executor);
        }

        @Override
        public void run()
        {
            try {
                QueryDriversOperator operator = new QueryDriversOperator(10,
                        Iterables.transform(servers, new Function<URI, QueryDriverProvider>()
                        {
                            @Override
                            public QueryDriverProvider apply(URI uri)
                            {
                                return new HttpQueryProvider("sum-partial", asyncHttpClient, uri, 1);
                            }
                        })
                );

                HashAggregationOperator aggregation = new HashAggregationOperator(operator,
                        0,
                        ImmutableList.of(LongSumAggregation.provider(1, 0)),
                        ImmutableList.of(concat(singleColumn(Type.VARIABLE_BINARY, 0, 0), singleColumn(Type.FIXED_INT_64, 2, 0))));

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

    private static class PartialSumQuery implements Runnable
    {
        private final Metadata metadata;
        private final StorageManager storageManager;
        private final QueryState queryState;

        public PartialSumQuery(Metadata metadata, StorageManager storageManager, QueryState queryState)
        {
            this.metadata = metadata;
            this.storageManager = storageManager;
            this.queryState = queryState;
        }

        @Override
        public void run()
        {
            try {
                BlockIterable groupBySource = getColumn("hivedba_query_stats", "pool_name");
                BlockIterable aggregateSource = getColumn("hivedba_query_stats", "cpu_msec");

                AlignmentOperator alignmentOperator = new AlignmentOperator(groupBySource, aggregateSource);
                HashAggregationOperator sumOperator = new HashAggregationOperator(alignmentOperator,
                        0,
                        ImmutableList.of(LongSumAggregation.provider(0, 0)),
                        ImmutableList.of(concat(singleColumn(Type.VARIABLE_BINARY, 0, 0), singleColumn(Type.FIXED_INT_64, 1, 0))));

                for (Page page : sumOperator) {
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

        private BlockIterable getColumn(String tableName, String columnName)
        {
            TableMetadata tableMetadata = metadata.getTable("default", "default", tableName);
            int index = 0;
            for (ColumnMetadata columnMetadata : tableMetadata.getColumns()) {
                if (columnName.equals(columnMetadata.getName())) {
                    break;
                }
                ++index;
            }
            final int columnIndex = index;
            return storageManager.getBlocks("default", tableName, columnIndex);
        }

    }

    private static class ImportHiveTableQuery
            implements Runnable
    {
        private static final int MAX_SIMULTANEOUS_IMPORTS = 100;

        private final QueryState queryState;
        private final HiveClient hiveClient;
        private final String databaseName;
        private final String tableName;
        private final AsyncHttpClient asyncHttpClient;

        private ImportHiveTableQuery(
                QueryState queryState,
                HiveClient hiveClient,
                ExecutorService executorService,
                String databaseName,
                String tableName)
        {
            this.queryState = queryState;
            this.hiveClient = hiveClient;
            this.databaseName = databaseName;
            this.tableName = tableName;

            ApacheHttpClient httpClient = new ApacheHttpClient(new HttpClientConfig()
                    .setConnectTimeout(new Duration(15, TimeUnit.MINUTES))
                    .setReadTimeout(new Duration(30, TimeUnit.MINUTES)));
            asyncHttpClient = new AsyncHttpClient(httpClient, executorService);
        }

        @Override
        public void run()
        {
            try {
                List<String> partitionNames = runWithRetry(new Callable<List<String>>()
                {
                    @Override
                    public List<String> call()
                            throws Exception
                    {
                        return Ordering.natural().immutableSortedCopy(hiveClient.getPartitionNames(databaseName, tableName));
                    }
                }, databaseName + "." + tableName + ".getPartitionNames");

                for (List<String> subPartitionList : Lists.partition(partitionNames, MAX_SIMULTANEOUS_IMPORTS)) {
                    ImmutableList.Builder<QueryDriverProvider> queryDriverProviderBuilder = ImmutableList.builder();
                    for (String subPartitionName : subPartitionList) {
                        queryDriverProviderBuilder.add(new HttpQueryProvider(
                                "import-hive-table-partition:" + databaseName + ":" + tableName + ":" + subPartitionName,
                                asyncHttpClient,
                                URI.create("http://localhost:8080/v1/presto/query") // TODO: HACK to distribute the queries locally
                                ,
                                1));
                    }
                    // TODO: this currently leaks query resources (need to delete)
                    QueryDriversOperator operator = new QueryDriversOperator(10, queryDriverProviderBuilder.build());
                    AggregationOperator sumOperator = new AggregationOperator(operator,
                            ImmutableList.of(LongSumAggregation.provider(0, 0)),
                            ImmutableList.of(concat(singleColumn(Type.FIXED_INT_64, 0, 0))));
                    for (Page page : sumOperator) {
                        queryState.addPage(page);
                    }
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

    private static class ImportHiveTablePartitionQuery
            implements Runnable
    {
        private final QueryState queryState;
        private final HiveImportManager hiveImportManager;
        private final String databaseName;
        private final String tableName;
        private final String partitionName;

        private ImportHiveTablePartitionQuery(
                QueryState queryState,
                HiveImportManager hiveImportManager,
                String databaseName,
                String tableName,
                String partitionName)
        {
            this.queryState = queryState;
            this.hiveImportManager = hiveImportManager;
            this.databaseName = databaseName;
            this.tableName = tableName;
            this.partitionName = partitionName;
        }

        @Override
        public void run()
        {
            try {
                long rowCount = hiveImportManager.importPartition(databaseName, tableName, partitionName);
                queryState.addPage(new Page(new BlockBuilder(0, TupleInfo.SINGLE_LONG).append(rowCount).build()));
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

    private static class ImportDelimited
            implements Runnable
    {
        private final QueryState queryState;
        private final StorageManager storageManager;
        private final String databaseName;
        private final String tableName;
        private final RecordProjectOperator source;

        public ImportDelimited(
                QueryState queryState,
                StorageManager storageManager,
                String databaseName,
                String tableName,
                TupleInfo tupleInfo,
                File delimitedFile,
                Splitter splitter)
        {
            this.queryState = queryState;
            this.storageManager = storageManager;
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
        public void run()
        {
            try {
                long rowCount = storageManager.importTableShard(source, databaseName, tableName);
                queryState.addPage(new Page(new BlockBuilder(0, TupleInfo.SINGLE_LONG).append(rowCount).build()));
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
}
