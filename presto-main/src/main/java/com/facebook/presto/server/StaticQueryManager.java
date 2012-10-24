/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.TupleInfo;
import com.facebook.presto.TupleInfo.Type;
import com.facebook.presto.aggregation.LongSumAggregation;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.ProjectionTupleStream;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.Cursors;
import com.facebook.presto.block.QuerySession;
import com.facebook.presto.block.TupleStream;
import com.facebook.presto.block.TupleStreamSerde;
import com.facebook.presto.block.dictionary.DictionarySerde;
import com.facebook.presto.block.rle.RunLengthEncodedSerde;
import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.facebook.presto.block.uncompressed.UncompressedSerde;
import com.facebook.presto.hive.HiveClient;
import com.facebook.presto.ingest.DelimitedTupleStream;
import com.facebook.presto.metadata.HiveImportManager;
import com.facebook.presto.metadata.StorageManager;
import com.facebook.presto.operator.AggregationOperator;
import com.facebook.presto.operator.GroupByOperator;
import com.facebook.presto.operator.HashAggregationOperator;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.Slices;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.io.Files;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.airlift.http.client.ApacheHttpClient;
import io.airlift.http.client.AsyncHttpClient;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.units.Duration;

import javax.annotation.Nullable;
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

import static com.facebook.presto.RetryDriver.runWithRetry;

public class StaticQueryManager implements QueryManager
{
    private final int blockBufferMax;
    private final ExecutorService executor;
    private final HiveClient hiveClient;
    private final StorageManager storageManager;
    private final HiveImportManager hiveImportManager;

    @GuardedBy("this")
    private int nextQueryId;

    // todo this is a potential memory leak if the query objects are not removed
    @GuardedBy("this")
    private final Map<String, QueryState> queries = new HashMap<>();

    @Inject
    public StaticQueryManager(HiveClient hiveClient, StorageManager storageManager, HiveImportManager hiveImportManager)
    {
        this(20, hiveClient, storageManager, hiveImportManager);
    }

    public StaticQueryManager(int blockBufferMax, HiveClient hiveClient, StorageManager storageManager, HiveImportManager hiveImportManager)
    {
        Preconditions.checkArgument(blockBufferMax > 0, "blockBufferMax must be at least 1");
        this.blockBufferMax = blockBufferMax;
        executor = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("http-query-processor-%d").build());
        this.hiveClient = hiveClient;
        this.storageManager = storageManager;
        this.hiveImportManager = hiveImportManager;
    }

    @Override
    public synchronized String createQuery(String query)
    {
        Preconditions.checkNotNull(query, "query is null");
        Preconditions.checkArgument(query.length() > 0, "query must not be empty string");

        String queryId = String.valueOf(nextQueryId++);
        final QueryState queryState = new QueryState(1, blockBufferMax);

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
                queryTask = new PartialSumQuery(queryState);
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
    public List<UncompressedBlock> getQueryResults(String queryId, int maxBlockCount)
            throws InterruptedException
    {
        Preconditions.checkNotNull(queryId, "queryId is null");
        Preconditions.checkArgument(maxBlockCount > 0, "maxBlockCount must be at least 1");

        return getQuery(queryId).getNextBlocks(maxBlockCount);
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
                QueryDriversTupleStream tupleStream = new QueryDriversTupleStream(new TupleInfo(Type.VARIABLE_BINARY, Type.FIXED_INT_64), 10,
                        Iterables.transform(servers, new Function<URI, QueryDriverProvider>()
                        {
                            @Override
                            public QueryDriverProvider apply(@Nullable URI uri)
                            {
                                return new HttpQueryProvider("sum-partial", asyncHttpClient, uri);
                            }
                        })
                );
                com.facebook.presto.operator.Splitter<UncompressedBlock> splitIterator = new com.facebook.presto.operator.Splitter<>(tupleStream.getTupleInfo(), 2, 10, tupleStream);

                TupleStream groupBy = new ProjectionTupleStream(splitIterator.getSplit(0), 0);
                TupleStream aggregateSource = new ProjectionTupleStream(splitIterator.getSplit(1), 1);
                HashAggregationOperator aggregation = new HashAggregationOperator(groupBy, aggregateSource, LongSumAggregation.PROVIDER);

                Cursor cursor = aggregation.cursor(new QuerySession());
                long position = 0;
                while (Cursors.advanceNextPositionNoYield(cursor)) {
                    // build a block
                    BlockBuilder blockBuilder = new BlockBuilder(position, cursor.getTupleInfo());
                    do {
                        blockBuilder.append(cursor.getTuple());
                    } while (!blockBuilder.isFull() && Cursors.advanceNextPositionNoYield(cursor));
                    UncompressedBlock block = blockBuilder.build();

                    queryState.addBlock(block);
                    position += block.getCount();
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
        private final QueryState queryState;

        public PartialSumQuery(QueryState queryState)
        {
            this.queryState = queryState;
        }

        @Override
        public void run()
        {
            try {
                //                // dictionary-raw not-sorted
                //                File groupByFile = new File("data/dic-raw/column5.string_dic-raw.data");
                //                TupleStreamSerde groupBySerde = new DictionarySerde(UncompressedSerde.INSTANCE);
                //                File aggregateFile = new File("data/dic-raw/column3.fmillis_raw.data");
                //                TupleStreamSerde aggregateSerde = UncompressedSerde.INSTANCE;

                // dictionary-rle
                File groupByFile = new File("data/dic-rle/column5.string_dic-rle.data");
                TupleStreamSerde groupBySerde = new DictionarySerde(new RunLengthEncodedSerde());
                File aggregateFile = new File("data/dic-rle/column3.fmillis_raw.data");
                TupleStreamSerde aggregateSerde = UncompressedSerde.INSTANCE;

                Slice groupBySlice = Slices.mapFileReadOnly(groupByFile);
                Slice aggregateSlice = Slices.mapFileReadOnly(aggregateFile);

                TupleStream groupBySource = groupBySerde.createDeserializer().deserialize(groupBySlice);
                TupleStream aggregateSource = aggregateSerde.createDeserializer().deserialize(aggregateSlice);

                GroupByOperator groupBy = new GroupByOperator(groupBySource);
                HashAggregationOperator aggregation = new HashAggregationOperator(groupBy, aggregateSource, LongSumAggregation.PROVIDER);

                Cursor cursor = aggregation.cursor(new QuerySession());
                long position = 0;
                while (Cursors.advanceNextPositionNoYield(cursor)) {
                    // build a block
                    BlockBuilder blockBuilder = new BlockBuilder(position, cursor.getTupleInfo());
                    do {
                        blockBuilder.append(cursor.getTuple());
                    } while (!blockBuilder.isFull() && Cursors.advanceNextPositionNoYield(cursor));
                    UncompressedBlock block = blockBuilder.build();

                    queryState.addBlock(block);
                    position += block.getCount();
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

    private class ImportHiveTableQuery
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
                        ));
                    }
                    // TODO: this currently leaks query resources (need to delete)
                    QueryDriversTupleStream tupleStream = new QueryDriversTupleStream(TupleInfo.SINGLE_LONG, 10, queryDriverProviderBuilder.build());
                    AggregationOperator sumOperator = new AggregationOperator(tupleStream, LongSumAggregation.PROVIDER);
                    queryState.addBlock(Iterators.getOnlyElement(sumOperator.iterator(new QuerySession())));
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
                queryState.addBlock(new BlockBuilder(0, TupleInfo.SINGLE_LONG).append(rowCount).build());
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
        private final TupleInfo tupleInfo;
        private final File delimitedFile;
        private final Splitter splitter;

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
            this.tupleInfo = tupleInfo;
            this.delimitedFile = delimitedFile;
            this.splitter = splitter;
        }

        @Override
        public void run()
        {
            try {
                try (InputStreamReader input = Files.newReaderSupplier(delimitedFile, Charsets.UTF_8).getInput()) {
                    DelimitedTupleStream delimitedTupleStream = new DelimitedTupleStream(input, splitter, tupleInfo);
                    long rowCount = storageManager.importTableShard(delimitedTupleStream, databaseName, tableName);
                    queryState.addBlock(new BlockBuilder(0, TupleInfo.SINGLE_LONG).append(rowCount).build());
                    queryState.sourceFinished();
                }
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
