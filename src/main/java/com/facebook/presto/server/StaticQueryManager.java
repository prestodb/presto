/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.Range;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.TupleInfo.Type;
import com.facebook.presto.aggregation.SumAggregation;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockIterator;
import com.facebook.presto.block.ColumnMappingTupleStream;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.Cursors;
import com.facebook.presto.block.QuerySession;
import com.facebook.presto.block.TupleStream;
import com.facebook.presto.block.TupleStreamSerde;
import com.facebook.presto.block.dictionary.DictionarySerde;
import com.facebook.presto.block.rle.RunLengthEncodedSerde;
import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.facebook.presto.block.uncompressed.UncompressedSerde;
import com.facebook.presto.operator.GenericCursor;
import com.facebook.presto.operator.GroupByOperator;
import com.facebook.presto.operator.HashAggregationOperator;
import com.facebook.presto.operator.SplitIterator;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.Slices;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.airlift.http.client.ApacheHttpClient;
import io.airlift.http.client.AsyncHttpClient;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.units.Duration;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;
import java.io.File;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class StaticQueryManager implements QueryManager
{
    private final int blockBufferMax;
    private final ExecutorService executor;

    @GuardedBy("this")
    private int nextQueryId;

    // todo this is a potential memory leak if the query objects are not removed
    @GuardedBy("this")
    private final Map<String, QueryState> queries = new HashMap<>();

    @Inject
    public StaticQueryManager()
    {
        this(20);
    }

    public StaticQueryManager(int blockBufferMax)
    {
        Preconditions.checkArgument(blockBufferMax > 0, "blockBufferMax must be at least 1");
        this.blockBufferMax = blockBufferMax;
        executor = Executors.newCachedThreadPool();
    }

    @Override
    public synchronized String createQuery(String query)
    {
        Preconditions.checkNotNull(query, "query is null");

        String queryId = String.valueOf(nextQueryId++);
        final QueryState queryState = new QueryState(1, blockBufferMax);

        Runnable queryTask;
        switch (query) {
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
        private final ExecutorService executor;
        private final List<URI> servers;
        private final AsyncHttpClient asyncHttpClient;

        public SumQuery(QueryState queryState, ExecutorService executor, URI... servers)
        {
            this(queryState, executor, ImmutableList.copyOf(servers));
        }

        public SumQuery(QueryState queryState, ExecutorService executor, Iterable<URI> servers)
        {
            this.queryState = queryState;
            this.executor = executor;
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
                SplitIterator<UncompressedBlock> splitIterator = new SplitIterator<>(tupleStream.getTupleInfo(), 2, 10, tupleStream);

                TupleStream groupBy = new ColumnMappingTupleStream(new OneTimeUseTupleStream(tupleStream.getTupleInfo(), splitIterator.getSplit(0)), 0);
                TupleStream aggregateSource = new ColumnMappingTupleStream(new OneTimeUseTupleStream(tupleStream.getTupleInfo(), splitIterator.getSplit(1)), 1);
                HashAggregationOperator aggregation = new HashAggregationOperator(groupBy, aggregateSource, SumAggregation.PROVIDER);

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
            catch (Exception | Error e) {
                queryState.queryFailed(e);
                throw Throwables.propagate(e);
            }
        }
    }

    private static class OneTimeUseTupleStream implements TupleStream
    {
        private final TupleInfo tupleInfo;
        private BlockIterator<? extends TupleStream> blockIterator;

        private OneTimeUseTupleStream(TupleInfo tupleInfo, BlockIterator<? extends TupleStream> blockIterator)
        {
            Preconditions.checkNotNull(tupleInfo, "tupleInfo is null");
            Preconditions.checkNotNull(blockIterator, "blockIterator is null");
            this.tupleInfo = tupleInfo;
            this.blockIterator = blockIterator;
        }

        @Override
        public TupleInfo getTupleInfo()
        {
            return tupleInfo;
        }

        @Override
        public Range getRange()
        {
            return Range.ALL;
        }

        @Override
        public Cursor cursor(QuerySession session)
        {
            Preconditions.checkNotNull(session, "session is null");
            Preconditions.checkState(blockIterator != null, "Cursor has already been used");
            GenericCursor cursor = new GenericCursor(session, tupleInfo, blockIterator);
            blockIterator = null;
            return cursor;
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

                TupleStream groupBySource = groupBySerde.deserialize(groupBySlice);
                TupleStream aggregateSource = aggregateSerde.deserialize(aggregateSlice);

                GroupByOperator groupBy = new GroupByOperator(groupBySource);
                HashAggregationOperator aggregation = new HashAggregationOperator(groupBy, aggregateSource, SumAggregation.PROVIDER);

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
            catch (Exception | Error e) {
                queryState.queryFailed(e);
                throw Throwables.propagate(e);
            }
        }
    }
}
