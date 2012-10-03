/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.Range;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.AbstractBlockIterator;
import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.TupleStream;
import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.facebook.presto.operator.GenericCursor;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.List;

public class QueryDriversTupleStream
        implements TupleStream, BlockIterable<UncompressedBlock>
{
    private final TupleInfo info;
    private final List<QueryDriverProvider> driverProviders;
    private final int blockBufferMax;

    public QueryDriversTupleStream(TupleInfo info, int blockBufferMax, QueryDriverProvider... driverProviders)
    {
        this(info, blockBufferMax, ImmutableList.copyOf(driverProviders));
    }

    public QueryDriversTupleStream(TupleInfo info, int blockBufferMax, Iterable<? extends QueryDriverProvider> driverProviders)
    {
        Preconditions.checkNotNull(info, "info is null");
        Preconditions.checkArgument(blockBufferMax > 0, "blockBufferMax must be at least 1");
        Preconditions.checkNotNull(driverProviders, "driverProviders is null");

        this.info = info;
        this.blockBufferMax = blockBufferMax;
        this.driverProviders = ImmutableList.copyOf(driverProviders);
    }

    @Override
    public QueryDriversBlockIterator iterator()
    {
        ImmutableList.Builder<QueryDriver> queries = ImmutableList.builder();
        try {
            QueryState queryState = new QueryState(driverProviders.size(), blockBufferMax);
            for (QueryDriverProvider provider : driverProviders) {
                QueryDriver queryDriver = provider.create(queryState, info);
                queries.add(queryDriver);
                queryDriver.start();
            }

            return new QueryDriversBlockIterator(queryState, queries.build());
        }
        catch (Throwable e) {
            for (QueryDriver queryDriver : queries.build()) {
                queryDriver.cancel();
            }
            throw Throwables.propagate(e);
        }
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return info;
    }

    @Override
    public Range getRange()
    {
        return Range.ALL;
    }

    @Override
    public Cursor cursor()
    {
        return new GenericCursor(info, iterator());
    }

    public static class QueryDriversBlockIterator extends AbstractBlockIterator<UncompressedBlock>
    {
        private final QueryState queryState;
        private final List<QueryDriver> queryDrivers;
        private long position;

        private QueryDriversBlockIterator(QueryState queryState, Iterable<QueryDriver> queries)
        {
            this.queryState = queryState;
            this.queryDrivers = ImmutableList.copyOf(queries);
        }

        public void cancel()
        {
            queryState.cancel();
            for (QueryDriver queryDriver : queryDrivers) {
                queryDriver.cancel();
            }
        }

        @Override
        protected UncompressedBlock computeNext()
        {
            try {
                if (queryState.isCanceled()) {
                    endOfData();
                    return null;
                }
                List<UncompressedBlock> nextBlocks = queryState.getNextBlocks(1);
                if (nextBlocks.isEmpty()) {
                    endOfData();
                    return null;
                }
                UncompressedBlock block = Iterables.getOnlyElement(nextBlocks);

                // rewrite the block positions
                block = new UncompressedBlock(
                        new Range(position, position + block.getCount() - 1),
                        block.getTupleInfo(),
                        block.getSlice());
                position = block.getRange().getEnd();

                return block;
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw Throwables.propagate(e);
            }
        }
    }

}
