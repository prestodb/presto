package com.facebook.presto.operator;

import com.facebook.presto.Range;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.aggregation.AggregationFunction;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.YieldingIterable;
import com.facebook.presto.block.YieldingIterator;
import com.facebook.presto.block.YieldingIterators;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.QuerySession;
import com.facebook.presto.block.TupleStream;
import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.google.common.base.Preconditions;

import javax.inject.Provider;

public class AggregationOperator
        implements TupleStream, YieldingIterable<UncompressedBlock>
{
    private final TupleInfo info;
    private final Provider<AggregationFunction> functionProvider;
    private final TupleStream source;

    public AggregationOperator(TupleStream source, Provider<AggregationFunction> functionProvider)
    {
        Preconditions.checkNotNull(source, "source is null");
        Preconditions.checkNotNull(functionProvider, "functionProvider is null");

        this.source = source;
        this.functionProvider = functionProvider;
        this.info = functionProvider.get().getTupleInfo();
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
    public Cursor cursor(QuerySession session)
    {
        Preconditions.checkNotNull(session, "session is null");
        return new GenericCursor(session, info, iterator(session));
    }

    public YieldingIterator<UncompressedBlock> iterator(QuerySession session)
    {
        Preconditions.checkNotNull(session, "session is null");
        AggregationFunction function = functionProvider.get();

        Cursor cursor = source.cursor(session);
        cursor.advanceNextPosition();
        function.add(cursor, Long.MAX_VALUE);

        UncompressedBlock block = new BlockBuilder(0, info)
                .append(function.evaluate())
                .build();

        return YieldingIterators.yieldingIterable(block);
    }
}
