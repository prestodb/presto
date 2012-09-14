package com.facebook.presto.operator;

import com.facebook.presto.Range;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.aggregation.AggregationFunction;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.TupleStream;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import javax.inject.Provider;
import java.util.Iterator;

public class AggregationOperator
        implements TupleStream
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
    public Cursor cursor()
    {
        return new GenericCursor(info, iterator());
    }

    public Iterator<UncompressedBlock> iterator()
    {
        AggregationFunction function = functionProvider.get();

        Cursor cursor = source.cursor();
        cursor.advanceNextPosition();
        function.add(cursor, Long.MAX_VALUE);

        UncompressedBlock block = new BlockBuilder(0, info)
                .append(function.evaluate())
                .build();

        return ImmutableList.of(block).iterator();
    }
}
