package com.facebook.presto.operator;

import com.facebook.presto.TupleInfo;
import com.facebook.presto.aggregation.AggregationFunction;
import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockStream;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.uncompressed.UncompressedValueBlock;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import javax.inject.Provider;
import java.util.Iterator;

public class AggregationOperator
        implements BlockStream<UncompressedValueBlock>
{
    private final TupleInfo info;
    private final Provider<AggregationFunction> functionProvider;
    private final BlockStream<? extends Block> source;

    public AggregationOperator(BlockStream<? extends Block> source, Provider<AggregationFunction> functionProvider)
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
    public Cursor cursor()
    {
        return new ValueCursor(info, iterator());
    }

    @Override
    public Iterator<UncompressedValueBlock> iterator()
    {
        AggregationFunction function = functionProvider.get();

        Cursor cursor = source.cursor();
        cursor.advanceNextPosition();
        function.add(cursor, Long.MAX_VALUE);

        UncompressedValueBlock block = new BlockBuilder(0, info)
                .append(function.evaluate())
                .build();

        return ImmutableList.of(block).iterator();
    }
}
