package com.facebook.presto.noperator;

import com.facebook.presto.TupleInfo;
import com.facebook.presto.nblock.Block;
import com.facebook.presto.nblock.BlockBuilder;
import com.facebook.presto.nblock.BlockCursor;
import com.facebook.presto.nblock.Blocks;
import com.facebook.presto.noperator.aggregation.AggregationFunction;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;

import javax.inject.Provider;
import java.util.Iterator;

public class AggregationOperator
        implements Blocks
{
    private final TupleInfo info;
    private final Provider<AggregationFunction> functionProvider;
    private final Blocks blocks;

    public AggregationOperator(Blocks blocks, Provider<AggregationFunction> functionProvider)
    {
        Preconditions.checkNotNull(blocks, "source is null");
        Preconditions.checkNotNull(functionProvider, "functionProvider is null");

        this.blocks = blocks;
        this.functionProvider = functionProvider;
        this.info = functionProvider.get().getTupleInfo();
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return info;
    }

    @Override
    public Iterator<Block> iterator()
    {
        AggregationFunction function = functionProvider.get();
        for (Block block : blocks) {
            BlockCursor cursor = block.cursor();
            while (cursor.advanceNextPosition()) {
                function.add(cursor, Long.MAX_VALUE);
            }
        }

        Block block = new BlockBuilder(0, info)
                .append(function.evaluate())
                .build();

        return Iterators.singletonIterator(block);
    }
}
