package com.facebook.presto.operator;

import com.facebook.presto.Range;
import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.TupleInfo.Type;
import com.facebook.presto.aggregation.AggregationFunction;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.TupleStream;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.facebook.presto.block.uncompressed.UncompressedCursor;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;

import javax.inject.Provider;
import java.util.Iterator;

/**
 * Group input data and produce a single block for each sequence of identical values.
 */
public class PipelinedAggregationOperator
        implements TupleStream, Iterable<UncompressedBlock>
{
    private final TupleStream groupBySource;
    private final TupleStream aggregationSource;
    private final Provider<AggregationFunction> functionProvider;
    private final TupleInfo info;

    public PipelinedAggregationOperator(TupleStream groupBySource,
            TupleStream aggregationSource,
            Provider<AggregationFunction> functionProvider)
    {
        Preconditions.checkNotNull(groupBySource, "groupBySource is null");
        Preconditions.checkNotNull(aggregationSource, "aggregationSource is null");
        Preconditions.checkNotNull(functionProvider, "functionProvider is null");

        this.groupBySource = groupBySource;
        this.aggregationSource = aggregationSource;
        this.functionProvider = functionProvider;

        info = new TupleInfo(ImmutableList.<Type>builder()
                .addAll(groupBySource.getTupleInfo().getTypes())
                .addAll(functionProvider.get().getTupleInfo().getTypes())
                .build());

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
        return new UncompressedCursor(getTupleInfo(), iterator());
    }

    @Override
    public Iterator<UncompressedBlock> iterator()
    {
        final Cursor groupByCursor = groupBySource.cursor();
        final Cursor aggregationCursor = aggregationSource.cursor();
        aggregationCursor.advanceNextPosition();

        return new AbstractIterator<UncompressedBlock>()
        {
            private long position;

            @Override
            protected UncompressedBlock computeNext()
            {
                // if no more data, return null
                if (!groupByCursor.advanceNextValue()) {
                    endOfData();
                    return null;
                }

                BlockBuilder builder = new BlockBuilder(position, info);

                do {
                    long groupEndPosition = groupByCursor.getCurrentValueEndPosition();
                    if (aggregationCursor.advanceToPosition(groupByCursor.getPosition()) && aggregationCursor.getPosition() <= groupEndPosition) {
                        // create a new aggregate for this group
                        AggregationFunction aggregation = functionProvider.get();

                        // process data
                        aggregation.add(aggregationCursor, groupEndPosition);

                        // calculate final value for this group
                        Tuple value = aggregation.evaluate();

                        builder.append(groupByCursor.getTuple());
                        builder.append(value);
                    }
                }
                while (!builder.isFull() && groupByCursor.advanceNextValue());

                // build an output block
                UncompressedBlock block = builder.build();
                position += block.getCount();
                return block;
            }
        };
    }
}
