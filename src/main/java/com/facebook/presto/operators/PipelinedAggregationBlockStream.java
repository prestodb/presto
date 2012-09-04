package com.facebook.presto.operators;

import com.facebook.presto.BlockBuilder;
import com.facebook.presto.BlockStream;
import com.facebook.presto.Cursor;
import com.facebook.presto.RunLengthEncodedBlock;
import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.TupleInfo.Type;
import com.facebook.presto.UncompressedCursor;
import com.facebook.presto.UncompressedValueBlock;
import com.facebook.presto.ValueBlock;
import com.facebook.presto.aggregations.AggregationFunction;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;

import javax.inject.Provider;
import java.util.Iterator;

/**
 * Group input data and produce a single block for each sequence of identical values.
 */
public class PipelinedAggregationBlockStream
    implements BlockStream<UncompressedValueBlock>
{
    private final BlockStream<RunLengthEncodedBlock> groupBySource;
    private final BlockStream<? extends ValueBlock> aggregationSource;
    private final Provider<AggregationFunction> functionProvider;
    private final TupleInfo info;

    public PipelinedAggregationBlockStream(BlockStream<RunLengthEncodedBlock> groupBySource,
            BlockStream<? extends ValueBlock> aggregationSource,
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
    public Cursor cursor()
    {
        return new UncompressedCursor(getTupleInfo(), iterator());
    }

    @Override
    public Iterator<UncompressedValueBlock> iterator()
    {
        final Iterator<RunLengthEncodedBlock> groupByIterator = groupBySource.iterator();
//        final SeekableIterator<? extends ValueBlock> aggregationIterator = new ForwardingSeekableIterator<>(aggregationSource.iterator());
        final Cursor aggregationCursor = aggregationSource.cursor();
        aggregationCursor.advanceNextPosition();

        return new AbstractIterator<UncompressedValueBlock>()
        {
            private long position;

            @Override
            protected UncompressedValueBlock computeNext()
            {
                // if no more data, return null
                if (!groupByIterator.hasNext()) {
                    endOfData();
                    return null;
                }

                BlockBuilder builder = new BlockBuilder(position, info);

                do {
                    // get next group
                    RunLengthEncodedBlock group = groupByIterator.next();

                    // create a new aggregate for this group
                    AggregationFunction aggregationFunction = functionProvider.get();

                    // process data
//                    AggregationUtil.processGroup(aggregationIterator, aggregationFunction, group.getRange());
                    aggregationFunction.add(aggregationCursor, group.getRange());

                    // calculate final value for this group
                    Tuple value = aggregationFunction.evaluate();

                    builder.append(group.getValue());
                    builder.append(value);
                }
                while (!builder.isFull() && groupByIterator.hasNext());

                // build an output block
                UncompressedValueBlock block = builder.build();
                position += block.getCount();
                return block;
            }
        };
    }
}
