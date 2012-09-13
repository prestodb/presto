package com.facebook.presto.operator;

import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.TupleInfo.Type;
import com.facebook.presto.aggregation.AggregationFunction;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockStream;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.uncompressed.UncompressedCursor;
import com.facebook.presto.block.uncompressed.UncompressedValueBlock;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;

import javax.inject.Provider;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Group input data and produce a single block for each sequence of identical values.
 */
public class HashAggregationBlockStream
    implements BlockStream, Iterable<UncompressedValueBlock>
{
    private final BlockStream groupBySource;
    private final BlockStream aggregationSource;
    private final Provider<AggregationFunction> functionProvider;
    private final TupleInfo info;

    public HashAggregationBlockStream(BlockStream groupBySource,
            BlockStream aggregationSource,
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
        final Cursor groupByCursor = groupBySource.cursor();
        final Cursor aggregationCursor = aggregationSource.cursor();
        aggregationCursor.advanceNextPosition();

        return new AbstractIterator<UncompressedValueBlock>()
        {
            private Iterator<Entry<Tuple, AggregationFunction>> aggregations;
            private long position;

            @Override
            protected UncompressedValueBlock computeNext()
            {
                // process all data ahead of time
                if (aggregations == null) {
                    Map<Tuple, AggregationFunction> aggregationMap = new HashMap<>();
                    while (groupByCursor.advanceNextValue()) {

                        Tuple key = groupByCursor.getTuple();
                        AggregationFunction aggregation = aggregationMap.get(key);
                        if (aggregation == null) {
                            aggregation = functionProvider.get();
                            aggregationMap.put(key, aggregation);
                        }
                        if (aggregationCursor.advanceToPosition(groupByCursor.getPosition())) {
                            aggregation.add(aggregationCursor, groupByCursor.getCurrentValueEndPosition());
                        }
                    }

                    this.aggregations = aggregationMap.entrySet().iterator();
                }

                // if no more data, return null
                if (!aggregations.hasNext()) {
                    endOfData();
                    return null;
                }

                BlockBuilder blockBuilder = new BlockBuilder(position, info);
                while (!blockBuilder.isFull() && aggregations.hasNext()) {
                    // get next aggregation
                    Entry<Tuple, AggregationFunction> aggregation = aggregations.next();

                    // calculate final value for this group
                    Tuple key = aggregation.getKey();
                    Tuple value = aggregation.getValue().evaluate();
                    blockBuilder.append(key);
                    blockBuilder.append(value);
                }

                // build output block
                UncompressedValueBlock block = blockBuilder.build();

                // update position
                position += block.getCount();

                return block;
            }
        };
    }
}
