package com.facebook.presto.noperator;

import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.TupleInfo.Type;
import com.facebook.presto.nblock.Block;
import com.facebook.presto.nblock.BlockBuilder;
import com.facebook.presto.nblock.BlockCursor;
import com.facebook.presto.nblock.uncompressed.UncompressedBlock;
import com.facebook.presto.noperator.aggregation.AggregationFunction;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;

import javax.inject.Provider;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Group input data and produce a single block for each sequence of identical values.
 */
public class HashAggregationOperator
        implements Operator
{
    private final Operator source;
    private final Provider<AggregationFunction> functionProvider;

    public HashAggregationOperator(Operator source,
            Provider<AggregationFunction> functionProvider)
    {
        Preconditions.checkNotNull(source, "source is null");
        Preconditions.checkNotNull(functionProvider, "functionProvider is null");

        this.source = source;
        this.functionProvider = functionProvider;
    }

    @Override
    public Iterator<Page> iterator()
    {
        return new AbstractIterator<Page>()
        {
            private final Map<Tuple, AggregationFunction> aggregationMap = new HashMap<>();
            private Iterator<Entry<Tuple, AggregationFunction>> aggregations;
            private long position;

            @Override
            protected Page computeNext()
            {
                // process all data ahead of time
                if (aggregations == null) {
                    for (Page page : source) {
                        Block groupByBlock = page.getBlock(0);
                        Block aggregationBlock = page.getBlock(1);

                        BlockCursor groupByCursor = groupByBlock.cursor();
                        BlockCursor aggregationCursor = aggregationBlock.cursor();

                        while (groupByCursor.advanceNextPosition()) {
                            Preconditions.checkState(aggregationCursor.advanceNextPosition());

                            Tuple key = groupByCursor.getTuple();
                            AggregationFunction aggregation = aggregationMap.get(key);
                            if (aggregation == null) {
                                aggregation = functionProvider.get();
                                aggregationMap.put(key, aggregation);
                            }
                            aggregation.add(aggregationCursor);

                        }
                    }
                    this.aggregations = aggregationMap.entrySet().iterator();
                }

                // if no more data, return null
                if (!aggregations.hasNext()) {
                    endOfData();
                    return null;
                }

                BlockBuilder blockBuilder = new BlockBuilder(position, new TupleInfo(Type.VARIABLE_BINARY, Type.FIXED_INT_64));
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
                UncompressedBlock block = blockBuilder.build();

                // update position
                position += block.getCount();

                return new Page(block);
            }
        };
    }
}
