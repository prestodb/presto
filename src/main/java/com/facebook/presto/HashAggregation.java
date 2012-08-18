package com.facebook.presto;

import com.google.common.collect.AbstractIterator;

import javax.inject.Provider;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import static com.facebook.presto.AggregationUtil.processGroup;

public class HashAggregation
        extends AbstractIterator<ValueBlock>
{
    private final Iterator<RunLengthEncodedBlock> groupBySource;
    private final SeekableIterator<ValueBlock> aggregationSource;

    private final Provider<AggregationFunction> functionProvider;

    private Iterator<Entry<Tuple, AggregationFunction>> aggregations;

    private long position;
    private final TupleInfo tupleInfo;

    public HashAggregation(TupleInfo tupleInfo, Iterator<RunLengthEncodedBlock> keySource,
            SeekableIterator<ValueBlock> valueSource,
            Provider<AggregationFunction> functionProvider)
    {
        this.groupBySource = keySource;
        this.aggregationSource = valueSource;

        this.functionProvider = functionProvider;
        this.tupleInfo = tupleInfo;
    }

    @Override
    protected ValueBlock computeNext()
    {
        // process all data ahead of time
        if (aggregations == null) {
            Map<Tuple, AggregationFunction> aggregationMap = new HashMap<>();
            while (groupBySource.hasNext()) {
                RunLengthEncodedBlock group = groupBySource.next();

                AggregationFunction aggregation = aggregationMap.get(group.getValue());
                if (aggregation == null) {
                    aggregation = functionProvider.get();
                    aggregationMap.put(group.getValue(), aggregation);
                }
                processGroup(aggregationSource, aggregation, group.getRange());
            }

            this.aggregations = aggregationMap.entrySet().iterator();
        }

        // if no more data, return null
        if (!aggregations.hasNext()) {
            endOfData();
            return null;
        }

        BlockBuilder blockBuilder = new BlockBuilder(position, tupleInfo);
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
        ValueBlock block = blockBuilder.build();

        // update position
        position += block.getCount();

        return block;
    }
}
