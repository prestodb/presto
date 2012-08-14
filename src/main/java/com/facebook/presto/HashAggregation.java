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

    private Iterator<Entry<Object, AggregationFunction>> aggregations;

    private long position;

    public HashAggregation(Iterator<RunLengthEncodedBlock> keySource, SeekableIterator<ValueBlock> valueSource, Provider<AggregationFunction> functionProvider)
    {
        this.groupBySource = keySource;
        this.aggregationSource = valueSource;

        this.functionProvider = functionProvider;
    }

    @Override
    protected ValueBlock computeNext()
    {
        // process all data ahead of time
        if (aggregations == null) {
            Map<Object, AggregationFunction> aggregationMap = new HashMap<>();
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

        // get next aggregation
        Entry<Object, AggregationFunction> aggregation = aggregations.next();

        // calculate final value for this group
        Object value = aggregation.getValue().evaluate();

        // build an output block
        return new UncompressedValueBlock(position++, new Tuple(aggregation.getKey(), value));
    }
}
