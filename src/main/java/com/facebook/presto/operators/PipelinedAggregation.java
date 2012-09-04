package com.facebook.presto.operators;

import com.facebook.presto.AggregationUtil;
import com.facebook.presto.BlockBuilder;
import com.facebook.presto.RunLengthEncodedBlock;
import com.facebook.presto.SeekableIterator;
import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.ValueBlock;
import com.facebook.presto.aggregations.AggregationFunction;
import com.google.common.collect.AbstractIterator;

import javax.inject.Provider;
import java.util.Iterator;

public class PipelinedAggregation
        extends AbstractIterator<ValueBlock>
{
    private final Iterator<RunLengthEncodedBlock> groupBySource;
    private final SeekableIterator<ValueBlock> aggregationSource;

    private final Provider<AggregationFunction> functionProvider;
    private final TupleInfo tupleInfo;
    private long position;

    public PipelinedAggregation(TupleInfo tupleInfo,
            Iterator<RunLengthEncodedBlock> keySource,
            SeekableIterator<ValueBlock> valueSource,
            Provider<AggregationFunction> functionProvider)
    {
        this.tupleInfo = tupleInfo;
        this.groupBySource = keySource;
        this.aggregationSource = valueSource;

        this.functionProvider = functionProvider;
    }

    @Override
    protected ValueBlock computeNext()
    {
        // if no more data, return null
        if (!groupBySource.hasNext()) {
            endOfData();
            return null;
        }

        BlockBuilder builder = new BlockBuilder(position, tupleInfo);

        do {
            // get next group
            RunLengthEncodedBlock group = groupBySource.next();

            // create a new aggregate for this group
            AggregationFunction aggregationFunction = functionProvider.get();

            AggregationUtil.processGroup(aggregationSource, aggregationFunction, group.getRange());

            // calculate final value for this group
            Tuple value = aggregationFunction.evaluate();

            builder.append(group.getValue());
            builder.append(value);
        }
        while (!builder.isFull() && groupBySource.hasNext());

        // build an output block
        ValueBlock block = builder.build();
        position += block.getCount();
        return block;
    }
}
