package com.facebook.presto;

import com.google.common.collect.AbstractIterator;

import javax.inject.Provider;
import java.util.Iterator;

public class PipelinedAggregation
        extends AbstractIterator<ValueBlock>
{
    private final Iterator<RunLengthEncodedBlock> groupBySource;
    private final SeekableIterator<ValueBlock> aggregationSource;

    private final Provider<AggregationFunction> functionProvider;

    public PipelinedAggregation(Iterator<RunLengthEncodedBlock> keySource, SeekableIterator<ValueBlock> valueSource, Provider<AggregationFunction> functionProvider)
    {
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

        // get next group
        RunLengthEncodedBlock group = groupBySource.next();

        // create a new aggregate for this group
        AggregationFunction aggregationFunction = functionProvider.get();

        AggregationUtil.processGroup(aggregationSource, aggregationFunction, group.getRange());

        // calculate final value for this group
        Object value = aggregationFunction.evaluate();

        // build an output block
        return new UncompressedValueBlock(group.getRange().lowerEndpoint(), new Tuple(group.getValue(), value));
    }
}
