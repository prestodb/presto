package com.facebook.presto;

import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Range;
import com.google.common.collect.Ranges;

import javax.inject.Provider;
import java.util.Iterator;

public class PipelinedAggregation
        extends AbstractIterator<ValueBlock>
{
    private final Iterator<ValueBlock> groupBySource;
    private final SeekableIterator<ValueBlock> aggregationSource;

    private final Provider<AggregationFunction> functionProvider;

    private PeekingIterator<Pair> currentGroupByBlock;

    public PipelinedAggregation(Iterator<ValueBlock> keySource, SeekableIterator<ValueBlock> valueSource, Provider<AggregationFunction> functionProvider)
    {
        this.groupBySource = keySource;
        this.aggregationSource = valueSource;

        this.functionProvider = functionProvider;
    }

    @Override
    protected ValueBlock computeNext()
    {
        // if no more data, return null
        if (!advanceGroupByBlock()) {
            endOfData();
            return null;
        }

        // form a group from the current position, until the value changes
        Pair entry = currentGroupByBlock.next();
        Object groupByKey = entry.getValue();
        long startPosition = entry.getPosition();

        while (true) {
            // skip entries until the current key changes or we've consumed this block
            while (currentGroupByBlock.hasNext() && currentGroupByBlock.peek().getValue().equals(groupByKey)) {
                entry = currentGroupByBlock.next();
            }

            // stop if there is more data in the current block since the next entry will be for a new group
            if (currentGroupByBlock.hasNext()) {
                break;
            }

            // stop if we are at the end of the stream
            if (!groupBySource.hasNext()) {
                break;
            }

            // process the next block
            currentGroupByBlock = groupBySource.next().pairIterator();
        }

        long endPosition = entry.getPosition();

        // process the group
        return processGroup(Ranges.closed(startPosition, endPosition));
    }

    private ValueBlock processGroup(Range<Long> positions)
    {
        // create a new aggregate for this group
        AggregationFunction aggregationFunction = functionProvider.get();

        RangePositionBlock positionBlock = new RangePositionBlock(positions);

        // goto start of range
        aggregationSource.seekTo(positions.lowerEndpoint());
        Preconditions.checkState(aggregationSource.hasNext(), "Group start position not found in aggregation source");

        // while we have data...
        while (aggregationSource.hasNext() && aggregationSource.peek().getRange().isConnected(positions)) {
            // process aggregation
            aggregationFunction.add(aggregationSource.next(), positionBlock);
        }

        // calculate final value for this group
        Object value = aggregationFunction.evaluate();

        // build an output block
        return new UncompressedValueBlock(positions.lowerEndpoint(), value);
    }

    private boolean advanceGroupByBlock()
    {
        // does current block iterator have more data?
        if (currentGroupByBlock != null && currentGroupByBlock.hasNext()) {
            return true;
        }

        // are there more blocks?
        if (!groupBySource.hasNext()) {
            return false;
        }

        // advance to next block and open an iterator
        currentGroupByBlock = groupBySource.next().pairIterator();
        return true;
    }
}
