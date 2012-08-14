package com.facebook.presto;

import com.google.common.base.Preconditions;
import com.google.common.collect.Range;

public class AggregationUtil {
    public static void processGroup(SeekableIterator<ValueBlock> aggregationSource, AggregationFunction aggregation, Range<Long> positions)
    {
        RangePositionBlock positionBlock = new RangePositionBlock(positions);

        // goto start of range
        aggregationSource.seekTo(positions.lowerEndpoint());
        Preconditions.checkState(aggregationSource.hasNext(), "Group start position not found in aggregation source");

        // while we have data...
        while (aggregationSource.hasNext() && aggregationSource.peek().getRange().isConnected(positions)) {
            // process aggregation
            aggregation.add(aggregationSource.next(), positionBlock);
        }
    }
}
