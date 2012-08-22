package com.facebook.presto;

import com.facebook.presto.aggregations.AggregationFunction;
import com.google.common.base.Preconditions;


public class AggregationUtil {
    public static void processGroup(SeekableIterator<ValueBlock> aggregationSource, AggregationFunction aggregation, Range positions)
    {
        RangePositionBlock positionBlock = new RangePositionBlock(positions);

        // goto start of range
        aggregationSource.seekTo(positions.getStart());
        Preconditions.checkState(aggregationSource.hasNext(), "Group start position not found in aggregation source");

        // while we have data...
        while (aggregationSource.hasNext() && aggregationSource.peek().getRange().overlaps(positions)) {
            // process aggregation
            aggregation.add(aggregationSource.next(), positionBlock);
        }
    }
}
