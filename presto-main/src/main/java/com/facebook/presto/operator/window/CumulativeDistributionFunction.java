package com.facebook.presto.operator.window;

import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.tuple.TupleInfo;

public class CumulativeDistributionFunction
        implements WindowFunction
{
    private long totalCount;
    private long count;

    @Override
    public TupleInfo getTupleInfo()
    {
        return TupleInfo.SINGLE_DOUBLE;
    }

    @Override
    public void reset(int partitionRowCount)
    {
        totalCount = partitionRowCount;
        count = 0;
    }

    @Override
    public void processRow(BlockBuilder output, boolean newPeerGroup, int peerGroupCount)
    {
        if (newPeerGroup) {
            count += peerGroupCount;
        }
        output.append(((double) count) / totalCount);
    }
}
