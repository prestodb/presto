package com.facebook.presto.operator.window;

import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.tuple.TupleInfo;

public class PercentRankFunction
        implements WindowFunction
{
    private long totalCount;
    private long rank;
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
        rank = 0;
        count = 1;
    }

    @Override
    public void processRow(BlockBuilder output, boolean newPeerGroup, int peerGroupCount)
    {
        if (totalCount == 1) {
            output.append(0.0);
            return;
        }

        if (newPeerGroup) {
            rank += count;
            count = 1;
        }
        else {
            count++;
        }

        output.append(((double) (rank - 1)) / (totalCount - 1));
    }
}
