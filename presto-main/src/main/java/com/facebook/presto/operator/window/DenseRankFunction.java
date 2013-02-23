package com.facebook.presto.operator.window;

import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.tuple.TupleInfo;

public class DenseRankFunction
        implements WindowFunction
{
    private long rank;

    @Override
    public TupleInfo getTupleInfo()
    {
        return TupleInfo.SINGLE_LONG;
    }

    @Override
    public void reset(int partitionRowCount)
    {
        rank = 0;
    }

    @Override
    public void processRow(BlockBuilder output, boolean newPeerGroup, int peerGroupCount)
    {
        if (newPeerGroup) {
            rank++;
        }
        output.append(rank);
    }
}
