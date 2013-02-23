package com.facebook.presto.operator.window;

import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.tuple.TupleInfo;

public class RankFunction
        implements WindowFunction
{
    private long rank;
    private long count;

    @Override
    public TupleInfo getTupleInfo()
    {
        return TupleInfo.SINGLE_LONG;
    }

    @Override
    public void reset(int partitionRowCount)
    {
        rank = 0;
        count = 1;
    }

    @Override
    public void processRow(BlockBuilder output, boolean newPeerGroup, int peerGroupCount)
    {
        if (newPeerGroup) {
            rank += count;
            count = 1;
        }
        else {
            count++;
        }
        output.append(rank);
    }
}
