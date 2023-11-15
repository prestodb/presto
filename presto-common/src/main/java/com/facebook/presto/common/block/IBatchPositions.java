package com.facebook.presto.common.block;

public interface IBatchPositions
{
    void capture(Block srcBlock, int srcPos, int destPos);

    void sink();

    Block buildBlock();
}
