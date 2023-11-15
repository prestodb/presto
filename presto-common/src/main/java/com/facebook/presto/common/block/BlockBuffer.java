package com.facebook.presto.common.block;

public interface BlockBuffer
{
    void reserveExtraCapacity(int newSize);

    void writePositions(Block block, BatchPositions positions);

    Block buildBlock();
}
