package com.facebook.presto.common.block;

import java.util.IdentityHashMap;
import java.util.Map;

public class IntArrayBatchPositions
        implements IBatchPositions
{
    private Map<Block, BatchPositions> blockPositions = new IdentityHashMap<>();
    private IntArrayBlockBuffer sinkBuffer = new IntArrayBlockBuffer();
    private int size;

    public void capture(Block srcBlock, int srcPos, int destPos)
    {
        BatchPositions batchPositions = blockPositions.computeIfAbsent(srcBlock, b -> new BatchPositions());
        batchPositions.addPosition(srcPos, destPos);
        size++;
    }

    @Override
    public void sink()
    {
        sinkBuffer.reserveExtraCapacity(size);

        for (Map.Entry<Block, BatchPositions> entry : blockPositions.entrySet()) {
            Block srcBlock = entry.getKey();
            BatchPositions batchPositions = entry.getValue();
            sinkBuffer.writePositions(srcBlock, batchPositions);
            batchPositions.reset();
        }

        size = 0;
    }

    @Override
    public Block buildBlock()
    {
        return sinkBuffer.buildBlock();
    }
}
