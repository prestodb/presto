package com.facebook.presto.common.block;

import java.util.Arrays;
import java.util.Optional;

public class IntArrayBlockBuffer
        implements BlockBuffer
{
    private int positionCount;
    private boolean[] valueIsNull;
    private int[] values;

    public void reserveExtraCapacity(int additionalPositions)
    {
        if (valueIsNull == null) {
            valueIsNull = new boolean[additionalPositions];
            values = new int[additionalPositions];
        }

        int minNewSize = positionCount + additionalPositions;
        int proposedNewSize = BlockUtil.calculateNewArraySize(positionCount);
        int newSize = Math.max(minNewSize, proposedNewSize);
        if (valueIsNull.length < newSize) {
            valueIsNull = Arrays.copyOf(valueIsNull, newSize);
            values = Arrays.copyOf(values, newSize);
        }
    }

    @Override
    public void writePositions(Block block, BatchPositions positions)
    {
        int[] srcPositions = positions.getSrcPositions();
        int[] destPositions = positions.getDestPositions();

        for (int i = 0; i < positions.getPositionCount(); i++) {
            valueIsNull[destPositions[i]] = block.isNull(srcPositions[i]);
        }

        for (int i = 0; i < positions.getPositionCount(); i++) {
            int destPosition = destPositions[i];
            if (!valueIsNull[destPosition]) {
                values[destPosition] = block.getInt(srcPositions[i]);
            }
        }

        positionCount += positions.getPositionCount();
    }

    @Override
    public Block buildBlock()
    {
        IntArrayBlock block = new IntArrayBlock(positionCount, Optional.of(valueIsNull), values);
        valueIsNull = null;
        values = null;
        positionCount = 0;
        return block;
    }
}
