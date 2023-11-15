package com.facebook.presto.common.block;

import java.util.Arrays;

import static java.lang.String.format;

public class BatchPositions
{
    private final static int INITIAL_CAPACITY = 4;
    private int[] srcPositions;
    private int[] destPositions;
    private int positionCount;

    public void reset()
    {
        positionCount = 0;
    }

    public void addPosition(int srcPos, int destPos)
    {
//        System.out.println("addPosition: "+srcPos+" -> "+destPos);
        growCapacity();
        srcPositions[positionCount] = srcPos;
        destPositions[positionCount] = destPos;
        positionCount++;
    }

    private void growCapacity()
    {
        if (srcPositions != null && srcPositions.length > positionCount) {
            return;
        }

        int newSize;
        if (srcPositions == null) {
            srcPositions = new int[INITIAL_CAPACITY];
            destPositions = new int[INITIAL_CAPACITY];
        }
        else {
            newSize = BlockUtil.calculateNewArraySize(srcPositions.length);
            srcPositions = Arrays.copyOf(srcPositions, newSize);
            destPositions = Arrays.copyOf(destPositions, newSize);
        }
    }

    public int getPositionCount()
    {
        return positionCount;
    }

    public int[] getSrcPositions()
    {
        return srcPositions;
    }

    public int[] getDestPositions()
    {
        return destPositions;
    }

    @Override
    public String toString()
    {
        return format("BatchPositions{positionCount=%d}", getPositionCount());
    }
}
