package com.facebook.presto.common.block;

import java.util.Arrays;

public class BatchPositions2
{
    private final static int INITIAL_CAPACITY = 4;
    private int[] positions;
    private int[] increments;
    private int size;
    private int positionCount;

    public void reset()
    {
        size = 0;
        positionCount = 0;
        Arrays.fill(positions, 0);
        Arrays.fill(increments, 0);
    }

    public void addPosition(int position)
    {
        growCapacity();
        positionCount++;

        if (size == 0) {
            positions[0] = position;
            increments[0] = 1;
            size++;
        }
        else if (positions[size - 1] + increments[size - 1] == position) {
            increments[size - 1]++;
        }
        else {
            positions[size] = position;
            increments[size] = 1;
            size++;
        }
    }

    private void growCapacity()
    {
        if (positions != null && positions.length > size) {
            return;
        }

        int newSize;
        if (positions == null) {
            positions = new int[INITIAL_CAPACITY];
            increments = new int[INITIAL_CAPACITY];
        }
        else {
            newSize = BlockUtil.calculateNewArraySize(increments.length);
            positions = Arrays.copyOf(positions, newSize);
            increments = Arrays.copyOf(increments, newSize);
        }
    }

    public int getSize()
    {
        return size;
    }

    public int getPositionCount()
    {
        return positionCount;
    }

    public int getPositionStart(int i)
    {
        return positions[i];
    }

    public int getPositionEnd(int i)
    {
        return Math.addExact(positions[i], increments[i]);
    }

    public int getPositionLength(int i)
    {
        return increments[i];
    }
}
