/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

import java.util.concurrent.atomic.AtomicLong;

public class OperatorStats
{

    private AtomicLong expectedDataSize = new AtomicLong();
    private AtomicLong expectedPositionCount = new AtomicLong();
    private AtomicLong actualDataSize = new AtomicLong();
    private AtomicLong actualPositionCount = new AtomicLong();

    public long getExpectedDataSize()
    {
        return expectedDataSize.get();
    }

    public void addExpectedDataSize(long expectedDataSize)
    {
        this.expectedDataSize.getAndAdd((expectedDataSize));
    }

    public long getExpectedPositionCount()
    {
        return expectedPositionCount.get();
    }

    public void addExpectedPositionCount(long expectedPositionCount)
    {
        this.expectedPositionCount.getAndAdd((expectedPositionCount));
    }

    public long getActualDataSize()
    {
        return actualDataSize.get();
    }

    public void addActualDataSize(long actualDataSize)
    {
        this.actualDataSize.getAndAdd((actualDataSize));
    }

    public long getActualPositionCount()
    {
        return actualPositionCount.get();
    }

    public void addActualPositionCount(long actualPositionCount)
    {
        this.actualPositionCount.getAndAdd((actualPositionCount));
    }
}
