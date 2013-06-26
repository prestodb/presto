package com.facebook.presto.execution;

import com.google.common.base.Objects;
import io.airlift.units.DataSize;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class TaskMemoryManager
{
    private final long maxMemory;
    private final DataSize operatorPreAllocatedMemory;
    private long currentSize;

    public TaskMemoryManager(DataSize maxMemory)
    {
        this(maxMemory, new DataSize(1, MEGABYTE));
    }

    public TaskMemoryManager(DataSize maxMemory, DataSize operatorPreAllocatedMemory)
    {
        this.maxMemory = checkNotNull(maxMemory, "maxMemory is null").toBytes();
        this.operatorPreAllocatedMemory = checkNotNull(operatorPreAllocatedMemory, "operatorPreAllocatedMemory is null");
    }

    public DataSize getMaxMemorySize()
    {
        return new DataSize(maxMemory, BYTE).convertToMostSuccinctDataSize();
    }

    public DataSize getOperatorPreAllocatedMemory()
    {
        return operatorPreAllocatedMemory;
    }

    public synchronized boolean reserve(DataSize size)

    {
        return reserveBytes(size.toBytes());
    }

    public synchronized boolean reserveBytes(long bytes)
    {
        checkArgument(bytes >= 0, "bytes is negative");

        if (currentSize + bytes > maxMemory) {
            return false;
        }
        currentSize += bytes;
        return true;
    }

    public synchronized void reserveOrFail(DataSize size)
    {
        checkState(reserve(size), "Task exceeded max memory size of %s", getMaxMemorySize());
    }

    public synchronized void reserveBytesOrFail(long bytes)
    {
        checkState(reserveBytes(bytes), "Task exceeded max memory size of %s", getMaxMemorySize());
    }

    public synchronized long updateOperatorReservation(long lastSize, DataSize newSize)
    {
        return updateOperatorReservation(lastSize, newSize.toBytes());
    }

    public synchronized long updateOperatorReservation(long lastSize, long newSize)
    {
        checkArgument(lastSize >= 0, "lastSize is negative");
        checkArgument(newSize >= 0, "lastSize is negative");

        long delta = newSize - lastSize;
        if (delta > 0) {
            reserveBytesOrFail(delta);
        }
        return newSize;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("maxMemory", maxMemory)
                .add("currentSize", currentSize)
                .toString();
    }
}
