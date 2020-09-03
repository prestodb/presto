package com.facebook.presto.dispatcher;

import com.facebook.presto.spi.memory.ClusterMemoryPoolManager;
import com.facebook.presto.spi.memory.MemoryPoolId;
import com.facebook.presto.spi.memory.MemoryPoolInfo;

import java.util.function.Consumer;

public class DistributedClusterMemoryManager
        implements ClusterMemoryPoolManager
{
    @Override
    public void addChangeListener(MemoryPoolId poolId, Consumer<MemoryPoolInfo> listener)
    {
        throw new UnsupportedOperationException();
    }
}
