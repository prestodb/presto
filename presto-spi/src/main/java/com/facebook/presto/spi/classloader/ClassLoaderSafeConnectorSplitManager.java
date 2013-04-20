package com.facebook.presto.spi.classloader;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.Partition;
import com.facebook.presto.spi.Split;
import com.facebook.presto.spi.TableHandle;

import java.util.List;
import java.util.Map;

@SuppressWarnings("UnusedDeclaration")
public final class ClassLoaderSafeConnectorSplitManager
        implements ConnectorSplitManager
{
    private final ConnectorSplitManager delegate;
    private final ClassLoader classLoader;

    public ClassLoaderSafeConnectorSplitManager(ConnectorSplitManager delegate, ClassLoader classLoader)
    {
        this.delegate = delegate;
        this.classLoader = classLoader;
    }

    @Override
    public String getConnectorId()
    {
        try (ThreadContextClassLoader threadContextClassLoader = new ThreadContextClassLoader(classLoader)) {
            return delegate.getConnectorId();
        }
    }

    @Override
    public boolean canHandle(TableHandle handle)
    {
        try (ThreadContextClassLoader threadContextClassLoader = new ThreadContextClassLoader(classLoader)) {
            return delegate.canHandle(handle);
        }
    }

    @Override
    public List<Partition> getPartitions(TableHandle table, Map<ColumnHandle, Object> bindings)
    {
        try (ThreadContextClassLoader threadContextClassLoader = new ThreadContextClassLoader(classLoader)) {
            return delegate.getPartitions(table, bindings);
        }
    }

    @Override
    public Iterable<Split> getPartitionSplits(List<Partition> partitions)
    {
        try (ThreadContextClassLoader threadContextClassLoader = new ThreadContextClassLoader(classLoader)) {
            return delegate.getPartitionSplits(partitions);
        }
    }

    @Override
    public String toString()
    {
        try (ThreadContextClassLoader threadContextClassLoader = new ThreadContextClassLoader(classLoader)) {
            return delegate.toString();
        }
    }
}
