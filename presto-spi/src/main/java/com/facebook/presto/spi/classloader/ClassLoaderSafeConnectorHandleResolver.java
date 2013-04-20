package com.facebook.presto.spi.classloader;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.Split;
import com.facebook.presto.spi.TableHandle;

@SuppressWarnings("UnusedDeclaration")
public class ClassLoaderSafeConnectorHandleResolver
        implements ConnectorHandleResolver
{
    private final ConnectorHandleResolver delegate;
    private final ClassLoader classLoader;

    public ClassLoaderSafeConnectorHandleResolver(ConnectorHandleResolver delegate, ClassLoader classLoader)
    {
        this.delegate = delegate;
        this.classLoader = classLoader;
    }

    @Override
    public boolean canHandle(TableHandle tableHandle)
    {
        try (ThreadContextClassLoader threadContextClassLoader = new ThreadContextClassLoader(classLoader)) {
            return delegate.canHandle(tableHandle);
        }
    }

    @Override
    public boolean canHandle(ColumnHandle columnHandle)
    {
        try (ThreadContextClassLoader threadContextClassLoader = new ThreadContextClassLoader(classLoader)) {
            return delegate.canHandle(columnHandle);
        }
    }

    @Override
    public boolean canHandle(Split split)
    {
        try (ThreadContextClassLoader threadContextClassLoader = new ThreadContextClassLoader(classLoader)) {
            return delegate.canHandle(split);
        }
    }

    @Override
    public Class<? extends TableHandle> getTableHandleClass()
    {
        try (ThreadContextClassLoader threadContextClassLoader = new ThreadContextClassLoader(classLoader)) {
            return delegate.getTableHandleClass();
        }
    }

    @Override
    public Class<? extends ColumnHandle> getColumnHandleClass()
    {
        try (ThreadContextClassLoader threadContextClassLoader = new ThreadContextClassLoader(classLoader)) {
            return delegate.getColumnHandleClass();
        }
    }

    @Override
    public Class<? extends Split> getSplitClass()
    {
        try (ThreadContextClassLoader threadContextClassLoader = new ThreadContextClassLoader(classLoader)) {
            return delegate.getSplitClass();
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
