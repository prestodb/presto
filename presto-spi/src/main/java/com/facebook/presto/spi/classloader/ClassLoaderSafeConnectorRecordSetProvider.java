package com.facebook.presto.spi.classloader;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.Split;

import java.util.List;

@SuppressWarnings("UnusedDeclaration")
public class ClassLoaderSafeConnectorRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private final ConnectorRecordSetProvider delegate;
    private final ClassLoader classLoader;

    public ClassLoaderSafeConnectorRecordSetProvider(ConnectorRecordSetProvider delegate, ClassLoader classLoader)
    {
        this.delegate = delegate;
        this.classLoader = classLoader;
    }

    @Override
    public boolean canHandle(Split split)
    {
        try (ThreadContextClassLoader threadContextClassLoader = new ThreadContextClassLoader(classLoader)) {
            return delegate.canHandle(split);
        }
    }

    @Override
    public RecordSet getRecordSet(Split split, List<? extends ColumnHandle> columns)
    {
        try (ThreadContextClassLoader threadContextClassLoader = new ThreadContextClassLoader(classLoader)) {
            return new ClassLoaderSafeRecordSet(delegate.getRecordSet(split, columns), classLoader);
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
