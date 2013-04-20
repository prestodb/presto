package com.facebook.presto.spi.classloader;

import com.facebook.presto.spi.ColumnType;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;

import java.util.List;

public class ClassLoaderSafeRecordSet
        implements RecordSet
{
    private final RecordSet delegate;
    private final ClassLoader classLoader;

    public ClassLoaderSafeRecordSet(RecordSet delegate, ClassLoader classLoader)
    {
        this.delegate = delegate;
        this.classLoader = classLoader;
    }

    @Override
    public List<ColumnType> getColumnTypes()
    {
        try (ThreadContextClassLoader threadContextClassLoader = new ThreadContextClassLoader(classLoader)) {
            return delegate.getColumnTypes();
        }
    }

    @Override
    public RecordCursor cursor()
    {
        try (ThreadContextClassLoader threadContextClassLoader = new ThreadContextClassLoader(classLoader)) {
            return delegate.cursor();
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
