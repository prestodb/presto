package com.facebook.presto.hive;

import java.io.Closeable;

class ThreadContextClassLoader
        implements Closeable
{
    private final ClassLoader originalThreadContextClassLoader;

    ThreadContextClassLoader(ClassLoader newThreadContextClassLoader)
    {
        this.originalThreadContextClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(newThreadContextClassLoader);
    }

    @Override
    public void close()
    {
        Thread.currentThread().setContextClassLoader(originalThreadContextClassLoader);
    }
}
