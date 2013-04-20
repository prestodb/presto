package com.facebook.presto.spi.classloader;

import java.io.Closeable;

public class ThreadContextClassLoader
        implements Closeable
{
    private final ClassLoader originalThreadContextClassLoader;

    public ThreadContextClassLoader(ClassLoader newThreadContextClassLoader)
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
