/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

public class ThreadContextClassLoader
        implements AutoCloseable
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
