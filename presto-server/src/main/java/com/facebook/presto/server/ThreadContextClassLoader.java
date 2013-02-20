/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import java.io.Closeable;

public class ThreadContextClassLoader
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
