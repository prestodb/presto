package com.facebook.presto.util;

import java.io.Closeable;

public class SetThreadName
        implements Closeable
{
    private final String originalThreadName;

    public SetThreadName(String format, Object... args)
    {
        originalThreadName = Thread.currentThread().getName();
        Thread.currentThread().setName(String.format(format, args) + "-" + Thread.currentThread().getId());
    }

    @Override
    public void close()
    {
        Thread.currentThread().setName(originalThreadName);
    }
}
