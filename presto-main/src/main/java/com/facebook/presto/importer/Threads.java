package com.facebook.presto.importer;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.ThreadFactory;

class Threads
{
    public static ThreadFactory threadsNamed(String nameFormat)
    {
        return new ThreadFactoryBuilder().setNameFormat(nameFormat).build();
    }
}
