package com.facebook.presto.util;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class Threads
{

    private static final Class<? extends ListeningExecutorService> GUAVA_SAME_THREAD_EXECUTOR_CLASS = MoreExecutors.sameThreadExecutor().getClass();

    public static ThreadFactory threadsNamed(String nameFormat)
    {
        return new ThreadFactoryBuilder().setNameFormat(nameFormat).build();
    }

    public static ThreadFactory daemonThreadsNamed(String nameFormat)
    {
        return new ThreadFactoryBuilder().setNameFormat(nameFormat).setDaemon(true).build();
    }

    public static Executor checkNotSameThreadExecutor(Executor executor, String name)
    {
        checkNotNull(executor, "%s is null", name);
        checkArgument(!isSameThreadExecutor(executor), "%s is a same thread executor", name);
        return executor;
    }

    public static boolean isSameThreadExecutor(Executor executor)
    {
        checkNotNull(executor, "executor is null");
        if (executor.getClass() == GUAVA_SAME_THREAD_EXECUTOR_CLASS) {
            return true;
        }

        final Thread thisThread = Thread.currentThread();
        final SettableFuture<Boolean> isSameThreadExecutor = SettableFuture.create();
        executor.execute(new Runnable()
        {
            @Override
            public void run()
            {
                isSameThreadExecutor.set(thisThread == Thread.currentThread());
            }
        });
        try {
            return Futures.get(isSameThreadExecutor, 10, TimeUnit.SECONDS, Exception.class);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
