package com.facebook.presto.hive;

import com.google.common.cache.CacheLoader;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

import java.util.concurrent.Callable;

import static com.google.common.base.Preconditions.checkNotNull;

abstract class BackgroundCacheLoader<K, V>
        extends CacheLoader<K, V>
{
    private final ListeningExecutorService executor;

    protected BackgroundCacheLoader(ListeningExecutorService executor)
    {
        this.executor = checkNotNull(executor, "executor is null");
    }

    @Override
    public final ListenableFuture<V> reload(final K key, V oldValue)
            throws Exception
    {
        return executor.submit(new Callable<V>()
        {
            @Override
            public V call()
                    throws Exception
            {
                return load(key);
            }
        });
    }
}
