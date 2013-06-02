package com.facebook.presto.util;

import javax.annotation.Nonnull;

import java.util.LinkedHashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Provides a ThreadLocal cache with a maximum cache size per thread.
 * Values must not be null.
 *
 * @param <K> - cache key type
 * @param <V> - cache value type
 */
public abstract class ThreadLocalCache<K, V>
{
    @SuppressWarnings("ThreadLocalNotStaticFinal")
    private final ThreadLocal<LinkedHashMap<K, V>> cache;

    public ThreadLocalCache(final int maxSizePerThread)
    {
        checkArgument(maxSizePerThread > 0, "max size must be greater than zero");
        cache = new ThreadLocal<LinkedHashMap<K, V>>()
        {
            @SuppressWarnings({"CloneableClassWithoutClone", "ClassExtendsConcreteCollection"})
            @Override
            protected LinkedHashMap<K, V> initialValue()
            {
                return new LinkedHashMap<K, V>()
                {
                    @Override
                    protected boolean removeEldestEntry(Map.Entry<K, V> eldest)
                    {
                        return size() > maxSizePerThread;
                    }
                };
            }
        };
    }

    @Nonnull
    protected abstract V load(K key);

    public final V get(K key)
    {
        LinkedHashMap<K, V> map = cache.get();

        V value = map.get(key);
        if (value != null) {
            return value;
        }

        value = load(key);
        checkNotNull(value, "value must not be null");
        map.put(key, value);
        return value;
    }
}
