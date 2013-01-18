package com.facebook.presto.util;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Provides a ThreadLocal cache with a maximum cache size per thread
 *
 * @param <K> - cache key type
 * @param <V> - cache value type
 */
public abstract class ThreadLocalCache<K, V>
{
    private final ThreadLocal<LinkedHashMap<K, V>> cache;

    public ThreadLocalCache(final int maxSizePerThread)
    {
        cache = new ThreadLocal<LinkedHashMap<K, V>>() {
            @Override
            protected LinkedHashMap<K, V> initialValue()
            {
                return new LinkedHashMap<K, V>() {
                    @Override
                    protected boolean removeEldestEntry(Map.Entry<K, V> eldest)
                    {
                        return size() > maxSizePerThread;
                    }
                };
            }
        };
    }

    protected abstract V load(K key);

    public V get(K key)
    {
        LinkedHashMap<K, V> map = cache.get();
        if (map.containsKey(key)) {
            return map.get(key);
        }
        else {
            V value = load(key);
            map.put(key, value);
            return value;
        }
    }
}
