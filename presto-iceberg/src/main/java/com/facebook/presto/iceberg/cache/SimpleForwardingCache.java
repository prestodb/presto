package com.facebook.presto.iceberg.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Policy;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

public class SimpleForwardingCache<K, V> implements Cache<K, V>
{
    private final Cache<K, V> delegate;

    public SimpleForwardingCache(Cache<K, V> delegate)
    {
        this.delegate = delegate;
    }

    @Override
    public @Nullable V getIfPresent(@NonNull Object key)
    {
        return delegate.getIfPresent(key);
    }

    @Override
    public @Nullable V get(@NonNull K key, @NonNull Function<? super K, ? extends V> mappingFunction)
    {
        return delegate.get(key, mappingFunction);
    }

    @Override
    public @NonNull Map<@NonNull K, @NonNull V> getAllPresent(@NonNull Iterable<@NonNull ?> keys)
    {
        return delegate.getAllPresent(keys);
    }

    @Override
    public void put(@NonNull K key, @NonNull V value)
    {
        delegate.put(key, value);
    }

    @Override
    public void putAll(@NonNull Map<? extends @NonNull K, ? extends @NonNull V> map)
    {
        delegate.putAll(map);
    }

    @Override
    public void invalidate(@NonNull Object key)
    {
        delegate.invalidate(key);
    }

    @Override
    public void invalidateAll(@NonNull Iterable<@NonNull ?> keys)
    {
        delegate.invalidateAll(keys);
    }

    @Override
    public void invalidateAll()
    {
        delegate.invalidateAll();
    }

    @Override
    public @NonNegative long estimatedSize()
    {
        return delegate.estimatedSize();
    }

    @Override
    public @NonNull CacheStats stats()
    {
        return delegate.stats();
    }

    @Override
    public @NonNull ConcurrentMap<@NonNull K, @NonNull V> asMap()
    {
        return delegate.asMap();
    }

    @Override
    public void cleanUp()
    {
        delegate.cleanUp();
    }

    @Override
    public @NonNull Policy<K, V> policy()
    {
        return delegate.policy();
    }
}
