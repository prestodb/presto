package com.facebook.presto.iceberg.cache;

import com.github.benmanes.caffeine.cache.Cache;
import org.weakref.jmx.Managed;

import static java.util.Objects.requireNonNull;

public class CaffeineCacheStatsMBean
{
    private final Cache<?, ?> cache;

    public CaffeineCacheStatsMBean(Cache<?, ?> cache)
    {
        this.cache = requireNonNull(cache, "cache is null");
    }

    @Managed
    public long getEstimatedSize()
    {
        return cache.estimatedSize();
    }

    @Managed
    public long getHitCount()
    {
        return cache.stats().hitCount();
    }

    @Managed
    public long getMissCount()
    {
        return cache.stats().missCount();
    }

    @Managed
    public double getHitRate()
    {
        return cache.stats().hitRate();
    }

    @Managed
    public double getMissRate()
    {
        return cache.stats().missRate();
    }

    @Managed
    public double getEvictionCount()
    {
        return cache.stats().evictionCount();
    }

    @Managed
    public double getEvictionWeight()
    {
        return cache.stats().evictionWeight();
    }

    @Managed
    public double getLoadCount()
    {
        return cache.stats().loadCount();
    }

    @Managed
    public double getRequestCount()
    {
        return cache.stats().requestCount();
    }

    @Managed
    public double getAverageLoadPenalty()
    {
        return cache.stats().averageLoadPenalty();
    }
}
