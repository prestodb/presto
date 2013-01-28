package com.facebook.presto.hive;

import com.facebook.presto.spi.ImportClient;
import com.facebook.presto.spi.MetadataCache;
import com.facebook.presto.spi.ObjectNotFoundException;
import com.facebook.presto.spi.PartitionInfo;
import com.facebook.presto.spi.SchemaField;
import com.google.common.base.Functions;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheStats;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Collections2;
import com.google.common.collect.Maps;
import io.airlift.units.Duration;
import org.weakref.jmx.Managed;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Implement the Metadata cache SPI.
 */
public class HiveMetadataCache
        implements MetadataCache
{
    private final Cache<String, List<String>> databaseNamesCache;
    private final Cache<String, List<String>> tableNamesCache;
    private final Cache<Map.Entry<String, String>, List<SchemaField>> tableSchemaCache;
    private final Cache<Map.Entry<String, String>, List<SchemaField>> partitionKeysCache;
    private final Cache<Map.Entry<String, String>, List<PartitionInfo>> partitionsCache;
    private final Cache<Map.Entry<String, String>, List<String>> partitionNamesCache;

    private final ConcurrentMap<String, Object> metadataStats = new ConcurrentHashMap<>();

    public HiveMetadataCache(Duration expiresAfterWrite)
    {
        long expiresAfterWriteMillis = (long) expiresAfterWrite.toMillis();

        this.databaseNamesCache = CacheBuilder.newBuilder().expireAfterWrite(expiresAfterWriteMillis, MILLISECONDS).recordStats().build();
        metadataStats.put("dbNames", new HiveMetadataCacheStats(databaseNamesCache));

        this.tableNamesCache = CacheBuilder.newBuilder().expireAfterWrite(expiresAfterWriteMillis, MILLISECONDS).recordStats().build();
        metadataStats.put("tableNames", new HiveMetadataCacheStats(tableNamesCache));

        this.tableSchemaCache = CacheBuilder.newBuilder().expireAfterWrite(expiresAfterWriteMillis, MILLISECONDS).recordStats().build();
        metadataStats.put("schemaCache", new HiveMetadataCacheStats(tableSchemaCache));

        this.partitionKeysCache = CacheBuilder.newBuilder().expireAfterWrite(expiresAfterWriteMillis, MILLISECONDS).recordStats().build();
        metadataStats.put("partitionKeys", new HiveMetadataCacheStats(partitionKeysCache));

        this.partitionsCache = CacheBuilder.newBuilder().expireAfterWrite(expiresAfterWriteMillis, MILLISECONDS).recordStats().build();
        metadataStats.put("partitions", new HiveMetadataCacheStats(partitionKeysCache));

        this.partitionNamesCache = CacheBuilder.newBuilder().expireAfterWrite(expiresAfterWriteMillis, MILLISECONDS).recordStats().build();
        metadataStats.put("partitionNames", new HiveMetadataCacheStats(partitionKeysCache));
    }

    private static <K, V, E extends Exception> V getWithCallable(Cache<K, V> map, K key, Callable<V> callable, Class<E> exceptionClass)
            throws E
    {
        try {
            return map.get(key, callable);
        }
        catch (ExecutionException ee) {
            Throwable t = ee.getCause();
            Throwables.propagateIfInstanceOf(t, exceptionClass);
            throw Throwables.propagate(t);
        }
    }

    @Override
    public List<String> getDatabaseNames(final ImportClient backingClient)
    {
        return getWithCallable(databaseNamesCache, "", new Callable<List<String>>()
        {
            @Override
            public List<String> call()
                    throws Exception
            {
                return backingClient.getDatabaseNames();
            }
        }, RuntimeException.class);
    }

    @Override
    public List<String> getTableNames(final ImportClient backingClient, final String databaseName)
            throws ObjectNotFoundException
    {
        return getWithCallable(tableNamesCache, databaseName, new Callable<List<String>>()
        {
            @Override
            public List<String> call()
                    throws Exception
            {
                return backingClient.getTableNames(databaseName);
            }
        }, ObjectNotFoundException.class);
    }

    @Override
    public List<SchemaField> getTableSchema(final ImportClient backingClient, final String databaseName, final String tableName)
            throws ObjectNotFoundException
    {
        return getWithCallable(tableSchemaCache, Maps.immutableEntry(databaseName, tableName), new Callable<List<SchemaField>>()
        {
            @Override
            public List<SchemaField> call()
                    throws Exception
            {
                return backingClient.getTableSchema(databaseName, tableName);
            }
        }, ObjectNotFoundException.class);
    }

    @Override
    public List<SchemaField> getPartitionKeys(final ImportClient backingClient, final String databaseName, final String tableName)
            throws ObjectNotFoundException
    {
        return getWithCallable(partitionKeysCache, Maps.immutableEntry(databaseName, tableName), new Callable<List<SchemaField>>()
        {
            @Override
            public List<SchemaField> call()
                    throws Exception
            {
                return backingClient.getPartitionKeys(databaseName, tableName);
            }
        }, ObjectNotFoundException.class);
    }

    public List<PartitionInfo> getPartitions(final ImportClient backingClient, final String databaseName, final String tableName)
            throws ObjectNotFoundException
    {
        return getWithCallable(partitionsCache, Maps.immutableEntry(databaseName, tableName), new Callable<List<PartitionInfo>>()
        {
            @Override
            public List<PartitionInfo> call()
                    throws Exception
            {
                return backingClient.getPartitions(databaseName, tableName);
            }
        }, ObjectNotFoundException.class);
    }

    public List<String> getPartitionNames(final ImportClient backingClient, final String databaseName, final String tableName)
            throws ObjectNotFoundException
    {
        return getWithCallable(partitionNamesCache, Maps.immutableEntry(databaseName, tableName), new Callable<List<String>>()
        {
            @Override
            public List<String> call()
                    throws Exception
            {
                return backingClient.getPartitionNames(databaseName, tableName);
            }
        }, ObjectNotFoundException.class);
    }

    @Override
    public Map<String, Object> getMetadataCacheStats()
    {
        return metadataStats;
    }

    public static class HiveMetadataCacheStats
    {
        private final Cache<? extends Object, ?> cache;

        // Cache the stats object for the cache so that not each
        // method call goes out and rebuilds the stats object
        private final LoadingCache<String, CacheStats> cacheStatsCache = CacheBuilder.newBuilder()
                .maximumSize(1)
                .expireAfterWrite(60, SECONDS)
                .build(new CacheLoader<String, CacheStats>() {

                    @Override
                    public CacheStats load(String key)
                            throws Exception
                    {
                        return cache.stats();
                    }
                });

        HiveMetadataCacheStats(Cache<? extends Object, ?> cache)
        {
            this.cache = cache;
        }

        @Managed
        public void flush()
        {
            cache.invalidateAll();
        }

        @Managed
        public long getSize()
        {
            return cache.size();
        }

        @Managed
        public Set<String> getKeys()
        {
            return new HashSet<>(Collections2.transform(cache.asMap().keySet(), Functions.toStringFunction()));
        }

        @Managed
        public double getAverageLoadPenalty()
        {
            CacheStats cacheStats = cacheStatsCache.getUnchecked("");
            return cacheStats.averageLoadPenalty();
        }

        @Managed
        public long getEvictionCount()
        {
            CacheStats cacheStats = cacheStatsCache.getUnchecked("");
            return cacheStats.evictionCount();
        }

        @Managed
        public long getHitCount()
        {
            CacheStats cacheStats = cacheStatsCache.getUnchecked("");
            return cacheStats.hitCount();
        }

        @Managed
        public double getHitRate()
        {
            CacheStats cacheStats = cacheStatsCache.getUnchecked("");
            return cacheStats.hitRate();
        }

        @Managed
        public long getLoadCount()
        {
            CacheStats cacheStats = cacheStatsCache.getUnchecked("");
            return cacheStats.loadCount();
        }

        @Managed
        public long getLoadExceptionCount()
        {
            CacheStats cacheStats = cacheStatsCache.getUnchecked("");
            return cacheStats.loadExceptionCount();
        }

        @Managed
        public double getLoadExceptionRate()
        {
            CacheStats cacheStats = cacheStatsCache.getUnchecked("");
            return cacheStats.loadExceptionRate();
        }

        @Managed
        public long getLoadSuccessCount()
        {
            CacheStats cacheStats = cacheStatsCache.getUnchecked("");
            return cacheStats.loadSuccessCount();
        }

        @Managed
        public long getMissCount()
        {
            CacheStats cacheStats = cacheStatsCache.getUnchecked("");
            return cacheStats.missCount();
        }

        @Managed
        public double getMissRate()
        {
            CacheStats cacheStats = cacheStatsCache.getUnchecked("");
            return cacheStats.missRate();
        }

        @Managed
        public long getRequestCount()
        {
            CacheStats cacheStats = cacheStatsCache.getUnchecked("");
            return cacheStats.requestCount();
        }

        @Managed
        public long getTotalLoadTime()
        {
            CacheStats cacheStats = cacheStatsCache.getUnchecked("");
            return cacheStats.totalLoadTime();
        }
    }
}
