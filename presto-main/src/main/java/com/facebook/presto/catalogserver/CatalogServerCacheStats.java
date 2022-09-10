/*
 * Copyright (C) 2013 Facebook, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.catalogserver;

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;

import java.util.concurrent.atomic.AtomicLong;

@ThriftStruct
public class CatalogServerCacheStats
{
    private final CacheStats catalogExistsCacheStats;
    private final CacheStats schemaExistsCacheStats;
    private final CacheStats listSchemaNamesCacheStats;
    private final CacheStats getTableHandleCacheStats;
    private final CacheStats listTablesCacheStats;
    private final CacheStats listViewsCacheStats;
    private final CacheStats getViewsCacheStats;
    private final CacheStats getViewCacheStats;
    private final CacheStats getMaterializedViewCacheStats;
    private final CacheStats getReferencedMaterializedViewsCacheStats;

    @ThriftConstructor
    public CatalogServerCacheStats(
            CacheStats catalogExistsCacheStats,
            CacheStats schemaExistsCacheStats,
            CacheStats listSchemaNamesCacheStats,
            CacheStats getTableHandleCacheStats,
            CacheStats listTablesCacheStats,
            CacheStats listViewsCacheStats,
            CacheStats getViewsCacheStats,
            CacheStats getViewCacheStats,
            CacheStats getMaterializedViewCacheStats,
            CacheStats getReferencedMaterializedViewsCacheStats
    ) {
        this.catalogExistsCacheStats = catalogExistsCacheStats;
        this.schemaExistsCacheStats = schemaExistsCacheStats;
        this.listSchemaNamesCacheStats = listSchemaNamesCacheStats;
        this.getTableHandleCacheStats = getTableHandleCacheStats;
        this.listTablesCacheStats = listTablesCacheStats;
        this.listViewsCacheStats = listViewsCacheStats;
        this.getViewsCacheStats = getViewsCacheStats;
        this.getViewCacheStats = getViewCacheStats;
        this.getMaterializedViewCacheStats = getMaterializedViewCacheStats;
        this.getReferencedMaterializedViewsCacheStats = getReferencedMaterializedViewsCacheStats;
    }

    public CatalogServerCacheStats()
    {
        this.catalogExistsCacheStats = new CacheStats();
        this.schemaExistsCacheStats = new CacheStats();
        this.listSchemaNamesCacheStats = new CacheStats();
        this.getTableHandleCacheStats = new CacheStats();
        this.listTablesCacheStats = new CacheStats();
        this.listViewsCacheStats = new CacheStats();
        this.getViewsCacheStats = new CacheStats();
        this.getViewCacheStats = new CacheStats();
        this.getMaterializedViewCacheStats = new CacheStats();
        this.getReferencedMaterializedViewsCacheStats = new CacheStats();
    }

    @ThriftField(1)
    public CacheStats getCatalogExistsCacheStats()
    {
        return catalogExistsCacheStats;
    }

    @ThriftField(2)
    public CacheStats getSchemaExistsCacheStats()
    {
        return schemaExistsCacheStats;
    }

    @ThriftField(3)
    public CacheStats getListSchemaNamesCacheStats()
    {
        return listSchemaNamesCacheStats;
    }

    @ThriftField(4)
    public CacheStats getGetTableHandleCacheStats()
    {
        return getTableHandleCacheStats;
    }

    @ThriftField(5)
    public CacheStats getListTablesCacheStats()
    {
        return listTablesCacheStats;
    }

    @ThriftField(6)
    public CacheStats getListViewsCacheStats()
    {
        return listViewsCacheStats;
    }

    @ThriftField(7)
    public CacheStats getGetViewsCacheStats()
    {
        return getViewsCacheStats;
    }

    @ThriftField(8)
    public CacheStats getGetViewCacheStats()
    {
        return getViewCacheStats;
    }

    @ThriftField(9)
    public CacheStats getGetMaterializedViewCacheStats()
    {
        return getMaterializedViewCacheStats;
    }

    @ThriftField(10)
    public CacheStats getGetReferencedMaterializedViewsCacheStats()
    {
        return getReferencedMaterializedViewsCacheStats;
    }

    public void clearAll()
    {
        this.catalogExistsCacheStats.clear();
        this.schemaExistsCacheStats.clear();
        this.listSchemaNamesCacheStats.clear();
        this.getTableHandleCacheStats.clear();
        this.listTablesCacheStats.clear();
        this.listViewsCacheStats.clear();
        this.getViewsCacheStats.clear();
        this.getViewCacheStats.clear();
        this.getMaterializedViewCacheStats.clear();
        this.getReferencedMaterializedViewsCacheStats.clear();
    }

    @ThriftStruct
    public static class CacheStats
    {
        private final AtomicLong cacheHitCount;
        private final AtomicLong cacheMissCount;

        @ThriftConstructor
        public CacheStats(long cacheHitCount, long cacheMissCount)
        {
            this.cacheHitCount = new AtomicLong(cacheHitCount);
            this.cacheMissCount = new AtomicLong(cacheMissCount);
        }

        public CacheStats()
        {
            this.cacheHitCount = new AtomicLong();
            this.cacheMissCount = new AtomicLong();
        }

        public void incrementCacheHit()
        {
            cacheHitCount.getAndIncrement();
        }

        public void incrementCacheMiss()
        {
            cacheMissCount.getAndIncrement();
        }

        public void clear()
        {
            this.cacheHitCount.set(0);
            this.cacheMissCount.set(0);
        }

        @ThriftField(1)
        public long getCacheHitCount()
        {
            return cacheHitCount.get();
        }

        @ThriftField(2)
        public long getCacheMissCount()
        {
            return cacheMissCount.get();
        }
    }
}
