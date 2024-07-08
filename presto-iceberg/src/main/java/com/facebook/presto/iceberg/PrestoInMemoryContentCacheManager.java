/*
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
package com.facebook.presto.iceberg;

import com.facebook.airlift.log.Logger;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.SystemConfigs;
import org.apache.iceberg.io.ContentCacheManager;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileIOContentCache;
import org.apache.iceberg.io.InMemoryContentCache;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.util.PropertyUtil;

import javax.inject.Inject;

import java.util.Map;

public final class PrestoInMemoryContentCacheManager
        implements ContentCacheManager
{
    private static final Logger LOG = Logger.get(PrestoInMemoryContentCacheManager.class);

    public Cache<String, InMemoryContentCache> getCache()
    {
        return cache;
    }

    private final Cache<String, InMemoryContentCache> cache = newManifestCacheBuilder().build();
    @Inject
    private static PrestoInMemoryContentCacheManager inMemoryContentCacheManager;

    public static PrestoInMemoryContentCacheManager create(Map<String, String> unused)
    {
        return inMemoryContentCacheManager;
    }

    @Override
    public FileIOContentCache contentCache(FileIO io)
    {
        return cache.get(
                io.getClass().getCanonicalName(),
            fileIO ->
              new InMemoryContentCache(
                  cacheDurationMs(io), cacheTotalBytes(io), cacheMaxContentLength(io)));
    }

    @Override
    public synchronized void dropCache(FileIO fileIO)
    {
        cache.invalidate(fileIO);
        cache.cleanUp();
    }

    static long cacheDurationMs(FileIO io)
    {
        return PropertyUtil.propertyAsLong(
          io.properties(),
          CatalogProperties.IO_MANIFEST_CACHE_EXPIRATION_INTERVAL_MS,
          CatalogProperties.IO_MANIFEST_CACHE_EXPIRATION_INTERVAL_MS_DEFAULT);
    }

    static long cacheTotalBytes(FileIO io)
    {
        return PropertyUtil.propertyAsLong(
          io.properties(),
          CatalogProperties.IO_MANIFEST_CACHE_MAX_TOTAL_BYTES,
          CatalogProperties.IO_MANIFEST_CACHE_MAX_TOTAL_BYTES_DEFAULT);
    }

    static long cacheMaxContentLength(FileIO io)
    {
        return PropertyUtil.propertyAsLong(
          io.properties(),
          CatalogProperties.IO_MANIFEST_CACHE_MAX_CONTENT_LENGTH,
          CatalogProperties.IO_MANIFEST_CACHE_MAX_CONTENT_LENGTH_DEFAULT);
    }

    @VisibleForTesting
    static Caffeine<Object, Object> newManifestCacheBuilder()
    {
        int maxSize = SystemConfigs.IO_MANIFEST_CACHE_MAX_FILEIO.value();
        return Caffeine.newBuilder()
          .softValues()
          .maximumSize(maxSize)
          .removalListener(
              (io, contentCache, cause) ->
                  LOG.debug("Evicted {} from FileIO-level cache ({})", io, cause))
          .recordStats();
    }
}
