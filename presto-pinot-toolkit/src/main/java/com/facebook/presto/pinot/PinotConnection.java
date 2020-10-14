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
package com.facebook.presto.pinot;

import com.facebook.presto.pinot.PinotClusterInfoFetcher.TimeBoundary;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.pinot.spi.data.Schema;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.pinot.PinotErrorCode.PINOT_UNCLASSIFIED_ERROR;
import static com.google.common.cache.CacheLoader.asyncReloading;
import static java.util.Objects.requireNonNull;

public class PinotConnection
{
    private static final Object ALL_TABLES_CACHE_KEY = new Object();

    private final LoadingCache<String, List<PinotColumn>> pinotTableColumnCache;
    private final LoadingCache<Object, List<String>> allTablesCache;
    private final PinotConfig pinotConfig;
    private final PinotClusterInfoFetcher pinotClusterInfoFetcher;

    @Inject
    public PinotConnection(
            PinotClusterInfoFetcher pinotClusterInfoFetcher,
            PinotConfig pinotConfig,
            @ForPinot Executor executor)
    {
        this.pinotConfig = requireNonNull(pinotConfig, "pinot config");
        final long metadataCacheExpiryMillis = this.pinotConfig.getMetadataCacheExpiry().roundTo(TimeUnit.MILLISECONDS);
        this.pinotClusterInfoFetcher = requireNonNull(pinotClusterInfoFetcher, "cluster info fetcher is null");
        this.allTablesCache = CacheBuilder.newBuilder()
                .refreshAfterWrite(metadataCacheExpiryMillis, TimeUnit.MILLISECONDS)
                .build(asyncReloading(CacheLoader.from(pinotClusterInfoFetcher::getAllTables), executor));

        this.pinotTableColumnCache =
                CacheBuilder.newBuilder()
                        .refreshAfterWrite(metadataCacheExpiryMillis, TimeUnit.MILLISECONDS)
                        .build(asyncReloading(new CacheLoader<String, List<PinotColumn>>()
                        {
                            @Override
                            public List<PinotColumn> load(String tableName)
                                    throws Exception
                            {
                                Schema tablePinotSchema = pinotClusterInfoFetcher.getTableSchema(tableName);
                                return PinotColumnUtils.getPinotColumnsForPinotSchema(tablePinotSchema, pinotConfig.isInferDateTypeInSchema(), pinotConfig.isInferTimestampTypeInSchema());
                            }
                        }, executor));

        executor.execute(() -> this.allTablesCache.refresh(ALL_TABLES_CACHE_KEY));
    }

    private static <K, V> V getFromCache(LoadingCache<K, V> cache, K key)
    {
        V value = cache.getIfPresent(key);
        if (value != null) {
            return value;
        }
        try {
            return cache.get(key);
        }
        catch (ExecutionException e) {
            throw new PinotException(PINOT_UNCLASSIFIED_ERROR, Optional.empty(), "Cannot fetch from cache " + key, e.getCause());
        }
    }

    public List<String> getTableNames()
    {
        return getFromCache(allTablesCache, ALL_TABLES_CACHE_KEY);
    }

    public PinotTable getTable(String tableName)
    {
        List<PinotColumn> columns = getPinotColumnsForTable(tableName);
        return new PinotTable(tableName, columns);
    }

    private List<PinotColumn> getPinotColumnsForTable(String tableName)
    {
        return getFromCache(pinotTableColumnCache, tableName);
    }

    public Map<String, Map<String, List<String>>> getRoutingTable(String tableName)
    {
        return pinotClusterInfoFetcher.getRoutingTableForTable(tableName);
    }

    public TimeBoundary getTimeBoundary(String tableName)
    {
        return pinotClusterInfoFetcher.getTimeBoundaryForTable(tableName);
    }

    public int getGrpcPort(String serverInstance)
    {
        return pinotClusterInfoFetcher.getGrpcPort(serverInstance);
    }
}
