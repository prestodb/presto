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
package com.facebook.presto.delta;

import com.facebook.presto.cache.CacheConfig;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;
import jakarta.inject.Inject;

import java.util.List;

import static com.facebook.presto.spi.session.PropertyMetadata.booleanProperty;

public final class DeltaSessionProperties
{
    private static final String CACHE_ENABLED = "cache_enabled";
    public static final String PARQUET_DEREFERENCE_PUSHDOWN_ENABLED = "parquet_dereference_pushdown_enabled";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public DeltaSessionProperties(
            DeltaConfig deltaConfigConfig,
            CacheConfig cacheConfig)
    {
        sessionProperties = ImmutableList.of(
                booleanProperty(
                        // required by presto-hive module, might be removed in future
                        CACHE_ENABLED,
                        "Enable cache for Delta tables",
                        cacheConfig.isCachingEnabled(),
                        false),
                booleanProperty(
                        PARQUET_DEREFERENCE_PUSHDOWN_ENABLED,
                        "Is dereference pushdown expression pushdown into Parquet reader enabled?",
                        deltaConfigConfig.isParquetDereferencePushdownEnabled(),
                        false));
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static boolean isParquetDereferencePushdownEnabled(ConnectorSession session)
    {
        return session.getProperty(PARQUET_DEREFERENCE_PUSHDOWN_ENABLED, Boolean.class);
    }
}
