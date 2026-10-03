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
package com.facebook.presto.ducklake;

import com.facebook.presto.cache.CacheConfig;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;
import jakarta.inject.Inject;

import java.util.List;

import static com.facebook.presto.spi.session.PropertyMetadata.booleanProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.doubleProperty;

public final class DuckLakeSessionProperties
{
    private static final String MINIMUM_ASSIGNED_SPLIT_WEIGHT = "minimum_assigned_split_weight";
    // Named and looked up exactly like HiveSessionProperties.CACHE_ENABLED: HdfsEnvironment's
    // caching HdfsConfiguration (bound in DuckLakeCommonModule) reads this session property by
    // name through com.facebook.presto.hive.HiveSessionProperties.isCacheEnabled, regardless of
    // which connector registered it (modeled on IcebergSessionProperties#CACHE_ENABLED).
    private static final String CACHE_ENABLED = "cache_enabled";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public DuckLakeSessionProperties(DuckLakeConfig duckLakeConfig, CacheConfig cacheConfig)
    {
        sessionProperties = ImmutableList.of(
                doubleProperty(
                        MINIMUM_ASSIGNED_SPLIT_WEIGHT,
                        "Minimum assigned split weight",
                        duckLakeConfig.getMinimumAssignedSplitWeight(),
                        false),
                booleanProperty(
                        CACHE_ENABLED,
                        "Enable cache for DuckLake",
                        cacheConfig.isCachingEnabled(),
                        false));
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static double getMinimumAssignedSplitWeight(ConnectorSession session)
    {
        return session.getProperty(MINIMUM_ASSIGNED_SPLIT_WEIGHT, Double.class);
    }
}
