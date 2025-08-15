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
package com.facebook.presto.spark.execution.property;

import com.facebook.airlift.configuration.Config;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;

import java.util.Map;

/**
 * This config class corresponds to catalog/<catalog-name>.properties for native execution process. Properties inside will be used in PrestoServer.cpp
 * Currently, presto-on-spark native only supports querying 1 catalog in a single spark session.
 */
public class NativeExecutionConnectorConfig
{
    private static final String CACHE_ENABLED = "cache.enabled";
    private static final String CACHE_MAX_CACHE_SIZE = "cache.max-cache-size";
    private static final String CONNECTOR_NAME = "connector.name";

    private boolean cacheEnabled;
    // maxCacheSize in MB
    private DataSize maxCacheSize = new DataSize(0, DataSize.Unit.MEGABYTE);
    private String connectorName = "hive";

    public Map<String, String> getAllProperties()
    {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        return builder.put(CACHE_ENABLED, String.valueOf(isCacheEnabled()))
                .put(CACHE_MAX_CACHE_SIZE, String.valueOf(getDataSizeInLong(getMaxCacheSize().convertTo(DataSize.Unit.MEGABYTE))))
                .put(CONNECTOR_NAME, getConnectorName())
                .build();
    }

    public boolean isCacheEnabled()
    {
        return cacheEnabled;
    }

    @Config(CACHE_ENABLED)
    public NativeExecutionConnectorConfig setCacheEnabled(boolean cacheEnabled)
    {
        this.cacheEnabled = cacheEnabled;
        return this;
    }

    public DataSize getMaxCacheSize()
    {
        return maxCacheSize;
    }

    @Config(CACHE_MAX_CACHE_SIZE)
    public NativeExecutionConnectorConfig setMaxCacheSize(DataSize maxCacheSize)
    {
        this.maxCacheSize = maxCacheSize;
        return this;
    }

    public String getConnectorName()
    {
        return connectorName;
    }

    @Config(CONNECTOR_NAME)
    public NativeExecutionConnectorConfig setConnectorName(String connectorName)
    {
        this.connectorName = connectorName;
        return this;
    }

    private Long getDataSizeInLong(DataSize size)
    {
        return Double.valueOf(size.getValue()).longValue();
    }
}
