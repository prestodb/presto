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
package com.facebook.presto.plugin.jdbc;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import com.facebook.airlift.units.Duration;
import com.facebook.airlift.units.MinDuration;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.util.concurrent.TimeUnit;

public class JdbcMetadataConfig
{
    private boolean allowDropTable;
    private boolean metadataTransactionCacheEnabled = true;
    private Duration metadataCacheTtl = new Duration(0, TimeUnit.SECONDS);
    private Duration metadataCacheRefreshInterval = new Duration(0, TimeUnit.SECONDS);
    private long metadataCacheMaximumSize = 10000;
    private long metadataTransactionCacheMaximumSize = 1000;

    public boolean isAllowDropTable()
    {
        return allowDropTable;
    }

    @Config("allow-drop-table")
    @ConfigDescription("Allow connector to drop tables")
    public JdbcMetadataConfig setAllowDropTable(boolean allowDropTable)
    {
        this.allowDropTable = allowDropTable;
        return this;
    }

    public boolean isMetadataTransactionCacheEnabled()
    {
        return metadataTransactionCacheEnabled;
    }

    @Config("metadata-transaction-cache-enabled")
    @ConfigDescription("Enable metadata caching within a connector transaction")
    public JdbcMetadataConfig setMetadataTransactionCacheEnabled(boolean metadataTransactionCacheEnabled)
    {
        this.metadataTransactionCacheEnabled = metadataTransactionCacheEnabled;
        return this;
    }

    @NotNull
    public Duration getMetadataCacheTtl()
    {
        return metadataCacheTtl;
    }

    @MinDuration("0ms")
    @Config("metadata-cache-ttl")
    public JdbcMetadataConfig setMetadataCacheTtl(Duration metadataCacheTtl)
    {
        this.metadataCacheTtl = metadataCacheTtl;
        return this;
    }

    @NotNull
    public Duration getMetadataCacheRefreshInterval()
    {
        return metadataCacheRefreshInterval;
    }

    @MinDuration("1ms")
    @Config("metadata-cache-refresh-interval")
    public JdbcMetadataConfig setMetadataCacheRefreshInterval(Duration metadataCacheRefreshInterval)
    {
        this.metadataCacheRefreshInterval = metadataCacheRefreshInterval;
        return this;
    }

    public long getMetadataCacheMaximumSize()
    {
        return metadataCacheMaximumSize;
    }

    @Min(1)
    @Config("metadata-cache-maximum-size")
    public JdbcMetadataConfig setMetadataCacheMaximumSize(long metadataCacheMaximumSize)
    {
        this.metadataCacheMaximumSize = metadataCacheMaximumSize;
        return this;
    }

    public long getMetadataTransactionCacheMaximumSize()
    {
        return metadataTransactionCacheMaximumSize;
    }

    @Min(1)
    @Config("metadata-transaction-cache-maximum-size")
    @ConfigDescription("Maximum number of metadata entries cached within a connector transaction")
    public JdbcMetadataConfig setMetadataTransactionCacheMaximumSize(long metadataTransactionCacheMaximumSize)
    {
        this.metadataTransactionCacheMaximumSize = metadataTransactionCacheMaximumSize;
        return this;
    }
}
