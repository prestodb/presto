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
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.util.concurrent.TimeUnit;

public class JdbcMetadataConfig
{
    private boolean allowDropTable;
    private Duration metadataCacheTtl = new Duration(0, TimeUnit.SECONDS);
    private Duration metadataCacheRefreshInterval = new Duration(0, TimeUnit.SECONDS);
    private long metadataCacheMaximumSize = 10000;

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
}
