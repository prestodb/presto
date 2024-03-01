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
package com.facebook.presto.parquet.cache;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.airlift.units.MinDataSize;
import io.airlift.units.MinDuration;

import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.concurrent.TimeUnit.SECONDS;

public class ParquetCacheConfig
{
    private boolean metadataCacheEnabled;
    private DataSize metadataCacheSize = new DataSize(0, BYTE);
    private Duration metadataCacheTtlSinceLastAccess = new Duration(0, SECONDS);

    public boolean isMetadataCacheEnabled()
    {
        return metadataCacheEnabled;
    }

    @Config("parquet.metadata-cache-enabled")
    @ConfigDescription("Enable cache for parquet metadata")
    public ParquetCacheConfig setMetadataCacheEnabled(boolean metadataCacheEnabled)
    {
        this.metadataCacheEnabled = metadataCacheEnabled;
        return this;
    }

    @MinDataSize("0B")
    public DataSize getMetadataCacheSize()
    {
        return metadataCacheSize;
    }

    @Config("parquet.metadata-cache-size")
    @ConfigDescription("Size of the parquet metadata cache")
    public ParquetCacheConfig setMetadataCacheSize(DataSize metadataCacheSize)
    {
        this.metadataCacheSize = metadataCacheSize;
        return this;
    }

    @MinDuration("0s")
    public Duration getMetadataCacheTtlSinceLastAccess()
    {
        return metadataCacheTtlSinceLastAccess;
    }

    @Config("parquet.metadata-cache-ttl-since-last-access")
    @ConfigDescription("Time-to-live for parquet metadata cache entry after last access")
    public ParquetCacheConfig setMetadataCacheTtlSinceLastAccess(Duration metadataCacheTtlSinceLastAccess)
    {
        this.metadataCacheTtlSinceLastAccess = metadataCacheTtlSinceLastAccess;
        return this;
    }
}
