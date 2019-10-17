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
package com.facebook.presto.orc.cache;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.airlift.units.MinDataSize;
import io.airlift.units.MinDuration;

import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.concurrent.TimeUnit.SECONDS;

public class OrcCacheConfig
{
    private boolean fileTailCacheEnabled;
    private DataSize fileTailCacheSize = new DataSize(0, BYTE);
    private Duration fileTailCacheTtlSinceLastAccess = new Duration(0, SECONDS);

    public boolean isFileTailCacheEnabled()
    {
        return fileTailCacheEnabled;
    }

    @Config("orc.file-tail-cache-enabled")
    @ConfigDescription("Enable cache for orc file tail")
    public OrcCacheConfig setFileTailCacheEnabled(boolean fileTailCacheEnabled)
    {
        this.fileTailCacheEnabled = fileTailCacheEnabled;
        return this;
    }

    @MinDataSize("0B")
    public DataSize getFileTailCacheSize()
    {
        return fileTailCacheSize;
    }

    @Config("orc.file-tail-cache-size")
    @ConfigDescription("Size of the orc file tail cache")
    public OrcCacheConfig setFileTailCacheSize(DataSize fileTailCacheSize)
    {
        this.fileTailCacheSize = fileTailCacheSize;
        return this;
    }

    @MinDuration("0s")
    public Duration getFileTailCacheTtlSinceLastAccess()
    {
        return fileTailCacheTtlSinceLastAccess;
    }

    @Config("orc.file-tail-cache-ttl-since-last-access")
    @ConfigDescription("Time-to-live for file tail cache entry after last access")
    public OrcCacheConfig setFileTailCacheTtlSinceLastAccess(Duration fileTailCacheTtlSinceLastAccess)
    {
        this.fileTailCacheTtlSinceLastAccess = fileTailCacheTtlSinceLastAccess;
        return this;
    }
}
