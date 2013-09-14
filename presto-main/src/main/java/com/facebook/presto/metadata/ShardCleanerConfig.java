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
package com.facebook.presto.metadata;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.util.concurrent.TimeUnit;

public class ShardCleanerConfig
{
    private boolean enabled = false;
    private Duration storageCleanerInterval = new Duration(60, TimeUnit.SECONDS);
    private int maxShardDropThreads = 32;

    public boolean isEnabled()
    {
        return enabled;
    }

    @Config("shard-cleaner.enabled")
    @ConfigDescription("Run the periodic importer")
    public ShardCleanerConfig setEnabled(boolean enabled)
    {
        this.enabled = enabled;
        return this;
    }

    @MinDuration("60s")
    @NotNull
    public Duration getCleanerInterval()
    {
        return storageCleanerInterval;
    }

    @Config("shard-cleaner.interval")
    public ShardCleanerConfig setCleanerInterval(Duration storageCleanerInterval)
    {
        this.storageCleanerInterval = storageCleanerInterval;
        return this;
    }

    @Min(1)
    public int getMaxThreads()
    {
        return maxShardDropThreads;
    }

    @Config("shard-cleaner.max-threads")
    public ShardCleanerConfig setMaxThreads(int maxShardDropThreads)
    {
        this.maxShardDropThreads = maxShardDropThreads;
        return this;
    }
}