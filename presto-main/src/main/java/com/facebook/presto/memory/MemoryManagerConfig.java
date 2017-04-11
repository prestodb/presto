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
package com.facebook.presto.memory;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.DefunctConfig;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.NotNull;

import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static java.util.concurrent.TimeUnit.MINUTES;

@DefunctConfig("experimental.cluster-memory-manager-enabled")
public class MemoryManagerConfig
{
    private DataSize maxQueryMemory = new DataSize(20, GIGABYTE);
    private boolean killOnOutOfMemory;
    private Duration killOnOutOfMemoryDelay = new Duration(5, MINUTES);

    public boolean isKillOnOutOfMemory()
    {
        return killOnOutOfMemory;
    }

    @Config("query.low-memory-killer.enabled")
    @ConfigDescription("Enable low memory killer")
    public MemoryManagerConfig setKillOnOutOfMemory(boolean killOnOutOfMemory)
    {
        this.killOnOutOfMemory = killOnOutOfMemory;
        return this;
    }

    @NotNull
    @MinDuration("5s")
    public Duration getKillOnOutOfMemoryDelay()
    {
        return killOnOutOfMemoryDelay;
    }

    @Config("query.low-memory-killer.delay")
    @ConfigDescription("Delay between cluster running low on memory and invoking killer")
    public MemoryManagerConfig setKillOnOutOfMemoryDelay(Duration killOnOutOfMemoryDelay)
    {
        this.killOnOutOfMemoryDelay = killOnOutOfMemoryDelay;
        return this;
    }

    @NotNull
    public DataSize getMaxQueryMemory()
    {
        return maxQueryMemory;
    }

    @Config("query.max-memory")
    public MemoryManagerConfig setMaxQueryMemory(DataSize maxQueryMemory)
    {
        this.maxQueryMemory = maxQueryMemory;
        return this;
    }
}
