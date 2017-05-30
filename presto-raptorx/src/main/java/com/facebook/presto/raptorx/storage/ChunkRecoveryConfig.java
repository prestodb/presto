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
package com.facebook.presto.raptorx.storage;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import static java.util.concurrent.TimeUnit.MINUTES;

public class ChunkRecoveryConfig
{
    private Duration missingChunkDiscoveryInterval = new Duration(5, MINUTES);
    private int recoveryThreads = 10;

    @NotNull
    @MinDuration("1m")
    public Duration getMissingChunkDiscoveryInterval()
    {
        return missingChunkDiscoveryInterval;
    }

    @Config("storage.missing-chunk-discovery-interval")
    @ConfigDescription("How often to check the database and local file system for missing chunks")
    public ChunkRecoveryConfig setMissingChunkDiscoveryInterval(Duration missingChunkDiscoveryInterval)
    {
        this.missingChunkDiscoveryInterval = missingChunkDiscoveryInterval;
        return this;
    }

    @Min(1)
    public int getRecoveryThreads()
    {
        return recoveryThreads;
    }

    @Config("storage.max-recovery-threads")
    @ConfigDescription("Maximum number of threads to use for recovering chunks")
    public ChunkRecoveryConfig setRecoveryThreads(int recoveryThreads)
    {
        this.recoveryThreads = recoveryThreads;
        return this;
    }
}
