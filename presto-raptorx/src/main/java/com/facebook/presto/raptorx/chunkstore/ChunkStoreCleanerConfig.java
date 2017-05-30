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
package com.facebook.presto.raptorx.chunkstore;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import static java.util.concurrent.TimeUnit.MINUTES;

public class ChunkStoreCleanerConfig
{
    private Duration interval = new Duration(5, MINUTES);
    private int threads = 50;

    @NotNull
    @MinDuration("1s")
    public Duration getInterval()
    {
        return interval;
    }

    @Config("chunk-store.cleaner.interval")
    @ConfigDescription("How often to cleanup deleted chunks from chunk store")
    public ChunkStoreCleanerConfig setInterval(Duration interval)
    {
        this.interval = interval;
        return this;
    }

    @Min(1)
    public int getThreads()
    {
        return threads;
    }

    @Config("chunk-store.cleaner.max-threads")
    @ConfigDescription("Maximum number of threads to use for deleting chunks from chunk store")
    public ChunkStoreCleanerConfig setThreads(int threads)
    {
        this.threads = threads;
        return this;
    }
}
