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
import io.airlift.units.MaxDuration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import static java.util.concurrent.TimeUnit.MINUTES;

public class ChunkStoreConfig
{
    private Duration timeout = new Duration(1, MINUTES);
    private int timeoutThreads = 1000;
    private String provider;
    private int writeThreads = 5;

    @NotNull
    @MinDuration("1s")
    @MaxDuration("1h")
    public Duration getTimeout()
    {
        return timeout;
    }

    @Config("chunk-store.timeout")
    @ConfigDescription("Timeout for per-chunk operations")
    public ChunkStoreConfig setTimeout(Duration timeout)
    {
        this.timeout = timeout;
        return this;
    }

    @Min(1)
    public int getTimeoutThreads()
    {
        return timeoutThreads;
    }

    @Config("chunk-store.max-timeout-threads")
    @ConfigDescription("Maximum number of timeout threads for chunk store operations")
    public ChunkStoreConfig setTimeoutThreads(int timeoutThreads)
    {
        this.timeoutThreads = timeoutThreads;
        return this;
    }

    @NotNull
    public String getProvider()
    {
        return provider;
    }

    @Config("chunk-store.provider")
    @ConfigDescription("Chunk store provider to use (supported types: file)")
    public ChunkStoreConfig setProvider(String provider)
    {
        this.provider = provider;
        return this;
    }

    @Min(1)
    public int getWriteThreads()
    {
        return writeThreads;
    }

    @Config("chunk-store.max-threads")
    @ConfigDescription("Maximum number of chunks to write at once")
    public ChunkStoreConfig setWriteThreads(int writeThreads)
    {
        this.writeThreads = writeThreads;
        return this;
    }
}
