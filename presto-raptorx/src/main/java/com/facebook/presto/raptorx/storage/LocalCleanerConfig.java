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

import static java.util.concurrent.TimeUnit.HOURS;

public class LocalCleanerConfig
{
    private Duration localCleanerInterval = new Duration(1, HOURS);
    private Duration localCleanTime = new Duration(4, HOURS);
    private int threads = 2;

    @NotNull
    @MinDuration("1m")
    public Duration getLocalCleanerInterval()
    {
        return localCleanerInterval;
    }

    @Config("raptor.local-cleaner-interval")
    @ConfigDescription("How often to discover local chunks that need to be cleaned up")
    public LocalCleanerConfig setLocalCleanerInterval(Duration localCleanerInterval)
    {
        this.localCleanerInterval = localCleanerInterval;
        return this;
    }

    @NotNull
    public Duration getLocalCleanTime()
    {
        return localCleanTime;
    }

    @Config("raptor.local-clean-time")
    @ConfigDescription("How long to wait after discovery before cleaning local chunks")
    public LocalCleanerConfig setLocalCleanTime(Duration localCleanTime)
    {
        this.localCleanTime = localCleanTime;
        return this;
    }

    @Min(1)
    public int getThreads()
    {
        return threads;
    }

    @Config("raptor.local-clean-threads")
    @ConfigDescription("Maximum number of threads to use for deleting chunk files locally")
    public LocalCleanerConfig setThreads(int threads)
    {
        this.threads = threads;
        return this;
    }
}
