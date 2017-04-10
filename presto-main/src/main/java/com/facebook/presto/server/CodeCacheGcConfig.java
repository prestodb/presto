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
package com.facebook.presto.server;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import static java.util.concurrent.TimeUnit.SECONDS;

public class CodeCacheGcConfig
{
    private Duration codeCacheCheckInterval = new Duration(20, SECONDS);
    private int codeCacheCollectionThreshold = 40;

    @NotNull
    @MinDuration("1s")
    public Duration getCodeCacheCheckInterval()
    {
        return codeCacheCheckInterval;
    }

    @Config("code-cache-check-interval")
    @ConfigDescription("How often to check if the code cache need to be collected")
    public CodeCacheGcConfig setCodeCacheCheckInterval(Duration codeCacheCheckInterval)
    {
        this.codeCacheCheckInterval = codeCacheCheckInterval;
        return this;
    }

    @Min(1)
    @Max(100)
    public int getCodeCacheCollectionThreshold()
    {
        return codeCacheCollectionThreshold;
    }

    @Config("code-cache-collection-threshold")
    @ConfigDescription("Maximum code cache usage (percentage) before attempting to force a collection")
    public CodeCacheGcConfig setCodeCacheCollectionThreshold(int codeCacheCollectionThreshold)
    {
        this.codeCacheCollectionThreshold = codeCacheCollectionThreshold;
        return this;
    }
}
