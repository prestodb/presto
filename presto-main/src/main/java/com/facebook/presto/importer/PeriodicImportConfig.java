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
package com.facebook.presto.importer;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.util.concurrent.TimeUnit;

public class PeriodicImportConfig
{
    private boolean enabled = false;
    private int threadCount = 2;
    private Duration checkInterval = new Duration(10, TimeUnit.SECONDS);

    public boolean isEnabled()
    {
        return enabled;
    }

    @Config("periodic-import.enabled")
    @ConfigDescription("Run the periodic importer")
    public PeriodicImportConfig setEnabled(boolean enabled)
    {
        this.enabled = enabled;
        return this;
    }

    @Min(1)
    public int getThreadCount()
    {
        return threadCount;
    }

    @Config("periodic-import.thread-count")
    @ConfigDescription("Number of execution threads for the periodic importer")
    public PeriodicImportConfig setThreadCount(int threadCount)
    {
        this.threadCount = threadCount;
        return this;
    }

    @NotNull
    public Duration getCheckInterval()
    {
        return checkInterval;
    }

    @Config("periodic-import.check-interval")
    @ConfigDescription("Check interval for the periodic importer")
    public PeriodicImportConfig setCheckInterval(Duration checkInterval)
    {
        this.checkInterval = checkInterval;
        return this;
    }
}
