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
package com.facebook.presto.atop;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.time.ZoneId;

import static java.util.concurrent.TimeUnit.MINUTES;

public class AtopConnectorConfig
{
    public static final String SECURITY_NONE = "none";
    public static final String SECURITY_FILE = "file";

    private String executablePath = "atop";
    private String timeZone = ZoneId.systemDefault().getId();
    private String security = SECURITY_NONE;
    private Duration readTimeout = new Duration(5, MINUTES);
    private int concurrentReadersPerNode = 1;
    private int maxHistoryDays = 30;

    @NotNull
    public String getSecurity()
    {
        return security;
    }

    @Config("atop.security")
    public AtopConnectorConfig setSecurity(String security)
    {
        this.security = security;
        return this;
    }

    @NotNull
    public String getExecutablePath()
    {
        return executablePath;
    }

    @Config("atop.executable-path")
    public AtopConnectorConfig setExecutablePath(String path)
    {
        this.executablePath = path;
        return this;
    }

    public ZoneId getTimeZoneId()
    {
        return ZoneId.of(timeZone);
    }

    @NotNull
    public String getTimeZone()
    {
        return timeZone;
    }

    @Config("atop.time-zone")
    @ConfigDescription("The timezone in which the atop data was collected. Generally the timezone of the host.")
    public AtopConnectorConfig setTimeZone(String id)
    {
        this.timeZone = id;
        return this;
    }

    @MinDuration("1ms")
    public Duration getReadTimeout()
    {
        return readTimeout;
    }

    @Config("atop.executable-read-timeout")
    @ConfigDescription("The timeout when reading from the atop process.")
    public AtopConnectorConfig setReadTimeout(Duration timeout)
    {
        this.readTimeout = timeout;
        return this;
    }

    @Min(1)
    public int getConcurrentReadersPerNode()
    {
        return concurrentReadersPerNode;
    }

    @Config("atop.concurrent-readers-per-node")
    public AtopConnectorConfig setConcurrentReadersPerNode(int readers)
    {
        this.concurrentReadersPerNode = readers;
        return this;
    }

    @Min(1)
    public int getMaxHistoryDays()
    {
        return maxHistoryDays;
    }

    @Config("atop.max-history-days")
    public AtopConnectorConfig setMaxHistoryDays(int maxHistoryDays)
    {
        this.maxHistoryDays = maxHistoryDays;
        return this;
    }
}
