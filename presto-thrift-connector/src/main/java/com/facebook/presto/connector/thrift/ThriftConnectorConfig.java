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
package com.facebook.presto.connector.thrift;

import io.airlift.configuration.Config;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.airlift.units.MaxDataSize;
import io.airlift.units.MinDataSize;
import io.airlift.units.MinDuration;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.util.concurrent.TimeUnit;

import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class ThriftConnectorConfig
{
    private DataSize maxResponseSize = new DataSize(16, MEGABYTE);
    private int metadataRefreshThreads = 1;
    private int retryDriverThreads = 8;
    private int lookupRequestsConcurrency = 1;
    private int maxRetryAttempts = 5;
    private double retryScaleFactor = 1.5;
    private Duration minRetrySleepTime = new Duration(10, TimeUnit.MILLISECONDS);
    private Duration maxRetrySleepTime = new Duration(100, TimeUnit.MILLISECONDS);
    private Duration maxRetryDuration = new Duration(30, TimeUnit.SECONDS);

    @NotNull
    @MinDataSize("1MB")
    @MaxDataSize("32MB")
    public DataSize getMaxResponseSize()
    {
        return maxResponseSize;
    }

    @Config("presto-thrift.max-response-size")
    public ThriftConnectorConfig setMaxResponseSize(DataSize maxResponseSize)
    {
        this.maxResponseSize = maxResponseSize;
        return this;
    }

    @Min(1)
    public int getMetadataRefreshThreads()
    {
        return metadataRefreshThreads;
    }

    @Config("presto-thrift.metadata-refresh-threads")
    public ThriftConnectorConfig setMetadataRefreshThreads(int metadataRefreshThreads)
    {
        this.metadataRefreshThreads = metadataRefreshThreads;
        return this;
    }

    @Min(1)
    public int getRetryDriverThreads()
    {
        return retryDriverThreads;
    }

    @Config("presto-thrift.retry-driver-threads")
    public ThriftConnectorConfig setRetryDriverThreads(int retryDriverThreads)
    {
        this.retryDriverThreads = retryDriverThreads;
        return this;
    }

    @Min(1)
    public int getLookupRequestsConcurrency()
    {
        return lookupRequestsConcurrency;
    }

    @Config("presto-thrift.lookup-requests-concurrency")
    public ThriftConnectorConfig setLookupRequestsConcurrency(int lookupRequestsConcurrency)
    {
        this.lookupRequestsConcurrency = lookupRequestsConcurrency;
        return this;
    }

    @Min(0)
    @Max(100)
    public int getMaxRetryAttempts()
    {
        return maxRetryAttempts;
    }

    @Config("presto-thrift.max-retry-attempts")
    public ThriftConnectorConfig setMaxRetryAttempts(int maxRetryAttempts)
    {
        this.maxRetryAttempts = maxRetryAttempts;
        return this;
    }

    @NotNull
    @MinDuration("1ms")
    public Duration getMinRetrySleepTime()
    {
        return minRetrySleepTime;
    }

    @Config("presto-thrift.min-retry-sleep-time")
    public ThriftConnectorConfig setMinRetrySleepTime(Duration minRetrySleepTime)
    {
        this.minRetrySleepTime = minRetrySleepTime;
        return this;
    }

    @NotNull
    @MinDuration("1ms")
    public Duration getMaxRetrySleepTime()
    {
        return maxRetrySleepTime;
    }

    @Config("presto-thrift.max-retry-sleep-time")
    public ThriftConnectorConfig setMaxRetrySleepTime(Duration maxRetrySleepTime)
    {
        this.maxRetrySleepTime = maxRetrySleepTime;
        return this;
    }

    @NotNull
    @MinDuration("1s")
    public Duration getMaxRetryDuration()
    {
        return maxRetryDuration;
    }

    @Config("presto-thrift.max-retry-duration")
    public ThriftConnectorConfig setMaxRetryDuration(Duration maxRetryDuration)
    {
        this.maxRetryDuration = maxRetryDuration;
        return this;
    }

    @Min(1)
    @Max(10)
    public double getRetryScaleFactor()
    {
        return retryScaleFactor;
    }

    @Config("presto-thrift.retry-scale-factor")
    public ThriftConnectorConfig setRetryScaleFactor(double scaleFactor)
    {
        this.retryScaleFactor = scaleFactor;
        return this;
    }
}
