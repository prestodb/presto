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
import io.airlift.units.MaxDataSize;
import io.airlift.units.MinDataSize;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class ThriftConnectorConfig
{
    private DataSize maxResponseSize = new DataSize(16, MEGABYTE);
    private int metadataRefreshThreads = 1;
    private int lookupRequestsConcurrency = 1;

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
}
