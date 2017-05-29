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
package com.facebook.presto.hive;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.net.URI;
import java.util.List;

import static com.google.common.collect.Iterables.transform;

public class StaticMetastoreConfig
{
    private static final Splitter SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

    private List<URI> metastoreUris;
    private boolean enableTransportPool;
    private int maxTransport = 128;
    private long transportIdleTimeout = 300_000L;
    private long transportEvictInterval = 10_000L;
    private int transportEvictNumTests = 3;

    @NotNull
    public List<URI> getMetastoreUris()
    {
        return metastoreUris;
    }

    @Config("hive.metastore.uri")
    @ConfigDescription("Hive metastore URIs (comma separated)")
    public StaticMetastoreConfig setMetastoreUris(String uris)
    {
        if (uris == null) {
            this.metastoreUris = null;
            return this;
        }

        this.metastoreUris = ImmutableList.copyOf(transform(SPLITTER.split(uris), URI::create));
        return this;
    }

    public boolean isEnableTransportPool()
    {
        return enableTransportPool;
    }

    @Config("hive.metastore.enable-transport-pool")
    public StaticMetastoreConfig setEnableTransportPool(boolean enableTransportPool)
    {
        this.enableTransportPool = enableTransportPool;
        return this;
    }

    @Min(1)
    public int getMaxTransport()
    {
        return maxTransport;
    }

    @Config("hive.metastore.max-transport-num-per-endpoint")
    public StaticMetastoreConfig setMaxTransport(int maxTransport)
    {
        this.maxTransport = maxTransport;
        return this;
    }

    public long getTransportIdleTimeout()
    {
        return transportIdleTimeout;
    }

    @Config("hive.metastore.transport-idle-timeout")
    public StaticMetastoreConfig setTransportIdleTimeout(long transportIdleTimeout)
    {
        this.transportIdleTimeout = transportIdleTimeout;
        return this;
    }

    public long getTransportEvictInterval()
    {
        return transportEvictInterval;
    }

    @Config("hive.metastore.transport-eviction-interval")
    public StaticMetastoreConfig setTransportEvictInterval(long transportEvictInterval)
    {
        this.transportEvictInterval = transportEvictInterval;
        return this;
    }

    @Min(0)
    public int getTransportEvictNumTests()
    {
        return transportEvictNumTests;
    }

    @Config("hive.metastore.transport-eviction-num-tests")
    public StaticMetastoreConfig setTransportEvictNumTests(int transportEvictNumTests)
    {
        this.transportEvictNumTests = transportEvictNumTests;
        return this;
    }
}
