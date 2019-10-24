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

import com.facebook.airlift.configuration.Config;
import com.google.common.net.HostAndPort;
import io.airlift.units.Duration;

import javax.validation.constraints.NotNull;

import java.util.concurrent.TimeUnit;

public class MetastoreClientConfig
{
    private HostAndPort metastoreSocksProxy;
    private Duration metastoreTimeout = new Duration(10, TimeUnit.SECONDS);
    private boolean verifyChecksum = true;

    public HostAndPort getMetastoreSocksProxy()
    {
        return metastoreSocksProxy;
    }

    @Config("hive.metastore.thrift.client.socks-proxy")
    public MetastoreClientConfig setMetastoreSocksProxy(HostAndPort metastoreSocksProxy)
    {
        this.metastoreSocksProxy = metastoreSocksProxy;
        return this;
    }

    @NotNull
    public Duration getMetastoreTimeout()
    {
        return metastoreTimeout;
    }

    @Config("hive.metastore-timeout")
    public MetastoreClientConfig setMetastoreTimeout(Duration metastoreTimeout)
    {
        this.metastoreTimeout = metastoreTimeout;
        return this;
    }

    public boolean isVerifyChecksum()
    {
        return verifyChecksum;
    }

    @Config("hive.dfs.verify-checksum")
    public MetastoreClientConfig setVerifyChecksum(boolean verifyChecksum)
    {
        this.verifyChecksum = verifyChecksum;
        return this;
    }
}
