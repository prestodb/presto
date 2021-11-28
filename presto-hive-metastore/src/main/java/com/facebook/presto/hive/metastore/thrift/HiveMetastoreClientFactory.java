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
package com.facebook.presto.hive.metastore.thrift;

import com.facebook.presto.hive.MetastoreClientConfig;
import com.facebook.presto.hive.authentication.HiveMetastoreAuthentication;
import com.google.common.net.HostAndPort;
import io.airlift.units.Duration;
import org.apache.thrift.transport.TTransportException;

import javax.inject.Inject;
import javax.net.ssl.SSLContext;

import java.util.Optional;

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class HiveMetastoreClientFactory
{
    private final Optional<SSLContext> sslContext;
    private final Optional<HostAndPort> socksProxy;
    private final int timeoutMillis;
    private final HiveMetastoreAuthentication metastoreAuthentication;

    public HiveMetastoreClientFactory(
            Optional<SSLContext> sslContext,
            Optional<HostAndPort> socksProxy,
            Duration timeout,
            HiveMetastoreAuthentication metastoreAuthentication)
    {
        this.sslContext = requireNonNull(sslContext, "sslContext is null");
        this.socksProxy = requireNonNull(socksProxy, "socksProxy is null");
        this.timeoutMillis = toIntExact(timeout.toMillis());
        this.metastoreAuthentication = requireNonNull(metastoreAuthentication, "metastoreAuthentication is null");
    }

    @Inject
    public HiveMetastoreClientFactory(MetastoreClientConfig metastoreClientConfig, HiveMetastoreAuthentication metastoreAuthentication)
    {
        this(Optional.empty(), Optional.ofNullable(metastoreClientConfig.getMetastoreSocksProxy()), metastoreClientConfig.getMetastoreTimeout(), metastoreAuthentication);
    }

    public HiveMetastoreClient create(HostAndPort address, Optional<String> token)
            throws TTransportException
    {
        return new ThriftHiveMetastoreClient(Transport.create(address, sslContext, socksProxy, timeoutMillis, metastoreAuthentication, token));
    }
}
