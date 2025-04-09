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
package com.facebook.presto.hive.metastore.hms.thrift;

import com.facebook.airlift.units.Duration;
import com.facebook.presto.hive.HiveCommonClientConfig;
import com.facebook.presto.hive.MetastoreClientConfig;
import com.facebook.presto.hive.authentication.HiveMetastoreAuthentication;
import com.facebook.presto.hive.metastore.hms.HiveMetastoreClient;
import com.facebook.presto.hive.metastore.hms.MetastoreClientFactory;
import com.facebook.presto.hive.metastore.hms.ThriftHiveMetastoreClient;
import com.google.common.net.HostAndPort;
import com.google.inject.Inject;
import org.apache.thrift.transport.TTransportException;

import javax.net.ssl.SSLContext;

import java.net.URI;
import java.util.Optional;

import static com.facebook.presto.hive.metastore.util.HiveMetastoreUtils.buildSslContext;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class ThriftHiveMetastoreClientFactory
        implements MetastoreClientFactory
{
    private final Optional<SSLContext> sslContext;
    private final Optional<HostAndPort> socksProxy;
    private final int timeoutMillis;
    private final HiveMetastoreAuthentication metastoreAuthentication;
    private final String catalogName;

    public ThriftHiveMetastoreClientFactory(
            Optional<SSLContext> sslContext,
            Optional<HostAndPort> socksProxy,
            Duration timeout,
            HiveMetastoreAuthentication metastoreAuthentication,
            String catalogName)
    {
        this.sslContext = requireNonNull(sslContext, "sslContext is null");
        this.socksProxy = requireNonNull(socksProxy, "socksProxy is null");
        this.timeoutMillis = toIntExact(timeout.toMillis());
        this.metastoreAuthentication = requireNonNull(metastoreAuthentication, "metastoreAuthentication is null");
        this.catalogName = catalogName;
    }

    @Inject
    public ThriftHiveMetastoreClientFactory(MetastoreClientConfig metastoreClientConfig, ThriftHiveMetastoreConfig thriftHiveMetastoreConfig, HiveMetastoreAuthentication metastoreAuthentication, HiveCommonClientConfig hiveCommonClientConfig)
    {
        this(buildSslContext(thriftHiveMetastoreConfig.isTlsEnabled(),
                Optional.ofNullable(thriftHiveMetastoreConfig.getKeystorePath()),
                Optional.ofNullable(thriftHiveMetastoreConfig.getKeystorePassword()),
                Optional.ofNullable(thriftHiveMetastoreConfig.getTruststorePath()),
                Optional.ofNullable(thriftHiveMetastoreConfig.getTrustStorePassword())),
                Optional.ofNullable(metastoreClientConfig.getMetastoreSocksProxy()),
                metastoreClientConfig.getMetastoreTimeout(), metastoreAuthentication, hiveCommonClientConfig.getCatalogName());
    }

    public HiveMetastoreClient create(URI uri, Optional<String> token)
            throws TTransportException
    {
        return new ThriftHiveMetastoreClient(Transport.create(HostAndPort.fromParts(uri.getHost(), uri.getPort()), sslContext, socksProxy, timeoutMillis, metastoreAuthentication, token), catalogName);
    }
}
