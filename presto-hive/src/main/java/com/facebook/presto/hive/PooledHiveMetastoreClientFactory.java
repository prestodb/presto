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

import com.facebook.presto.hive.authentication.HiveMetastoreAuthentication;
import com.facebook.presto.hive.metastore.HiveMetastoreClient;
import com.facebook.presto.hive.thrift.PooledTTransportFactory;
import com.facebook.presto.hive.thrift.TTransportPool;
import com.google.common.net.HostAndPort;
import com.google.common.primitives.Ints;
import io.airlift.units.Duration;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import javax.annotation.Nullable;
import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class PooledHiveMetastoreClientFactory
    extends HiveMetastoreClientFactory
{
    private final HostAndPort socksProxy;
    private final int timeoutMillis;
    private final HiveMetastoreAuthentication metastoreAuthentication;
    private final TTransportPool transportPool;

    public PooledHiveMetastoreClientFactory(@Nullable HostAndPort socksProxy, Duration timeout, HiveMetastoreAuthentication metastoreAuthentication)
    {
        super(socksProxy, timeout, metastoreAuthentication);
        this.socksProxy = socksProxy;
        this.timeoutMillis = Ints.checkedCast(timeout.toMillis());
        this.metastoreAuthentication = requireNonNull(metastoreAuthentication, "metastoreAuthentication is null");
        this.transportPool = new TTransportPool();
    }

    @Inject
    public PooledHiveMetastoreClientFactory(HiveClientConfig config, HiveMetastoreAuthentication metastoreAuthentication)
    {
        this(config.getMetastoreSocksProxy(), config.getMetastoreTimeout(), metastoreAuthentication);
    }

    @Override
    public HiveMetastoreClient create(String host, int port)
            throws TTransportException
    {
        try {
            TTransport transport = transportPool.borrowObject(host, port);
            if (transport == null) {
                transport = transportPool.borrowObject(host, port, new PooledTTransportFactory(transportPool, host, port, socksProxy, timeoutMillis, metastoreAuthentication));
            }
            return new ThriftHiveMetastoreClient(transport);
        }
        catch (Exception e) {
            throw new TTransportException(String.format("%s: %s", host, e.getMessage()), e.getCause());
        }
    }
}
