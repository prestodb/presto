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

import com.facebook.presto.hive.metastore.HiveMetastoreClient;
import com.facebook.presto.hive.thrift.Transport;
import com.google.common.net.HostAndPort;
import com.google.common.primitives.Ints;
import io.airlift.units.Duration;
import org.apache.thrift.transport.TTransportException;

import javax.annotation.Nullable;
import javax.inject.Inject;

public class HiveMetastoreClientFactory
{
    private final HostAndPort socksProxy;
    private final int timeoutMillis;

    public HiveMetastoreClientFactory(@Nullable HostAndPort socksProxy, Duration timeout)
    {
        this.socksProxy = socksProxy;
        this.timeoutMillis = Ints.checkedCast(timeout.toMillis());
    }

    @Inject
    public HiveMetastoreClientFactory(HiveClientConfig config)
    {
        this(config.getMetastoreSocksProxy(), config.getMetastoreTimeout());
    }

    public HiveMetastoreClient create(String host, int port)
            throws TTransportException
    {
        return new ThriftHiveMetastoreClient(Transport.create(host, port, socksProxy, timeoutMillis));
    }
}
