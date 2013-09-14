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

import com.facebook.presto.hive.shaded.org.apache.thrift.transport.TSocket;
import com.facebook.presto.hive.shaded.org.apache.thrift.transport.TTransport;
import com.facebook.presto.hive.shaded.org.apache.thrift.transport.TTransportException;
import com.google.common.net.HostAndPort;
import com.google.common.primitives.Ints;
import com.google.inject.Inject;
import io.airlift.units.Duration;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.Socket;
import java.net.SocketAddress;

import static com.google.common.base.Preconditions.checkNotNull;

public class HiveMetastoreClientFactory
{
    private final HostAndPort socksProxy;
    private final Duration timeout;

    public HiveMetastoreClientFactory(@Nullable HostAndPort socksProxy, Duration timeout)
    {
        this.socksProxy = socksProxy;
        this.timeout = checkNotNull(timeout, "timeout is null");
    }

    @Inject
    public HiveMetastoreClientFactory(HiveClientConfig config)
    {
        this(config.getMetastoreSocksProxy(), config.getMetastoreTimeout());
    }

    @SuppressWarnings("SocketOpenedButNotSafelyClosed")
    public HiveMetastoreClient create(String host, int port)
            throws TTransportException
    {
        TTransport transport;
        if (socksProxy == null) {
            transport = new TSocket(host, port, (int) timeout.toMillis());
            transport.open();
        }
        else {
            SocketAddress address = InetSocketAddress.createUnresolved(socksProxy.getHostText(), socksProxy.getPort());
            Socket socks = new Socket(new Proxy(Proxy.Type.SOCKS, address));
            try {
                socks.connect(InetSocketAddress.createUnresolved(host, port), (int) timeout.toMillis());
                socks.setSoTimeout(Ints.checkedCast(timeout.toMillis()));
            }
            catch (IOException e) {
                throw new TTransportException(e);
            }
            transport = new TSocket(socks);
        }

        return new HiveMetastoreClient(transport);
    }
}
