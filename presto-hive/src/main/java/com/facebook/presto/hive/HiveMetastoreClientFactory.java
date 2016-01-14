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

import com.facebook.presto.hive.metastore.HiveMetastoreAuthentication;
import com.facebook.presto.hive.metastore.HiveMetastoreClient;
import com.google.common.net.HostAndPort;
import com.google.common.primitives.Ints;
import io.airlift.units.Duration;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.Socket;
import java.net.SocketAddress;

import static java.util.Objects.requireNonNull;

public class HiveMetastoreClientFactory
{
    private final HostAndPort socksProxy;
    private final int timeoutMillis;
    private final HiveMetastoreAuthentication metastoreAuthentication;

    public HiveMetastoreClientFactory(@Nullable HostAndPort socksProxy,
            Duration timeout,
            HiveMetastoreAuthentication metastoreAuthentication)
    {
        this.socksProxy = socksProxy;
        this.timeoutMillis = Ints.checkedCast(timeout.toMillis());
        this.metastoreAuthentication = requireNonNull(metastoreAuthentication, "metastoreAuthentication is null");
    }

    @Inject
    public HiveMetastoreClientFactory(HiveClientConfig config,
            HiveMetastoreAuthentication metastoreAuthentication)
    {
        this(config.getMetastoreSocksProxy(), config.getMetastoreTimeout(), metastoreAuthentication);
    }

    private static Socket createSocksSocket(HostAndPort proxy)
    {
        SocketAddress address = InetSocketAddress.createUnresolved(proxy.getHostText(), proxy.getPort());
        return new Socket(new Proxy(Proxy.Type.SOCKS, address));
    }

    private static TTransportException rewriteException(TTransportException e, String host)
    {
        return new TTransportException(e.getType(), String.format("%s: %s", host, e.getMessage()), e.getCause());
    }

    public HiveMetastoreClient create(String host, int port)
            throws TTransportException
    {
        TTransport transport = createTransport(host, port);
        try {
            transport.open();
            return new ThriftHiveMetastoreClient(transport);
        }
        catch (Throwable t) {
            transport.close();
            throw t;
        }
    }

    protected TTransport createRawTransport(String host, int port)
            throws TTransportException
    {
        if (socksProxy == null) {
            return new TSocket(host, port, timeoutMillis);
        }

        Socket socks = createSocksSocket(socksProxy);
        try {
            try {
                socks.connect(InetSocketAddress.createUnresolved(host, port), timeoutMillis);
                socks.setSoTimeout(timeoutMillis);
                return new TSocket(socks);
            }
            catch (Throwable t) {
                closeQuietly(socks);
                throw t;
            }
        }
        catch (IOException e) {
            throw new TTransportException(e);
        }
    }

    protected TTransport createTransport(String host, int port)
            throws TTransportException
    {
        try {
            TTransport rawTransport = createRawTransport(host, port);
            TTransport authTransport = metastoreAuthentication.createAuthenticatedTransport(rawTransport, host);
            return new TTransportWrapper(authTransport, host);
        }
        catch (TTransportException e) {
            throw rewriteException(e, host);
        }
    }

    private static void closeQuietly(Closeable closeable)
    {
        try {
            closeable.close();
        }
        catch (IOException e) {
            // ignored
        }
    }

    private static class TTransportWrapper
            extends TTransport
    {
        private final TTransport transport;
        private final String host;

        TTransportWrapper(TTransport transport, String host)
        {
            this.transport = transport;
            this.host = host;
        }

        @Override
        public boolean isOpen()
        {
            return transport.isOpen();
        }

        @Override
        public boolean peek()
        {
            return transport.peek();
        }

        @Override
        public byte[] getBuffer()
        {
            return transport.getBuffer();
        }

        @Override
        public int getBufferPosition()
        {
            return transport.getBufferPosition();
        }

        @Override
        public int getBytesRemainingInBuffer()
        {
            return transport.getBytesRemainingInBuffer();
        }

        @Override
        public void consumeBuffer(int len)
        {
            transport.consumeBuffer(len);
        }

        @Override
        public void close()
        {
            transport.close();
        }

        @Override
        public void open()
                throws TTransportException
        {
            try {
                transport.open();
            }
            catch (TTransportException e) {
                throw rewriteException(e, host);
            }
        }

        @Override
        public int readAll(byte[] bytes, int off, int len)
                throws TTransportException
        {
            try {
                return transport.readAll(bytes, off, len);
            }
            catch (TTransportException e) {
                throw rewriteException(e, host);
            }
        }

        @Override
        public int read(byte[] bytes, int off, int len)
                throws TTransportException
        {
            try {
                return transport.read(bytes, off, len);
            }
            catch (TTransportException e) {
                throw rewriteException(e, host);
            }
        }

        @Override
        public void write(byte[] bytes)
                throws TTransportException
        {
            try {
                transport.write(bytes);
            }
            catch (TTransportException e) {
                throw rewriteException(e, host);
            }
        }

        @Override
        public void write(byte[] bytes, int off, int len)
                throws TTransportException
        {
            try {
                transport.write(bytes, off, len);
            }
            catch (TTransportException e) {
                throw rewriteException(e, host);
            }
        }

        @Override
        public void flush()
                throws TTransportException
        {
            try {
                transport.flush();
            }
            catch (TTransportException e) {
                throw rewriteException(e, host);
            }
        }
    }
}
