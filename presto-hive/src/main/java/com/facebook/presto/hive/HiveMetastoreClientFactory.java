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
            transport = new TTransportWrapper(new TSocket(host, port, (int) timeout.toMillis()), host);
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
                throw rewriteException(new TTransportException(e), host);
            }
            try {
                transport = new TTransportWrapper(new TSocket(socks), host);
            }
            catch (TTransportException e) {
                throw rewriteException(e, host);
            }
        }

        return new HiveMetastoreClient(transport);
    }

    private static TTransportException rewriteException(TTransportException e, String host)
    {
        return new TTransportException(e.getType(), String.format("%s: %s", host, e.getMessage()), e.getCause());
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
