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

import com.facebook.presto.hive.authentication.HiveMetastoreAuthentication;
import com.google.common.net.HostAndPort;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.Optional;

public final class Transport
{
    public static TTransport create(HostAndPort address, Optional<HostAndPort> socksProxy, int timeoutMillis, HiveMetastoreAuthentication authentication)
            throws TTransportException
    {
        try {
            TTransport rawTransport = createRaw(address, socksProxy, timeoutMillis);
            TTransport authenticatedTransport = authentication.authenticate(rawTransport, address.getHost());
            if (!authenticatedTransport.isOpen()) {
                authenticatedTransport.open();
            }
            return new TTransportWrapper(authenticatedTransport, address);
        }
        catch (TTransportException e) {
            throw rewriteException(e, address);
        }
    }

    private Transport() {}

    private static TTransport createRaw(HostAndPort address, Optional<HostAndPort> socksProxy, int timeoutMillis)
            throws TTransportException
    {
        if (!socksProxy.isPresent()) {
            return new TSocket(address.getHost(), address.getPort(), timeoutMillis);
        }

        Socket socks = createSocksSocket(socksProxy.get());
        try {
            try {
                socks.connect(InetSocketAddress.createUnresolved(address.getHost(), address.getPort()), timeoutMillis);
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

    private static void closeQuietly(Closeable closeable)
    {
        try {
            closeable.close();
        }
        catch (IOException e) {
            // ignored
        }
    }

    private static Socket createSocksSocket(HostAndPort proxy)
    {
        SocketAddress address = InetSocketAddress.createUnresolved(proxy.getHost(), proxy.getPort());
        return new Socket(new Proxy(Proxy.Type.SOCKS, address));
    }

    private static TTransportException rewriteException(TTransportException e, HostAndPort address)
    {
        return new TTransportException(e.getType(), String.format("%s: %s", address, e.getMessage()), e.getCause());
    }

    private static class TTransportWrapper
            extends TTransport
    {
        private final TTransport transport;
        private final HostAndPort address;

        TTransportWrapper(TTransport transport, HostAndPort address)
        {
            this.transport = transport;
            this.address = address;
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
                throw rewriteException(e, address);
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
                throw rewriteException(e, address);
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
                throw rewriteException(e, address);
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
                throw rewriteException(e, address);
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
                throw rewriteException(e, address);
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
                throw rewriteException(e, address);
            }
        }
    }
}
