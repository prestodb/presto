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

import javax.net.ssl.SSLContext;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.Socket;
import java.util.Optional;

import static java.net.Proxy.Type.SOCKS;

public final class Transport
{
    public static TTransport create(
            HostAndPort address,
            Optional<SSLContext> sslContext,
            Optional<HostAndPort> socksProxy,
            int timeoutMillis,
            HiveMetastoreAuthentication authentication,
            Optional<String> tokenString)
            throws TTransportException
    {
        try {
            TTransport rawTransport = createRaw(address, sslContext, socksProxy, timeoutMillis);
            TTransport authenticatedTransport = authentication.authenticate(rawTransport, address.getHost(), tokenString);
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

    private static TTransport createRaw(HostAndPort address, Optional<SSLContext> sslContext, Optional<HostAndPort> socksProxy, int timeoutMillis)
            throws TTransportException
    {
        Proxy proxy = socksProxy
                .map(socksAddress -> new Proxy(SOCKS, InetSocketAddress.createUnresolved(socksAddress.getHost(), socksAddress.getPort())))
                .orElse(Proxy.NO_PROXY);

        Socket socket = new Socket(proxy);
        try {
            socket.connect(new InetSocketAddress(address.getHost(), address.getPort()), timeoutMillis);
            socket.setSoTimeout(timeoutMillis);

            if (sslContext.isPresent()) {
                // SSL will connect to the SOCKS address when present
                HostAndPort sslConnectAddress = socksProxy.orElse(address);

                socket = sslContext.get().getSocketFactory().createSocket(socket, sslConnectAddress.getHost(), sslConnectAddress.getPort(), true);
            }
            return new TSocket(socket);
        }
        catch (Throwable t) {
            // something went wrong, close the socket and rethrow
            try {
                socket.close();
            }
            catch (IOException e) {
                t.addSuppressed(e);
            }
            throw new TTransportException(t);
        }
    }

    private static TTransportException rewriteException(TTransportException e, HostAndPort address)
    {
        return new TTransportException(e.getType(), String.format("%s: %s", address, e.getMessage()), e);
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
