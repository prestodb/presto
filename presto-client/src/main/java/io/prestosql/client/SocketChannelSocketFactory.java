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
package io.prestosql.client;

import javax.net.SocketFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.nio.channels.SocketChannel;

/**
 * Workaround for JDK IPv6 bug on Mac. Sockets created with the basic socket
 * API often cannot connect to IPv6 destinations due to JDK-8131133. However,
 * NIO sockets do not have this problem, even if used in blocking mode.
 */
public class SocketChannelSocketFactory
        extends SocketFactory
{
    @Override
    public Socket createSocket()
            throws IOException
    {
        return SocketChannel.open().socket();
    }

    @Override
    public Socket createSocket(String host, int port)
            throws IOException
    {
        return SocketChannel.open(new InetSocketAddress(host, port)).socket();
    }

    @Override
    public Socket createSocket(String host, int port, InetAddress localAddress, int localPort)
            throws IOException
    {
        throw new SocketException("not supported");
    }

    @Override
    public Socket createSocket(InetAddress address, int port)
            throws IOException
    {
        return SocketChannel.open(new InetSocketAddress(address, port)).socket();
    }

    @Override
    public Socket createSocket(InetAddress address, int port, InetAddress localAddress, int localPort)
            throws IOException
    {
        throw new SocketException("not supported");
    }
}
