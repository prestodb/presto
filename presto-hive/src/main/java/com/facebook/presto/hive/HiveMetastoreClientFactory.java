package com.facebook.presto.hive;

import com.facebook.presto.hive.shaded.org.apache.thrift.transport.TSocket;
import com.facebook.presto.hive.shaded.org.apache.thrift.transport.TTransport;
import com.facebook.presto.hive.shaded.org.apache.thrift.transport.TTransportException;
import com.google.common.net.HostAndPort;
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
                socks.setSoTimeout((int) timeout.toMillis());
            }
            catch (IOException e) {
                throw new TTransportException(e);
            }
            transport = new TSocket(socks);
        }

        return new HiveMetastoreClient(transport);
    }
}
