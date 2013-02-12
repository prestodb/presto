package com.facebook.presto.hive;

import com.facebook.presto.hive.shaded.org.apache.thrift.transport.TSocket;
import com.facebook.presto.hive.shaded.org.apache.thrift.transport.TTransport;
import com.facebook.presto.hive.shaded.org.apache.thrift.transport.TTransportException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.HostAndPort;
import com.google.inject.Inject;
import io.airlift.units.Duration;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.Socket;
import java.util.concurrent.TimeUnit;

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

    @VisibleForTesting
    HiveMetastoreClientFactory()
    {
        this(null, new Duration(5, TimeUnit.SECONDS));
    }

    public HiveMetastoreClient create(String host, int port)
            throws TTransportException
    {
        return new HiveMetastoreClient(socksProxy == null ? createStandard(host, port) : createSocks(host, port));
    }

    private TTransport createStandard(String host, int port)
            throws TTransportException
    {
        TTransport transport = new TSocket(host, port, (int) timeout.toMillis());
        transport.open();
        return transport;
    }

    private TTransport createSocks(String host, int port)
            throws TTransportException
    {
        try {
            assert socksProxy != null; // IDEA-60343
            Socket socks = new Socket(new Proxy(Proxy.Type.SOCKS, InetSocketAddress.createUnresolved(socksProxy.getHostText(), socksProxy.getPort())));
            socks.connect(InetSocketAddress.createUnresolved(host, port), (int) timeout.toMillis());
            return new TSocket(socks);
        }
        catch (IOException e) {
            throw new TTransportException(e);
        }
    }
}
