package com.facebook.presto.hive;

import com.facebook.presto.hive.shaded.org.apache.thrift.transport.TSocket;
import com.facebook.presto.hive.shaded.org.apache.thrift.transport.TTransport;
import com.facebook.presto.hive.shaded.org.apache.thrift.transport.TTransportException;
import com.google.common.base.Throwables;
import io.airlift.units.Duration;

import java.util.concurrent.TimeUnit;

public class SimpleHiveCluster
        implements HiveCluster
{
    private static final Duration TIMEOUT = new Duration(10, TimeUnit.SECONDS);
    private final String host;
    private final int port;

    public SimpleHiveCluster(String host, int port)
    {
        this.host = host;
        this.port = port;
    }

    @Override
    public HiveMetastoreClient createMetastoreClient()
    {
        try {
            TTransport transport = new TSocket(host, port, (int) TIMEOUT.toMillis());
            transport.open();
            return new HiveMetastoreClient(transport);
        }
        catch (TTransportException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SimpleHiveCluster that = (SimpleHiveCluster) o;

        if (port != that.port) {
            return false;
        }
        if (!host.equals(that.host)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = host.hashCode();
        result = 31 * result + port;
        return result;
    }
}
