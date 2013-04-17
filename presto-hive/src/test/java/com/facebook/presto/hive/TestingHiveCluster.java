package com.facebook.presto.hive;

import com.facebook.presto.hive.shaded.org.apache.thrift.transport.TSocket;
import com.facebook.presto.hive.shaded.org.apache.thrift.transport.TTransport;
import com.facebook.presto.hive.shaded.org.apache.thrift.transport.TTransportException;
import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import io.airlift.units.Duration;

import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

public class TestingHiveCluster
        implements HiveCluster
{
    private static final Duration TIMEOUT = new Duration(10, TimeUnit.SECONDS);
    private final String host;
    private final int port;

    public TestingHiveCluster(String host, int port)
    {
        this.host = checkNotNull(host, "host is null");
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
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        TestingHiveCluster o = (TestingHiveCluster) obj;

        return Objects.equal(this.host, o.host) &&
                Objects.equal(this.port, o.port);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(host, port);
    }
}
