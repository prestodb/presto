package com.facebook.presto.hive;

import com.facebook.presto.hive.shaded.org.apache.thrift.transport.TTransportException;
import com.google.common.base.Objects;
import com.google.common.base.Throwables;

import static com.google.common.base.Preconditions.checkNotNull;

public class TestingHiveCluster
        implements HiveCluster
{
    private final HiveClientConfig config;
    private final String host;
    private final int port;

    public TestingHiveCluster(HiveClientConfig config, String host, int port)
    {
        this.config = checkNotNull(config, "config is null");
        this.host = checkNotNull(host, "host is null");
        this.port = port;
    }

    @Override
    public HiveMetastoreClient createMetastoreClient()
    {
        try {
            return new HiveMetastoreClientFactory(config).create(host, port);
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
