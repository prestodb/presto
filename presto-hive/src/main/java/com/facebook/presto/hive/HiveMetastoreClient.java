package com.facebook.presto.hive;

import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import com.facebook.presto.hive.shaded.org.apache.thrift.protocol.TBinaryProtocol;
import com.facebook.presto.hive.shaded.org.apache.thrift.transport.TSocket;
import com.facebook.presto.hive.shaded.org.apache.thrift.transport.TTransport;
import com.facebook.presto.hive.shaded.org.apache.thrift.transport.TTransportException;

import java.io.Closeable;

class HiveMetastoreClient
        extends ThriftHiveMetastore.Client
        implements Closeable
{
    private final TTransport transport;

    private HiveMetastoreClient(TTransport transport)
    {
        super(new TBinaryProtocol(transport));
        this.transport = transport;
    }

    @Override
    public void close()
    {
        transport.close();
    }

    public static HiveMetastoreClient create(String host, int port, int timeoutMillis)
            throws TTransportException
    {
        TTransport transport = new TSocket(host, port, timeoutMillis);
        transport.open();
        return new HiveMetastoreClient(transport);
    }

    public static HiveMetastoreClient create(String host, int port)
            throws TTransportException
    {
        return create(host, port, 20 * 1000);
    }
}
