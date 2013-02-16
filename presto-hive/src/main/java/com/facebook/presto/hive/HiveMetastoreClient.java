package com.facebook.presto.hive;

import com.facebook.presto.hive.shaded.org.apache.thrift.protocol.TBinaryProtocol;
import com.facebook.presto.hive.shaded.org.apache.thrift.transport.TSocket;
import com.facebook.presto.hive.shaded.org.apache.thrift.transport.TTransport;
import com.facebook.presto.hive.shaded.org.apache.thrift.transport.TTransportException;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;

import java.io.Closeable;

class HiveMetastoreClient
        extends ThriftHiveMetastore.Client
        implements Closeable
{
    private final TTransport transport;

    HiveMetastoreClient(TTransport transport)
    {
        super(new TBinaryProtocol(transport));
        this.transport = transport;
    }

    @Override
    public void close()
    {
        transport.close();
    }

    public static HiveMetastoreClient create(String host, int port)
            throws TTransportException
    {
        TSocket tSocket = new TSocket(host, port);
        tSocket.open();
        return new HiveMetastoreClient(tSocket);
    }
}
