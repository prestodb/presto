package com.facebook.presto.server.thrift.codec;

import com.facebook.drift.codec.ThriftCodec;
import com.facebook.drift.codec.internal.coercion.FromThrift;
import com.facebook.drift.codec.internal.coercion.ToThrift;
import com.facebook.drift.codec.metadata.ThriftCatalog;
import com.facebook.drift.codec.metadata.ThriftType;
import com.facebook.drift.protocol.TProtocolReader;
import com.facebook.drift.protocol.TProtocolWriter;
import io.airlift.units.DataSize;

import javax.inject.Inject;

import java.nio.ByteBuffer;

import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.Objects.requireNonNull;

public class DataSizeToBytesThriftCodec
        implements ThriftCodec<DataSize>
{
    @Inject
    public DataSizeToBytesThriftCodec(ThriftCatalog thriftCatalog)
    {
        thriftCatalog.addDefaultCoercions(getClass());
    }

    @Override
    public ThriftType getType()
    {
        return new ThriftType(ThriftType.I64, DataSize.class);
    }

    @Override
    public DataSize read(TProtocolReader protocol)
            throws Exception
    {
        return bytesToDataSize(protocol.readI64());
    }

    @Override
    public void write(DataSize dataSize, TProtocolWriter protocol)
            throws Exception
    {
        requireNonNull(dataSize, "dataSize is null");
        protocol.writeI64(dataSizeToBytes(dataSize));
    }

    @FromThrift
    public static DataSize bytesToDataSize(long bytes)
    {
        return new DataSize(bytes, BYTE);
    }

    @ToThrift
    public static long dataSizeToBytes(DataSize dataSize)
    {
        return dataSize.toBytes();
    }
}
