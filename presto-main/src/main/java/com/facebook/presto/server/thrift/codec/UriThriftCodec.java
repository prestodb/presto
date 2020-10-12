package com.facebook.presto.server.thrift.codec;

import com.facebook.drift.codec.ThriftCodec;
import com.facebook.drift.codec.internal.coercion.FromThrift;
import com.facebook.drift.codec.internal.coercion.ToThrift;
import com.facebook.drift.codec.metadata.ThriftCatalog;
import com.facebook.drift.codec.metadata.ThriftType;
import com.facebook.drift.protocol.TProtocolReader;
import com.facebook.drift.protocol.TProtocolWriter;

import javax.inject.Inject;

import java.net.URI;

import static java.util.Objects.requireNonNull;

public class UriThriftCodec
        implements ThriftCodec<URI>
{
    @Inject
    public UriThriftCodec(ThriftCatalog thriftCatalog)
    {
        thriftCatalog.addDefaultCoercions(getClass());
    }

    @Override
    public ThriftType getType()
    {
        return new ThriftType(ThriftType.STRING, URI.class);
    }

    @Override
    public URI read(TProtocolReader protocol)
            throws Exception
    {
        return URI.create(protocol.readString());
    }

    @Override
    public void write(URI uri, TProtocolWriter protocol)
            throws Exception
    {
        requireNonNull(uri, "uri is null");
        protocol.writeString(uri.toString());
    }

    @FromThrift
    public static URI stringToUri(String uri)
    {
        return URI.create(uri);
    }

    @ToThrift
    public static String uriToString(URI uri)
    {
        return uri.toString();
    }
}
