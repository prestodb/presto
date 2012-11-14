package com.facebook.presto.ingest;

import com.facebook.presto.hive.ImportClient;
import com.facebook.presto.hive.PartitionChunk;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonValue;

import javax.annotation.concurrent.Immutable;

import static com.google.common.base.Preconditions.checkNotNull;

@Immutable
public class SerializedPartitionChunk
{
    private final byte[] bytes;

    @JsonCreator
    public SerializedPartitionChunk(byte[] bytes)
    {
        this.bytes = checkNotNull(bytes, "bytes is null");
    }

    @JsonValue
    public byte[] getBytes()
    {
        return bytes;
    }

    public PartitionChunk deserialize(ImportClient client)
    {
        return client.deserializePartitionChunk(bytes);
    }

    public static SerializedPartitionChunk create(ImportClient client, PartitionChunk chunk)
    {
        return new SerializedPartitionChunk(client.serializePartitionChunk(chunk));
    }
}
