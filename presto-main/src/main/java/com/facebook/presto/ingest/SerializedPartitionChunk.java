package com.facebook.presto.ingest;

import com.facebook.presto.hive.ImportClient;
import com.facebook.presto.hive.PartitionChunk;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonValue;

import javax.annotation.concurrent.Immutable;

@Immutable
public class SerializedPartitionChunk
{
    private final byte[] bytes;

    @JsonCreator
    public SerializedPartitionChunk(byte[] bytes)
    {
        this.bytes = bytes;
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
