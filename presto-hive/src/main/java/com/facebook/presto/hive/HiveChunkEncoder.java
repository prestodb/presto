package com.facebook.presto.hive;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import io.airlift.json.JsonCodec;

import javax.annotation.concurrent.ThreadSafe;

import static com.google.common.base.Charsets.UTF_8;

@ThreadSafe
class HiveChunkEncoder
{
    private final JsonCodec<HivePartitionChunk> partitionChunkCodec;

    @Inject
    HiveChunkEncoder(JsonCodec<HivePartitionChunk> partitionChunkCodec)
    {
        this.partitionChunkCodec = Preconditions.checkNotNull(partitionChunkCodec, "partitionChunkCodec is null");
    }

    byte[] serialize(HivePartitionChunk chunk)
    {
        return partitionChunkCodec.toJson(chunk).getBytes(UTF_8);
    }

    HivePartitionChunk deserialize(byte[] bytes)
    {
        return partitionChunkCodec.fromJson(new String(bytes, UTF_8));
    }
}
