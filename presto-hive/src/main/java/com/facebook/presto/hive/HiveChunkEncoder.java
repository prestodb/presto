package com.facebook.presto.hive;

import com.facebook.presto.spi.PartitionChunk;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import io.airlift.json.JsonCodec;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkArgument;

class HiveChunkEncoder
{
    private final JsonCodec<HivePartitionChunk> partitionChunkCodec;

    @Inject
    HiveChunkEncoder(JsonCodec<HivePartitionChunk> partitionChunkCodec)
    {
        this.partitionChunkCodec = Preconditions.checkNotNull(partitionChunkCodec, "partitionChunkCodec is null");
    }

    byte[] serialize(PartitionChunk partitionChunk)
    {
        checkArgument(partitionChunk instanceof HivePartitionChunk,
                "expected instance of %s: %s", HivePartitionChunk.class, partitionChunk.getClass());
        return partitionChunkCodec.toJson((HivePartitionChunk) partitionChunk).getBytes(UTF_8);
    }

    PartitionChunk deserialize(byte[] bytes)
    {
        return partitionChunkCodec.fromJson(new String(bytes, UTF_8));
    }
}
