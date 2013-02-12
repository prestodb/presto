package com.facebook.presto.hive;

import com.google.common.net.HostAndPort;
import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.airlift.units.DataSize;

import static com.google.common.base.Preconditions.checkNotNull;

public class HiveClientFactory
{
    private final DataSize maxChunkSize;
    private final int maxOutstandingChunks;
    private final int maxChunkIteratorThreads;
    private final JsonCodec<HivePartitionChunk> partitionChunkCodec;

    @Inject
    public HiveClientFactory(
            HiveClientConfig hiveClientConfig,
            JsonCodec<HivePartitionChunk> partitionChunkCodec)
    {
        checkNotNull(hiveClientConfig, "hiveClientConfig is null");
        checkNotNull(partitionChunkCodec, "partitionChunkCodec is null");

        maxChunkSize = hiveClientConfig.getMaxChunkSize();
        maxOutstandingChunks = hiveClientConfig.getMaxOutstandingChunks();
        maxChunkIteratorThreads = hiveClientConfig.getMaxChunkIteratorThreads();
        this.partitionChunkCodec = partitionChunkCodec;
    }

    public HiveClient get(HostAndPort metastore)
    {
        return new HiveClient(metastore.getHostText(), metastore.getPort(), maxChunkSize.toBytes(), maxOutstandingChunks, maxChunkIteratorThreads, partitionChunkCodec);
    }
}
