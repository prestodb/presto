package com.facebook.presto.hive;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.inject.Inject;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.annotation.concurrent.ThreadSafe;

import static com.google.common.base.Preconditions.checkNotNull;

@ThreadSafe
public class HiveClientFactory
{
    private final DataSize maxChunkSize;
    private final int maxOutstandingChunks;
    private final int maxChunkIteratorThreads;
    private final HiveChunkEncoder hiveChunkEncoder;
    private final LoadingCache<HiveCluster, CachingHiveMetastore> metastores;

    @Inject
    public HiveClientFactory(
            HiveClientConfig hiveClientConfig,
            HiveChunkEncoder hiveChunkEncoder)
    {
        checkNotNull(hiveClientConfig, "hiveClientConfig is null");
        checkNotNull(hiveChunkEncoder, "hiveChunkEncoder is null");

        maxChunkSize = hiveClientConfig.getMaxChunkSize();
        maxOutstandingChunks = hiveClientConfig.getMaxOutstandingChunks();
        maxChunkIteratorThreads = hiveClientConfig.getMaxChunkIteratorThreads();
        this.hiveChunkEncoder = hiveChunkEncoder;

        final Duration metastoreCacheTtl = hiveClientConfig.getMetastoreCacheTtl();
        metastores = CacheBuilder.newBuilder()
                .build(new CacheLoader<HiveCluster, CachingHiveMetastore>()
                {
                    @Override
                    public CachingHiveMetastore load(HiveCluster hiveCluster)
                            throws Exception
                    {
                        // Get a different metastore cache for each HiveCluster
                        return new CachingHiveMetastore(hiveCluster, metastoreCacheTtl);
                    }
                });
    }

    public HiveClient get(HiveCluster hiveCluster)
    {
        CachingHiveMetastore metastore = metastores.getUnchecked(hiveCluster);
        return new HiveClient(maxChunkSize.toBytes(), maxOutstandingChunks, maxChunkIteratorThreads, hiveChunkEncoder, metastore);
    }
}
