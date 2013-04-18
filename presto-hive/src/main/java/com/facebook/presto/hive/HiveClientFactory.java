package com.facebook.presto.hive;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.annotation.PreDestroy;
import javax.annotation.concurrent.ThreadSafe;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.google.common.base.Preconditions.checkNotNull;

@ThreadSafe
public class HiveClientFactory
{
    private final DataSize maxSplitSize;
    private final int maxOutstandingSplits;
    private final int masSplitIteratorThreads;
    private final int partitionBatchSize;
    private final HdfsEnvironment hdfsEnvironment;
    private final LoadingCache<HiveCluster, CachingHiveMetastore> metastores;
    private final ExecutorService executorService = Executors.newCachedThreadPool(
            new ThreadFactoryBuilder()
                    .setNameFormat("hive-client-%d")
                    .build());

    @Inject
    public HiveClientFactory(HiveClientConfig hiveClientConfig,  HdfsEnvironment hdfsEnvironment)
    {
        checkNotNull(hiveClientConfig, "hiveClientConfig is null");
        checkNotNull(hdfsEnvironment, "hdfsEnvironment is null");

        maxSplitSize = hiveClientConfig.getMaxSplitSize();
        maxOutstandingSplits = hiveClientConfig.getMaxOutstandingSplits();
        masSplitIteratorThreads = hiveClientConfig.getMaxSplitIteratorThreads();
        partitionBatchSize = hiveClientConfig.getPartitionBatchSize();

        this.hdfsEnvironment = hdfsEnvironment;

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

    @PreDestroy
    public void stop()
    {
        executorService.shutdownNow();
    }

    public HiveClient get(String clientId, HiveCluster hiveCluster)
    {
        CachingHiveMetastore metastore = metastores.getUnchecked(hiveCluster);
        return new HiveClient(clientId,
                maxSplitSize.toBytes(),
                maxOutstandingSplits,
                masSplitIteratorThreads,
                partitionBatchSize,
                metastore,
                hdfsEnvironment,
                executorService);
    }
}
