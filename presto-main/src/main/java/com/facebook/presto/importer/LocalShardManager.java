package com.facebook.presto.importer;

import com.facebook.presto.ingest.ImportPartition;
import com.facebook.presto.ingest.RecordProjectOperator;
import com.facebook.presto.metadata.ImportColumnHandle;
import com.facebook.presto.metadata.StorageManager;
import com.facebook.presto.spi.ImportClient;
import com.facebook.presto.spi.PartitionChunk;
import com.facebook.presto.split.ImportClientManager;
import com.facebook.presto.util.KeyBoundedExecutor;
import io.airlift.log.Logger;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.importer.ImportField.targetColumnHandleGetter;

import static com.facebook.presto.importer.ImportField.sourceColumnHandleGetter;
import static com.facebook.presto.util.RetryDriver.retry;
import static com.facebook.presto.util.Threads.threadsNamed;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.transform;
import static java.util.concurrent.Executors.newFixedThreadPool;

public class LocalShardManager
{
    private static final Logger log = Logger.get(LocalShardManager.class);

    private static final int TASKS_PER_NODE = 32;

    private final ExecutorService executor = newFixedThreadPool(TASKS_PER_NODE, threadsNamed("local-shard-manager-%s"));
    private final KeyBoundedExecutor<Long> shardBoundedExecutor = new KeyBoundedExecutor<>(executor);
    private final ImportClientManager importClientManager;
    private final StorageManager storageManager;

    @Inject
    public LocalShardManager(ImportClientManager importClientManager, StorageManager storageManager)
    {
        this.importClientManager = checkNotNull(importClientManager, "importClientFactory is null");
        this.storageManager = checkNotNull(storageManager, "storageManager is null");
    }

    @PreDestroy
    public void stop()
    {
        executor.shutdown();
    }

    public void importShard(long shardId, ShardImport shardImport)
    {
        shardBoundedExecutor.execute(shardId, new ImportJob(shardId, shardImport));
    }

    public void dropShard(long shardId)
    {
        shardBoundedExecutor.execute(shardId, new DropJob(shardId));
    }

    public boolean isShardActive(long shardId)
    {
        return shardBoundedExecutor.isActive(shardId);
    }

    private class ImportJob
            implements Runnable
    {
        private final long shardId;
        private final ShardImport shardImport;

        public ImportJob(long shardId, ShardImport shardImport)
        {
            this.shardId = shardId;
            this.shardImport = checkNotNull(shardImport, "shardImport is null");
        }

        @Override
        public void run()
        {
            try {
                retry().stopOnIllegalExceptions().runUnchecked(new Callable<Void>()
                {
                    @Override
                    public Void call()
                            throws Exception
                    {
                        importShard();
                        return null;
                    }
                });
            } catch (Exception e) {
                log.error(e, "shard import failed");
            }
        }

        private void importShard()
                throws IOException
        {
            ImportClient importClient = importClientManager.getClient(shardImport.getSourceName());

            PartitionChunk chunk = shardImport.getPartitionChunk().deserialize(importClient);

            ImportPartition importPartition = new ImportPartition(importClient, chunk);
            List<ImportColumnHandle> sourceColumns = transform(shardImport.getFields(), sourceColumnHandleGetter());
            RecordProjectOperator source = new RecordProjectOperator(importPartition, sourceColumns);

            storageManager.importShard(shardId, transform(shardImport.getFields(), targetColumnHandleGetter()), source);
        }
    }

    private class DropJob
            implements Runnable
    {
        private final long shardId;

        private DropJob(long shardId)
        {
            this.shardId = shardId;
        }

        @Override
        public void run()
        {
            try {
                retry().stopOnIllegalExceptions().runUnchecked(new Callable<Void>()
                {
                    @Override
                    public Void call()
                            throws Exception
                    {
                        storageManager.dropShard(shardId);
                        return null;
                    }
                });
            } catch (Exception e) {
                log.error(e, "shard drop failed");
            }
        }
    }
}
