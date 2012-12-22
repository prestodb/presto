package com.facebook.presto.importer;

import com.facebook.presto.ingest.ImportPartition;
import com.facebook.presto.ingest.RecordProjectOperator;
import com.facebook.presto.ingest.RecordProjection;
import com.facebook.presto.metadata.StorageManager;
import com.facebook.presto.server.ShardImport;
import com.facebook.presto.spi.ImportClient;
import com.facebook.presto.spi.PartitionChunk;
import com.facebook.presto.split.ImportClientFactory;
import com.facebook.presto.util.ShardBoundedExecutor;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.ingest.RecordProjections.createProjection;
import static com.facebook.presto.util.RetryDriver.runWithRetry;
import static com.facebook.presto.util.Threads.threadsNamed;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.Executors.newFixedThreadPool;

public class LocalShardManager
{
    private static final Logger log = Logger.get(LocalShardManager.class);

    private static final int TASKS_PER_NODE = 32;

    private final ExecutorService executor = newFixedThreadPool(TASKS_PER_NODE, threadsNamed("local-shard-manager-%s"));
    private final ShardBoundedExecutor<Long> shardBoundedExecutor = new ShardBoundedExecutor<>(executor);
    private final ImportClientFactory importClientFactory;
    private final StorageManager storageManager;

    @Inject
    public LocalShardManager(ImportClientFactory importClientFactory, StorageManager storageManager)
    {
        this.importClientFactory = checkNotNull(importClientFactory, "importClientFactory is null");
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
                runWithRetry(new Callable<Void>()
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
            ImportClient importClient = importClientFactory.getClient(shardImport.getSourceName());

            PartitionChunk chunk = shardImport.getPartitionChunk().deserialize(importClient);
            List<Long> columnIds = getColumnIds(shardImport.getFields());
            List<RecordProjection> projections = getRecordProjections(shardImport.getFields());

            ImportPartition importPartition = new ImportPartition(importClient, chunk, shardImport.getFields().size());
            DataSize partitionSize = new DataSize(chunk.getLength(), Unit.BYTE);
            RecordProjectOperator source = new RecordProjectOperator(importPartition, partitionSize, projections);

            storageManager.importShard(shardId, columnIds, source);
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
                runWithRetry(new Callable<Void>()
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

    private List<Long> getColumnIds(List<ImportField> fields)
    {
        ImmutableList.Builder<Long> columnIds = ImmutableList.builder();
        for (ImportField field : fields) {
            columnIds.add(field.getColumnId());
        }
        return columnIds.build();
    }

    private List<RecordProjection> getRecordProjections(List<ImportField> fields)
    {
        ImmutableList.Builder<RecordProjection> projections = ImmutableList.builder();
        for (int i = 0; i < fields.size(); i++) {
            ImportField field = fields.get(i);
            projections.add(createProjection((int) field.getColumnId(), field.getColumnType()));
        }
        return projections.build();
    }
}
