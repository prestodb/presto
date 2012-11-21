package com.facebook.presto.importer;

import com.facebook.presto.ingest.ImportPartition;
import com.facebook.presto.ingest.RecordProjectOperator;
import com.facebook.presto.ingest.RecordProjection;
import com.facebook.presto.metadata.StorageManager;
import com.facebook.presto.server.ShardImport;
import com.facebook.presto.spi.ImportClient;
import com.facebook.presto.spi.PartitionChunk;
import com.facebook.presto.split.ImportClientFactory;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.util.Threads.threadsNamed;
import static com.facebook.presto.ingest.RecordProjections.createProjection;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.Executors.newFixedThreadPool;

public class ShardImporter
{
    private static final Logger log = Logger.get(ShardImporter.class);

    private static final int tasksPerNode = 32;
    private static final int importAttempts = 10;

    private final ExecutorService executor = newFixedThreadPool(tasksPerNode, threadsNamed("shard-importer-%s"));
    private final Set<Long> importingShards = Collections.newSetFromMap(new ConcurrentHashMap<Long, Boolean>());
    private final ImportClientFactory importClientFactory;
    private final StorageManager storageManager;

    @Inject
    public ShardImporter(ImportClientFactory importClientFactory, StorageManager storageManager)
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
        if (importingShards.add(shardId)) {
            executor.execute(new ImportJob(shardId, shardImport));
        }
    }

    public boolean isShardImporting(long shardId)
    {
        return importingShards.contains(shardId);
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
                importWithRetry();
            }
            finally {
                importingShards.remove(shardId);
            }
        }

        private void importWithRetry()
        {
            for (int i = 0; i < importAttempts; i++) {
                try {
                    importShard();
                    return;
                }
                catch (IOException e) {
                    log.warn(e, "error importing shard");
                }
            }
            log.error("shard import failed");
        }

        private void importShard()
                throws IOException
        {
            ImportClient importClient = importClientFactory.getClient(shardImport.getSourceName());

            PartitionChunk chunk = shardImport.getPartitionChunk().deserialize(importClient);
            List<String> fieldNames = getFieldNames(shardImport.getFields());
            List<Long> columnIds = getColumnIds(shardImport.getFields());
            List<RecordProjection> projections = getRecordProjections(shardImport.getFields());

            ImportPartition importPartition = new ImportPartition(importClient, chunk, fieldNames);
            RecordProjectOperator source = new RecordProjectOperator(importPartition, projections);

            storageManager.importShard(shardId, columnIds, source);
        }
    }

    private static List<String> getFieldNames(List<ImportField> fields)
    {
        ImmutableList.Builder<String> names = ImmutableList.builder();
        for (ImportField field : fields) {
            names.add(field.getImportFieldName());
        }
        return names.build();
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
            projections.add(createProjection(i, fields.get(i).getColumnType()));
        }
        return projections.build();
    }
}
