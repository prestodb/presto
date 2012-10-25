package com.facebook.presto.metadata;

import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.StaticTupleAppendingTupleStream;
import com.facebook.presto.block.TupleStream;
import com.facebook.presto.hive.HiveClient;
import com.facebook.presto.hive.PartitionChunk;
import com.facebook.presto.hive.RecordIterator;
import com.facebook.presto.hive.SchemaField;
import com.facebook.presto.ingest.HiveTupleStream;
import com.facebook.presto.slice.Slices;
import com.google.common.base.Charsets;
import com.google.inject.Inject;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.skife.jdbi.v2.util.LongMapper;

import java.util.List;
import java.util.concurrent.Callable;

import static com.facebook.presto.RetryDriver.runWithRetry;
import static com.facebook.presto.ingest.HiveSchemaUtil.createColumnMetadata;
import static com.google.common.base.Preconditions.checkNotNull;

public class HiveImportManager
{
    private final HiveClient hiveClient;
    private final StorageManager storageManager;
    private final Metadata metadata;
    private final HiveImportRegistry hiveImportRegistry;

    @Inject
    public HiveImportManager(HiveClient hiveClient, StorageManager storageManager, Metadata metadata, @ForStorageManager IDBI dbi)
    {
        this.hiveClient = checkNotNull(hiveClient, "hiveClient is null");
        this.storageManager = checkNotNull(storageManager, "storageManager is null");
        this.metadata = metadata;
        hiveImportRegistry = new HiveImportRegistry(checkNotNull(dbi, "dbi is null"));
    }

    public long importPartition(final String databaseName, final String tableName, final String partitionName)
            throws Exception
    {
        checkNotNull(databaseName, "databaseName is null");
        checkNotNull(tableName, "tableName is null");
        checkNotNull(partitionName, "partitionName is null");

        // TODO: prevent multiple simultaneous imports on same partition (race condition)
        if (hiveImportRegistry.isPartitionImported(databaseName, tableName, partitionName)) {
            // Already imported
            return 0;
        }

        final Tuple partitionTuple = TupleInfo.SINGLE_VARBINARY.builder()
                .append(Slices.wrappedBuffer(partitionName.getBytes(Charsets.UTF_8)))
                .build();

        List<PartitionChunk> chunks = runWithRetry(new Callable<List<PartitionChunk>>()
        {
            @Override
            public List<PartitionChunk> call()
                    throws Exception
            {
                return hiveClient.getPartitionChunks(databaseName, tableName, partitionName);
            }
        }, databaseName + "." + tableName + "." + partitionName + ".getPartitionChunks");

        final List<SchemaField> schemaFields = runWithRetry(new Callable<List<SchemaField>>()
        {
            @Override
            public List<SchemaField> call()
                    throws Exception
            {
                return hiveClient.getTableSchema(databaseName, tableName);
            }
        }, databaseName + "." + tableName + "." + partitionName + ".getTableSchema");

        // TODO: do this properly in Metadata
        synchronized (this) {
            String catalogName = "default";
            String schemaName = "default";
            if (metadata.getTable(catalogName, schemaName, tableName) == null) {
                List<ColumnMetadata> columns = createColumnMetadata(schemaFields);
                metadata.createTable(new TableMetadata(catalogName, schemaName, tableName, columns));
            }
        }

        long rowCount = 0;
        // TODO: right now, failures can result in partial partitions to be loaded (smallest unit needs to be transactional)
        for (final PartitionChunk chunk : chunks) {
            rowCount += runWithRetry(new Callable<Long>()
            {
                @Override
                public Long call()
                        throws Exception
                {
                    try (RecordIterator records = hiveClient.getRecords(chunk)) {
                        TupleStream sourceTupleStream = new StaticTupleAppendingTupleStream(
                                new HiveTupleStream(records, schemaFields),
                                partitionTuple
                        );
                        // TODO: add layer to break up incoming TupleStream based on size
                        return storageManager.importTableShard(sourceTupleStream, databaseName, tableName);
                    }
                }
            }, databaseName + "." + tableName + "." + partitionName + "." + chunk + ".import");
        }
        hiveImportRegistry.markPartitionImported(databaseName, tableName, partitionName);
        return rowCount;
    }

    // TODO: the import registry should use the CHUNK as the smallest unit of import
    private static class HiveImportRegistry
    {
        private final IDBI dbi;

        public HiveImportRegistry(IDBI dbi)
        {
            this.dbi = checkNotNull(dbi, "dbi is null");
            initializeDatabaseIfNecessary();
        }

        private void initializeDatabaseIfNecessary()
        {
            dbi.withHandle(new HandleCallback<Void>()
            {
                @Override
                public Void withHandle(Handle handle)
                        throws Exception
                {
                    // TODO: use ids for database, table, and partition
                    handle.createStatement("CREATE TABLE IF NOT EXISTS imported_hive_partitions (database VARCHAR(256), table VARCHAR(256), partition VARCHAR(256), PRIMARY KEY(database, table, partition))")
                            .execute();
                    return null;
                }
            });
        }

        public boolean isPartitionImported(final String databaseName, final String tableName, final String partitionName)
        {
            checkNotNull(databaseName, "databaseName is null");
            checkNotNull(tableName, "tableName is null");
            checkNotNull(partitionName, "partitionName is null");

            return dbi.withHandle(new HandleCallback<Boolean>()
            {
                @Override
                public Boolean withHandle(Handle handle)
                        throws Exception
                {
                    return handle.createQuery(
                            "SELECT COUNT(*) " +
                                    "FROM imported_hive_partitions " +
                                    "WHERE database = :database " +
                                    "AND table = :table " +
                                    "AND partition = :partition")
                            .bind("database", databaseName)
                            .bind("table", tableName)
                            .bind("partition", partitionName)
                            .map(LongMapper.FIRST)
                            .first() != 0;
                }
            });
        }

        public void markPartitionImported(final String databaseName, final String tableName, final String partitionName)
        {
            checkNotNull(databaseName, "databaseName is null");
            checkNotNull(tableName, "tableName is null");
            checkNotNull(partitionName, "partitionName is null");

            dbi.withHandle(new HandleCallback<Void>()
            {
                @Override
                public Void withHandle(Handle handle)
                        throws Exception
                {
                    handle.createStatement("INSERT INTO imported_hive_partitions (database, table, partition) values (:database, :table, :partition)")
                            .bind("database", databaseName)
                            .bind("table", tableName)
                            .bind("partition", partitionName)
                            .execute();
                    return null;
                }
            });
        }
    }
}
