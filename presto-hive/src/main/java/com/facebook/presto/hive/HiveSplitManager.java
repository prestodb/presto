/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.hive;

import com.facebook.presto.hive.metastore.HiveMetastore;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.units.DataSize;
import org.apache.hadoop.hive.metastore.ProtectMode;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

import javax.inject.Inject;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_PARTITION_SCHEMA_MISMATCH;
import static com.facebook.presto.hive.HivePartition.UNPARTITIONED_ID;
import static com.facebook.presto.hive.HiveUtil.createPartitionName;
import static com.facebook.presto.hive.UnpartitionedPartition.UNPARTITIONED_PARTITION;
import static com.facebook.presto.hive.util.Types.checkType;
import static com.facebook.presto.spi.StandardErrorCode.INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.SERVER_SHUTTING_DOWN;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Iterables.transform;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.hive.metastore.ProtectMode.getProtectModeFromString;

public class HiveSplitManager
        implements ConnectorSplitManager
{
    public static final String PRESTO_OFFLINE = "presto_offline";

    private final String connectorId;
    private final HiveMetastore metastore;
    private final NamenodeStats namenodeStats;
    private final HdfsEnvironment hdfsEnvironment;
    private final DirectoryLister directoryLister;
    private final Executor executor;
    private final int maxOutstandingSplits;
    private final int minPartitionBatchSize;
    private final int maxPartitionBatchSize;
    private final DataSize maxSplitSize;
    private final DataSize maxInitialSplitSize;
    private final int maxInitialSplits;
    private final boolean recursiveDfsWalkerEnabled;

    @Inject
    public HiveSplitManager(
            HiveConnectorId connectorId,
            HiveClientConfig hiveClientConfig,
            HiveMetastore metastore,
            NamenodeStats namenodeStats,
            HdfsEnvironment hdfsEnvironment,
            DirectoryLister directoryLister,
            @ForHiveClient ExecutorService executorService)
    {
        this(connectorId,
                metastore,
                namenodeStats,
                hdfsEnvironment,
                directoryLister,
                new BoundedExecutor(executorService, hiveClientConfig.getMaxSplitIteratorThreads()),
                hiveClientConfig.getMaxOutstandingSplits(),
                hiveClientConfig.getMinPartitionBatchSize(),
                hiveClientConfig.getMaxPartitionBatchSize(),
                hiveClientConfig.getMaxSplitSize(),
                hiveClientConfig.getMaxInitialSplitSize(),
                hiveClientConfig.getMaxInitialSplits(),
                hiveClientConfig.getRecursiveDirWalkerEnabled()
        );
    }

    public HiveSplitManager(
            HiveConnectorId connectorId,
            HiveMetastore metastore,
            NamenodeStats namenodeStats,
            HdfsEnvironment hdfsEnvironment,
            DirectoryLister directoryLister,
            Executor executor,
            int maxOutstandingSplits,
            int minPartitionBatchSize,
            int maxPartitionBatchSize,
            DataSize maxSplitSize,
            DataSize maxInitialSplitSize,
            int maxInitialSplits,
            boolean recursiveDfsWalkerEnabled)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.namenodeStats = requireNonNull(namenodeStats, "namenodeStats is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.directoryLister = requireNonNull(directoryLister, "directoryLister is null");
        this.executor = new ErrorCodedExecutor(executor);
        checkArgument(maxOutstandingSplits >= 1, "maxOutstandingSplits must be at least 1");
        this.maxOutstandingSplits = maxOutstandingSplits;
        this.minPartitionBatchSize = minPartitionBatchSize;
        this.maxPartitionBatchSize = maxPartitionBatchSize;
        this.maxSplitSize = requireNonNull(maxSplitSize, "maxSplitSize is null");
        this.maxInitialSplitSize = requireNonNull(maxInitialSplitSize, "maxInitialSplitSize is null");
        this.maxInitialSplits = maxInitialSplits;
        this.recursiveDfsWalkerEnabled = recursiveDfsWalkerEnabled;
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorTableLayoutHandle layoutHandle)
    {
        HiveTableLayoutHandle layout = checkType(layoutHandle, HiveTableLayoutHandle.class, "layoutHandle");

        List<HivePartition> partitions = Lists.transform(layout.getPartitions().get(), partition -> checkType(partition, HivePartition.class, "partition"));

        HivePartition partition = Iterables.getFirst(partitions, null);
        if (partition == null) {
            return new FixedSplitSource(connectorId, ImmutableList.<ConnectorSplit>of());
        }
        SchemaTableName tableName = partition.getTableName();
        Optional<HiveBucketing.HiveBucket> bucket = partition.getBucket();

        // sort partitions
        partitions = Ordering.natural().onResultOf(HivePartition::getPartitionId).reverse().sortedCopy(partitions);

        Optional<Table> table = metastore.getTable(tableName.getSchemaName(), tableName.getTableName());
        if (!table.isPresent()) {
            throw new TableNotFoundException(tableName);
        }
        Iterable<HivePartitionMetadata> hivePartitions = getPartitionMetadata(table.get(), tableName, partitions);

        HiveSplitLoader hiveSplitLoader = new BackgroundHiveSplitLoader(
                connectorId,
                table.get(),
                hivePartitions,
                bucket,
                maxSplitSize,
                session,
                hdfsEnvironment,
                namenodeStats,
                directoryLister,
                executor,
                maxPartitionBatchSize,
                maxInitialSplitSize,
                maxInitialSplits,
                recursiveDfsWalkerEnabled);

        HiveSplitSource splitSource = new HiveSplitSource(connectorId, maxOutstandingSplits, hiveSplitLoader, executor);
        hiveSplitLoader.start(splitSource);

        return splitSource;
    }

    private Iterable<HivePartitionMetadata> getPartitionMetadata(Table table, SchemaTableName tableName, List<HivePartition> hivePartitions)
    {
        if (hivePartitions.isEmpty()) {
            return ImmutableList.of();
        }

        if (hivePartitions.size() == 1) {
            HivePartition firstPartition = getOnlyElement(hivePartitions);
            if (firstPartition.getPartitionId().equals(UNPARTITIONED_ID)) {
                return ImmutableList.of(new HivePartitionMetadata(firstPartition, UNPARTITIONED_PARTITION));
            }
        }

        Iterable<List<HivePartition>> partitionNameBatches = partitionExponentially(hivePartitions, minPartitionBatchSize, maxPartitionBatchSize);
        Iterable<List<HivePartitionMetadata>> partitionBatches = transform(partitionNameBatches, partitionBatch -> {
            Optional<Map<String, Partition>> batch = metastore.getPartitionsByNames(
                    tableName.getSchemaName(),
                    tableName.getTableName(),
                    Lists.transform(partitionBatch, HivePartition::getPartitionId));
            if (!batch.isPresent()) {
                throw new PrestoException(HIVE_METASTORE_ERROR, "Partition metadata not available");
            }
            Map<String, Partition> partitions = batch.get();
            if (partitionBatch.size() != partitions.size()) {
                throw new PrestoException(INTERNAL_ERROR, format("Expected %s partitions but found %s", partitionBatch.size(), partitions.size()));
            }

            ImmutableList.Builder<HivePartitionMetadata> results = ImmutableList.builder();
            for (HivePartition hivePartition : partitionBatch) {
                Partition partition = partitions.get(hivePartition.getPartitionId());
                if (partition == null) {
                    throw new PrestoException(INTERNAL_ERROR, "Partition not loaded: " + hivePartition);
                }

                // verify all partition is online
                String protectMode = partition.getParameters().get(ProtectMode.PARAMETER_NAME);
                String partName = createPartitionName(partition, table);
                if (protectMode != null && getProtectModeFromString(protectMode).offline) {
                    throw new PartitionOfflineException(tableName, partName);
                }
                String prestoOffline = partition.getParameters().get(PRESTO_OFFLINE);
                if (!isNullOrEmpty(prestoOffline)) {
                    throw new PartitionOfflineException(tableName, partName, format("Partition '%s' is offline for Presto: %s", partName, prestoOffline));
                }

                // Verify that the partition schema matches the table schema.
                // Either adding or dropping columns from the end of the table
                // without modifying existing partitions is allowed, but every
                // column that exists in both the table and partition must have
                // the same type.
                List<FieldSchema> tableColumns = table.getSd().getCols();
                List<FieldSchema> partitionColumns = partition.getSd().getCols();
                if ((tableColumns == null) || (partitionColumns == null)) {
                    throw new PrestoException(HIVE_INVALID_METADATA, format("Table '%s' or partition '%s' has null columns", tableName, partName));
                }
                for (int i = 0; i < min(partitionColumns.size(), tableColumns.size()); i++) {
                    String tableType = tableColumns.get(i).getType();
                    String partitionType = partitionColumns.get(i).getType();
                    if (!tableType.equals(partitionType)) {
                        throw new PrestoException(HIVE_PARTITION_SCHEMA_MISMATCH, format("" +
                                        "There is a mismatch between the table and partition schemas. " +
                                        "The column '%s' in table '%s' is declared as type '%s', " +
                                        "but partition '%s' declared column '%s' as type '%s'.",
                                tableColumns.get(i).getName(),
                                tableName,
                                tableType,
                                partName,
                                partitionColumns.get(i).getName(),
                                partitionType));
                    }
                }

                results.add(new HivePartitionMetadata(hivePartition, partition));
            }

            return results.build();
        });
        return concat(partitionBatches);
    }

    /**
     * Partition the given list in exponentially (power of 2) increasing batch sizes starting at 1 up to maxBatchSize
     */
    private static <T> Iterable<List<T>> partitionExponentially(List<T> values, int minBatchSize, int maxBatchSize)
    {
        return () -> new AbstractIterator<List<T>>()
        {
            private int currentSize = minBatchSize;
            private final Iterator<T> iterator = values.iterator();

            @Override
            protected List<T> computeNext()
            {
                if (!iterator.hasNext()) {
                    return endOfData();
                }

                int count = 0;
                ImmutableList.Builder<T> builder = ImmutableList.builder();
                while (iterator.hasNext() && count < currentSize) {
                    builder.add(iterator.next());
                    ++count;
                }

                currentSize = min(maxBatchSize, currentSize * 2);
                return builder.build();
            }
        };
    }

    private static class ErrorCodedExecutor
            implements Executor
    {
        private final Executor delegate;

        private ErrorCodedExecutor(Executor delegate)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
        }

        @Override
        public void execute(Runnable command)
        {
            try {
                delegate.execute(command);
            }
            catch (RejectedExecutionException e) {
                throw new PrestoException(SERVER_SHUTTING_DOWN, "Server is shutting down", e);
            }
        }
    }
}
