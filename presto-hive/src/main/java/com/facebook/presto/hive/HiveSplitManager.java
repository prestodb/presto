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

import com.facebook.airlift.concurrent.BoundedExecutor;
import com.facebook.airlift.stats.CounterStat;
import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.SortedRangeSet;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.predicate.ValueSet;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.hive.HiveBucketing.HiveBucketFilter;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.DateStatistics;
import com.facebook.presto.hive.metastore.DecimalStatistics;
import com.facebook.presto.hive.metastore.DoubleStatistics;
import com.facebook.presto.hive.metastore.HiveColumnStatistics;
import com.facebook.presto.hive.metastore.IntegerStatistics;
import com.facebook.presto.hive.metastore.MetastoreContext;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.PartitionStatistics;
import com.facebook.presto.hive.metastore.SemiTransactionalHiveMetastore;
import com.facebook.presto.hive.metastore.StorageFormat;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.PrestoWarning;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import io.airlift.units.DataSize;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hudi.hadoop.HoodieParquetInputFormat;
import org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.inject.Inject;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.stream.Stream;

import static com.facebook.presto.common.type.Decimals.encodeScaledValue;
import static com.facebook.presto.common.type.Decimals.isShortDecimal;
import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.hive.HiveColumnHandle.isPathColumnHandle;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_PARTITION_DROPPED_DURING_QUERY;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_PARTITION_SCHEMA_MISMATCH;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_TRANSACTION_NOT_FOUND;
import static com.facebook.presto.hive.HivePartition.UNPARTITIONED_ID;
import static com.facebook.presto.hive.HiveSessionProperties.getLeaseDuration;
import static com.facebook.presto.hive.HiveSessionProperties.isOfflineDataDebugModeEnabled;
import static com.facebook.presto.hive.HiveSessionProperties.isPartitionStatisticsBasedOptimizationEnabled;
import static com.facebook.presto.hive.HiveSessionProperties.isUseParquetColumnNames;
import static com.facebook.presto.hive.HiveSessionProperties.shouldIgnoreUnreadablePartition;
import static com.facebook.presto.hive.HiveStorageFormat.PARQUET;
import static com.facebook.presto.hive.HiveStorageFormat.getHiveStorageFormat;
import static com.facebook.presto.hive.HiveType.getPrimitiveType;
import static com.facebook.presto.hive.HiveWarningCode.PARTITION_NOT_READABLE;
import static com.facebook.presto.hive.StoragePartitionLoader.BucketSplitInfo.createBucketSplitInfo;
import static com.facebook.presto.hive.TableToPartitionMapping.mapColumnsByIndex;
import static com.facebook.presto.hive.metastore.MetastoreUtil.getMetastoreHeaders;
import static com.facebook.presto.hive.metastore.MetastoreUtil.getProtectMode;
import static com.facebook.presto.hive.metastore.MetastoreUtil.makePartName;
import static com.facebook.presto.hive.metastore.MetastoreUtil.verifyOnline;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.SERVER_SHUTTING_DOWN;
import static com.facebook.presto.spi.connector.ConnectorSplitManager.SplitSchedulingStrategy.GROUPED_SCHEDULING;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Iterables.transform;
import static java.lang.Float.floatToIntBits;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.reducing;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.PRIMITIVE;

public class HiveSplitManager
        implements ConnectorSplitManager
{
    public static final String OBJECT_NOT_READABLE = "object_not_readable";

    private final HiveTransactionManager hiveTransactionManager;
    private final NamenodeStats namenodeStats;
    private final HdfsEnvironment hdfsEnvironment;
    private final DirectoryLister directoryLister;
    private final Executor executor;
    private final CoercionPolicy coercionPolicy;
    private final int maxOutstandingSplits;
    private final DataSize maxOutstandingSplitsSize;
    private final int minPartitionBatchSize;
    private final int maxPartitionBatchSize;
    private final int maxInitialSplits;
    private final int splitLoaderConcurrency;
    private final boolean recursiveDfsWalkerEnabled;
    private final CounterStat highMemorySplitSourceCounter;
    private final CacheQuotaRequirementProvider cacheQuotaRequirementProvider;
    private final HiveEncryptionInformationProvider encryptionInformationProvider;

    @Inject
    public HiveSplitManager(
            HiveClientConfig hiveClientConfig,
            CacheQuotaRequirementProvider cacheQuotaRequirementProvider,
            HiveTransactionManager hiveTransactionManager,
            NamenodeStats namenodeStats,
            HdfsEnvironment hdfsEnvironment,
            DirectoryLister directoryLister,
            @ForHiveClient ExecutorService executorService,
            CoercionPolicy coercionPolicy,
            HiveEncryptionInformationProvider encryptionInformationProvider)
    {
        this(
                hiveTransactionManager,
                namenodeStats,
                hdfsEnvironment,
                directoryLister,
                new BoundedExecutor(executorService, hiveClientConfig.getMaxSplitIteratorThreads()),
                coercionPolicy,
                new CounterStat(),
                hiveClientConfig.getMaxOutstandingSplits(),
                hiveClientConfig.getMaxOutstandingSplitsSize(),
                hiveClientConfig.getMinPartitionBatchSize(),
                hiveClientConfig.getMaxPartitionBatchSize(),
                hiveClientConfig.getMaxInitialSplits(),
                hiveClientConfig.getSplitLoaderConcurrency(),
                hiveClientConfig.getRecursiveDirWalkerEnabled(),
                cacheQuotaRequirementProvider,
                encryptionInformationProvider);
    }

    public HiveSplitManager(
            HiveTransactionManager hiveTransactionManager,
            NamenodeStats namenodeStats,
            HdfsEnvironment hdfsEnvironment,
            DirectoryLister directoryLister,
            Executor executor,
            CoercionPolicy coercionPolicy,
            CounterStat highMemorySplitSourceCounter,
            int maxOutstandingSplits,
            DataSize maxOutstandingSplitsSize,
            int minPartitionBatchSize,
            int maxPartitionBatchSize,
            int maxInitialSplits,
            int splitLoaderConcurrency,
            boolean recursiveDfsWalkerEnabled,
            CacheQuotaRequirementProvider cacheQuotaRequirementProvider,
            HiveEncryptionInformationProvider encryptionInformationProvider)
    {
        this.hiveTransactionManager = requireNonNull(hiveTransactionManager, "hiveTransactionManager is null");
        this.namenodeStats = requireNonNull(namenodeStats, "namenodeStats is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.directoryLister = requireNonNull(directoryLister, "directoryLister is null");
        this.executor = new ErrorCodedExecutor(executor);
        this.coercionPolicy = requireNonNull(coercionPolicy, "coercionPolicy is null");
        this.highMemorySplitSourceCounter = requireNonNull(highMemorySplitSourceCounter, "highMemorySplitSourceCounter is null");
        checkArgument(maxOutstandingSplits >= 1, "maxOutstandingSplits must be at least 1");
        this.maxOutstandingSplits = maxOutstandingSplits;
        this.maxOutstandingSplitsSize = maxOutstandingSplitsSize;
        this.minPartitionBatchSize = minPartitionBatchSize;
        this.maxPartitionBatchSize = maxPartitionBatchSize;
        this.maxInitialSplits = maxInitialSplits;
        this.splitLoaderConcurrency = splitLoaderConcurrency;
        this.recursiveDfsWalkerEnabled = recursiveDfsWalkerEnabled;
        this.cacheQuotaRequirementProvider = requireNonNull(cacheQuotaRequirementProvider, "cacheQuotaRequirementProvider is null");
        this.encryptionInformationProvider = requireNonNull(encryptionInformationProvider, "encryptionInformationProvider is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableLayoutHandle layoutHandle,
            SplitSchedulingContext splitSchedulingContext)
    {
        HiveTableLayoutHandle layout = (HiveTableLayoutHandle) layoutHandle;
        SchemaTableName tableName = layout.getSchemaTableName();

        // get table metadata
        TransactionalMetadata metadata = hiveTransactionManager.get(transaction);
        if (metadata == null) {
            throw new PrestoException(HIVE_TRANSACTION_NOT_FOUND, format("Transaction not found: %s", transaction));
        }
        SemiTransactionalHiveMetastore metastore = metadata.getMetastore();
        Table table = metastore.getTable(new MetastoreContext(session.getIdentity(), session.getQueryId(), session.getClientInfo(), session.getSource(), getMetastoreHeaders(session)), tableName.getSchemaName(), tableName.getTableName())
                .orElseThrow(() -> new TableNotFoundException(tableName));

        if (!isOfflineDataDebugModeEnabled(session)) {
            // verify table is not marked as non-readable
            String tableNotReadable = table.getParameters().get(OBJECT_NOT_READABLE);
            if (!isNullOrEmpty(tableNotReadable)) {
                throw new HiveNotReadableException(tableName, Optional.empty(), tableNotReadable);
            }
        }

        // get partitions
        List<HivePartition> partitions = layout.getPartitions()
                .orElseThrow(() -> new PrestoException(GENERIC_INTERNAL_ERROR, "Layout does not contain partitions"));

        // short circuit if we don't have any partitions
        HivePartition partition = Iterables.getFirst(partitions, null);
        if (partition == null) {
            return new FixedSplitSource(ImmutableList.of());
        }

        Optional<HiveBucketFilter> bucketFilter = layout.getBucketFilter();

        // validate bucket bucketed execution
        Optional<HiveBucketHandle> bucketHandle = layout.getBucketHandle();
        if ((splitSchedulingContext.getSplitSchedulingStrategy() == GROUPED_SCHEDULING) && !bucketHandle.isPresent()) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "SchedulingPolicy is bucketed, but BucketHandle is not present");
        }

        if (bucketHandle.isPresent()) {
            if (bucketHandle.get().getReadBucketCount() > bucketHandle.get().getTableBucketCount()) {
                throw new PrestoException(
                        GENERIC_INTERNAL_ERROR,
                        "readBucketCount (%s) is greater than the tableBucketCount (%s) which generally points to an issue in plan generation");
            }
        }

        // sort partitions
        partitions = Ordering.natural().onResultOf(HivePartition::getPartitionId).reverse().sortedCopy(partitions);

        Iterable<HivePartitionMetadata> hivePartitions = getPartitionMetadata(
                metastore,
                table,
                tableName,
                partitions,
                bucketHandle,
                session,
                splitSchedulingContext.getWarningCollector(),
                layout.getRequestedColumns(),
                layout.getPredicateColumns(),
                layout.getDomainPredicate().getDomains());

        HiveSplitLoader hiveSplitLoader = new BackgroundHiveSplitLoader(
                table,
                hivePartitions,
                getPathDomain(layout.getDomainPredicate(), layout.getPredicateColumns()),
                createBucketSplitInfo(bucketHandle, bucketFilter),
                session,
                hdfsEnvironment,
                namenodeStats,
                directoryLister,
                executor,
                min(splitLoaderConcurrency, partitions.size()), // Avoid over-committing split loader concurrency
                recursiveDfsWalkerEnabled,
                splitSchedulingContext.schedulerUsesHostAddresses(),
                layout.isPartialAggregationsPushedDown());

        HiveSplitSource splitSource;
        CacheQuotaRequirement cacheQuotaRequirement = cacheQuotaRequirementProvider.getCacheQuotaRequirement(table.getDatabaseName(), table.getTableName());
        switch (splitSchedulingContext.getSplitSchedulingStrategy()) {
            case UNGROUPED_SCHEDULING:
                splitSource = HiveSplitSource.allAtOnce(
                        session,
                        table.getDatabaseName(),
                        table.getTableName(),
                        cacheQuotaRequirement,
                        maxInitialSplits,
                        maxOutstandingSplits,
                        maxOutstandingSplitsSize,
                        hiveSplitLoader,
                        executor,
                        new CounterStat());
                break;
            case GROUPED_SCHEDULING:
                splitSource = HiveSplitSource.bucketed(
                        session,
                        table.getDatabaseName(),
                        table.getTableName(),
                        cacheQuotaRequirement,
                        maxInitialSplits,
                        maxOutstandingSplits,
                        maxOutstandingSplitsSize,
                        hiveSplitLoader,
                        executor,
                        new CounterStat());
                break;
            case REWINDABLE_GROUPED_SCHEDULING:
                splitSource = HiveSplitSource.bucketedRewindable(
                        session,
                        table.getDatabaseName(),
                        table.getTableName(),
                        cacheQuotaRequirement,
                        maxInitialSplits,
                        maxOutstandingSplitsSize,
                        hiveSplitLoader,
                        executor,
                        new CounterStat());
                break;
            default:
                throw new IllegalArgumentException("Unknown splitSchedulingStrategy: " + splitSchedulingContext.getSplitSchedulingStrategy());
        }
        hiveSplitLoader.start(splitSource);

        return splitSource;
    }

    private static Optional<Domain> getPathDomain(TupleDomain<Subfield> domainPredicate, Map<String, HiveColumnHandle> predicateColumns)
    {
        checkArgument(!domainPredicate.isNone(), "Unexpected domain predicate: none");

        return domainPredicate.getDomains().get().entrySet().stream()
                .filter(entry -> isPathColumnHandle(predicateColumns.get(entry.getKey().getRootName())))
                .findFirst()
                .map(Map.Entry::getValue);
    }

    @Managed
    @Nested
    public CounterStat getHighMemorySplitSource()
    {
        return highMemorySplitSourceCounter;
    }

    private Iterable<HivePartitionMetadata> getPartitionMetadata(
            SemiTransactionalHiveMetastore metastore,
            Table table,
            SchemaTableName tableName,
            List<HivePartition> hivePartitions,
            Optional<HiveBucketHandle> hiveBucketHandle,
            ConnectorSession session,
            WarningCollector warningCollector,
            Optional<Set<HiveColumnHandle>> requestedColumns,
            Map<String, HiveColumnHandle> predicateColumns,
            Optional<Map<Subfield, Domain>> domains)
    {
        if (hivePartitions.isEmpty()) {
            return ImmutableList.of();
        }

        Optional<Set<HiveColumnHandle>> allRequestedColumns = mergeRequestedAndPredicateColumns(requestedColumns, ImmutableSet.copyOf(predicateColumns.values()));

        if (hivePartitions.size() == 1) {
            HivePartition firstPartition = getOnlyElement(hivePartitions);
            if (firstPartition.getPartitionId().equals(UNPARTITIONED_ID)) {
                return ImmutableList.of(new HivePartitionMetadata(
                        firstPartition,
                        Optional.empty(),
                        TableToPartitionMapping.empty(),
                        encryptionInformationProvider.getReadEncryptionInformation(session, table, allRequestedColumns),
                        ImmutableSet.of()));
            }
        }

        StorageFormat storageFormat = table.getStorage().getStorageFormat();
        Optional<HiveStorageFormat> hiveStorageFormat = getHiveStorageFormat(storageFormat);

        Optional<HiveStorageFormat> resolvedHiveStorageFormat;

        if (isUseParquetColumnNames(session)) {
            // Use Hive Storage Format as Parquet if table is of HUDI format
            resolvedHiveStorageFormat = (!hiveStorageFormat.isPresent() && isHudiFormat(storageFormat)) ? Optional.of(PARQUET) : hiveStorageFormat;
        }
        else {
            resolvedHiveStorageFormat = hiveStorageFormat;
        }

        Iterable<List<HivePartition>> partitionNameBatches = partitionExponentially(hivePartitions, minPartitionBatchSize, maxPartitionBatchSize);
        Iterable<List<HivePartitionMetadata>> partitionBatches = transform(partitionNameBatches, partitionBatch -> {
            Map<String, PartitionSplitInfo> partitionSplitInfo = getPartitionSplitInfo(session, metastore, tableName, partitionBatch, predicateColumns, domains);
            if (partitionBatch.size() != partitionSplitInfo.size()) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, format("Expected %s partitions but found %s", partitionBatch.size(), partitionSplitInfo.size()));
            }

            Map<String, Partition> partitions = partitionSplitInfo.entrySet().stream()
                    .collect(toImmutableMap(Entry::getKey, entry -> entry.getValue().getPartition()));
            Optional<Map<String, EncryptionInformation>> encryptionInformationForPartitions = encryptionInformationProvider.getReadEncryptionInformation(
                    session,
                    table,
                    allRequestedColumns,
                    partitions);

            ImmutableList.Builder<HivePartitionMetadata> results = ImmutableList.builder();
            Map<String, Set<String>> partitionsNotReadable = new HashMap<>();
            int unreadablePartitionsSkipped = 0;
            for (HivePartition hivePartition : partitionBatch) {
                Partition partition = partitions.get(hivePartition.getPartitionId());
                if (partitionSplitInfo.get(hivePartition.getPartitionId()).isPruned()) {
                    continue;
                }

                if (partition == null) {
                    throw new PrestoException(GENERIC_INTERNAL_ERROR, "Partition not loaded: " + hivePartition);
                }
                String partitionName = makePartName(table.getPartitionColumns(), partition.getValues());
                Optional<EncryptionInformation> encryptionInformation = encryptionInformationForPartitions.map(metadata -> metadata.get(hivePartition.getPartitionId()));

                if (!isOfflineDataDebugModeEnabled(session)) {
                    // verify partition is online
                    verifyOnline(tableName, Optional.of(partitionName), getProtectMode(partition), partition.getParameters());

                    // verify partition is not marked as non-readable
                    String reason = partition.getParameters().get(OBJECT_NOT_READABLE);
                    if (!isNullOrEmpty(reason)) {
                        if (!shouldIgnoreUnreadablePartition(session) || !partition.isEligibleToIgnore()) {
                            throw new HiveNotReadableException(tableName, Optional.of(partitionName), reason);
                        }
                        unreadablePartitionsSkipped++;
                        if (partitionsNotReadable.size() <= 3) {
                            partitionsNotReadable.putIfAbsent(reason, new HashSet<>(ImmutableSet.of(partitionName)));
                            if (partitionsNotReadable.get(reason).size() <= 3) {
                                partitionsNotReadable.get(reason).add(partitionName);
                            }
                        }
                        continue;
                    }
                }

                // Verify that the partition schema matches the table schema.
                // Either adding or dropping columns from the end of the table
                // without modifying existing partitions is allowed, but every
                // column that exists in both the table and partition must have
                // the same type.
                List<Column> tableColumns = table.getDataColumns();
                List<Column> partitionColumns = partition.getColumns();
                if ((tableColumns == null) || (partitionColumns == null)) {
                    throw new PrestoException(HIVE_INVALID_METADATA, format("Table '%s' or partition '%s' has null columns", tableName, partitionName));
                }
                TableToPartitionMapping tableToPartitionMapping = getTableToPartitionMapping(session, resolvedHiveStorageFormat, tableName, partitionName, tableColumns, partitionColumns);

                if (hiveBucketHandle.isPresent() && !hiveBucketHandle.get().isVirtuallyBucketed()) {
                    Optional<HiveBucketProperty> partitionBucketProperty = partition.getStorage().getBucketProperty();
                    if (!partitionBucketProperty.isPresent()) {
                        throw new PrestoException(HIVE_PARTITION_SCHEMA_MISMATCH, format(
                                "Hive table (%s) is bucketed but partition (%s) is not bucketed",
                                hivePartition.getTableName(),
                                hivePartition.getPartitionId()));
                    }
                    int tableBucketCount = hiveBucketHandle.get().getTableBucketCount();
                    int partitionBucketCount = partitionBucketProperty.get().getBucketCount();
                    List<String> tableBucketColumns = hiveBucketHandle.get().getColumns().stream()
                            .map(HiveColumnHandle::getName)
                            .collect(toImmutableList());
                    List<String> partitionBucketColumns = partitionBucketProperty.get().getBucketedBy();
                    if (!tableBucketColumns.equals(partitionBucketColumns) || !isBucketCountCompatible(tableBucketCount, partitionBucketCount)) {
                        throw new PrestoException(HIVE_PARTITION_SCHEMA_MISMATCH, format(
                                "Hive table (%s) bucketing (columns=%s, buckets=%s) is not compatible with partition (%s) bucketing (columns=%s, buckets=%s)",
                                hivePartition.getTableName(),
                                tableBucketColumns,
                                tableBucketCount,
                                hivePartition.getPartitionId(),
                                partitionBucketColumns,
                                partitionBucketCount));
                    }
                }

                results.add(
                        new HivePartitionMetadata(
                                hivePartition,
                                Optional.of(partition),
                                tableToPartitionMapping,
                                encryptionInformation,
                                partitionSplitInfo.get(hivePartition.getPartitionId()).getRedundantColumnDomains()));
            }
            if (unreadablePartitionsSkipped > 0) {
                StringBuilder warningMessage = new StringBuilder(format("Table '%s' has %s out of %s partitions unreadable: ", tableName, unreadablePartitionsSkipped, partitionBatch.size()));
                for (Entry<String, Set<String>> entry : partitionsNotReadable.entrySet()) {
                    warningMessage.append(String.join(", ", entry.getValue())).append("... are due to ").append(entry.getKey()).append(". ");
                }
                warningCollector.add(new PrestoWarning(PARTITION_NOT_READABLE, warningMessage.toString()));
            }
            return results.build();
        });
        return concat(partitionBatches);
    }

    /**
     * This method is called in the case of ordinal-based matching.
     * It checks for whether column coercions are possible and also creates the partitionSchemaDifference map.
     *
     * @param session ConnectorSession
     * @param storageFormat HiveStorageFormat
     * @param tableName SchemaTableName
     * @param partName Partition Name
     * @param tableColumns List of columns in the table
     * @param partitionColumns List of columns in the partition
     * @return {@link TableToPartitionMapping}
     */
    private TableToPartitionMapping getTableToPartitionMapping(ConnectorSession session, Optional<HiveStorageFormat> storageFormat, SchemaTableName tableName, String partName, List<Column> tableColumns, List<Column> partitionColumns)
    {
        if (storageFormat.isPresent() && storageFormat.get().equals(PARQUET) && isUseParquetColumnNames(session)) {
            return getTableToPartitionMappingByColumnNames(tableName, partName, tableColumns, partitionColumns);
        }
        ImmutableMap.Builder<Integer, Column> partitionSchemaDifference = ImmutableMap.builder();
        for (int i = 0; i < partitionColumns.size(); i++) {
            Column partitionColumn = partitionColumns.get(i);

            if (i >= tableColumns.size()) {
                partitionSchemaDifference.put(i, partitionColumn);
                continue;
            }

            HiveType tableType = tableColumns.get(i).getType();
            if (!tableType.equals(partitionColumn.getType())) {
                if (!coercionPolicy.canCoerce(partitionColumn.getType(), tableType)) {
                    throw tablePartitionColumnMismatchException(tableName, partName, tableColumns.get(i).getName(), tableType, partitionColumn.getName(), partitionColumn.getType());
                }
                partitionSchemaDifference.put(i, partitionColumn);
                continue;
            }

            if (!tableColumns.get(i).getName().equals(partitionColumn.getName())) {
                partitionSchemaDifference.put(i, partitionColumn);
            }
        }
        return mapColumnsByIndex(partitionSchemaDifference.build());
    }

    /**
     * This method is called in case we have name-based matching enabled. The aim of this method is to:
     * 1. Generate a mapping between table and partition column indexes based on column names
     * 2. Generate partitionSchemaDifference object which contains all the columns which either have different name or datatype as compared to table schema.
     * 3. Check for column coercions.
     *
     * @param tableName SchemaTableName
     * @param partName Partition Name
     * @param tableColumns List of columns in the table
     * @param partitionColumns List of columns in the partition
     * @return {@link TableToPartitionMapping}
     */
    private TableToPartitionMapping getTableToPartitionMappingByColumnNames(SchemaTableName tableName, String partName, List<Column> tableColumns, List<Column> partitionColumns)
    {
        ImmutableMap.Builder<String, Integer> partitionColumnIndexesBuilder = ImmutableMap.builder();
        for (int i = 0; i < partitionColumns.size(); i++) {
            partitionColumnIndexesBuilder.put(partitionColumns.get(i).getName().toLowerCase(ENGLISH), i);
        }
        Map<String, Integer> partitionColumnsByIndex = partitionColumnIndexesBuilder.build();

        ImmutableMap.Builder<Integer, Column> partitionSchemaDifference = ImmutableMap.builder();
        ImmutableMap.Builder<Integer, Integer> tableToPartitionColumns = ImmutableMap.builder();
        for (int tableColumnIndex = 0; tableColumnIndex < tableColumns.size(); tableColumnIndex++) {
            Column tableColumn = tableColumns.get(tableColumnIndex);
            HiveType tableType = tableColumn.getType();
            Integer partitionColumnIndex = partitionColumnsByIndex.get(tableColumn.getName().toLowerCase(ENGLISH));
            if (partitionColumnIndex == null) {
                continue;
            }
            tableToPartitionColumns.put(tableColumnIndex, partitionColumnIndex);
            Column partitionColumn = partitionColumns.get(partitionColumnIndex);
            HiveType partitionType = partitionColumn.getType();
            if (!tableType.equals(partitionType)) {
                if (!coercionPolicy.canCoerce(partitionType, tableType)) {
                    throw tablePartitionColumnMismatchException(tableName, partName, tableColumn.getName(), tableType, partitionColumn.getName(), partitionType);
                }
                partitionSchemaDifference.put(partitionColumnIndex, partitionColumn);
            }
        }

        ImmutableMap<Integer, Integer> tableToPartitionColumnsMap = tableToPartitionColumns.build();
        for (int partitionColumnIndex = 0; partitionColumnIndex < partitionColumns.size(); partitionColumnIndex++) {
            if (!tableToPartitionColumnsMap.containsValue(partitionColumnIndex)) {
                partitionSchemaDifference.put(partitionColumnIndex, partitionColumns.get(partitionColumnIndex));
            }
        }

        return new TableToPartitionMapping(Optional.of(tableToPartitionColumnsMap), partitionSchemaDifference.build());
    }

    private PrestoException tablePartitionColumnMismatchException(SchemaTableName tableName, String partName, String tableColumnName, HiveType tableType, String partitionColumnName, HiveType partitionType)
    {
        return new PrestoException(HIVE_PARTITION_SCHEMA_MISMATCH, format("" +
                        "There is a mismatch between the table and partition schemas. " +
                        "The types are incompatible and cannot be coerced. " +
                        "The column '%s' in table '%s' is declared as type '%s', " +
                        "but partition '%s' declared column '%s' as type '%s'.",
                tableColumnName,
                tableName,
                tableType,
                partName,
                partitionColumnName,
                partitionType));
    }

    /**
     * This method is used to check if a table is of HUDI format
     *
     * @param storageFormat Table Storage Format
     * @return true if table is of HUDI format, else false
     */
    private boolean isHudiFormat(StorageFormat storageFormat)
    {
        String serde = storageFormat.getSerDeNullable();
        String inputFormat = storageFormat.getInputFormatNullable();
        return serde != null && serde.equals(ParquetHiveSerDe.class.getName())
                && (inputFormat != null && (inputFormat.equals(HoodieParquetInputFormat.class.getName())
                || inputFormat.equals(HoodieParquetRealtimeInputFormat.class.getName())));
    }

    private Map<String, PartitionSplitInfo> getPartitionSplitInfo(
            ConnectorSession session,
            SemiTransactionalHiveMetastore metastore,
            SchemaTableName tableName,
            List<HivePartition> partitionBatch,
            Map<String, HiveColumnHandle> predicateColumns,
            Optional<Map<Subfield, Domain>> domains)
    {
        MetastoreContext metastoreContext = new MetastoreContext(session.getIdentity(), session.getQueryId(), session.getClientInfo(), session.getSource(), getMetastoreHeaders(session));
        Map<String, Optional<Partition>> partitions = metastore.getPartitionsByNames(
                metastoreContext,
                tableName.getSchemaName(),
                tableName.getTableName(),
                Lists.transform(partitionBatch, HivePartition::getPartitionId));
        Map<String, PartitionStatistics> partitionStatistics = ImmutableMap.of();
        if (domains.isPresent() && isPartitionStatisticsBasedOptimizationEnabled(session)) {
            partitionStatistics = metastore.getPartitionStatistics(
                    metastoreContext,
                    tableName.getSchemaName(),
                    tableName.getTableName(),
                    partitionBatch.stream()
                            .map(HivePartition::getPartitionId)
                            .collect(toImmutableSet()));
        }

        Map<String, String> partitionNameToLocation = new HashMap<>();
        ImmutableMap.Builder<String, PartitionSplitInfo> partitionSplitInfoBuilder = ImmutableMap.builder();
        for (Map.Entry<String, Optional<Partition>> entry : partitions.entrySet()) {
            ImmutableSet.Builder<ColumnHandle> redundantColumnDomainsBuilder = ImmutableSet.builder();
            if (!entry.getValue().isPresent()) {
                throw new PrestoException(HIVE_PARTITION_DROPPED_DURING_QUERY, "Partition no longer exists: " + entry.getKey());
            }
            boolean pruned = false;
            if (partitionStatistics.containsKey(entry.getKey())) {
                Map<String, HiveColumnStatistics> columnStatistics = partitionStatistics.get(entry.getKey()).getColumnStatistics();
                for (Map.Entry<String, HiveColumnHandle> predicateColumnEntry : predicateColumns.entrySet()) {
                    if (columnStatistics.containsKey(predicateColumnEntry.getKey())) {
                        Optional<ValueSet> columnsStatisticsValueSet = getColumnStatisticsValueSet(columnStatistics.get(predicateColumnEntry.getKey()), predicateColumnEntry.getValue().getHiveType());
                        Subfield subfield = new Subfield(predicateColumnEntry.getKey());
                        if (columnsStatisticsValueSet.isPresent() && domains.get().containsKey(subfield)) {
                            ValueSet columnPredicateValueSet = domains.get().get(subfield).getValues();
                            if (!columnPredicateValueSet.overlaps(columnsStatisticsValueSet.get())) {
                                pruned = true;
                                break;
                            }
                            if (columnPredicateValueSet.contains(columnsStatisticsValueSet.get())) {
                                redundantColumnDomainsBuilder.add(predicateColumnEntry.getValue());
                            }
                        }
                    }
                }
            }

            if (!pruned) {
                partitionNameToLocation.put(entry.getKey(), entry.getValue().get().getStorage().getLocation());
            }

            partitionSplitInfoBuilder.put(entry.getKey(), new PartitionSplitInfo(entry.getValue().get(), pruned, redundantColumnDomainsBuilder.build()));
        }
        metastore.setPartitionLeases(metastoreContext, tableName.getSchemaName(), tableName.getTableName(), partitionNameToLocation, getLeaseDuration(session));

        return partitionSplitInfoBuilder.build();
    }

    private Optional<ValueSet> getColumnStatisticsValueSet(HiveColumnStatistics statistics, HiveType hiveType)
    {
        if (hiveType.getCategory() != PRIMITIVE) {
            return Optional.empty();
        }

        PrimitiveTypeInfo typeInfo = (PrimitiveTypeInfo) hiveType.getTypeInfo();
        switch (typeInfo.getPrimitiveCategory()) {
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
                return getIntegerColumnStatisticsValueSet(statistics, getPrimitiveType(typeInfo));
            case FLOAT:
                return getFloatColumnStatisticsValueSet(statistics, getPrimitiveType(typeInfo));
            case DOUBLE:
                return getDoubleColumnStatisticsValueSet(statistics, getPrimitiveType(typeInfo));
            case DECIMAL:
                return getDecimalColumnStatisticsValueSet(statistics, getPrimitiveType(typeInfo));
            case DATE:
                return getDateColumnStatisticsValueSet(statistics, getPrimitiveType(typeInfo));
            default:
                return Optional.empty();
        }
    }

    private Optional<ValueSet> getIntegerColumnStatisticsValueSet(HiveColumnStatistics statistics, Type type)
    {
        if (!statistics.getIntegerStatistics().isPresent()) {
            return Optional.empty();
        }

        IntegerStatistics hiveColumnStatistics = statistics.getIntegerStatistics().get();
        ValueSet result = ValueSet.all(type);
        if (hiveColumnStatistics.getMin().isPresent()) {
            result = result.intersect(SortedRangeSet.copyOf(type, ImmutableList.of(Range.greaterThanOrEqual(type, hiveColumnStatistics.getMin().getAsLong()))));
        }
        if (hiveColumnStatistics.getMax().isPresent()) {
            result = result.intersect(SortedRangeSet.copyOf(type, ImmutableList.of(Range.lessThanOrEqual(type, hiveColumnStatistics.getMax().getAsLong()))));
        }
        return Optional.of(result);
    }

    private Optional<ValueSet> getFloatColumnStatisticsValueSet(HiveColumnStatistics statistics, Type type)
    {
        if (!statistics.getDoubleStatistics().isPresent()) {
            return Optional.empty();
        }

        DoubleStatistics hiveColumnStatistics = statistics.getDoubleStatistics().get();
        ValueSet result = ValueSet.all(type);
        if (hiveColumnStatistics.getMin().isPresent()) {
            result = result.intersect(
                    SortedRangeSet.copyOf(
                            type,
                            ImmutableList.of(Range.greaterThanOrEqual(type, (long) floatToIntBits((float) hiveColumnStatistics.getMin().getAsDouble())))));
        }
        if (hiveColumnStatistics.getMax().isPresent()) {
            result = result.intersect(
                    SortedRangeSet.copyOf(
                            type,
                            ImmutableList.of(Range.lessThanOrEqual(type, (long) floatToIntBits((float) hiveColumnStatistics.getMax().getAsDouble())))));
        }
        return Optional.of(result);
    }

    private Optional<ValueSet> getDoubleColumnStatisticsValueSet(HiveColumnStatistics statistics, Type type)
    {
        if (!statistics.getDoubleStatistics().isPresent()) {
            return Optional.empty();
        }

        DoubleStatistics hiveColumnStatistics = statistics.getDoubleStatistics().get();
        ValueSet result = ValueSet.all(type);
        if (hiveColumnStatistics.getMin().isPresent()) {
            result = result.intersect(SortedRangeSet.copyOf(type, ImmutableList.of(Range.greaterThanOrEqual(type, hiveColumnStatistics.getMin().getAsDouble()))));
        }
        if (hiveColumnStatistics.getMax().isPresent()) {
            result = result.intersect(SortedRangeSet.copyOf(type, ImmutableList.of(Range.lessThanOrEqual(type, hiveColumnStatistics.getMax().getAsDouble()))));
        }
        return Optional.of(result);
    }

    private Optional<ValueSet> getDecimalColumnStatisticsValueSet(HiveColumnStatistics statistics, Type type)
    {
        if (!statistics.getDecimalStatistics().isPresent()) {
            return Optional.empty();
        }

        DecimalStatistics hiveColumnStatistics = statistics.getDecimalStatistics().get();
        ValueSet result = ValueSet.all(type);
        if (hiveColumnStatistics.getMin().isPresent()) {
            Object min = isShortDecimal(type) ? hiveColumnStatistics.getMin().get().longValue() : encodeScaledValue(hiveColumnStatistics.getMin().get());
            result = result.intersect(SortedRangeSet.copyOf(type, ImmutableList.of(Range.greaterThanOrEqual(type, min))));
        }
        if (hiveColumnStatistics.getMax().isPresent()) {
            Object max = isShortDecimal(type) ? hiveColumnStatistics.getMax().get().longValue() : encodeScaledValue(hiveColumnStatistics.getMax().get());
            result = result.intersect(SortedRangeSet.copyOf(type, ImmutableList.of(Range.lessThanOrEqual(type, max))));
        }
        return Optional.of(result);
    }

    private Optional<ValueSet> getDateColumnStatisticsValueSet(HiveColumnStatistics statistics, Type type)
    {
        if (!statistics.getDateStatistics().isPresent()) {
            return Optional.empty();
        }

        DateStatistics hiveColumnStatistics = statistics.getDateStatistics().get();
        ValueSet result = ValueSet.all(type);
        if (hiveColumnStatistics.getMin().isPresent()) {
            result = result.intersect(SortedRangeSet.copyOf(type, ImmutableList.of(Range.greaterThanOrEqual(type, hiveColumnStatistics.getMin().get().toEpochDay()))));
        }
        if (hiveColumnStatistics.getMax().isPresent()) {
            result = result.intersect(SortedRangeSet.copyOf(type, ImmutableList.of(Range.lessThanOrEqual(type, hiveColumnStatistics.getMax().get().toEpochDay()))));
        }
        return Optional.of(result);
    }

    private static class PartitionSplitInfo
    {
        private final Partition partition;
        private final boolean pruned;
        private final Set<ColumnHandle> redundantColumnDomains;

        public PartitionSplitInfo(Partition partition, boolean pruned, Set<ColumnHandle> redundantColumnDomains)
        {
            this.partition = requireNonNull(partition, "partition is null");
            this.pruned = pruned;
            this.redundantColumnDomains = ImmutableSet.copyOf(requireNonNull(redundantColumnDomains, "redundantColumnDomains is null"));
        }

        public Partition getPartition()
        {
            return partition;
        }

        public boolean isPruned()
        {
            return pruned;
        }

        public Set<ColumnHandle> getRedundantColumnDomains()
        {
            return redundantColumnDomains;
        }
    }

    @VisibleForTesting
    static Optional<Set<HiveColumnHandle>> mergeRequestedAndPredicateColumns(Optional<Set<HiveColumnHandle>> requestedColumns, Set<HiveColumnHandle> predicateColumns)
    {
        if (!requestedColumns.isPresent() || predicateColumns.isEmpty()) {
            return requestedColumns;
        }

        return Optional.of(
                Stream.concat(requestedColumns.get().stream(), predicateColumns.stream())
                        .filter(column -> column.getColumnType() == REGULAR)
                        .collect(groupingBy(
                                HiveColumnHandle::getName,
                                reducing((handle1, handle2) -> {
                                    if (handle1.getRequiredSubfields().isEmpty()) {
                                        return handle1;
                                    }

                                    if (handle2.getRequiredSubfields().isEmpty()) {
                                        return handle2;
                                    }

                                    return (HiveColumnHandle) handle1.withRequiredSubfields(ImmutableList.copyOf(ImmutableSet.copyOf(
                                            ImmutableList.<Subfield>builder().addAll(handle1.getRequiredSubfields()).addAll(handle2.getRequiredSubfields()).build())));
                                })))
                        .values()
                        .stream()
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .collect(toImmutableSet()));
    }

    static boolean isBucketCountCompatible(int tableBucketCount, int partitionBucketCount)
    {
        checkArgument(tableBucketCount > 0 && partitionBucketCount > 0);
        int larger = Math.max(tableBucketCount, partitionBucketCount);
        int smaller = Math.min(tableBucketCount, partitionBucketCount);
        if (larger % smaller != 0) {
            // must be evenly divisible
            return false;
        }
        if (Integer.bitCount(larger / smaller) != 1) {
            // ratio must be power of two
            return false;
        }
        return true;
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
