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
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.ConnectorPartitionResult;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.Domain;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SerializableNativeValue;
import com.facebook.presto.spi.SortedRangeSet;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.metastore.ProtectMode;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.joda.time.DateTimeZone;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.hive.HiveBucketing.getHiveBucket;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_PARTITION_SCHEMA_MISMATCH;
import static com.facebook.presto.hive.HivePartition.UNPARTITIONED_ID;
import static com.facebook.presto.hive.HiveUtil.getPartitionKeyColumnHandles;
import static com.facebook.presto.hive.HiveUtil.parsePartitionValue;
import static com.facebook.presto.hive.HiveUtil.schemaTableName;
import static com.facebook.presto.hive.UnpartitionedPartition.UNPARTITIONED_PARTITION;
import static com.facebook.presto.hive.util.Types.checkType;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.StandardErrorCode.SERVER_SHUTTING_DOWN;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.not;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Iterables.transform;
import static java.lang.Math.min;
import static java.lang.String.format;
import static org.apache.hadoop.hive.metastore.ProtectMode.getProtectModeFromString;
import static org.apache.hadoop.hive.metastore.Warehouse.makePartName;

public class HiveSplitManager
        implements ConnectorSplitManager
{
    public static final String PRESTO_OFFLINE = "presto_offline";
    private static final String PARTITION_VALUE_WILDCARD = "";

    private static final Logger log = Logger.get(HiveSplitManager.class);

    private final String connectorId;
    private final HiveMetastore metastore;
    private final NamenodeStats namenodeStats;
    private final HdfsEnvironment hdfsEnvironment;
    private final DirectoryLister directoryLister;
    private final DateTimeZone timeZone;
    private final Executor executor;
    private final int maxOutstandingSplits;
    private final int minPartitionBatchSize;
    private final int maxPartitionBatchSize;
    private final DataSize maxSplitSize;
    private final DataSize maxInitialSplitSize;
    private final int maxInitialSplits;
    private final boolean forceLocalScheduling;
    private final boolean recursiveDfsWalkerEnabled;
    private final boolean assumeCanonicalPartitionKeys;

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
                DateTimeZone.forTimeZone(hiveClientConfig.getTimeZone()),
                new BoundedExecutor(executorService, hiveClientConfig.getMaxSplitIteratorThreads()),
                hiveClientConfig.getMaxOutstandingSplits(),
                hiveClientConfig.getMinPartitionBatchSize(),
                hiveClientConfig.getMaxPartitionBatchSize(),
                hiveClientConfig.getMaxSplitSize(),
                hiveClientConfig.getMaxInitialSplitSize(),
                hiveClientConfig.getMaxInitialSplits(),
                hiveClientConfig.isForceLocalScheduling(),
                hiveClientConfig.isAssumeCanonicalPartitionKeys(),
                hiveClientConfig.getRecursiveDirWalkerEnabled());
    }

    public HiveSplitManager(
            HiveConnectorId connectorId,
            HiveMetastore metastore,
            NamenodeStats namenodeStats,
            HdfsEnvironment hdfsEnvironment,
            DirectoryLister directoryLister,
            DateTimeZone timeZone,
            Executor executor,
            int maxOutstandingSplits,
            int minPartitionBatchSize,
            int maxPartitionBatchSize,
            DataSize maxSplitSize,
            DataSize maxInitialSplitSize,
            int maxInitialSplits,
            boolean forceLocalScheduling,
            boolean assumeCanonicalPartitionKeys,
            boolean recursiveDfsWalkerEnabled)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null").toString();
        this.metastore = checkNotNull(metastore, "metastore is null");
        this.namenodeStats = checkNotNull(namenodeStats, "namenodeStats is null");
        this.hdfsEnvironment = checkNotNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.directoryLister = checkNotNull(directoryLister, "directoryLister is null");
        this.timeZone = checkNotNull(timeZone, "timeZone is null");
        this.executor = new ErrorCodedExecutor(executor);
        checkArgument(maxOutstandingSplits >= 1, "maxOutstandingSplits must be at least 1");
        this.maxOutstandingSplits = maxOutstandingSplits;
        this.minPartitionBatchSize = minPartitionBatchSize;
        this.maxPartitionBatchSize = maxPartitionBatchSize;
        this.maxSplitSize = checkNotNull(maxSplitSize, "maxSplitSize is null");
        this.maxInitialSplitSize = checkNotNull(maxInitialSplitSize, "maxInitialSplitSize is null");
        this.maxInitialSplits = maxInitialSplits;
        this.forceLocalScheduling = forceLocalScheduling;
        this.recursiveDfsWalkerEnabled = recursiveDfsWalkerEnabled;
        this.assumeCanonicalPartitionKeys = assumeCanonicalPartitionKeys;
    }

    @Override
    public ConnectorPartitionResult getPartitions(ConnectorTableHandle tableHandle, TupleDomain<ColumnHandle> effectivePredicate)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkNotNull(effectivePredicate, "effectivePredicate is null");

        if (effectivePredicate.isNone()) {
            return new ConnectorPartitionResult(ImmutableList.of(), TupleDomain.none());
        }

        SchemaTableName tableName = schemaTableName(tableHandle);
        Table table = getTable(tableName);
        Optional<HiveBucketing.HiveBucket> bucket = getHiveBucket(table, effectivePredicate.extractFixedValues());

        TupleDomain<HiveColumnHandle> compactEffectivePredicate = toCompactTupleDomain(effectivePredicate);

        if (table.getPartitionKeys().isEmpty()) {
            return new ConnectorPartitionResult(ImmutableList.of(new HivePartition(tableName, compactEffectivePredicate, bucket)), effectivePredicate);
        }

        List<HiveColumnHandle> partitionColumns = getPartitionKeyColumnHandles(connectorId, table, 0);
        List<String> partitionNames = getFilteredPartitionNames(tableName, partitionColumns, effectivePredicate);

        // do a final pass to filter based on fields that could not be used to filter the partitions
        ImmutableList.Builder<ConnectorPartition> partitions = ImmutableList.builder();
        for (String partitionName : partitionNames) {
            Optional<Map<ColumnHandle, SerializableNativeValue>> values = parseValuesAndFilterPartition(partitionName, partitionColumns, effectivePredicate);

            if (values.isPresent()) {
                partitions.add(new HivePartition(tableName, compactEffectivePredicate, partitionName, values.get(), bucket));
            }
        }

        // All partition key domains will be fully evaluated, so we don't need to include those
        TupleDomain<ColumnHandle> remainingTupleDomain = TupleDomain.withColumnDomains(Maps.filterKeys(effectivePredicate.getDomains(), not(Predicates.<ColumnHandle>in(partitionColumns))));
        return new ConnectorPartitionResult(partitions.build(), remainingTupleDomain);
    }

    private static TupleDomain<HiveColumnHandle> toCompactTupleDomain(TupleDomain<ColumnHandle> effectivePredicate)
    {
        ImmutableMap.Builder<HiveColumnHandle, Domain> builder = ImmutableMap.builder();
        for (Map.Entry<ColumnHandle, Domain> entry : effectivePredicate.getDomains().entrySet()) {
            HiveColumnHandle hiveColumnHandle = checkType(entry.getKey(), HiveColumnHandle.class, "ConnectorColumnHandle");

            SortedRangeSet ranges = entry.getValue().getRanges();
            if (!ranges.isNone()) {
                // compact the range to a single span
                ranges = SortedRangeSet.of(entry.getValue().getRanges().getSpan());
            }

            builder.put(hiveColumnHandle, new Domain(ranges, entry.getValue().isNullAllowed()));
        }
        return TupleDomain.withColumnDomains(builder.build());
    }

    private Optional<Map<ColumnHandle, SerializableNativeValue>> parseValuesAndFilterPartition(String partitionName, List<HiveColumnHandle> partitionColumns, TupleDomain<ColumnHandle> predicate)
    {
        List<String> partitionValues = extractPartitionKeyValues(partitionName);

        ImmutableMap.Builder<ColumnHandle, SerializableNativeValue> builder = ImmutableMap.builder();
        for (int i = 0; i < partitionColumns.size(); i++) {
            HiveColumnHandle column = partitionColumns.get(i);
            SerializableNativeValue parsedValue = parsePartitionValue(partitionName, partitionValues.get(i), column.getHiveType(), timeZone);

            Domain allowedDomain = predicate.getDomains().get(column);
            if (allowedDomain != null && !allowedDomain.includesValue(parsedValue.getValue())) {
                return Optional.empty();
            }
            builder.put(column, parsedValue);
        }

        return Optional.of(builder.build());
    }

    private Table getTable(SchemaTableName tableName)
    {
        try {
            Table table = metastore.getTable(tableName.getSchemaName(), tableName.getTableName());

            String protectMode = table.getParameters().get(ProtectMode.PARAMETER_NAME);
            if (protectMode != null && getProtectModeFromString(protectMode).offline) {
                throw new TableOfflineException(tableName);
            }

            String prestoOffline = table.getParameters().get(PRESTO_OFFLINE);
            if (!isNullOrEmpty(prestoOffline)) {
                throw new TableOfflineException(tableName, format("Table '%s' is offline for Presto: %s", tableName, prestoOffline));
            }

            return table;
        }
        catch (NoSuchObjectException e) {
            throw new TableNotFoundException(tableName);
        }
    }

    private List<String> getFilteredPartitionNames(SchemaTableName tableName, List<HiveColumnHandle> partitionKeys, TupleDomain<ColumnHandle> effectivePredicate)
    {
        List<String> filter = new ArrayList<>();
        for (HiveColumnHandle partitionKey : partitionKeys) {
            Domain domain = effectivePredicate.getDomains().get(partitionKey);
            if (domain != null && domain.isNullableSingleValue()) {
                Comparable<?> value = domain.getNullableSingleValue();
                if (value == null) {
                    filter.add(HivePartitionKey.HIVE_DEFAULT_DYNAMIC_PARTITION);
                }
                else if (value instanceof Slice) {
                    filter.add(((Slice) value).toStringUtf8());
                }
                else if ((value instanceof Boolean) || (value instanceof Double) || (value instanceof Long)) {
                    if (assumeCanonicalPartitionKeys) {
                        filter.add(value.toString());
                    }
                    else {
                        // Hive treats '0', 'false', and 'False' the same. However, the metastore differentiates between these.
                        filter.add(PARTITION_VALUE_WILDCARD);
                    }
                }
                else {
                    throw new PrestoException(NOT_SUPPORTED, "Only Boolean, Double and Long partition keys are supported");
                }
            }
            else {
                filter.add(PARTITION_VALUE_WILDCARD);
            }
        }

        try {
            // fetch the partition names
            return metastore.getPartitionNamesByParts(tableName.getSchemaName(), tableName.getTableName(), filter);
        }
        catch (NoSuchObjectException e) {
            throw new TableNotFoundException(tableName);
        }
    }

    private static List<String> extractPartitionKeyValues(String partitionName)
    {
        ImmutableList.Builder<String> values = ImmutableList.builder();

        boolean inKey = true;
        int valueStart = -1;
        for (int i = 0; i < partitionName.length(); i++) {
            char current = partitionName.charAt(i);
            if (inKey) {
                checkArgument(current != '/', "Invalid partition spec: %s", partitionName);
                if (current == '=') {
                    inKey = false;
                    valueStart = i + 1;
                }
            }
            else if (current == '/') {
                checkArgument(valueStart != -1, "Invalid partition spec: %s", partitionName);
                values.add(FileUtils.unescapePathName(partitionName.substring(valueStart, i)));
                inKey = true;
                valueStart = -1;
            }
        }
        checkArgument(!inKey, "Invalid partition spec: %s", partitionName);
        values.add(FileUtils.unescapePathName(partitionName.substring(valueStart, partitionName.length())));

        return values.build();
    }

    @Override
    public ConnectorSplitSource getPartitionSplits(ConnectorTableHandle tableHandle, List<ConnectorPartition> connectorPartitions)
    {
        HiveTableHandle hiveTableHandle = checkType(tableHandle, HiveTableHandle.class, "tableHandle");

        checkNotNull(connectorPartitions, "connectorPartitions is null");
        List<HivePartition> partitions = Lists.transform(connectorPartitions, partition -> checkType(partition, HivePartition.class, "partition"));

        HivePartition partition = Iterables.getFirst(partitions, null);
        if (partition == null) {
            return new FixedSplitSource(connectorId, ImmutableList.<ConnectorSplit>of());
        }
        SchemaTableName tableName = partition.getTableName();
        Optional<HiveBucketing.HiveBucket> bucket = partition.getBucket();

        // sort partitions
        partitions = Ordering.natural().onResultOf(ConnectorPartition::getPartitionId).reverse().sortedCopy(partitions);

        Table table;
        Iterable<HivePartitionMetadata> hivePartitions;
        try {
            table = metastore.getTable(tableName.getSchemaName(), tableName.getTableName());
            hivePartitions = getPartitionMetadata(table, tableName, partitions);
        }
        catch (NoSuchObjectException e) {
            throw new TableNotFoundException(tableName);
        }

        HiveSplitLoader hiveSplitLoader = new BackgroundHiveSplitLoader(
                connectorId,
                table,
                hivePartitions,
                bucket,
                maxSplitSize,
                hiveTableHandle.getSession(),
                hdfsEnvironment,
                namenodeStats,
                directoryLister,
                executor,
                maxPartitionBatchSize,
                maxInitialSplitSize,
                maxInitialSplits,
                forceLocalScheduling,
                recursiveDfsWalkerEnabled);

        HiveSplitSource splitSource = new HiveSplitSource(connectorId, maxOutstandingSplits, hiveSplitLoader);
        hiveSplitLoader.start(splitSource);

        return splitSource;
    }

    private Iterable<HivePartitionMetadata> getPartitionMetadata(Table table, SchemaTableName tableName, List<HivePartition> partitions)
            throws NoSuchObjectException
    {
        if (partitions.isEmpty()) {
            return ImmutableList.of();
        }

        if (partitions.size() == 1) {
            HivePartition firstPartition = getOnlyElement(partitions);
            if (firstPartition.getPartitionId().equals(UNPARTITIONED_ID)) {
                return ImmutableList.of(new HivePartitionMetadata(firstPartition, UNPARTITIONED_PARTITION));
            }
        }

        Iterable<List<HivePartition>> partitionNameBatches = partitionExponentially(partitions, minPartitionBatchSize, maxPartitionBatchSize);
        Iterable<List<HivePartitionMetadata>> partitionBatches = transform(partitionNameBatches, new Function<List<HivePartition>, List<HivePartitionMetadata>>()
        {
            @Override
            public List<HivePartitionMetadata> apply(List<HivePartition> partitionBatch)
            {
                Exception exception = null;
                for (int attempt = 0; attempt < 10; attempt++) {
                    try {
                        Map<String, Partition> partitions = metastore.getPartitionsByNames(
                                tableName.getSchemaName(),
                                tableName.getTableName(),
                                Lists.transform(partitionBatch, ConnectorPartition::getPartitionId));
                        checkState(partitionBatch.size() == partitions.size(), "expected %s partitions but found %s", partitionBatch.size(), partitions.size());

                        ImmutableList.Builder<HivePartitionMetadata> results = ImmutableList.builder();
                        for (HivePartition hivePartition : partitionBatch) {
                            Partition partition = partitions.get(hivePartition.getPartitionId());
                            checkState(partition != null, "Partition %s was not loaded", hivePartition.getPartitionId());

                            // verify all partition is online
                            String protectMode = partition.getParameters().get(ProtectMode.PARAMETER_NAME);
                            String partName = makePartName(table.getPartitionKeys(), partition.getValues());
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
                    }
                    catch (PrestoException | NoSuchObjectException | NullPointerException | IllegalStateException | IllegalArgumentException e) {
                        throw Throwables.propagate(e);
                    }
                    catch (MetaException | RuntimeException e) {
                        exception = e;
                        log.debug("getPartitions attempt %s failed, will retry. Exception: %s", attempt, e.getMessage());
                    }

                    try {
                        TimeUnit.SECONDS.sleep(1);
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw Throwables.propagate(e);
                    }
                }
                assert exception != null; // impossible
                throw Throwables.propagate(exception);
            }
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
            this.delegate = checkNotNull(delegate, "delegate is null");
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
