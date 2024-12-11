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

import com.facebook.airlift.concurrent.ThreadPoolExecutorMBean;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.NullableValue;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.hive.HiveBucketing.HiveBucketFilter;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.MetastoreContext;
import com.facebook.presto.hive.metastore.SemiTransactionalHiveMetastore;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.google.common.base.Predicates;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.joda.time.DateTimeZone;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.hive.HiveBucketing.getHiveBucketFilter;
import static com.facebook.presto.hive.HiveBucketing.getHiveBucketHandle;
import static com.facebook.presto.hive.HiveColumnHandle.BUCKET_COLUMN_NAME;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_EXCEEDED_PARTITION_LIMIT;
import static com.facebook.presto.hive.HiveSessionProperties.getMaxBucketsForGroupedExecution;
import static com.facebook.presto.hive.HiveSessionProperties.getMinBucketCountToNotIgnoreTableBucketing;
import static com.facebook.presto.hive.HiveSessionProperties.isLegacyTimestampBucketing;
import static com.facebook.presto.hive.HiveSessionProperties.isOfflineDataDebugModeEnabled;
import static com.facebook.presto.hive.HiveSessionProperties.isParallelParsingOfPartitionValuesEnabled;
import static com.facebook.presto.hive.HiveSessionProperties.shouldIgnoreTableBucketing;
import static com.facebook.presto.hive.HiveUtil.getPartitionKeyColumnHandles;
import static com.facebook.presto.hive.HiveUtil.parsePartitionValue;
import static com.facebook.presto.hive.metastore.MetastoreUtil.extractPartitionValues;
import static com.facebook.presto.hive.metastore.MetastoreUtil.getMetastoreHeaders;
import static com.facebook.presto.hive.metastore.MetastoreUtil.getProtectMode;
import static com.facebook.presto.hive.metastore.MetastoreUtil.isUserDefinedTypeEncodingEnabled;
import static com.facebook.presto.hive.metastore.MetastoreUtil.makePartName;
import static com.facebook.presto.hive.metastore.MetastoreUtil.verifyOnline;
import static com.facebook.presto.hive.metastore.PrestoTableType.TEMPORARY_TABLE;
import static com.facebook.presto.spi.Constraint.alwaysTrue;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class HivePartitionManager
{
    private static final Logger log = Logger.get(HivePartitionManager.class);
    private static final int PARTITION_NAMES_BATCH_SIZE = 500;

    private final DateTimeZone timeZone;
    private final boolean assumeCanonicalPartitionKeys;
    private final TypeManager typeManager;
    private final int maxPartitionsPerScan;
    private final int domainCompactionThreshold;
    private final boolean partitionFilteringFromMetastoreEnabled;
    private final ListeningExecutorService executorService;
    private final ThreadPoolExecutorMBean executorServiceMBean;

    @Inject
    public HivePartitionManager(
            TypeManager typeManager,
            HiveClientConfig hiveClientConfig)
    {
        this(
                typeManager,
                hiveClientConfig.getDateTimeZone(),
                hiveClientConfig.isAssumeCanonicalPartitionKeys(),
                hiveClientConfig.getMaxPartitionsPerScan(),
                hiveClientConfig.getDomainCompactionThreshold(),
                hiveClientConfig.isPartitionFilteringFromMetastoreEnabled(),
                hiveClientConfig.getMaxParallelParsingConcurrency());
    }

    public HivePartitionManager(
            TypeManager typeManager,
            DateTimeZone timeZone,
            boolean assumeCanonicalPartitionKeys,
            int maxPartitionsPerScan,
            int domainCompactionThreshold,
            boolean partitionFilteringFromMetastoreEnabled,
            int maxParallelParsingConcurrency)
    {
        this.timeZone = requireNonNull(timeZone, "timeZone is null");
        this.assumeCanonicalPartitionKeys = assumeCanonicalPartitionKeys;
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.maxPartitionsPerScan = maxPartitionsPerScan;
        checkArgument(domainCompactionThreshold >= 1, "domainCompactionThreshold must be at least 1");
        this.domainCompactionThreshold = domainCompactionThreshold;
        this.partitionFilteringFromMetastoreEnabled = partitionFilteringFromMetastoreEnabled;
        ExecutorService threadPoolExecutor = new ThreadPoolExecutor(0, maxParallelParsingConcurrency,
                60L, TimeUnit.SECONDS, new LinkedBlockingQueue<>(), daemonThreadsNamed("partition-value-parser-%s"));
        this.executorService = listeningDecorator(threadPoolExecutor);
        this.executorServiceMBean = new ThreadPoolExecutorMBean((ThreadPoolExecutor) threadPoolExecutor);
    }

    public List<HivePartition> getPartitionsList(
            SemiTransactionalHiveMetastore metastore,
            ConnectorTableHandle tableHandle,
            Constraint<ColumnHandle> constraint,
            ConnectorSession session)
    {
        HiveTableHandle hiveTableHandle = (HiveTableHandle) tableHandle;
        TupleDomain<ColumnHandle> effectivePredicateColumnHandles = constraint.getSummary();

        SchemaTableName tableName = hiveTableHandle.getSchemaTableName();
        Table table = getTable(session, metastore, hiveTableHandle, isOfflineDataDebugModeEnabled(session));

        List<HiveColumnHandle> partitionColumns = getPartitionKeyColumnHandles(table);

        List<Type> partitionTypes = partitionColumns.stream()
                .map(column -> typeManager.getType(column.getTypeSignature()))
                .collect(toList());

        Map<Column, Domain> effectivePredicate = createPartitionPredicates(
                metastore,
                session,
                effectivePredicateColumnHandles,
                partitionColumns,
                assumeCanonicalPartitionKeys);

        if (partitionColumns.isEmpty()) {
            return ImmutableList.of(new HivePartition(tableName));
        }
        else {
            List<PartitionNameWithVersion> partitionNames = partitionFilteringFromMetastoreEnabled ? getFilteredPartitionNames(session, metastore, hiveTableHandle, effectivePredicate) : getAllPartitionNames(session, metastore, hiveTableHandle, constraint);

            if (isParallelParsingOfPartitionValuesEnabled(session) && partitionNames.size() > PARTITION_NAMES_BATCH_SIZE) {
                List<List<PartitionNameWithVersion>> partitionNameBatches = Lists.partition(partitionNames, PARTITION_NAMES_BATCH_SIZE);
                // Use ConcurrentLinkedQueue to prevent race condition when multiple threads try to add partitions to this list
                ConcurrentLinkedQueue<HivePartition> result = new ConcurrentLinkedQueue<>();
                List<ListenableFuture<?>> futures = new ArrayList<>();
                try {
                    partitionNameBatches.forEach(batch -> futures.add(executorService.submit(() -> result.addAll(getPartitionListFromPartitionNames(batch, tableName, partitionColumns, partitionTypes, constraint)))));
                    Futures.transform(Futures.allAsList(futures), input -> result, directExecutor()).get();
                    return Arrays.asList(result.toArray(new HivePartition[0]));
                }
                catch (InterruptedException | ExecutionException e) {
                    log.error(e, "Parallel parsing of partition values failed");
                }
            }
            return getPartitionListFromPartitionNames(partitionNames, tableName, partitionColumns, partitionTypes, constraint);
        }
    }

    private List<HivePartition> getPartitionListFromPartitionNames(
            List<PartitionNameWithVersion> partitionNames,
            SchemaTableName tableName,
            List<HiveColumnHandle> partitionColumns,
            List<Type> partitionTypes,
            Constraint<ColumnHandle> constraint)
    {
        return partitionNames.stream()
                // Apply extra filters which could not be done by getFilteredPartitionNames
                .map(partitionName -> parseValuesAndFilterPartition(tableName, partitionName, partitionColumns, partitionTypes, constraint))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toImmutableList());
    }

    private Map<Column, Domain> createPartitionPredicates(
            SemiTransactionalHiveMetastore metastore,
            ConnectorSession session,
            TupleDomain<ColumnHandle> effectivePredicateColumnHandles,
            List<HiveColumnHandle> partitionColumns,
            boolean assumeCanonicalPartitionKeys)
    {
        Optional<Map<ColumnHandle, Domain>> domains = effectivePredicateColumnHandles.getDomains();
        if (domains.isPresent()) {
            Map<ColumnHandle, Domain> columnHandleDomainMap = domains.get();
            ImmutableMap.Builder<Column, Domain> partitionPredicateBuilder = ImmutableMap.builder();
            MetastoreContext metastoreContext = new MetastoreContext(session.getIdentity(), session.getQueryId(), session.getClientInfo(), session.getClientTags(), session.getSource(), getMetastoreHeaders(session), isUserDefinedTypeEncodingEnabled(session), metastore.getColumnConverterProvider(), session.getWarningCollector(), session.getRuntimeStats());
            for (HiveColumnHandle partitionColumn : partitionColumns) {
                Column key = new Column(
                        partitionColumn.getName(),
                        partitionColumn.getHiveType(),
                        partitionColumn.getComment(),
                        metastoreContext.getColumnConverter().getTypeMetadata(partitionColumn.getHiveType(), partitionColumn.getTypeSignature()));

                if (columnHandleDomainMap.containsKey(partitionColumn)) {
                    if (assumeCanonicalPartitionKeys) {
                        partitionPredicateBuilder.put(key, columnHandleDomainMap.get(partitionColumn));
                    }
                    else {
                        Type type = typeManager.getType(partitionColumn.getTypeSignature());
                        if (type instanceof VarcharType || type instanceof CharType) {
                            partitionPredicateBuilder.put(key, columnHandleDomainMap.get(partitionColumn));
                        }
                        else {
                            Domain allDomain = Domain.all(typeManager.getType(partitionColumn.getTypeSignature()));
                            partitionPredicateBuilder.put(key, allDomain);
                        }
                    }
                }
                else {
                    Domain allDomain = Domain.all(typeManager.getType(partitionColumn.getTypeSignature()));
                    partitionPredicateBuilder.put(key, allDomain);
                }
            }
            return partitionPredicateBuilder.build();
        }
        else {
            return ImmutableMap.of();
        }
    }

    public HivePartitionResult getPartitions(SemiTransactionalHiveMetastore metastore, ConnectorTableHandle tableHandle, Constraint<ColumnHandle> constraint, ConnectorSession session)
    {
        HiveTableHandle hiveTableHandle = (HiveTableHandle) tableHandle;
        TupleDomain<ColumnHandle> effectivePredicate = constraint.getSummary();

        Table table = getTable(session, metastore, hiveTableHandle, isOfflineDataDebugModeEnabled(session));

        List<HiveColumnHandle> partitionColumns = getPartitionKeyColumnHandles(table);

        List<HivePartition> partitions = getPartitionsList(metastore, tableHandle, constraint, session);
        if (partitions.size() > maxPartitionsPerScan) {
            throw new PrestoException(HIVE_EXCEEDED_PARTITION_LIMIT, format(
                    "Query over table '%s' can potentially read more than %s partitions",
                    hiveTableHandle.getSchemaTableName().toString(),
                    maxPartitionsPerScan));
        }

        Optional<HiveBucketHandle> hiveBucketHandle = getBucketHandle(table, session, effectivePredicate);
        Optional<HiveBucketFilter> bucketFilter = hiveBucketHandle.flatMap(value -> getHiveBucketFilter(table, effectivePredicate, isLegacyTimestampBucketing(session)));

        if (!queryUsesHiveBucketColumn(effectivePredicate)
                && hiveBucketHandle.isPresent()
                && queryAccessesTooManyBuckets(hiveBucketHandle.get(), bucketFilter, partitions, session)) {
            hiveBucketHandle = Optional.empty();
            bucketFilter = Optional.empty();
        }

        if (effectivePredicate.isNone()) {
            return new HivePartitionResult(
                    ImmutableList.copyOf(partitionColumns),
                    table.getDataColumns(),
                    table.getParameters(),
                    partitions,
                    TupleDomain.none(),
                    TupleDomain.none(),
                    TupleDomain.none(),
                    hiveBucketHandle,
                    Optional.empty());
        }

        TupleDomain<ColumnHandle> compactEffectivePredicate = effectivePredicate.compact(domainCompactionThreshold);

        if (partitionColumns.isEmpty()) {
            return new HivePartitionResult(
                    ImmutableList.copyOf(partitionColumns),
                    table.getDataColumns(),
                    table.getParameters(),
                    partitions,
                    compactEffectivePredicate,
                    effectivePredicate,
                    TupleDomain.all(),
                    hiveBucketHandle,
                    bucketFilter);
        }

        // All partition key domains will be fully evaluated, so we don't need to include those
        TupleDomain<ColumnHandle> remainingTupleDomain = TupleDomain.withColumnDomains(Maps.filterKeys(effectivePredicate.getDomains().get(), not(Predicates.in(partitionColumns))));
        TupleDomain<ColumnHandle> enforcedTupleDomain = TupleDomain.withColumnDomains(Maps.filterKeys(effectivePredicate.getDomains().get(), Predicates.in(partitionColumns)));
        return new HivePartitionResult(
                ImmutableList.copyOf(partitionColumns),
                table.getDataColumns(),
                table.getParameters(),
                partitions,
                compactEffectivePredicate,
                remainingTupleDomain,
                enforcedTupleDomain,
                hiveBucketHandle,
                bucketFilter);
    }

    private Optional<HiveBucketHandle> getBucketHandle(
            Table table,
            ConnectorSession session,
            TupleDomain<ColumnHandle> effectivePredicate)
    {
        // never ignore table bucketing for temporary tables as those are created such explicitly by the engine request
        if (table.getTableType().equals(TEMPORARY_TABLE)) {
            return getHiveBucketHandle(session, table);
        }

        Optional<HiveBucketHandle> hiveBucketHandle = getHiveBucketHandle(session, table);
        if (!hiveBucketHandle.isPresent() || shouldIgnoreTableBucketing(session)) {
            return Optional.empty();
        }

        if (queryUsesHiveBucketColumn(effectivePredicate)) {
            return hiveBucketHandle;
        }

        int requiredTableBucketCount = getMinBucketCountToNotIgnoreTableBucketing(session);
        if (hiveBucketHandle.get().getTableBucketCount() < requiredTableBucketCount) {
            return Optional.empty();
        }

        return hiveBucketHandle;
    }

    private boolean queryUsesHiveBucketColumn(TupleDomain<ColumnHandle> effectivePredicate)
    {
        if (!effectivePredicate.getDomains().isPresent()) {
            return false;
        }
        return effectivePredicate.getDomains().get().keySet().stream().anyMatch(key -> ((HiveColumnHandle) key).getName().equals(BUCKET_COLUMN_NAME));
    }

    private boolean queryAccessesTooManyBuckets(HiveBucketHandle handle, Optional<HiveBucketFilter> filter, List<HivePartition> partitions, ConnectorSession session)
    {
        int bucketsPerPartition = filter.map(hiveBucketFilter -> hiveBucketFilter.getBucketsToKeep().size())
                .orElseGet(handle::getReadBucketCount);
        return bucketsPerPartition * partitions.size() > getMaxBucketsForGroupedExecution(session);
    }

    public HivePartitionResult getPartitions(SemiTransactionalHiveMetastore metastore, ConnectorTableHandle tableHandle, List<List<String>> partitionValuesList, ConnectorSession session)
    {
        HiveTableHandle hiveTableHandle = (HiveTableHandle) tableHandle;
        SchemaTableName tableName = hiveTableHandle.getSchemaTableName();

        Table table = getTable(session, metastore, hiveTableHandle, isOfflineDataDebugModeEnabled(session));

        List<HiveColumnHandle> partitionColumns = getPartitionKeyColumnHandles(table);
        List<Type> partitionColumnTypes = partitionColumns.stream()
                .map(column -> typeManager.getType(column.getTypeSignature()))
                .collect(toImmutableList());

        List<HivePartition> partitionList = partitionValuesList.stream()
                .map(partitionValues -> makePartName(table.getPartitionColumns(), partitionValues))
                .map(partitionName -> parseValuesAndFilterPartition(tableName, new PartitionNameWithVersion(partitionName, Optional.empty()), partitionColumns, partitionColumnTypes, alwaysTrue()))
                .map(partition -> partition.orElseThrow(() -> new VerifyException("partition must exist")))
                .collect(toImmutableList());

        Optional<HiveBucketHandle> bucketHandle = shouldIgnoreTableBucketing(session) ? Optional.empty() : getHiveBucketHandle(session, table);
        return new HivePartitionResult(
                ImmutableList.copyOf(partitionColumns),
                table.getDataColumns(),
                table.getParameters(),
                partitionList,
                TupleDomain.all(),
                TupleDomain.all(),
                TupleDomain.none(),
                bucketHandle,
                Optional.empty());
    }

    private Optional<HivePartition> parseValuesAndFilterPartition(
            SchemaTableName tableName,
            PartitionNameWithVersion partitionNameWithVersion,
            List<HiveColumnHandle> partitionColumns,
            List<Type> partitionColumnTypes,
            Constraint<ColumnHandle> constraint)
    {
        HivePartition partition = parsePartition(tableName, partitionNameWithVersion, partitionColumns, partitionColumnTypes, timeZone);

        Map<ColumnHandle, Domain> domains = constraint.getSummary().getDomains().get();
        for (HiveColumnHandle column : partitionColumns) {
            NullableValue value = partition.getKeys().get(column);
            Domain allowedDomain = domains.get(column);
            if (allowedDomain != null && !allowedDomain.includesNullableValue(value.getValue())) {
                return Optional.empty();
            }
        }

        if (constraint.predicate().isPresent() && !constraint.predicate().get().test(partition.getKeys())) {
            return Optional.empty();
        }

        return Optional.of(partition);
    }

    private Table getTable(ConnectorSession session, SemiTransactionalHiveMetastore metastore, HiveTableHandle hiveTableHandle, boolean offlineDataDebugModeEnabled)
    {
        MetastoreContext context = new MetastoreContext(session.getIdentity(), session.getQueryId(), session.getClientInfo(), session.getClientTags(), session.getSource(), getMetastoreHeaders(session), isUserDefinedTypeEncodingEnabled(session), metastore.getColumnConverterProvider(), session.getWarningCollector(), session.getRuntimeStats());
        Optional<Table> target = metastore.getTable(context, hiveTableHandle);
        if (!target.isPresent()) {
            throw new TableNotFoundException(hiveTableHandle.getSchemaTableName());
        }
        Table table = target.get();

        if (!offlineDataDebugModeEnabled) {
            verifyOnline(hiveTableHandle.getSchemaTableName(), Optional.empty(), getProtectMode(table), table.getParameters());
        }

        return table;
    }

    private List<PartitionNameWithVersion> getFilteredPartitionNames(ConnectorSession session, SemiTransactionalHiveMetastore metastore, HiveTableHandle hiveTableHandle, Map<Column, Domain> partitionPredicates)
    {
        if (partitionPredicates.isEmpty()) {
            return ImmutableList.of();
        }

        MetastoreContext context = new MetastoreContext(session.getIdentity(), session.getQueryId(), session.getClientInfo(), session.getClientTags(), session.getSource(), getMetastoreHeaders(session), isUserDefinedTypeEncodingEnabled(session), metastore.getColumnConverterProvider(), session.getWarningCollector(), session.getRuntimeStats());
        // fetch the partition names
        return metastore.getPartitionNamesByFilter(context, hiveTableHandle, partitionPredicates)
                .orElseThrow(() -> new TableNotFoundException(hiveTableHandle.getSchemaTableName()));
    }

    private List<PartitionNameWithVersion> getAllPartitionNames(ConnectorSession session, SemiTransactionalHiveMetastore metastore, HiveTableHandle hiveTableHandle, Constraint<ColumnHandle> constraint)
    {
        if (constraint.getSummary().isNone()) {
            return ImmutableList.of();
        }
        // fetch the partition names
        MetastoreContext context = new MetastoreContext(session.getIdentity(), session.getQueryId(), session.getClientInfo(), session.getClientTags(), session.getSource(), getMetastoreHeaders(session), isUserDefinedTypeEncodingEnabled(session), metastore.getColumnConverterProvider(), session.getWarningCollector(), session.getRuntimeStats());
        return metastore.getPartitionNames(context, hiveTableHandle)
                .orElseThrow(() -> new TableNotFoundException(hiveTableHandle.getSchemaTableName()));
    }

    public static HivePartition parsePartition(
            SchemaTableName tableName,
            PartitionNameWithVersion partitionNameWithVersion,
            List<HiveColumnHandle> partitionColumns,
            List<Type> partitionColumnTypes,
            DateTimeZone timeZone)
    {
        List<String> partitionColumnNames = partitionColumns.stream()
                .map(HiveColumnHandle::getName)
                .collect(Collectors.toList());
        List<String> partitionValues = extractPartitionValues(partitionNameWithVersion.getPartitionName(), Optional.of(partitionColumnNames));
        ImmutableMap.Builder<ColumnHandle, NullableValue> builder = ImmutableMap.builder();
        for (int i = 0; i < partitionColumns.size(); i++) {
            HiveColumnHandle column = partitionColumns.get(i);
            NullableValue parsedValue = parsePartitionValue(partitionNameWithVersion.getPartitionName(), partitionValues.get(i), partitionColumnTypes.get(i), timeZone);
            builder.put(column, parsedValue);
        }
        Map<ColumnHandle, NullableValue> values = builder.build();
        return new HivePartition(tableName, partitionNameWithVersion, values);
    }

    @Managed
    @Nested
    public ThreadPoolExecutorMBean getExecutor()
    {
        return executorServiceMBean;
    }
}
