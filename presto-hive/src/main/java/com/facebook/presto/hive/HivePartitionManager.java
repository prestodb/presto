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

import com.facebook.presto.hive.HiveBucketing.HiveBucketFilter;
import com.facebook.presto.hive.metastore.SemiTransactionalHiveMetastore;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.NullableValue;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.CharType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Decimals;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.RealType;
import com.facebook.presto.spi.type.SmallintType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.TinyintType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.base.Predicates;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.airlift.slice.Slice;
import org.apache.hadoop.hive.common.FileUtils;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.hive.HiveBucketing.getHiveBucketFilter;
import static com.facebook.presto.hive.HiveBucketing.getHiveBucketHandle;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_EXCEEDED_PARTITION_LIMIT;
import static com.facebook.presto.hive.HiveSessionProperties.getMaxBucketsForGroupedExecution;
import static com.facebook.presto.hive.HiveSessionProperties.shouldIgnoreTableBucketing;
import static com.facebook.presto.hive.HiveUtil.getPartitionKeyColumnHandles;
import static com.facebook.presto.hive.HiveUtil.parsePartitionValue;
import static com.facebook.presto.hive.metastore.MetastoreUtil.getProtectMode;
import static com.facebook.presto.hive.metastore.MetastoreUtil.makePartName;
import static com.facebook.presto.hive.metastore.MetastoreUtil.verifyOnline;
import static com.facebook.presto.hive.metastore.PrestoTableType.TEMPORARY_TABLE;
import static com.facebook.presto.spi.Constraint.alwaysTrue;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.Chars.padSpaces;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class HivePartitionManager
{
    private static final String PARTITION_VALUE_WILDCARD = "";

    private final DateTimeZone timeZone;
    private final boolean assumeCanonicalPartitionKeys;
    private final TypeManager typeManager;
    private final int maxPartitionsPerScan;
    private final int domainCompactionThreshold;

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
                hiveClientConfig.getDomainCompactionThreshold());
    }

    public HivePartitionManager(
            TypeManager typeManager,
            DateTimeZone timeZone,
            boolean assumeCanonicalPartitionKeys,
            int maxPartitionsPerScan,
            int domainCompactionThreshold)
    {
        this.timeZone = requireNonNull(timeZone, "timeZone is null");
        this.assumeCanonicalPartitionKeys = assumeCanonicalPartitionKeys;
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.maxPartitionsPerScan = maxPartitionsPerScan;
        checkArgument(domainCompactionThreshold >= 1, "domainCompactionThreshold must be at least 1");
        this.domainCompactionThreshold = domainCompactionThreshold;
    }

    public Iterable<HivePartition> getPartitionsIterator(SemiTransactionalHiveMetastore metastore, ConnectorTableHandle tableHandle, Constraint<ColumnHandle> constraint)
    {
        HiveTableHandle hiveTableHandle = (HiveTableHandle) tableHandle;
        TupleDomain<ColumnHandle> effectivePredicate = constraint.getSummary();

        SchemaTableName tableName = hiveTableHandle.getSchemaTableName();
        Table table = getTable(metastore, tableName);

        List<HiveColumnHandle> partitionColumns = getPartitionKeyColumnHandles(table);

        List<Type> partitionTypes = partitionColumns.stream()
                .map(column -> typeManager.getType(column.getTypeSignature()))
                .collect(toList());

        return partitionColumns.isEmpty() ? ImmutableList.of(new HivePartition(tableName)) : () -> getFilteredPartitionNames(metastore, tableName, partitionColumns, effectivePredicate).stream()
                // Apply extra filters which could not be done by getFilteredPartitionNames
                .map(partitionName -> parseValuesAndFilterPartition(tableName, partitionName, partitionColumns, partitionTypes, constraint))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .iterator();
    }

    public HivePartitionResult getPartitions(SemiTransactionalHiveMetastore metastore, ConnectorTableHandle tableHandle, Constraint<ColumnHandle> constraint, ConnectorSession session)
    {
        HiveTableHandle hiveTableHandle = (HiveTableHandle) tableHandle;
        TupleDomain<ColumnHandle> effectivePredicate = constraint.getSummary();

        SchemaTableName tableName = hiveTableHandle.getSchemaTableName();
        Table table = getTable(metastore, tableName);

        List<HiveColumnHandle> partitionColumns = getPartitionKeyColumnHandles(table);

        List<HivePartition> partitions = getPartitionsAsList(getPartitionsIterator(metastore, tableHandle, constraint).iterator());

        // never ignore table bucketing for temporary tables as those are created such explicitly by the engine request
        boolean shouldIgnoreTableBucketing = !table.getTableType().equals(TEMPORARY_TABLE) && shouldIgnoreTableBucketing(session);
        Optional<HiveBucketHandle> hiveBucketHandle = shouldIgnoreTableBucketing ? Optional.empty() : getHiveBucketHandle(table);
        Optional<HiveBucketFilter> bucketFilter = shouldIgnoreTableBucketing ? Optional.empty() : getHiveBucketFilter(table, effectivePredicate);

        if (hiveBucketHandle.isPresent() && hiveBucketHandle.get().getReadBucketCount() * partitions.size() > getMaxBucketsForGroupedExecution(session)) {
            hiveBucketHandle = Optional.empty();
            bucketFilter = Optional.empty();
        }

        if (effectivePredicate.isNone()) {
            return new HivePartitionResult(partitionColumns, partitions, TupleDomain.none(), TupleDomain.none(), TupleDomain.none(), hiveBucketHandle, Optional.empty());
        }

        TupleDomain<ColumnHandle> compactEffectivePredicate = effectivePredicate.compact(domainCompactionThreshold);

        if (partitionColumns.isEmpty()) {
            return new HivePartitionResult(
                    partitionColumns,
                    partitions,
                    compactEffectivePredicate,
                    effectivePredicate,
                    TupleDomain.none(),
                    hiveBucketHandle,
                    bucketFilter);
        }

        // All partition key domains will be fully evaluated, so we don't need to include those
        TupleDomain<ColumnHandle> remainingTupleDomain = TupleDomain.withColumnDomains(Maps.filterKeys(effectivePredicate.getDomains().get(), not(Predicates.in(partitionColumns))));
        TupleDomain<ColumnHandle> enforcedTupleDomain = TupleDomain.withColumnDomains(Maps.filterKeys(effectivePredicate.getDomains().get(), Predicates.in(partitionColumns)));
        return new HivePartitionResult(partitionColumns, partitions, compactEffectivePredicate, remainingTupleDomain, enforcedTupleDomain, hiveBucketHandle, bucketFilter);
    }

    private List<HivePartition> getPartitionsAsList(Iterator<HivePartition> partitionsIterator)
    {
        ImmutableList.Builder<HivePartition> partitionList = ImmutableList.builder();
        int count = 0;
        while (partitionsIterator.hasNext()) {
            HivePartition partition = partitionsIterator.next();
            if (count == maxPartitionsPerScan) {
                throw new PrestoException(HIVE_EXCEEDED_PARTITION_LIMIT, format(
                        "Query over table '%s' can potentially read more than %s partitions",
                        partition.getTableName(),
                        maxPartitionsPerScan));
            }
            partitionList.add(partition);
            count++;
        }
        return partitionList.build();
    }

    public HivePartitionResult getPartitions(SemiTransactionalHiveMetastore metastore, ConnectorTableHandle tableHandle, List<List<String>> partitionValuesList, ConnectorSession session)
    {
        HiveTableHandle hiveTableHandle = (HiveTableHandle) tableHandle;
        SchemaTableName tableName = hiveTableHandle.getSchemaTableName();

        Table table = getTable(metastore, tableName);

        List<HiveColumnHandle> partitionColumns = getPartitionKeyColumnHandles(table);
        List<Type> partitionColumnTypes = partitionColumns.stream()
                .map(column -> typeManager.getType(column.getTypeSignature()))
                .collect(toImmutableList());

        List<HivePartition> partitionList = partitionValuesList.stream()
                .map(partitionValues -> makePartName(table.getPartitionColumns(), partitionValues))
                .map(partitionName -> parseValuesAndFilterPartition(tableName, partitionName, partitionColumns, partitionColumnTypes, alwaysTrue()))
                .map(partition -> partition.orElseThrow(() -> new VerifyException("partition must exist")))
                .collect(toImmutableList());

        Optional<HiveBucketHandle> bucketHandle = shouldIgnoreTableBucketing(session) ? Optional.empty() : getHiveBucketHandle(table);
        return new HivePartitionResult(partitionColumns, partitionList, TupleDomain.all(), TupleDomain.all(), TupleDomain.none(), bucketHandle, Optional.empty());
    }

    private Optional<HivePartition> parseValuesAndFilterPartition(
            SchemaTableName tableName,
            String partitionId,
            List<HiveColumnHandle> partitionColumns,
            List<Type> partitionColumnTypes,
            Constraint<ColumnHandle> constraint)
    {
        HivePartition partition = parsePartition(tableName, partitionId, partitionColumns, partitionColumnTypes, timeZone);

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

    private Table getTable(SemiTransactionalHiveMetastore metastore, SchemaTableName tableName)
    {
        Optional<Table> target = metastore.getTable(tableName.getSchemaName(), tableName.getTableName());
        if (!target.isPresent()) {
            throw new TableNotFoundException(tableName);
        }
        Table table = target.get();
        verifyOnline(tableName, Optional.empty(), getProtectMode(table), table.getParameters());
        return table;
    }

    private List<String> getFilteredPartitionNames(SemiTransactionalHiveMetastore metastore, SchemaTableName tableName, List<HiveColumnHandle> partitionKeys, TupleDomain<ColumnHandle> effectivePredicate)
    {
        if (effectivePredicate.isNone()) {
            return ImmutableList.of();
        }

        List<String> filter = new ArrayList<>();
        for (HiveColumnHandle partitionKey : partitionKeys) {
            Domain domain = effectivePredicate.getDomains().get().get(partitionKey);
            if (domain != null && domain.isNullableSingleValue()) {
                Object value = domain.getNullableSingleValue();
                Type type = domain.getType();
                if (value == null) {
                    filter.add(HivePartitionKey.HIVE_DEFAULT_DYNAMIC_PARTITION);
                }
                else if (type instanceof CharType) {
                    Slice slice = (Slice) value;
                    filter.add(padSpaces(slice, type).toStringUtf8());
                }
                else if (type instanceof VarcharType) {
                    Slice slice = (Slice) value;
                    filter.add(slice.toStringUtf8());
                }
                // Types above this have only a single possible representation for each value.
                // Types below this may have multiple representations for a single value.  For
                // example, a boolean column may represent the false value as "0", "false" or "False".
                // The metastore distinguishes between these representations, so we cannot prune partitions
                // unless we know that all partition values use the canonical Java representation.
                else if (!assumeCanonicalPartitionKeys) {
                    filter.add(PARTITION_VALUE_WILDCARD);
                }
                else if (type instanceof DecimalType && !((DecimalType) type).isShort()) {
                    Slice slice = (Slice) value;
                    filter.add(Decimals.toString(slice, ((DecimalType) type).getScale()));
                }
                else if (type instanceof DecimalType && ((DecimalType) type).isShort()) {
                    filter.add(Decimals.toString((long) value, ((DecimalType) type).getScale()));
                }
                else if (type instanceof DateType) {
                    DateTimeFormatter dateTimeFormatter = ISODateTimeFormat.date().withZoneUTC();
                    filter.add(dateTimeFormatter.print(TimeUnit.DAYS.toMillis((long) value)));
                }
                else if (type instanceof TimestampType) {
                    // we don't have time zone info, so just add a wildcard
                    filter.add(PARTITION_VALUE_WILDCARD);
                }
                else if (type instanceof TinyintType
                        || type instanceof SmallintType
                        || type instanceof IntegerType
                        || type instanceof BigintType
                        || type instanceof DoubleType
                        || type instanceof RealType
                        || type instanceof BooleanType) {
                    filter.add(value.toString());
                }
                else {
                    throw new PrestoException(NOT_SUPPORTED, format("Unsupported partition key type: %s", type.getDisplayName()));
                }
            }
            else {
                filter.add(PARTITION_VALUE_WILDCARD);
            }
        }

        // fetch the partition names
        return metastore.getPartitionNamesByParts(tableName.getSchemaName(), tableName.getTableName(), filter)
                .orElseThrow(() -> new TableNotFoundException(tableName));
    }

    public static HivePartition parsePartition(
            SchemaTableName tableName,
            String partitionName,
            List<HiveColumnHandle> partitionColumns,
            List<Type> partitionColumnTypes,
            DateTimeZone timeZone)
    {
        List<String> partitionValues = extractPartitionValues(partitionName);
        ImmutableMap.Builder<ColumnHandle, NullableValue> builder = ImmutableMap.builder();
        for (int i = 0; i < partitionColumns.size(); i++) {
            HiveColumnHandle column = partitionColumns.get(i);
            NullableValue parsedValue = parsePartitionValue(partitionName, partitionValues.get(i), partitionColumnTypes.get(i), timeZone);
            builder.put(column, parsedValue);
        }
        Map<ColumnHandle, NullableValue> values = builder.build();
        return new HivePartition(tableName, partitionName, values);
    }

    public static List<String> extractPartitionValues(String partitionName)
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
}
