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
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.Domain;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SerializableNativeValue;
import com.facebook.presto.spi.SortedRangeSet;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.airlift.slice.Slice;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.metastore.ProtectMode;
import org.apache.hadoop.hive.metastore.api.Table;
import org.joda.time.DateTimeZone;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.hive.HiveBucketing.getHiveBucket;
import static com.facebook.presto.hive.HiveUtil.getPartitionKeyColumnHandles;
import static com.facebook.presto.hive.HiveUtil.parsePartitionValue;
import static com.facebook.presto.hive.util.Types.checkType;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Predicates.not;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.hive.metastore.ProtectMode.getProtectModeFromString;

public class HivePartitionManager
{
    public static final String PRESTO_OFFLINE = "presto_offline";
    private static final String PARTITION_VALUE_WILDCARD = "";

    private final String connectorId;
    private final DateTimeZone timeZone;
    private final boolean assumeCanonicalPartitionKeys;
    private final int domainCompactionThreshold;

    @Inject
    public HivePartitionManager(
            HiveConnectorId connectorId,
            HiveClientConfig hiveClientConfig)
    {
        this(connectorId,
                hiveClientConfig.getDateTimeZone(),
                hiveClientConfig.getMaxOutstandingSplits(),
                hiveClientConfig.isAssumeCanonicalPartitionKeys(),
                hiveClientConfig.getDomainCompactionThreshold());
    }

    public HivePartitionManager(
            HiveConnectorId connectorId,
            DateTimeZone timeZone,
            int maxOutstandingSplits,
            boolean assumeCanonicalPartitionKeys,
            int domainCompactionThreshold)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.timeZone = requireNonNull(timeZone, "timeZone is null");
        checkArgument(maxOutstandingSplits >= 1, "maxOutstandingSplits must be at least 1");
        this.assumeCanonicalPartitionKeys = assumeCanonicalPartitionKeys;
        checkArgument(domainCompactionThreshold >= 1, "domainCompactionThreshold must be at least 1");
        this.domainCompactionThreshold = domainCompactionThreshold;
    }

    public HivePartitionResult getPartitions(ConnectorSession session, HiveMetastore metastore, ConnectorTableHandle tableHandle, TupleDomain<ColumnHandle> effectivePredicate)
    {
        HiveTableHandle hiveTableHandle = checkType(tableHandle, HiveTableHandle.class, "tableHandle");
        requireNonNull(effectivePredicate, "effectivePredicate is null");

        if (effectivePredicate.isNone()) {
            return new HivePartitionResult(ImmutableList.of(), TupleDomain.none(), TupleDomain.none());
        }

        SchemaTableName tableName = hiveTableHandle.getSchemaTableName();
        Table table = getTable(metastore, tableName);
        Optional<HiveBucketing.HiveBucket> bucket = getHiveBucket(table, effectivePredicate.extractFixedValues());

        TupleDomain<HiveColumnHandle> compactEffectivePredicate = toCompactTupleDomain(effectivePredicate, domainCompactionThreshold);

        if (table.getPartitionKeys().isEmpty()) {
            return new HivePartitionResult(ImmutableList.of(new HivePartition(tableName, compactEffectivePredicate, bucket)), effectivePredicate, TupleDomain.none());
        }

        List<HiveColumnHandle> partitionColumns = getPartitionKeyColumnHandles(connectorId, table);
        List<String> partitionNames = getFilteredPartitionNames(metastore, tableName, partitionColumns, effectivePredicate);

        // do a final pass to filter based on fields that could not be used to filter the partitions
        ImmutableList.Builder<HivePartition> partitions = ImmutableList.builder();
        for (String partitionName : partitionNames) {
            Optional<Map<ColumnHandle, SerializableNativeValue>> values = parseValuesAndFilterPartition(partitionName, partitionColumns, effectivePredicate);

            if (values.isPresent()) {
                partitions.add(new HivePartition(tableName, compactEffectivePredicate, partitionName, values.get(), bucket));
            }
        }

        // All partition key domains will be fully evaluated, so we don't need to include those
        TupleDomain<ColumnHandle> remainingTupleDomain = TupleDomain.withColumnDomains(Maps.filterKeys(effectivePredicate.getDomains(), not(Predicates.in(partitionColumns))));
        TupleDomain<ColumnHandle> enforcedTupleDomain = TupleDomain.withColumnDomains(Maps.filterKeys(effectivePredicate.getDomains(), Predicates.in(partitionColumns)));
        return new HivePartitionResult(partitions.build(), remainingTupleDomain, enforcedTupleDomain);
    }

    private static TupleDomain<HiveColumnHandle> toCompactTupleDomain(TupleDomain<ColumnHandle> effectivePredicate, int threshold)
    {
        ImmutableMap.Builder<HiveColumnHandle, Domain> builder = ImmutableMap.builder();
        for (Map.Entry<ColumnHandle, Domain> entry : effectivePredicate.getDomains().entrySet()) {
            HiveColumnHandle hiveColumnHandle = checkType(entry.getKey(), HiveColumnHandle.class, "ConnectorColumnHandle");

            SortedRangeSet ranges = entry.getValue().getRanges();
            if (ranges.getRangeCount() > threshold) {
                // compact the range to a single span
                ranges = SortedRangeSet.of(ranges.getSpan());
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

    private Table getTable(HiveMetastore metastore, SchemaTableName tableName)
    {
        Optional<Table> target = metastore.getTable(tableName.getSchemaName(), tableName.getTableName());
        if (!target.isPresent()) {
            throw new TableNotFoundException(tableName);
        }
        Table table = target.get();

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

    private List<String> getFilteredPartitionNames(HiveMetastore metastore, SchemaTableName tableName, List<HiveColumnHandle> partitionKeys, TupleDomain<ColumnHandle> effectivePredicate)
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

        // fetch the partition names
        return metastore.getPartitionNamesByParts(tableName.getSchemaName(), tableName.getTableName(), filter)
                .orElseThrow(() -> new TableNotFoundException(tableName));
    }

    public static List<String> extractPartitionKeyValues(String partitionName)
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
