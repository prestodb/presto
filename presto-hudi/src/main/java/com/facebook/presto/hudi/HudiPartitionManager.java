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

package com.facebook.presto.hudi;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.NullableValue;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.MetastoreContext;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.PrestoException;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;

import java.time.ZoneId;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_EXCEEDED_PARTITION_LIMIT;
import static com.facebook.presto.hive.HiveUtil.parsePartitionValue;
import static com.facebook.presto.hive.metastore.MetastoreUtil.extractPartitionValues;
import static com.facebook.presto.hudi.HudiMetadata.fromPartitionColumns;
import static com.facebook.presto.hudi.HudiMetadata.toMetastoreContext;
import static com.facebook.presto.hudi.HudiSessionProperties.getMaxPartitionsPerScan;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class HudiPartitionManager
{
    private final TypeManager typeManager;

    @Inject
    public HudiPartitionManager(TypeManager typeManager)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    public List<String> getEffectivePartitions(
            ConnectorSession connectorSession,
            ExtendedHiveMetastore metastore,
            String schemaName,
            String tableName,
            TupleDomain<ColumnHandle> tupleDomain)
    {
        MetastoreContext metastoreContext = toMetastoreContext(connectorSession);
        Optional<Table> table = metastore.getTable(metastoreContext, schemaName, tableName);
        Verify.verify(table.isPresent());
        List<Column> partitionColumns = table.get().getPartitionColumns();
        if (partitionColumns.isEmpty()) {
            return ImmutableList.of("");
        }

        Map<Column, Domain> partitionPredicate = new HashMap<>();
        Map<ColumnHandle, Domain> domains = tupleDomain.getDomains().orElse(ImmutableMap.of());

        List<HudiColumnHandle> hudiColumnHandles = fromPartitionColumns(partitionColumns);

        for (int i = 0; i < hudiColumnHandles.size(); i++) {
            HudiColumnHandle column = hudiColumnHandles.get(i);
            Column partitionColumn = partitionColumns.get(i);
            if (domains.containsKey(column)) {
                partitionPredicate.put(partitionColumn, domains.get(column));
            }
            else {
                partitionPredicate.put(partitionColumn, Domain.all(column.getHiveType().getType(typeManager)));
            }
        }

        // fetch partition names from metastore
        List<String> partitionNames = metastore.getPartitionNamesByFilter(metastoreContext, schemaName, tableName, partitionPredicate);
        List<Type> partitionTypes = partitionColumns.stream()
                .map(column -> column.getType().getType(typeManager))
                .collect(toList());
        Constraint<ColumnHandle> constraint = new Constraint<>(tupleDomain);
        // match partition and get partition only we need
        Iterable<HudiPartition> partitionsIterable = () -> partitionNames.stream()
                .map(partitionName -> parseValuesAndFilterPartition(table.get(), partitionName, hudiColumnHandles, partitionTypes, constraint))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .iterator();
        return getPartitionsAsList(partitionsIterable.iterator(), connectorSession).stream()
                .map(HudiPartition::getPartitionName)
                .collect(toList());
    }

    private Optional<HudiPartition> parseValuesAndFilterPartition(
            Table table,
            String partitionId,
            List<HudiColumnHandle> partitionColumns,
            List<Type> partitionColumnTypes,
            Constraint<ColumnHandle> constraint)
    {
        HudiPartition partition = parsePartition(table, partitionId, partitionColumns, partitionColumnTypes, ZoneId.of("UTC"));

        Map<ColumnHandle, Domain> domains = constraint.getSummary().getDomains().get();
        for (HudiColumnHandle column : partitionColumns) {
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

    public static HudiPartition parsePartition(
            Table table,
            String partitionName,
            List<HudiColumnHandle> partitionColumns,
            List<Type> partitionColumnTypes,
            ZoneId timeZone)
    {
        List<String> partitionColumnNames = partitionColumns.stream()
                .map(HudiColumnHandle::getName)
                .collect(Collectors.toList());
        List<String> partitionValues = extractPartitionValues(partitionName, Optional.of(partitionColumnNames));
        ImmutableMap.Builder<ColumnHandle, NullableValue> builder = ImmutableMap.builder();
        for (int i = 0; i < partitionColumns.size(); i++) {
            HudiColumnHandle column = partitionColumns.get(i);
            NullableValue parsedValue = parsePartitionValue(partitionName, partitionValues.get(i), partitionColumnTypes.get(i), timeZone);
            builder.put(column, parsedValue);
        }
        Map<ColumnHandle, NullableValue> values = builder.build();
        HudiPartition hudiPartition = new HudiPartition(table.getTableName(), partitionName, ImmutableList.of(), ImmutableMap.of(), table.getStorage(), ImmutableList.of());
        hudiPartition.setKeys(values);
        return hudiPartition;
    }

    private List<HudiPartition> getPartitionsAsList(Iterator<HudiPartition> partitionsIterator, ConnectorSession connectorSession)
    {
        ImmutableList.Builder<HudiPartition> partitionList = ImmutableList.builder();
        int count = 0;
        int maxPartitionsPerScan = getMaxPartitionsPerScan(connectorSession);
        while (partitionsIterator.hasNext()) {
            HudiPartition partition = partitionsIterator.next();
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
}
