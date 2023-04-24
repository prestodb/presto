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
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;

import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import java.util.stream.Collectors;

import static com.facebook.presto.hive.HiveUtil.parsePartitionValue;
import static com.facebook.presto.hive.metastore.MetastoreUtil.extractPartitionValues;
import static com.facebook.presto.hudi.HudiMetadata.fromPartitionColumns;
import static com.facebook.presto.hudi.HudiMetadata.toMetastoreContext;
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
            SchemaTableName schemaTableName,
            TupleDomain<ColumnHandle> constraintSummary)
    {
        MetastoreContext metastoreContext = toMetastoreContext(connectorSession);
        Optional<Table> table = metastore.getTable(metastoreContext, schemaTableName.getSchemaName(), schemaTableName.getTableName());
        Verify.verify(table.isPresent());
        List<Column> partitionColumns = table.get().getPartitionColumns();
        if (partitionColumns.isEmpty()) {
            return ImmutableList.of("");
        }

        Map<Column, Domain> partitionPredicate = new HashMap<>();
        Map<ColumnHandle, Domain> domains = constraintSummary.getDomains().orElseGet(ImmutableMap::of);
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
        List<String> partitionNames = metastore.getPartitionNamesByFilter(metastoreContext, schemaTableName.getSchemaName(), schemaTableName.getTableName(), partitionPredicate);
        List<Type> partitionTypes = partitionColumns.stream()
                .map(column -> typeManager.getType(column.getType().getTypeSignature()))
                .collect(toList());

        return partitionNames.stream()
                // Apply extra filters which could not be done by getPartitionNamesByFilter, similar to filtering in HivePartitionManager#getPartitionsIterator
                .filter(partitionName -> parseValuesAndFilterPartition(
                        partitionName,
                        hudiColumnHandles,
                        partitionTypes,
                        constraintSummary))
                .collect(toList());
    }

    private boolean parseValuesAndFilterPartition(
            String partitionName,
            List<HudiColumnHandle> partitionColumns,
            List<Type> partitionColumnTypes,
            TupleDomain<ColumnHandle> constraintSummary)
    {
        if (constraintSummary.isNone()) {
            return false;
        }

        Map<ColumnHandle, Domain> domains = constraintSummary.getDomains().orElseGet(ImmutableMap::of);
        Map<HudiColumnHandle, NullableValue> partitionValues = parsePartition(partitionName, partitionColumns, partitionColumnTypes);
        for (HudiColumnHandle column : partitionColumns) {
            NullableValue value = partitionValues.get(column);
            Domain allowedDomain = domains.get(column);
            if (allowedDomain != null && !allowedDomain.includesNullableValue(value.getValue())) {
                return false;
            }
        }

        return true;
    }

    private static Map<HudiColumnHandle, NullableValue> parsePartition(
            String partitionName,
            List<HudiColumnHandle> partitionColumns,
            List<Type> partitionColumnTypes)
    {
        List<String> partitionColumnNames = partitionColumns.stream()
                .map(HudiColumnHandle::getName)
                .collect(Collectors.toList());
        List<String> partitionValues = extractPartitionValues(partitionName, Optional.of(partitionColumnNames));
        ImmutableMap.Builder<HudiColumnHandle, NullableValue> builder = ImmutableMap.builder();
        for (int i = 0; i < partitionColumns.size(); i++) {
            HudiColumnHandle column = partitionColumns.get(i);
            NullableValue parsedValue = parsePartitionValue(partitionName, partitionValues.get(i), partitionColumnTypes.get(i), ZoneId.of(TimeZone.getDefault().getID()));
            builder.put(column, parsedValue);
        }
        return builder.build();
    }
}
