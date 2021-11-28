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

import com.facebook.presto.common.predicate.NullableValue;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.MetastoreContext;
import com.facebook.presto.hive.metastore.SemiTransactionalHiveMetastore;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.ConnectorMaterializedViewDefinition;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.joda.time.DateTimeZone;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.common.predicate.TupleDomain.extractFixedValues;
import static com.facebook.presto.common.predicate.TupleDomain.toLinkedMap;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_INVALID_PARTITION_VALUE;
import static com.facebook.presto.hive.HiveUtil.getPartitionKeyColumnHandles;
import static com.facebook.presto.hive.HiveUtil.parsePartitionValue;
import static com.facebook.presto.hive.metastore.MetastoreUtil.toPartitionNamesAndValues;
import static com.facebook.presto.spi.MaterializedViewStatus.MaterializedDataPredicates;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.lang.String.format;
import static java.util.Collections.emptyMap;

public class HiveMaterializedViewUtils
{
    private static final MaterializedDataPredicates EMPTY_MATERIALIZED_VIEW_DATA_PREDICATES = new MaterializedDataPredicates(ImmutableList.of(), ImmutableList.of());

    private HiveMaterializedViewUtils()
    {
    }

    /**
     * Validate the partition columns of a materialized view to ensure 1) a materialized view is partitioned; and 2) it has at least one partition
     * directly mapped to all base tables.
     * <p>
     * A column is directly mapped to a base table column if it is derived directly or transitively from the base table column,
     * by only selecting a column or an aliased column without any function or operator applied.
     * For example, with SELECT column_b AS column_a, column_a is directly mapped to column_b.
     * With SELECT column_b + column_c AS column_a, column_a is not directly mapped to any column.
     * <p>
     * {@code viewToBaseColumnMap} only contains direct column mappings.
     */
    public static void validateMaterializedViewPartitionColumns(
            SemiTransactionalHiveMetastore metastore,
            MetastoreContext metastoreContext,
            Table viewTable,
            ConnectorMaterializedViewDefinition viewDefinition)
    {
        SchemaTableName viewName = new SchemaTableName(viewTable.getDatabaseName(), viewTable.getTableName());

        Map<String, Map<SchemaTableName, String>> viewToBaseColumnMap = viewDefinition.getColumnMappingsAsMap();
        if (viewToBaseColumnMap.isEmpty()) {
            throw new PrestoException(
                    NOT_SUPPORTED,
                    format("Materialized view %s must have at least one column directly defined by a base table column.", viewName));
        }

        List<Column> viewPartitions = viewTable.getPartitionColumns();
        if (viewPartitions.isEmpty()) {
            throw new PrestoException(NOT_SUPPORTED, "Unpartitioned materialized view is not supported.");
        }

        viewDefinition.getBaseTables().stream()
                .map(baseTableName -> metastore.getTable(metastoreContext, baseTableName.getSchemaName(), baseTableName.getTableName())
                        .orElseThrow(() -> new TableNotFoundException(baseTableName)))
                .forEach(table -> {
                    if (!isCommonPartitionFound(table, viewPartitions, viewToBaseColumnMap)) {
                        throw new PrestoException(
                                NOT_SUPPORTED,
                                format("Materialized view %s must have at least partition to base table partition mapping for all base tables.", viewName));
                    }
                });
    }

    private static boolean isCommonPartitionFound(
            Table baseTable,
            List<Column> viewPartitions,
            Map<String, Map<SchemaTableName, String>> viewToBaseColumnMap)
    {
        SchemaTableName baseTableName = new SchemaTableName(baseTable.getDatabaseName(), baseTable.getTableName());
        for (Column viewPartition : viewPartitions) {
            String viewPartitionMapToBaseTablePartition = viewToBaseColumnMap
                    .getOrDefault(viewPartition.getName(), emptyMap())
                    .getOrDefault(baseTableName, "");
            if (baseTable.getPartitionColumns().stream().anyMatch(baseTablePartition -> baseTablePartition.getName().equals(viewPartitionMapToBaseTablePartition))) {
                return true;
            }
        }
        return false;
    }

    public static MaterializedDataPredicates getMaterializedDataPredicates(
            SemiTransactionalHiveMetastore metastore,
            MetastoreContext metastoreContext,
            TypeManager typeManager,
            Table table,
            DateTimeZone timeZone)
    {
        List<Column> partitionColumns = table.getPartitionColumns();

        for (Column partitionColumn : partitionColumns) {
            HiveType hiveType = partitionColumn.getType();
            if (!hiveType.isSupportedType()) {
                throw new PrestoException(
                        NOT_SUPPORTED,
                        String.format("Unsupported Hive type %s found in partition keys of table %s.%s", hiveType, table.getDatabaseName(), table.getTableName()));
            }
        }

        List<HiveColumnHandle> partitionKeyColumnHandles = getPartitionKeyColumnHandles(table);
        Map<String, Type> partitionTypes = partitionKeyColumnHandles.stream()
                .collect(toImmutableMap(HiveColumnHandle::getName, column -> typeManager.getType(column.getTypeSignature())));

        List<String> partitionNames = metastore.getPartitionNames(metastoreContext, table.getDatabaseName(), table.getTableName())
                .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(table.getDatabaseName(), table.getTableName())));

        ImmutableList.Builder<TupleDomain<String>> partitionNamesAndValues = ImmutableList.builder();
        for (String partitionName : partitionNames) {
            ImmutableMap.Builder<String, NullableValue> partitionNameAndValuesMap = ImmutableMap.builder();
            Map<String, String> partitions = toPartitionNamesAndValues(partitionName);
            if (partitionColumns.size() != partitions.size()) {
                throw new PrestoException(HIVE_INVALID_METADATA, String.format(
                        "Expected %d partition key values, but got %d", partitionColumns.size(), partitions.size()));
            }
            partitionTypes.forEach((name, type) -> {
                String value = partitions.get(name);
                if (value == null) {
                    throw new PrestoException(HIVE_INVALID_PARTITION_VALUE, String.format("partition key value cannot be null for field: %s", name));
                }

                partitionNameAndValuesMap.put(name, parsePartitionValue(name, value, type, timeZone));
            });

            TupleDomain<String> tupleDomain = TupleDomain.fromFixedValues(partitionNameAndValuesMap.build());
            partitionNamesAndValues.add(tupleDomain);
        }

        return new MaterializedDataPredicates(partitionNamesAndValues.build(), partitionColumns.stream()
                .map(Column::getName)
                .collect(toImmutableList()));
    }

    public static MaterializedDataPredicates differenceDataPredicates(
            MaterializedDataPredicates leftPredicatesInfo,
            MaterializedDataPredicates rightPredicatesInfo,
            Map<String, String> rightToLeftPredicatesKeyMap)
    {
        if (rightToLeftPredicatesKeyMap.isEmpty()) {
            return EMPTY_MATERIALIZED_VIEW_DATA_PREDICATES;
        }
        if (rightPredicatesInfo.isEmpty()) {
            return leftPredicatesInfo;
        }

        Set<String> leftMappedCommonKeys = new HashSet<>();
        Set<String> rightMappedCommonKeys = new HashSet<>();
        for (String rightKey : rightPredicatesInfo.getColumnNames()) {
            String leftKey = rightToLeftPredicatesKeyMap.get(rightKey);
            if (leftKey != null && leftPredicatesInfo.getColumnNames().contains(leftKey)) {
                leftMappedCommonKeys.add(leftKey);
                rightMappedCommonKeys.add(rightKey);
            }
        }
        if (leftMappedCommonKeys.isEmpty()) {
            return EMPTY_MATERIALIZED_VIEW_DATA_PREDICATES;
        }

        // Intentionally used linkedHashMap so that equal guarantees are kept even if underlying implementation is changed.
        Set<LinkedHashMap<String, NullableValue>> rightPredicatesMappedToLeftCommonKeys = new HashSet<>();
        for (TupleDomain<String> rightPredicate : rightPredicatesInfo.getPredicateDisjuncts()) {
            LinkedHashMap<String, NullableValue> rightPredicateKeyValue = getLinkedHashMap(
                    extractFixedValues(rightPredicate).orElseThrow(() -> new IllegalStateException("rightPredicateKeyValue is not present!")));

            LinkedHashMap<String, NullableValue> rightPredicateMappedToLeftCommonKeys = getLinkedHashMap(
                    rightPredicateKeyValue.keySet().stream()
                            .filter(rightMappedCommonKeys::contains)
                            .collect(toLinkedMap(
                                    rightToLeftPredicatesKeyMap::get,
                                    rightPredicateKeyValue::get)));

            rightPredicatesMappedToLeftCommonKeys.add(rightPredicateMappedToLeftCommonKeys);
        }

        ImmutableList.Builder<TupleDomain<String>> leftMinusRight = ImmutableList.builder();

        for (TupleDomain<String> leftPredicate : leftPredicatesInfo.getPredicateDisjuncts()) {
            LinkedHashMap<String, NullableValue> leftPredicateKeyValue = getLinkedHashMap(
                    extractFixedValues(leftPredicate).orElseThrow(() -> new IllegalStateException("leftPredicateKeyValue is not present!")));

            LinkedHashMap<String, NullableValue> leftPredicateMappedToLeftCommonKeys = getLinkedHashMap(
                    leftPredicateKeyValue.keySet().stream()
                            .filter(leftMappedCommonKeys::contains)
                            .collect(Collectors.toMap(
                                    columnName -> columnName,
                                    leftPredicateKeyValue::get)));

            if (!rightPredicatesMappedToLeftCommonKeys.contains(leftPredicateMappedToLeftCommonKeys)) {
                leftMinusRight.add(leftPredicate);
            }
        }

        return new MaterializedDataPredicates(leftMinusRight.build(), leftPredicatesInfo.getColumnNames());
    }

    public static MaterializedDataPredicates getEmptyMaterializedViewDataPredicates()
    {
        return EMPTY_MATERIALIZED_VIEW_DATA_PREDICATES;
    }

    private static <K, V> LinkedHashMap<K, V> getLinkedHashMap(Map<K, V> map)
    {
        return (map instanceof LinkedHashMap) ? (LinkedHashMap<K, V>) map : new LinkedHashMap<>(map);
    }

    public static Map<SchemaTableName, Map<String, String>> getViewToBasePartitionMap(
            Table view,
            List<Table> baseTables,
            Map<String, Map<SchemaTableName, String>> viewToBaseColumnMap)
    {
        List<String> viewPartitions = view.getPartitionColumns()
                .stream()
                .map(Column::getName)
                .collect(Collectors.toList());

        Map<SchemaTableName, List<String>> baseTablePartitions = baseTables.stream()
                .collect(Collectors.toMap(
                        table -> new SchemaTableName(table.getDatabaseName(), table.getTableName()),
                        table -> table.getPartitionColumns().stream().map(Column::getName).collect(toImmutableList())));

        ImmutableMap.Builder<SchemaTableName, Map<String, String>> viewToBasePartitionMap = ImmutableMap.builder();
        for (SchemaTableName baseTable : baseTablePartitions.keySet()) {
            Map<String, String> partitionMap = new HashMap<>();
            for (String viewPartition : viewPartitions) {
                for (String basePartition : baseTablePartitions.get(baseTable)) {
                    if (viewToBaseColumnMap.containsKey(viewPartition) &&
                            viewToBaseColumnMap.get(viewPartition).containsKey(baseTable) &&
                            viewToBaseColumnMap.get(viewPartition).get(baseTable).equals(basePartition)) {
                        partitionMap.put(viewPartition, basePartition);
                    }
                }
            }
            if (!partitionMap.isEmpty()) {
                viewToBasePartitionMap.put(baseTable, ImmutableMap.copyOf(partitionMap));
            }
        }
        return viewToBasePartitionMap.build();
    }
}
