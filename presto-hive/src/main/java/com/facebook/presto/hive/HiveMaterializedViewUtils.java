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
import java.util.Optional;
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
     * Validate the partition columns of a materialized view to ensure 1) a materialized view is partitioned; 2) it has at least one partition
     * directly mapped to all base tables and 3) Outer join conditions have common partitions that are partitions in the view as well
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

        Map<String, Map<SchemaTableName, String>> viewToBaseDirectColumnMap = viewDefinition.getDirectColumnMappingsAsMap();
        if (viewToBaseDirectColumnMap.isEmpty()) {
            throw new PrestoException(
                    NOT_SUPPORTED,
                    format("Materialized view %s must have at least one column directly defined by a base table column.", viewName));
        }

        List<Column> viewPartitions = viewTable.getPartitionColumns();
        if (viewPartitions.isEmpty()) {
            throw new PrestoException(NOT_SUPPORTED, "Unpartitioned materialized view is not supported.");
        }

        List<Table> baseTables = viewDefinition.getBaseTables().stream()
                .map(baseTableName -> metastore.getTable(metastoreContext, baseTableName.getSchemaName(), baseTableName.getTableName())
                        .orElseThrow(() -> new TableNotFoundException(baseTableName)))
                .collect(toImmutableList());

        Map<Table, List<Column>> baseTablePartitions = baseTables.stream()
                .collect(toImmutableMap(
                        table -> table,
                        Table::getPartitionColumns));

        for (Table baseTable : baseTablePartitions.keySet()) {
            SchemaTableName schemaBaseTable = new SchemaTableName(baseTable.getDatabaseName(), baseTable.getTableName());
            if (!isCommonPartitionFound(schemaBaseTable, baseTablePartitions.get(baseTable), viewPartitions, viewToBaseDirectColumnMap)) {
                throw new PrestoException(
                        NOT_SUPPORTED,
                        format("Materialized view %s must have at least one partition column that exists in %s as well", viewName, baseTable.getTableName()));
            }
            if (viewDefinition.getBaseTablesOnOuterJoinSide().contains(schemaBaseTable) && viewToBaseTableOnOuterJoinSideIndirectMappedPartitions(viewDefinition, baseTable).get().isEmpty()) {
                throw new PrestoException(
                        NOT_SUPPORTED,
                        format("Outer join conditions in Materialized view %s must have at least one common partition equality constraint", viewName));
            }
        }
    }

    private static boolean isCommonPartitionFound(
            SchemaTableName baseTable,
            List<Column> baseTablePartitions,
            List<Column> viewPartitions,
            Map<String, Map<SchemaTableName, String>> viewToBaseColumnMap)
    {
        for (Column viewPartition : viewPartitions) {
            for (Column basePartition : baseTablePartitions) {
                if (viewToBaseColumnMap
                        .getOrDefault(viewPartition.getName(), emptyMap())
                        .getOrDefault(baseTable, "")
                        .equals(basePartition.getName())) {
                    return true;
                }
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

    // Every table on outer join side, must have a partition which is in EQ clause and present in Materialized View as well.
    // For a given base table, this function computes partition columns of Materialized View which are not directly mapped to base table,
    // and are directly mapped to some other base table which is not on outer join side.
    // For example:
    // Materialized View: SELECT t1_a as t1.a, t2_a as t2.a FROM t1 LEFT JOIN t2 ON t1.a = t2.a, partitioned by [t1_a, t2_a]
    // baseTable: t2, partitioned by [a]
    // Output: t1_a -> t2.a
    public static Optional<Map<String, String>> viewToBaseTableOnOuterJoinSideIndirectMappedPartitions(
            ConnectorMaterializedViewDefinition viewDefinition,
            Table baseTable)
    {
        SchemaTableName schemaBaseTable = new SchemaTableName(baseTable.getDatabaseName(), baseTable.getTableName());
        if (!viewDefinition.getBaseTablesOnOuterJoinSide().contains(schemaBaseTable)) {
            return Optional.empty();
        }
        Map<String, String> viewToBaseIndirectMappedColumns = new HashMap<>();

        Map<String, Map<SchemaTableName, String>> columnMappings = viewDefinition.getColumnMappingsAsMap();
        Map<String, Map<SchemaTableName, String>> directColumnMappings = viewDefinition.getDirectColumnMappingsAsMap();

        for (String viewPartition : viewDefinition.getValidRefreshColumns().orElse(ImmutableList.of())) {
            String baseTablePartition = columnMappings.get(viewPartition).get(schemaBaseTable);
            // Check if it is a base table partition column
            if (baseTable.getPartitionColumns().stream().noneMatch(col -> col.getName().equals(baseTablePartition))) {
                continue;
            }
            // Check if view partition column directly maps to some partition column of other base table which is not on outer join side
            // For e.g. in case of left outer join, we want to find partition which maps to left table
            if (directColumnMappings.get(viewPartition).keySet().stream().allMatch(e -> !e.equals(schemaBaseTable)) &&
                    directColumnMappings.get(viewPartition).keySet().stream().allMatch(t -> !viewDefinition.getBaseTablesOnOuterJoinSide().contains(t))) {
                viewToBaseIndirectMappedColumns.put(viewPartition, baseTablePartition);
            }
        }
        return Optional.of(viewToBaseIndirectMappedColumns);
    }

    public static MaterializedDataPredicates differenceDataPredicates(
            MaterializedDataPredicates baseTablePredicatesInfo,
            MaterializedDataPredicates viewPredicatesInfo,
            Map<String, String> viewToBaseTablePredicatesKeyMap)
    {
        return differenceDataPredicates(baseTablePredicatesInfo, viewPredicatesInfo, viewToBaseTablePredicatesKeyMap, ImmutableMap.of());
    }

  /**
   * From given base table partitions, removes all partitions that are already used to compute
   * related view partitions, only returning partitions that are not reflected in Materialized View.
   * We assume that given materialized view partitions are still fresh,
   * and in sync with base table partitions, i.e. related Materialized View partitions are already invalidated
   * when new base table partitions land.
   * @param baseTablePredicatesInfo Partitions info for base table
   * @param viewPredicatesInfo Partitions info for view
   * @param viewToBaseTablePredicatesKeyMap Partitions mapping from view to base table. Only includes direct mapping, i.e. excludes mapping from outer joins EQ clauses.
   * @param viewToBaseTableIndirectMap Extra partitions mapping from view to base table, computed from viewToBaseTableOnOuterJoinSideIndirectMappedPartitions()
   * @return Base Table partitions that have not been used to refresh view.
   */
    public static MaterializedDataPredicates differenceDataPredicates(
            MaterializedDataPredicates baseTablePredicatesInfo,
            MaterializedDataPredicates viewPredicatesInfo,
            Map<String, String> viewToBaseTablePredicatesKeyMap,
            Map<String, String> viewToBaseTableIndirectMap)
    {
        if (viewToBaseTablePredicatesKeyMap.isEmpty()) {
            return EMPTY_MATERIALIZED_VIEW_DATA_PREDICATES;
        }
        if (viewPredicatesInfo.isEmpty()) {
            return baseTablePredicatesInfo;
        }

        Set<String> baseTableMappedCommonKeys = new HashSet<>();
        Set<String> viewMappedCommonKeys = new HashSet<>();
        for (String rightKey : viewPredicatesInfo.getColumnNames()) {
            String leftKey = viewToBaseTablePredicatesKeyMap.get(rightKey);
            if (leftKey != null && baseTablePredicatesInfo.getColumnNames().contains(leftKey)) {
                baseTableMappedCommonKeys.add(leftKey);
                viewMappedCommonKeys.add(rightKey);
            }
        }

        if (baseTableMappedCommonKeys.isEmpty()) {
            return EMPTY_MATERIALIZED_VIEW_DATA_PREDICATES;
        }

        // Intentionally used linkedHashMap so that equal guarantees are kept even if underlying implementation is changed.
        Set<LinkedHashMap<String, NullableValue>> viewPredicatesMappedToBaseTableCommonKeys = new HashSet<>();
        for (TupleDomain<String> rightPredicate : viewPredicatesInfo.getPredicateDisjuncts()) {
            LinkedHashMap<String, NullableValue> viewPredicateKeyValue = getLinkedHashMap(
                    extractFixedValues(rightPredicate).orElseThrow(() -> new IllegalStateException("rightPredicateKeyValue is not present!")));

            LinkedHashMap<String, NullableValue> viewPredicateMappedToBaseTableCommonKeys = getLinkedHashMap(
                    viewPredicateKeyValue.keySet().stream()
                            .filter(viewMappedCommonKeys::contains)
                            .collect(toLinkedMap(
                                    viewToBaseTablePredicatesKeyMap::get,
                                    viewPredicateKeyValue::get)));

            viewToBaseTableIndirectMap.entrySet().forEach(
                    e -> viewPredicateMappedToBaseTableCommonKeys.put(e.getValue(), viewPredicateKeyValue.get(e.getKey())));

            viewPredicatesMappedToBaseTableCommonKeys.add(viewPredicateMappedToBaseTableCommonKeys);
        }

        ImmutableList.Builder<TupleDomain<String>> difference = ImmutableList.builder();

        for (TupleDomain<String> leftPredicate : baseTablePredicatesInfo.getPredicateDisjuncts()) {
            LinkedHashMap<String, NullableValue> baseTablePredicateKeyValue = getLinkedHashMap(
                    extractFixedValues(leftPredicate).orElseThrow(() -> new IllegalStateException("leftPredicateKeyValue is not present!")));

            LinkedHashMap<String, NullableValue> baseTablePredicateMappedToBaseTableCommonKeys = getLinkedHashMap(
                    baseTablePredicateKeyValue.keySet().stream()
                            .filter(baseTableMappedCommonKeys::contains)
                            .collect(Collectors.toMap(
                                    columnName -> columnName,
                                    baseTablePredicateKeyValue::get)));

            if (!viewPredicatesMappedToBaseTableCommonKeys.contains(baseTablePredicateMappedToBaseTableCommonKeys)) {
                difference.add(leftPredicate);
            }
            else if (!viewToBaseTableIndirectMap.isEmpty()) {
                // If base table is part of an outer join, its columns can be null. So check if an equivalent partition exists for view with null values
                LinkedHashMap<String, NullableValue> baseTablePredicateMappedToBaseTableAllKeys = getLinkedHashMap(
                        baseTablePredicateKeyValue.keySet().stream()
                                .filter(baseTableMappedCommonKeys::contains)
                                .collect(Collectors.toMap(
                                        columnName -> columnName,
                                        columnName -> NullableValue.asNull(baseTablePredicateKeyValue.get(columnName).getType()))));

                viewToBaseTableIndirectMap.entrySet().forEach(e -> baseTablePredicateMappedToBaseTableAllKeys.put(e.getValue(), baseTablePredicateKeyValue.get(e.getValue())));

                if (!viewPredicatesMappedToBaseTableCommonKeys.contains(baseTablePredicateMappedToBaseTableCommonKeys)) {
                    difference.add(leftPredicate);
                }
            }
        }

        return new MaterializedDataPredicates(difference.build(), baseTablePredicatesInfo.getColumnNames());
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
