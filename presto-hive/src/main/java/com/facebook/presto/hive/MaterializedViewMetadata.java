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

import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.MetastoreUtil;
import com.facebook.presto.hive.metastore.SemiTransactionalHiveMetastore;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_INVALID_PARTITION_VALUE;
import static com.facebook.presto.hive.metastore.MetastoreUtil.HIVE_DEFAULT_DYNAMIC_PARTITION;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;

public class MaterializedViewMetadata
{
    private final Table view;
    private final List<Table> baseTables;
    private final PartitionSpecs partitionsFromView;
    private final Map<SchemaTableName, PartitionSpecs> partitionsFromBaseTables;

    private MaterializedViewMetadata(Table view, List<Table> baseTables, PartitionSpecs partitionsFromView, Map<SchemaTableName, PartitionSpecs> partitionsFromBaseTables)
    {
        this.view = requireNonNull(view, "viewName is null");
        this.baseTables = unmodifiableList(new ArrayList<>(requireNonNull(baseTables, "baseTableNames is null")));
        this.partitionsFromView = requireNonNull(partitionsFromView, "partitionsFromView is null");
        this.partitionsFromBaseTables = unmodifiableMap(new HashMap<>(requireNonNull(partitionsFromBaseTables, "partitionsFromBaseTables is null")));
    }

    public static MaterializedViewMetadata from(Table view, List<Table> baseTables, Map<String, Map<SchemaTableName, String>> viewToBaseColumnMap, SemiTransactionalHiveMetastore hiveMetastore)
    {
        Map<SchemaTableName, Map<String, String>> viewToBasePartitionMap = getViewToBasePartitionMap(view, baseTables, viewToBaseColumnMap);

        PartitionSpecs partitionsFromView = getPartitionSpecs(view, hiveMetastore);
        // Partitions to keep track of for materialized view freshness are the partitions of every base table
        // that are not available/updated to the materialized view yet. Compute it by partition specs "difference".
        Map<SchemaTableName, PartitionSpecs> partitionsFromBaseTables = baseTables.stream()
                .collect(Collectors.toMap(
                        baseTable -> new SchemaTableName(baseTable.getDatabaseName(), baseTable.getTableName()),
                        baseTable -> PartitionSpecs.difference(
                                getPartitionSpecs(baseTable, hiveMetastore),
                                partitionsFromView,
                                viewToBasePartitionMap.getOrDefault(new SchemaTableName(baseTable.getDatabaseName(), baseTable.getTableName()), emptyMap()))));

        return new MaterializedViewMetadata(view, baseTables, partitionsFromView, partitionsFromBaseTables);
    }

    public PartitionSpecs getPartitionsFromView()
    {
        return this.partitionsFromView;
    }

    public PartitionSpecs getPartitionsFromBaseTable(SchemaTableName baseTable)
    {
        return this.partitionsFromBaseTables.get(baseTable);
    }

    private static PartitionSpecs getPartitionSpecs(Table table, SemiTransactionalHiveMetastore metastore)
    {
        List<Column> partitionColumns = table.getPartitionColumns();
        // Hive Metastore getPartitionNames API returns partition names in natural order.
        List<String> partitionNamesInOrder = metastore.getPartitionNames(table.getDatabaseName(), table.getTableName())
                .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(table.getDatabaseName(), table.getTableName())));

        List<Map<String, String>> partitionSpecsInOrder = metastore.getPartitionNames(table.getDatabaseName(), table.getTableName())
                .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(table.getDatabaseName(), table.getTableName())))
                .stream()
                .map(MetastoreUtil::toPartitionSpec)
                .collect(Collectors.toList());

        List<Map<String, String>> partitionSpecs = new ArrayList<>();
        for (String partitionName : partitionNamesInOrder) {
            Map<String, String> partitionSpec = MetastoreUtil.toPartitionSpec(partitionName);
            if (partitionColumns.size() != partitionSpec.size()) {
                throw new PrestoException(HIVE_INVALID_METADATA, String.format("Expected %d partition key values, but got %d", partitionColumns.size(), partitionSpec.size()));
            }
            for (int i = 0; i < partitionColumns.size(); i++) {
                String name = partitionColumns.get(i).getName();
                HiveType hiveType = partitionColumns.get(i).getType();
                if (!hiveType.isSupportedType()) {
                    throw new PrestoException(NOT_SUPPORTED, String.format("Unsupported Hive type %s found in partition keys of table %s.%s", hiveType, table.getDatabaseName(), table.getTableName()));
                }
                if (partitionSpec.get(name) == null) {
                    throw new PrestoException(HIVE_INVALID_PARTITION_VALUE, String.format("partition key value cannot be null for field: %s", name));
                }
                partitionSpec.replace(name, HIVE_DEFAULT_DYNAMIC_PARTITION, "\\N");
            }
            partitionSpecs.add(unmodifiableMap(partitionSpec));
        }

        return new PartitionSpecs(partitionSpecs, partitionColumns.stream().map(Column::getName).collect(Collectors.toList()));
    }

    private static Map<SchemaTableName, Map<String, String>> getViewToBasePartitionMap(Table view, List<Table> baseTables, Map<String, Map<SchemaTableName, String>> viewToBaseColumnMap)
    {
        List<String> viewPartitions = view.getPartitionColumns()
                .stream()
                .map(Column::getName)
                .collect(Collectors.toList());

        Map<SchemaTableName, List<String>> baseTablePartitions = baseTables.stream()
                .collect(Collectors.toMap(
                        table -> new SchemaTableName(table.getDatabaseName(), table.getTableName()),
                        table -> table.getPartitionColumns().stream().map(Column::getName).collect(Collectors.toList())));

        Map<SchemaTableName, Map<String, String>> viewToBasePartitionMap = new HashMap<>();
        for (SchemaTableName baseTable : baseTablePartitions.keySet()) {
            Map<String, String> partitionMap = new HashMap<>();
            for (String viewPart : viewPartitions) {
                for (String basePart : baseTablePartitions.get(baseTable)) {
                    if (viewToBaseColumnMap.containsKey(viewPart) &&
                            viewToBaseColumnMap.get(viewPart).containsKey(baseTable) &&
                            viewToBaseColumnMap.get(viewPart).get(baseTable).equals(basePart)) {
                        partitionMap.put(viewPart, basePart);
                    }
                }
            }
            if (!partitionMap.isEmpty()) {
                viewToBasePartitionMap.put(baseTable, unmodifiableMap(partitionMap));
            }
        }

        return unmodifiableMap(viewToBasePartitionMap);
    }

    private static final class PartitionSpecs
    {
        private final List<Map<String, String>> partitions;
        private final List<String> partitionKeys;

        public PartitionSpecs(List<Map<String, String>> partitions, List<String> keys)
        {
            this.partitions = unmodifiableList(new ArrayList<>(requireNonNull(partitions, "partitionSpecs is null")));
            this.partitionKeys = unmodifiableList(new ArrayList<>(requireNonNull(keys, "keys is null")));
        }

        public static PartitionSpecs emptySpecs()
        {
            return new PartitionSpecs(emptyList(), emptyList());
        }

        public static PartitionSpecs difference(PartitionSpecs leftSpecs, PartitionSpecs rightSpecs, Map<String, String> rightToLeftPartitionKeyMap)
        {
            if (rightToLeftPartitionKeyMap.isEmpty()) {
                return emptySpecs();
            }
            if (rightSpecs.partitions.isEmpty()) {
                return leftSpecs;
            }

            Set<String> leftMappedCommonKeys = new HashSet<>();
            Set<String> rightMappedCommonKeys = new HashSet<>();
            for (String rightKey : rightSpecs.partitionKeys) {
                String leftKey = rightToLeftPartitionKeyMap.get(rightKey);
                if (leftKey != null && leftSpecs.partitionKeys.contains(leftKey)) {
                    leftMappedCommonKeys.add(leftKey);
                    rightMappedCommonKeys.add(rightKey);
                }
            }
            if (leftMappedCommonKeys.isEmpty()) {
                return emptySpecs();
            }

            Set<Map<String, String>> rightSpecsMappedToLeftCommonKeys = new HashSet<>();
            for (Map<String, String> rightSpec : rightSpecs.partitions) {
                Map<String, String> rightSpecMappedToLeftCommonKeys = rightSpec.entrySet()
                        .stream()
                        .filter(entry -> rightMappedCommonKeys.contains(entry.getKey()))
                        .collect(Collectors.toMap(
                                entry -> rightToLeftPartitionKeyMap.get(entry.getKey()),
                                entry -> entry.getValue()));
                rightSpecsMappedToLeftCommonKeys.add(rightSpecMappedToLeftCommonKeys);
            }

            List<Map<String, String>> leftMinusRight = new ArrayList<>();
            for (Map<String, String> leftSpec : leftSpecs.partitions) {
                Map<String, String> leftSpecMappedToLeftCommonKeys = leftSpec.entrySet()
                        .stream()
                        .filter(entry -> leftMappedCommonKeys.contains(entry.getKey()))
                        .collect(Collectors.toMap(
                                entry -> entry.getKey(),
                                entry -> entry.getValue()));
                if (!rightSpecsMappedToLeftCommonKeys.contains(leftSpecMappedToLeftCommonKeys)) {
                    leftMinusRight.add(leftSpec);
                }
            }

            return new PartitionSpecs(leftMinusRight, leftSpecs.partitionKeys);
        }

        public boolean isEmpty()
        {
            return partitions.isEmpty();
        }
    }
}
