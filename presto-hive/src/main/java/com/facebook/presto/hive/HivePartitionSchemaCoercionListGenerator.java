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
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.hive.parquet.ParquetReaderConstants;
import com.facebook.presto.orc.OrcReaderConstants;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_PARTITION_SCHEMA_MISMATCH;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class HivePartitionSchemaCoercionListGenerator
        implements PartitionSchemaCoercionListGenerator
{
    private final boolean isUseParquetColumnNamesFromConfig;
    private final boolean isUseOrcColumnNamesFromConfig;
    private final CoercionPolicy coercionPolicy;
    private List<String> readByNameSerdeList;

    @Inject
    public HivePartitionSchemaCoercionListGenerator(CoercionPolicy coercionPolicy, HiveClientConfig hiveClientConfig)
    {
        this.coercionPolicy = requireNonNull(coercionPolicy, "coercionPolicy is null");
        this.isUseParquetColumnNamesFromConfig = requireNonNull(hiveClientConfig, "hiveClientConfig is null").isUseParquetColumnNames();
        this.isUseOrcColumnNamesFromConfig = requireNonNull(hiveClientConfig, "hiveClientConfig is null").isUseOrcColumnNames();
        this.readByNameSerdeList = requireNonNull(hiveClientConfig, "hiveClientConfig is null").getReadAccessByNameSerdeList();
    }

    @Override
    public ImmutableMap<Integer, HiveTypeName> validateAndCoercePartitionSchema(Table table,
            SchemaTableName tableName, Partition partition, String partName) throws PrestoException
    {
        List<Column> tableColumns = table.getDataColumns();
        List<Column> partitionColumns = partition.getColumns();
        if ((tableColumns == null) || (partitionColumns == null)) {
            throw new PrestoException(HIVE_INVALID_METADATA, format("Table '%s' or partition '%s' has null columns", tableName, partName));
        }

        ImmutableMap.Builder<Integer, HiveTypeName> columnCoercions;
        if (shouldVerifyColumnDatatypesByIndex(table)) {
            columnCoercions = verifyColumnDataTypesByIndex(tableColumns, tableName, partitionColumns, partName);
        }
        else {
            columnCoercions = verifyColumnDataTypesByName(tableColumns, tableName, partitionColumns, partName);
        }
        return columnCoercions.build();
    }

    // Check whether the table and the associated reader reads by name or by ordering (index).
    // This will determine what type of validation we perform on the partitions.
    private boolean shouldVerifyColumnDatatypesByIndex(Table table)
    {
        String serdeClassName = table.getStorage().getStorageFormat().getSerDeNullable();
        if (ParquetReaderConstants.PARQUET_SERDE_CLASS_NAMES.contains(serdeClassName)) {
            return getReadPropertyFromSchemaOrConfig(table.getStorage().getSerdeParameters(), ParquetReaderConstants.PARQUET_COLUMN_INDEX_ACCESS,
                    !isUseParquetColumnNamesFromConfig);
        }
        else if (OrcReaderConstants.ORC_SERDE_CLASS_NAMES.contains(serdeClassName)) {
            return getReadPropertyFromSchemaOrConfig(table.getStorage().getSerdeParameters(), OrcReaderConstants.ORC_COLUMN_INDEX_ACCESS,
                    !isUseOrcColumnNamesFromConfig);
        }
        //For SerDes that always read by Name like JSON, specified in config
        if (readByNameSerdeList.contains(serdeClassName)) {
            return false;
        }
        //Default: Read by Index
        return true;
    }

    private boolean getReadPropertyFromSchemaOrConfig(Map<String, String> tableSerdeParameters, String schemaProperty, boolean configValue)
    {
        String accessValue = tableSerdeParameters.get(schemaProperty);
        if (accessValue != null) {
            return Boolean.parseBoolean(accessValue);
        }
        return configValue;
    }

    // Verify that the partitions schema is compatible with the tables schema
    // for file formats that require or can be configured to read columns
    // in a certain order, like CSV. Compatibility is maintained by
    // either adding or dropping columns from the end of the table
    // without modifying existing partitions is allowed, but every
    // column that exists in both the table and partition must either
    // be the same or can be coerced to another data type.
    private ImmutableMap.Builder<Integer, HiveTypeName> verifyColumnDataTypesByIndex(List<Column> tableColumns, SchemaTableName tableName,
            List<Column> partitionColumns, String partName)
    {
        ImmutableMap.Builder<Integer, HiveTypeName> columnCoercions = ImmutableMap.builder();
        for (int i = 0; i < min(partitionColumns.size(), tableColumns.size()); i++) {
            HiveType tableType = tableColumns.get(i).getType();
            HiveType partitionType = partitionColumns.get(i).getType();
            if (!tableType.equals(partitionType)) {
                checkDataTypes(tableName, partName, tableColumns, partitionColumns, i, tableType, partitionType);
                columnCoercions.put(i, partitionType.getHiveTypeName());
            }
        }
        return columnCoercions;
    }

    // Verify that the partitions schema is compatible with the tables schema.
    // For file formats that are self-describing where ordering does not
    // matter, like JSON, Parquet and Orc. Every column which matches by name
    // must have the same data type or can be coerced to another data type.
    private ImmutableMap.Builder<Integer, HiveTypeName> verifyColumnDataTypesByName(List<Column> tableColumns, SchemaTableName tableName, List<Column> partitionColumns,
            String partName)
    {
        ImmutableMap.Builder<Integer, HiveTypeName> columnCoercions = ImmutableMap.builder();
        Map<String, HiveType> partitionNameToTypeMapping = partitionColumns.stream().collect(Collectors.toMap(key -> key.getName(), val -> val.getType()));
        for (int i = 0; i < tableColumns.size(); i++) {
            HiveType tableType = tableColumns.get(i).getType();
            HiveType partitionType = partitionNameToTypeMapping.get(tableColumns.get(i).getName());
            if (partitionType != null) {
                if (!tableType.equals(partitionType)) {
                    checkDataTypes(tableName, partName, tableColumns, partitionColumns, i, tableType, partitionType);
                    columnCoercions.put(i, partitionType.getHiveTypeName());
                }
            }
        }
        return columnCoercions;
    }

    private void checkDataTypes(SchemaTableName tableName, String partName, List<Column> tableColumns, List<Column> partitionColumns, int i, HiveType tableType,
            HiveType partitionType)
    {
        if (!coercionPolicy.canCoerce(partitionType, tableType)) {
            throw new PrestoException(HIVE_PARTITION_SCHEMA_MISMATCH, format("" +
                            "There is a mismatch between the table and partition schemas. " +
                            "The types are incompatible and cannot be coerced. " +
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
}
