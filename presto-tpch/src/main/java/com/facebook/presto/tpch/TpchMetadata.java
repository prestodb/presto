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
package com.facebook.presto.tpch;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.ReadOnlyConnectorMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.tpch.TpchColumn;
import io.airlift.tpch.TpchEntity;
import io.airlift.tpch.TpchTable;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class TpchMetadata
        extends ReadOnlyConnectorMetadata
{
    public static final String TINY_SCHEMA_NAME = "tiny";
    public static final double TINY_SCALE_FACTOR = 0.01;

    private final String connectorId;
    private final Set<String> tableNames;

    public TpchMetadata(String connectorId)
    {
        ImmutableSet.Builder<String> tableNames = ImmutableSet.builder();
        for (TpchTable<?> tpchTable : TpchTable.getTables()) {
            tableNames.add(tpchTable.getTableName());
        }
        this.tableNames = tableNames.build();
        this.connectorId = connectorId;
    }

    @Override
    public boolean canHandle(TableHandle tableHandle)
    {
        return tableHandle instanceof TpchTableHandle && ((TpchTableHandle) tableHandle).getConnectorId().equals(connectorId);
    }

    @Override
    public List<String> listSchemaNames()
    {
        return ImmutableList.of(TINY_SCHEMA_NAME, "sf1", "sf100", "sf300", "sf1000", "sf3000", "sf10000", "sf30000", "sf100000");
    }

    @Override
    public TpchTableHandle getTableHandle(SchemaTableName tableName)
    {
        checkNotNull(tableName, "tableName is null");
        if (!tableNames.contains(tableName.getTableName())) {
            return null;
        }

        // parse the scale factor
        double scaleFactor = schemaNameToScaleFactor(tableName.getSchemaName());
        if (scaleFactor < 0) {
            return null;
        }

        return new TpchTableHandle(connectorId, tableName.getTableName(), scaleFactor);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(TableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkArgument(tableHandle instanceof TpchTableHandle, "tableHandle is not an instance of TpchTableHandle");
        TpchTableHandle tpchTableHandle = (TpchTableHandle) tableHandle;

        TpchTable<?> tpchTable = TpchTable.getTable(tpchTableHandle.getTableName());
        String schemaName = scaleFactorSchemaName(tpchTableHandle.getScaleFactor());

        return getTableMetadata(schemaName, tpchTable);
    }

    private ConnectorTableMetadata getTableMetadata(String schemaName, TpchTable<?> tpchTable)
    {
        ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
        int ordinalPosition = 0;
        for (TpchColumn<? extends TpchEntity> column : tpchTable.getColumns()) {
            columns.add(new ColumnMetadata(column.getColumnName(), getPrestoType(column.getType()), ordinalPosition++, false));
        }

        SchemaTableName tableName = new SchemaTableName(schemaName, tpchTable.getTableName());
        return new ConnectorTableMetadata(tableName, columns.build());
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(TableHandle tableHandle)
    {
        ImmutableMap.Builder<String, ColumnHandle> builder = ImmutableMap.builder();
        for (ColumnMetadata columnMetadata : getTableMetadata(tableHandle).getColumns()) {
            builder.put(columnMetadata.getName(), new TpchColumnHandle(columnMetadata.getName(), columnMetadata.getOrdinalPosition(), columnMetadata.getType()));
        }
        return builder.build();
    }

    @Override
    public ColumnHandle getColumnHandle(TableHandle tableHandle, String columnName)
    {
        return getColumnHandles(tableHandle).get(columnName);
    }

    @Override
    public ColumnHandle getSampleWeightColumnHandle(TableHandle tableHandle)
    {
        return null;
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(SchemaTablePrefix prefix)
    {
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> tableColumns = ImmutableMap.builder();
        for (String schemaName : getSchemaNames(prefix.getSchemaName())) {
            for (TpchTable<?> tpchTable : TpchTable.getTables()) {
                if (prefix.getTableName() == null || tpchTable.getTableName().equals(prefix.getTableName())) {
                    ConnectorTableMetadata tableMetadata = getTableMetadata(schemaName, tpchTable);
                    tableColumns.put(new SchemaTableName(schemaName, tpchTable.getTableName()), tableMetadata.getColumns());
                }
            }
        }
        return tableColumns.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(TableHandle tableHandle, ColumnHandle columnHandle)
    {
        ConnectorTableMetadata tableMetadata = getTableMetadata(tableHandle);
        checkArgument(columnHandle instanceof TpchColumnHandle, "columnHandle is not an instance of TpchColumnHandle");
        String columnName = ((TpchColumnHandle) columnHandle).getColumnName();

        for (ColumnMetadata column : tableMetadata.getColumns()) {
            if (column.getName().equals(columnName)) {
                return column;
            }
        }
        throw new IllegalArgumentException(String.format("Table %s does not have column %s", tableMetadata.getTable(), columnName));
    }

    @Override
    public List<SchemaTableName> listTables(String schemaNameOrNull)
    {
        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        for (String schemaName : getSchemaNames(schemaNameOrNull)) {
            for (TpchTable<?> tpchTable : TpchTable.getTables()) {
                builder.add(new SchemaTableName(schemaName, tpchTable.getTableName()));
            }
        }
        return builder.build();
    }

    private List<String> getSchemaNames(String schemaNameOrNull)
    {
        List<String> schemaNames;
        if (schemaNameOrNull == null) {
            schemaNames = listSchemaNames();
        }
        else if (schemaNameToScaleFactor(schemaNameOrNull) > 0) {
            schemaNames = ImmutableList.of(schemaNameOrNull);
        }
        else {
            schemaNames = ImmutableList.of();
        }
        return schemaNames;
    }

    private static String scaleFactorSchemaName(double scaleFactor)
    {
        return "sf" + scaleFactor;
    }

    private static double schemaNameToScaleFactor(String schemaName)
    {
        if (TINY_SCHEMA_NAME.equals(schemaName)) {
            return TINY_SCALE_FACTOR;
        }

        if (!schemaName.startsWith("sf")) {
            return -1;
        }

        try {
            return Double.parseDouble(schemaName.substring(2));
        }
        catch (Exception ignored) {
            return -1;
        }
    }

    public static Type getPrestoType(Class<?> javaType)
    {
        if (javaType == Boolean.class) {
            return BOOLEAN;
        }
        if (javaType == Long.class) {
            return BIGINT;
        }
        if (javaType == Double.class) {
            return DOUBLE;
        }
        if (javaType == String.class) {
            return VARCHAR;
        }
        throw new IllegalArgumentException("Unsupported type " + javaType.getName());
    }
}
