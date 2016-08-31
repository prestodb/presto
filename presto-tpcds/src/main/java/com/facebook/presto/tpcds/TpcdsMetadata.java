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
package com.facebook.presto.tpcds;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.TimeType;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.teradata.tpcds.Table;
import com.teradata.tpcds.column.Column;
import com.teradata.tpcds.column.ColumnType;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.spi.type.CharType.createCharType;
import static com.facebook.presto.spi.type.DecimalType.createDecimalType;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.facebook.presto.tpcds.Types.checkType;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class TpcdsMetadata
        implements ConnectorMetadata
{
    public static final List<String> SCHEMA_NAMES = ImmutableList.of(
            "sf1", "sf10", "sf100", "sf300", "sf1000", "sf3000", "sf10000", "sf30000", "sf100000");

    private final Set<String> tableNames;

    public TpcdsMetadata()
    {
        ImmutableSet.Builder<String> tableNames = ImmutableSet.builder();
        for (Table tpcdsTable : Table.getBaseTables()) {
            tableNames.add(tpcdsTable.getName().toLowerCase(ENGLISH));
        }
        this.tableNames = tableNames.build();
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return SCHEMA_NAMES;
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        requireNonNull(tableName, "tableName is null");
        if (!tableNames.contains(tableName.getTableName())) {
            return null;
        }

        // parse the scale factor
        int scaleFactor = schemaNameToScaleFactor(tableName.getSchemaName());
        if (scaleFactor < 0) {
            return null;
        }

        return new TpcdsTableHandle(tableName.getTableName(), scaleFactor);
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(
            ConnectorSession session,
            ConnectorTableHandle table,
            Constraint<ColumnHandle> constraint,
            Optional<Set<ColumnHandle>> desiredColumns)
    {
        TpcdsTableHandle tableHandle = checkType(table, TpcdsTableHandle.class, "table");
        ConnectorTableLayout layout = new ConnectorTableLayout(
                new TpcdsTableLayoutHandle(tableHandle),
                Optional.empty(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of());

        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        TpcdsTableLayoutHandle layout = checkType(handle, TpcdsTableLayoutHandle.class, "layout");

        return getTableLayouts(session, layout.getTable(), Constraint.alwaysTrue(), Optional.empty())
                .get(0)
                .getTableLayout();
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        TpcdsTableHandle tpcdsTableHandle = checkType(tableHandle, TpcdsTableHandle.class, "tableHandle");

        Table table = Table.getTable(tpcdsTableHandle.getTableName());
        String schemaName = scaleFactorSchemaName(tpcdsTableHandle.getScaleFactor());

        return getTableMetadata(schemaName, table);
    }

    private static ConnectorTableMetadata getTableMetadata(String schemaName, Table tpcdsTable)
    {
        ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
        for (Column column : tpcdsTable.getColumns()) {
            columns.add(new ColumnMetadata(column.getName(), getPrestoType(column.getType())));
        }
        SchemaTableName tableName = new SchemaTableName(schemaName, tpcdsTable.getName());
        return new ConnectorTableMetadata(tableName, columns.build());
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        ImmutableMap.Builder<String, ColumnHandle> builder = ImmutableMap.builder();
        for (ColumnMetadata columnMetadata : getTableMetadata(session, tableHandle).getColumns()) {
            builder.put(columnMetadata.getName(), new TpcdsColumnHandle(columnMetadata.getName(), columnMetadata.getType()));
        }
        return builder.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        ConnectorTableMetadata tableMetadata = getTableMetadata(session, tableHandle);
        String columnName = checkType(columnHandle, TpcdsColumnHandle.class, "columnHandle").getColumnName();

        for (ColumnMetadata column : tableMetadata.getColumns()) {
            if (column.getName().equals(columnName)) {
                return column;
            }
        }
        throw new IllegalArgumentException(format("Table %s does not have column %s", tableMetadata.getTable(), columnName));
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> tableColumns = ImmutableMap.builder();
        for (String schemaName : getSchemaNames(session, prefix.getSchemaName())) {
            for (Table tpcdsTable : Table.getBaseTables()) {
                if (prefix.getTableName() == null || tpcdsTable.getName().equals(prefix.getTableName())) {
                    ConnectorTableMetadata tableMetadata = getTableMetadata(schemaName, tpcdsTable);
                    tableColumns.put(new SchemaTableName(schemaName, tpcdsTable.getName()), tableMetadata.getColumns());
                }
            }
        }
        return tableColumns.build();
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        for (String schemaName : getSchemaNames(session, schemaNameOrNull)) {
            for (Table tpcdsTable : Table.getBaseTables()) {
                builder.add(new SchemaTableName(schemaName, tpcdsTable.getName()));
            }
        }
        return builder.build();
    }

    private List<String> getSchemaNames(ConnectorSession session, String schemaNameOrNull)
    {
        if (schemaNameOrNull == null) {
            return listSchemaNames(session);
        }
        else if (schemaNameToScaleFactor(schemaNameOrNull) > 0) {
            return ImmutableList.of(schemaNameOrNull);
        }
        return ImmutableList.of();
    }

    private static String scaleFactorSchemaName(int scaleFactor)
    {
        return "sf" + scaleFactor;
    }

    private static int schemaNameToScaleFactor(String schemaName)
    {
        if (!schemaName.startsWith("sf")) {
            return -1;
        }

        try {
            return Integer.parseInt(schemaName.substring(2));
        }
        catch (Exception ignored) {
            return -1;
        }
    }

    public static Type getPrestoType(ColumnType tpcdsType)
    {
        switch (tpcdsType.getBase()) {
            case IDENTIFIER:
                return BigintType.BIGINT;
            case INTEGER:
                return IntegerType.INTEGER;
            case DATE:
                return DateType.DATE;
            case DECIMAL:
                return createDecimalType(tpcdsType.getPrecision().get(), tpcdsType.getScale().get());
            case CHAR:
                return createCharType(tpcdsType.getPrecision().get());
            case VARCHAR:
                return createVarcharType(tpcdsType.getPrecision().get());
            case TIME:
                return TimeType.TIME;
        }
        throw new IllegalArgumentException("Unsupported TPC-DS type " + tpcdsType);
    }
}
