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

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.TimeType;
import com.facebook.presto.common.type.Type;
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
import com.facebook.presto.spi.statistics.TableStatistics;
import com.facebook.presto.tpcds.statistics.TpcdsTableStatisticsFactory;
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

import static com.facebook.presto.common.type.CharType.createCharType;
import static com.facebook.presto.common.type.DecimalType.createDecimalType;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class TpcdsMetadata
        implements ConnectorMetadata
{
    public static final String TINY_SCHEMA_NAME = "tiny";
    public static final double TINY_SCALE_FACTOR = 0.01;

    public static final List<String> SCHEMA_NAMES = ImmutableList.of(
            TINY_SCHEMA_NAME, "sf1", "sf10", "sf100", "sf300", "sf1000", "sf3000", "sf10000", "sf30000", "sf100000");

    private final Set<String> tableNames;
    private final TpcdsTableStatisticsFactory tpcdsTableStatisticsFactory = new TpcdsTableStatisticsFactory();

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
        double scaleFactor = schemaNameToScaleFactor(tableName.getSchemaName());
        if (scaleFactor <= 0) {
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
        TpcdsTableHandle tableHandle = (TpcdsTableHandle) table;
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
        TpcdsTableLayoutHandle layout = (TpcdsTableLayoutHandle) handle;

        return getTableLayouts(session, layout.getTable(), Constraint.alwaysTrue(), Optional.empty())
                .get(0)
                .getTableLayout();
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        TpcdsTableHandle tpcdsTableHandle = (TpcdsTableHandle) tableHandle;

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
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<ConnectorTableLayoutHandle> tableLayoutHandle, List<ColumnHandle> columnHandles, Constraint<ColumnHandle> constraint)
    {
        TpcdsTableHandle tpcdsTableHandle = (TpcdsTableHandle) tableHandle;

        Table table = Table.getTable(tpcdsTableHandle.getTableName());
        String schemaName = scaleFactorSchemaName(tpcdsTableHandle.getScaleFactor());

        return tpcdsTableStatisticsFactory.create(schemaName, table, columnHandles);
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
        String columnName = ((TpcdsColumnHandle) columnHandle).getColumnName();

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
        for (String schemaName : getSchemaNames(session, Optional.ofNullable(prefix.getSchemaName()))) {
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
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> filterSchema)
    {
        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        for (String schemaName : getSchemaNames(session, filterSchema)) {
            for (Table tpcdsTable : Table.getBaseTables()) {
                builder.add(new SchemaTableName(schemaName, tpcdsTable.getName()));
            }
        }
        return builder.build();
    }

    private List<String> getSchemaNames(ConnectorSession session, Optional<String> schemaName)
    {
        if (!schemaName.isPresent()) {
            return listSchemaNames(session);
        }
        if (schemaNameToScaleFactor(schemaName.get()) > 0) {
            return ImmutableList.of(schemaName.get());
        }
        return ImmutableList.of();
    }

    private static String scaleFactorSchemaName(double scaleFactor)
    {
        return "sf" + scaleFactor;
    }

    public static double schemaNameToScaleFactor(String schemaName)
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
