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
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.LocalProperty;
import com.facebook.presto.spi.ReadOnlyConnectorMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.SortingProperty;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.tpch.LineItemColumn;
import io.airlift.tpch.OrderColumn;
import io.airlift.tpch.TpchColumn;
import io.airlift.tpch.TpchColumnType;
import io.airlift.tpch.TpchEntity;
import io.airlift.tpch.TpchTable;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.tpch.Types.checkType;
import static com.google.common.base.Preconditions.checkNotNull;

public class TpchMetadata
        extends ReadOnlyConnectorMetadata
{
    public static final String TINY_SCHEMA_NAME = "tiny";
    public static final double TINY_SCALE_FACTOR = 0.01;

    public static final String ROW_NUMBER_COLUMN_NAME = "row_number";

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
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.of(TINY_SCHEMA_NAME, "sf1", "sf100", "sf300", "sf1000", "sf3000", "sf10000", "sf30000", "sf100000");
    }

    @Override
    public TpchTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
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
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        TpchTableHandle tableHandle = checkType(table, TpchTableHandle.class, "table");

        Optional<Set<ColumnHandle>> partitioningColumns = Optional.empty();
        List<LocalProperty<ColumnHandle>> localProperties = ImmutableList.of();

        Map<String, ColumnHandle> columns = getColumnHandles(tableHandle);
        if (tableHandle.getTableName().equals(TpchTable.ORDERS.getTableName())) {
            partitioningColumns = Optional.of(ImmutableSet.of(columns.get(OrderColumn.ORDER_KEY.getColumnName())));
            localProperties = ImmutableList.of(new SortingProperty<>(columns.get(OrderColumn.ORDER_KEY.getColumnName()), SortOrder.ASC_NULLS_FIRST));
        }
        else if (tableHandle.getTableName().equals(TpchTable.LINE_ITEM.getTableName())) {
            partitioningColumns = Optional.of(ImmutableSet.of(columns.get(LineItemColumn.ORDER_KEY.getColumnName())));
            localProperties = ImmutableList.of(
                    new SortingProperty<>(columns.get(LineItemColumn.ORDER_KEY.getColumnName()), SortOrder.ASC_NULLS_FIRST),
                    new SortingProperty<>(columns.get(LineItemColumn.LINE_NUMBER.getColumnName()), SortOrder.ASC_NULLS_FIRST));
        }

        ConnectorTableLayout layout = new ConnectorTableLayout(
                new TpchTableLayoutHandle(tableHandle),
                Optional.<List<ColumnHandle>>empty(),
                TupleDomain.<ColumnHandle>all(), // TODO: return well-known properties (e.g., orderkey > 0, etc)
                partitioningColumns,
                Optional.empty(),
                localProperties);

        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorTableLayoutHandle handle)
    {
        TpchTableLayoutHandle layout = checkType(handle, TpchTableLayoutHandle.class, "layout");

        // tables in this connector have a single layout
        return getTableLayouts(layout.getTable(), Constraint.<ColumnHandle>alwaysTrue(), Optional.empty())
                .get(0)
                .getTableLayout();
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorTableHandle tableHandle)
    {
        TpchTableHandle tpchTableHandle = checkType(tableHandle, TpchTableHandle.class, "tableHandle");

        TpchTable<?> tpchTable = TpchTable.getTable(tpchTableHandle.getTableName());
        String schemaName = scaleFactorSchemaName(tpchTableHandle.getScaleFactor());

        return getTableMetadata(schemaName, tpchTable);
    }

    private static ConnectorTableMetadata getTableMetadata(String schemaName, TpchTable<?> tpchTable)
    {
        ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
        for (TpchColumn<? extends TpchEntity> column : tpchTable.getColumns()) {
            columns.add(new ColumnMetadata(column.getColumnName(), getPrestoType(column.getType()), false));
        }
        columns.add(new ColumnMetadata(ROW_NUMBER_COLUMN_NAME, BIGINT, false, null, true));

        SchemaTableName tableName = new SchemaTableName(schemaName, tpchTable.getTableName());
        return new ConnectorTableMetadata(tableName, columns.build());
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorTableHandle tableHandle)
    {
        ImmutableMap.Builder<String, ColumnHandle> builder = ImmutableMap.builder();
        for (ColumnMetadata columnMetadata : getTableMetadata(tableHandle).getColumns()) {
            builder.put(columnMetadata.getName(), new TpchColumnHandle(columnMetadata.getName(), columnMetadata.getType()));
        }
        return builder.build();
    }

    @Override
    public ColumnHandle getSampleWeightColumnHandle(ConnectorTableHandle tableHandle)
    {
        return null;
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> tableColumns = ImmutableMap.builder();
        for (String schemaName : getSchemaNames(session, prefix.getSchemaName())) {
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
    public ColumnMetadata getColumnMetadata(ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        ConnectorTableMetadata tableMetadata = getTableMetadata(tableHandle);
        String columnName = checkType(columnHandle, TpchColumnHandle.class, "columnHandle").getColumnName();

        for (ColumnMetadata column : tableMetadata.getColumns()) {
            if (column.getName().equals(columnName)) {
                return column;
            }
        }
        throw new IllegalArgumentException(String.format("Table %s does not have column %s", tableMetadata.getTable(), columnName));
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        for (String schemaName : getSchemaNames(session, schemaNameOrNull)) {
            for (TpchTable<?> tpchTable : TpchTable.getTables()) {
                builder.add(new SchemaTableName(schemaName, tpchTable.getTableName()));
            }
        }
        return builder.build();
    }

    private List<String> getSchemaNames(ConnectorSession session, String schemaNameOrNull)
    {
        List<String> schemaNames;
        if (schemaNameOrNull == null) {
            schemaNames = listSchemaNames(session);
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

    public static Type getPrestoType(TpchColumnType tpchType)
    {
        switch (tpchType) {
            case BIGINT:
                return BIGINT;
            case DATE:
                return DATE;
            case DOUBLE:
                return DOUBLE;
            case VARCHAR:
                return VARCHAR;
        }
        throw new IllegalArgumentException("Unsupported type " + tpchType);
    }
}
