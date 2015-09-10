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
import com.facebook.presto.spi.ConnectorNodePartitioning;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.LocalProperty;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.SortingProperty;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.tpch.LineItemColumn;
import io.airlift.tpch.OrderColumn;
import io.airlift.tpch.OrderGenerator;
import io.airlift.tpch.TpchColumn;
import io.airlift.tpch.TpchColumnType;
import io.airlift.tpch.TpchEntity;
import io.airlift.tpch.TpchTable;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DecimalType.createDecimalType;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.StandardTypes.DECIMAL;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.tpch.Types.checkType;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class TpchMetadata
        implements ConnectorMetadata
{
    public static final int TPCH_MAX_BEFORE_DOT_DIGITS = 6; // derived from lineitem.extenderprice
    public static final int TPCH_GENERATOR_SCALE = 2;

    public static final String TINY_SCHEMA_NAME = "tiny";
    public static final String TINY_SHORT_DECIMAL_SCHEMA_NAME = "tiny_short_decimal";
    public static final String TINY_LONG_DECIMAL_SCHEMA_NAME = "tiny_long_decimal";
    public static final double TINY_SCALE_FACTOR = 0.01;
    public static final String ROW_NUMBER_COLUMN_NAME = "row_number";

    private static final String SHORT_DECIMAL = "short_decimal";
    private static final String LONG_DECIMAL = "long_decimal";
    private static final String CUSTOM_DECIMAL = "decimal_(\\d+)_(\\d+)";

    private static final Pattern CUSTOM_SCHEMA_PATTERN = Pattern.compile(CUSTOM_DECIMAL);
    private static final int PRECISION_GROUP = 1;
    private static final int SCALE_GROUP = 2;

    private static final Pattern SCHEMA_PATTERN = Pattern.compile("\\bsf([0-9]*\\.?[0-9]+)(_(" + SHORT_DECIMAL + "|" + LONG_DECIMAL + "|" + CUSTOM_DECIMAL + "))?\\b");
    private static final int SCALE_FACTOR_GROUP = 1;
    private static final int NUMERIC_TYPE_GROUP = 3;

    private static final TypeSignature DOUBLE_TYPE_SIGNATURE = DOUBLE.getTypeSignature();
    private static final TypeSignature SHORT_DECIMAL_TYPE_SIGNATURE = createDecimalType(12, 2).getTypeSignature();
    private static final TypeSignature LONG_DECIMAL_TYPE_SIGNATURE = createDecimalType(38, 2).getTypeSignature();

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
        return listSchemaNames();
    }

    @Override
    public TpchTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        requireNonNull(tableName, "tableName is null");
        if (!tableNames.contains(tableName.getTableName())) {
            return null;
        }

        Optional<SchemaParameters> schemaParameters = extractSchemaParameters(session, tableName.getSchemaName());
        if (!schemaParameters.isPresent()) {
            return null;
        }

        return new TpchTableHandle(connectorId, tableName.getTableName(), schemaParameters.get().scaleFactor, tableName.getSchemaName(), schemaParameters.get().numericTypeSignature);
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(
            ConnectorSession session,
            ConnectorTableHandle table,
            Constraint<ColumnHandle> constraint,
            Optional<Set<ColumnHandle>> desiredColumns)
    {
        TpchTableHandle tableHandle = checkType(table, TpchTableHandle.class, "table");

        Optional<ConnectorNodePartitioning> nodePartition = Optional.empty();
        Optional<Set<ColumnHandle>> partitioningColumns = Optional.empty();
        List<LocalProperty<ColumnHandle>> localProperties = ImmutableList.of();

        Map<String, ColumnHandle> columns = getColumnHandles(session, tableHandle);
        if (tableHandle.getTableName().equals(TpchTable.ORDERS.getTableName())) {
            ColumnHandle orderKeyColumn = columns.get(OrderColumn.ORDER_KEY.getColumnName());
            nodePartition = Optional.of(new ConnectorNodePartitioning(
                    new TpchPartitioningHandle(
                            TpchTable.ORDERS.getTableName(),
                            calculateTotalRows(OrderGenerator.SCALE_BASE, tableHandle.getScaleFactor())),
                    ImmutableList.of(orderKeyColumn)));
            partitioningColumns = Optional.of(ImmutableSet.of(orderKeyColumn));
            localProperties = ImmutableList.of(new SortingProperty<>(orderKeyColumn, SortOrder.ASC_NULLS_FIRST));
        }
        else if (tableHandle.getTableName().equals(TpchTable.LINE_ITEM.getTableName())) {
            ColumnHandle orderKeyColumn = columns.get(OrderColumn.ORDER_KEY.getColumnName());
            nodePartition = Optional.of(new ConnectorNodePartitioning(
                    new TpchPartitioningHandle(
                            TpchTable.ORDERS.getTableName(),
                            calculateTotalRows(OrderGenerator.SCALE_BASE, tableHandle.getScaleFactor())),
                    ImmutableList.of(orderKeyColumn)));
            partitioningColumns = Optional.of(ImmutableSet.of(orderKeyColumn));
            localProperties = ImmutableList.of(
                    new SortingProperty<>(orderKeyColumn, SortOrder.ASC_NULLS_FIRST),
                    new SortingProperty<>(columns.get(LineItemColumn.LINE_NUMBER.getColumnName()), SortOrder.ASC_NULLS_FIRST));
        }

        ConnectorTableLayout layout = new ConnectorTableLayout(
                new TpchTableLayoutHandle(tableHandle),
                Optional.<List<ColumnHandle>>empty(),
                TupleDomain.<ColumnHandle>all(), // TODO: return well-known properties (e.g., orderkey > 0, etc)
                nodePartition,
                partitioningColumns,
                Optional.empty(),
                localProperties);

        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        TpchTableLayoutHandle layout = checkType(handle, TpchTableLayoutHandle.class, "layout");

        // tables in this connector have a single layout
        return getTableLayouts(session, layout.getTable(), Constraint.<ColumnHandle>alwaysTrue(), Optional.empty())
                .get(0)
                .getTableLayout();
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        TpchTableHandle tpchTableHandle = checkType(tableHandle, TpchTableHandle.class, "tableHandle");

        TpchTable<?> tpchTable = TpchTable.getTable(tpchTableHandle.getTableName());
        String schemaName = schemaNameForTpchTableHandle(tpchTableHandle);

        return getTableMetadata(schemaName, tpchTable, tpchTableHandle.getNumericTypeSignature());
    }

    private static ConnectorTableMetadata getTableMetadata(String schemaName, TpchTable<?> tpchTable, TypeSignature numericTypeSignature)
    {
        ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
        for (TpchColumn<? extends TpchEntity> column : tpchTable.getColumns()) {
            columns.add(new ColumnMetadata(column.getColumnName(), getPrestoType(column.getType(), numericTypeSignature), false));
        }
        columns.add(new ColumnMetadata(ROW_NUMBER_COLUMN_NAME, BIGINT, false, null, true));

        SchemaTableName tableName = new SchemaTableName(schemaName, tpchTable.getTableName());
        return new ConnectorTableMetadata(tableName, columns.build());
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        ImmutableMap.Builder<String, ColumnHandle> builder = ImmutableMap.builder();
        for (ColumnMetadata columnMetadata : getTableMetadata(session, tableHandle).getColumns()) {
            builder.put(columnMetadata.getName(), new TpchColumnHandle(columnMetadata.getName(), columnMetadata.getType()));
        }
        return builder.build();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> tableColumns = ImmutableMap.builder();
        for (String schemaName : getSchemaNames(session, prefix.getSchemaName())) {
            for (TpchTable<?> tpchTable : TpchTable.getTables()) {
                if (prefix.getTableName() == null || tpchTable.getTableName().equals(prefix.getTableName())) {
                    Optional<SchemaParameters> schemaParameters = extractSchemaParameters(session, schemaName);
                    ConnectorTableMetadata tableMetadata = getTableMetadata(schemaName, tpchTable, schemaParameters.get().numericTypeSignature);
                    tableColumns.put(new SchemaTableName(schemaName, tpchTable.getTableName()), tableMetadata.getColumns());
                }
            }
        }
        return tableColumns.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        ConnectorTableMetadata tableMetadata = getTableMetadata(session, tableHandle);
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
        else if (extractSchemaParameters(session, schemaNameOrNull).isPresent()) {
            schemaNames = ImmutableList.of(schemaNameOrNull);
        }
        else {
            schemaNames = ImmutableList.of();
        }
        return schemaNames;
    }

    private static String schemaNameForTpchTableHandle(TpchTableHandle tpchTableHandle)
    {
        return tpchTableHandle.getSchemaName();
    }

    private static Optional<SchemaParameters> extractSchemaParameters(ConnectorSession session, String schemaName)
    {
        if (TINY_SCHEMA_NAME.equals(schemaName)) {
            return Optional.of(new SchemaParameters(TINY_SCALE_FACTOR, DOUBLE_TYPE_SIGNATURE));
        }
        else if (TINY_SHORT_DECIMAL_SCHEMA_NAME.equals(schemaName)) {
            return Optional.of(new SchemaParameters(TINY_SCALE_FACTOR, SHORT_DECIMAL_TYPE_SIGNATURE));
        }
        else if (TINY_LONG_DECIMAL_SCHEMA_NAME.equals(schemaName)) {
            return Optional.of(new SchemaParameters(TINY_SCALE_FACTOR, LONG_DECIMAL_TYPE_SIGNATURE));
        }

        Matcher match = SCHEMA_PATTERN.matcher(schemaName);
        if (!match.matches()) {
            return Optional.empty();
        }

        double scaleFactor = Double.parseDouble(match.group(SCALE_FACTOR_GROUP));
        String numericType = match.group(NUMERIC_TYPE_GROUP);
        TypeSignature numericTypeSignature;
        if (numericType == null) {
            numericTypeSignature = DOUBLE_TYPE_SIGNATURE;
        }
        else if (numericType.equals(SHORT_DECIMAL)) {
            numericTypeSignature = SHORT_DECIMAL_TYPE_SIGNATURE;
        }
        else if (numericType.equals(LONG_DECIMAL)) {
            numericTypeSignature = LONG_DECIMAL_TYPE_SIGNATURE;
        }
        else {
            checkState(CUSTOM_SCHEMA_PATTERN.matcher(numericType).matches());
            numericTypeSignature = getCustomDecimalTypeSignature(numericType);
        }

        return Optional.of(new SchemaParameters(scaleFactor, numericTypeSignature));
    }

    private static TypeSignature getCustomDecimalTypeSignature(String customNumericType)
    {
        Matcher match = CUSTOM_SCHEMA_PATTERN.matcher(customNumericType);
        checkState(match.matches());
        int precision = Integer.parseInt(match.group(PRECISION_GROUP));
        int scale = Integer.parseInt(match.group(SCALE_GROUP));
        checkState(precision - scale >= TPCH_MAX_BEFORE_DOT_DIGITS, "decimals should accommodate at least %s before dot digits", TPCH_MAX_BEFORE_DOT_DIGITS);
        checkState(scale >= TPCH_GENERATOR_SCALE, "custom decimal scale too small (should be greater or equal to: %s)", TPCH_GENERATOR_SCALE);
        return createDecimalType(precision, scale).getTypeSignature();
    }

    public static List<String> listSchemaNames()
    {
        List<String> scaleFactors = ImmutableList.of("1", "100", "300", "1000", "3000", "10000", "30000", "100000");
        List<String> numericTypes = ImmutableList.of(SHORT_DECIMAL, LONG_DECIMAL);

        ImmutableList.Builder<String> schemaNames = ImmutableList.builder();

        schemaNames.add(TINY_SCHEMA_NAME);
        schemaNames.add(TINY_SHORT_DECIMAL_SCHEMA_NAME);
        schemaNames.add(TINY_LONG_DECIMAL_SCHEMA_NAME);

        for (String scaleFactor : scaleFactors) {
            for (String numericType : numericTypes) {
                schemaNames.add("sf" + scaleFactor + "_" + numericType);
            }
            schemaNames.add("sf" + scaleFactor);
        }

        return schemaNames.build();
    }

    public static Type getPrestoType(TpchColumnType tpchType, TypeSignature numericTypeSignature)
    {
        switch (tpchType) {
            case BIGINT:
                return BIGINT;
            case DATE:
                return DATE;
            case DOUBLE:
                return getNumericType(numericTypeSignature);
            case VARCHAR:
                return VARCHAR;
        }
        throw new IllegalArgumentException("Unsupported type " + tpchType);
    }

    private long calculateTotalRows(int scaleBase, double scaleFactor)
    {
        double totalRows = scaleBase * scaleFactor;
        if (totalRows > Long.MAX_VALUE) {
            throw new IllegalArgumentException("Total rows is larger than 2^64");
        }
        return (long) totalRows;
    }

    public static Type getNumericType(TypeSignature numericTypeSignature)
    {
        if (numericTypeSignature.getBase().equals(DECIMAL)) {
            long precision = numericTypeSignature.getParameters().get(0).getLongLiteral();
            long scale = numericTypeSignature.getParameters().get(1).getLongLiteral();
            return createDecimalType((int) precision, (int) scale);
        }
        else {
            checkState(numericTypeSignature.equals(DOUBLE_TYPE_SIGNATURE));
            return DOUBLE;
        }
    }

    private static class SchemaParameters
    {
        final double scaleFactor;
        final TypeSignature numericTypeSignature;

        SchemaParameters(double scaleFactor, TypeSignature numericTypeSignature)
        {
            this.scaleFactor = scaleFactor;
            this.numericTypeSignature = numericTypeSignature;
        }
    }
}
