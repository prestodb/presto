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
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.NullableValue;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
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
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.facebook.presto.tpch.Types.checkType;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;

public class TpchMetadata
        implements ConnectorMetadata
{
    public static final String TINY_SCHEMA_NAME = "tiny";
    public static final double TINY_SCALE_FACTOR = 0.01;

    public static final List<String> SCHEMA_NAMES = ImmutableList.of(
            TINY_SCHEMA_NAME, "sf1", "sf100", "sf300", "sf1000", "sf3000", "sf10000", "sf30000", "sf100000");

    public static final String ROW_NUMBER_COLUMN_NAME = "row_number";

    private static final Set<String> ORDER_STATUS_VALUES = ImmutableSet.of("F", "O", "P");
    private static final Set<NullableValue> ORDER_STATUS_NULLABLE_VALUES = ORDER_STATUS_VALUES.stream()
            .map(value -> new NullableValue(getPrestoType(OrderColumn.ORDER_STATUS.getType()), Slices.utf8Slice(value)))
            .collect(toSet());

    private final String connectorId;
    private final Set<String> tableNames;
    private final boolean predicatePushdownEnabled;

    public TpchMetadata(String connectorId)
    {
        this(connectorId, TpchConnectorFactory.DEFAULT_PREDICATE_PUSHDOWN_ENABLED);
    }

    public TpchMetadata(String connectorId, boolean predicatePushdownEnabled)
    {
        ImmutableSet.Builder<String> tableNames = ImmutableSet.builder();
        for (TpchTable<?> tpchTable : TpchTable.getTables()) {
            tableNames.add(tpchTable.getTableName());
        }
        this.tableNames = tableNames.build();
        this.connectorId = connectorId;
        this.predicatePushdownEnabled = predicatePushdownEnabled;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return SCHEMA_NAMES;
    }

    @Override
    public TpchTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        requireNonNull(tableName, "tableName is null");
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

        Optional<TupleDomain<ColumnHandle>> predicate = Optional.empty();
        TupleDomain<ColumnHandle> unenforcedConstraint = constraint.getSummary();
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

            if (predicatePushdownEnabled) {
                predicate = Optional.of(toTupleDomain(ImmutableMap.of(
                        toColumnHandle(OrderColumn.ORDER_STATUS),
                        ORDER_STATUS_NULLABLE_VALUES.stream()
                                .filter(convertToPredicate(constraint.getSummary(), OrderColumn.ORDER_STATUS))
                                .collect(toSet()))));
                unenforcedConstraint = filterOutColumnFromPredicate(constraint.getSummary(), OrderColumn.ORDER_STATUS);
            }
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
                new TpchTableLayoutHandle(tableHandle, predicate),
                Optional.empty(),
                predicate.orElse(TupleDomain.all()), // TODO: return well-known properties (e.g., orderkey > 0, etc)
                nodePartition,
                partitioningColumns,
                Optional.empty(),
                localProperties);

        return ImmutableList.of(new ConnectorTableLayoutResult(layout, unenforcedConstraint));
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
        String schemaName = scaleFactorSchemaName(tpchTableHandle.getScaleFactor());

        return getTableMetadata(schemaName, tpchTable);
    }

    private static ConnectorTableMetadata getTableMetadata(String schemaName, TpchTable<?> tpchTable)
    {
        ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
        for (TpchColumn<? extends TpchEntity> column : tpchTable.getColumns()) {
            columns.add(new ColumnMetadata(column.getColumnName(), getPrestoType(column.getType())));
        }
        columns.add(new ColumnMetadata(ROW_NUMBER_COLUMN_NAME, BIGINT, null, true));

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
                    ConnectorTableMetadata tableMetadata = getTableMetadata(schemaName, tpchTable);
                    tableColumns.put(new SchemaTableName(schemaName, tpchTable.getTableName()), tableMetadata.getColumns());
                }
            }
        }
        return tableColumns.build();
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle, ConnectorTableLayoutHandle layoutHandle)
    {
        TpchTableLayoutHandle layout = checkType(layoutHandle, TpchTableLayoutHandle.class, "layoutHandle");
        TpchTableHandle table = layout.getTable();

        return new TableStatistics(new Estimate(getRowCount(table, layout.getPredicate())), ImmutableMap.of());
    }

    public long getRowCount(TpchTableHandle tpchTableHandle, Optional<TupleDomain<ColumnHandle>> predicate)
    {
        // todo expose row counts from airlift-tpch instead of hardcoding it here
        // todo add stats for columns
        String tableName = tpchTableHandle.getTableName();
        double scaleFactor = tpchTableHandle.getScaleFactor();
        switch (tableName) {
            case "customer":
                return (long) (150_000 * scaleFactor);
            case "orders":
                Set<String> orderStatusValues = predicate.map(tupleDomain ->
                        ORDER_STATUS_NULLABLE_VALUES.stream()
                                .filter(convertToPredicate(tupleDomain, OrderColumn.ORDER_STATUS))
                                .map(nullableValue -> ((Slice) nullableValue.getValue()).toStringUtf8())
                                .collect(toSet()))
                        .orElse(ORDER_STATUS_VALUES);

                long totalRows = 0L;
                if (orderStatusValues.contains("F")) {
                    totalRows = 729_413;
                }
                if (orderStatusValues.contains("O")) {
                    totalRows += 732_044;
                }
                if (orderStatusValues.contains("P")) {
                    totalRows += 38_543;
                }
                return (long) (totalRows * scaleFactor);
            case "lineitem":
                return (long) (6_000_000 * scaleFactor);
            case "part":
                return (long) (200_000 * scaleFactor);
            case "partsupp":
                return (long) (800_000 * scaleFactor);
            case "supplier":
                return (long) (10_000 * scaleFactor);
            case "nation":
                return 25;
            case "region":
                return 5;
            default:
                throw new IllegalArgumentException("unknown tpch table name '" + tableName + "'");
        }
    }

    private TpchColumnHandle toColumnHandle(TpchColumn column)
    {
        return new TpchColumnHandle(column.getColumnName(), getPrestoType(column.getType()));
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

    private TupleDomain<ColumnHandle> toTupleDomain(Map<TpchColumnHandle, Set<NullableValue>> predicate)
    {
        return TupleDomain.withColumnDomains(predicate.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> {
                    Type type = entry.getKey().getType();
                    return entry.getValue().stream()
                            .map(nullableValue -> Domain.singleValue(type, nullableValue.getValue()))
                            .reduce((Domain::union))
                            .orElse(Domain.none(type));
                })));
    }

    private Predicate<NullableValue> convertToPredicate(TupleDomain<ColumnHandle> predicate, TpchColumn column)
    {
        return nullableValue -> predicate.contains(TupleDomain.fromFixedValues(ImmutableMap.of(toColumnHandle(column), nullableValue)));
    }

    private TupleDomain<ColumnHandle> filterOutColumnFromPredicate(TupleDomain<ColumnHandle> predicate, TpchColumn column)
    {
        return filterColumns(predicate, tpchColumnHandle -> !tpchColumnHandle.equals(toColumnHandle(column)));
    }

    private TupleDomain<ColumnHandle> filterColumns(TupleDomain<ColumnHandle> predicate, Predicate<TpchColumnHandle> filterPredicate)
    {
        return predicate.transform(columnHandle -> {
            TpchColumnHandle tpchColumnHandle = checkType(columnHandle, TpchColumnHandle.class, "columnHandle");
            if (filterPredicate.test(tpchColumnHandle)) {
                return tpchColumnHandle;
            }

            return null;
        });
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
        switch (tpchType.getBase()) {
            case IDENTIFIER:
                return BigintType.BIGINT;
            case INTEGER:
                return IntegerType.INTEGER;
            case DATE:
                return DateType.DATE;
            case DOUBLE:
                return DoubleType.DOUBLE;
            case VARCHAR:
                return createVarcharType((int) (long) tpchType.getPrecision().get());
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
}
