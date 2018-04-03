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
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorTableLayoutProvider;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.statistics.ColumnStatistics;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.tpch.statistics.ColumnStatisticsData;
import com.facebook.presto.tpch.statistics.StatisticsEstimator;
import com.facebook.presto.tpch.statistics.TableStatisticsData;
import com.facebook.presto.tpch.statistics.TableStatisticsDataRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
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
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Maps.asMap;
import static io.airlift.tpch.OrderColumn.ORDER_STATUS;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class TpchMetadata
        implements ConnectorMetadata
{
    public static final String TINY_SCHEMA_NAME = "tiny";
    public static final double TINY_SCALE_FACTOR = 0.01;

    public static final List<String> SCHEMA_NAMES = ImmutableList.of(
            TINY_SCHEMA_NAME, "sf1", "sf100", "sf300", "sf1000", "sf3000", "sf10000", "sf30000", "sf100000");

    public static final String ROW_NUMBER_COLUMN_NAME = "row_number";

    private static final Set<Slice> ORDER_STATUS_VALUES = ImmutableSet.of("F", "O", "P").stream()
            .map(Slices::utf8Slice)
            .collect(toImmutableSet());

    private final String connectorId;
    private final Set<String> tableNames;
    private final ColumnNaming columnNaming;
    private final StatisticsEstimator statisticsEstimator;

    public TpchMetadata(String connectorId)
    {
        this(connectorId, ColumnNaming.SIMPLIFIED);
    }

    public TpchMetadata(String connectorId, ColumnNaming columnNaming)
    {
        ImmutableSet.Builder<String> tableNames = ImmutableSet.builder();
        for (TpchTable<?> tpchTable : TpchTable.getTables()) {
            tableNames.add(tpchTable.getTableName());
        }
        this.tableNames = tableNames.build();
        this.connectorId = connectorId;
        this.columnNaming = columnNaming;
        this.statisticsEstimator = createStatisticsEstimator();
    }

    private static StatisticsEstimator createStatisticsEstimator()
    {
        ObjectMapper objectMapper = new ObjectMapper()
                .registerModule(new Jdk8Module());
        TableStatisticsDataRepository tableStatisticsDataRepository = new TableStatisticsDataRepository(objectMapper);
        return new StatisticsEstimator(tableStatisticsDataRepository);
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
    public ConnectorTableLayoutProvider getTableLayoutProvider(ConnectorSession session, ConnectorTableHandle table, Optional<ConnectorTableLayoutHandle> layoutHandle)
    {
        return new TpchTableLayoutProvider(this, (TpchTableHandle) table, columnNaming, layoutHandle);
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        TpchTableLayoutHandle layout = (TpchTableLayoutHandle) handle;

        // tables in this connector have a single layout
        return getTableLayoutProvider(session, layout.getTable(), Optional.of(handle)).provide(session)
                .get(0)
                .getTableLayout();
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        TpchTableHandle tpchTableHandle = (TpchTableHandle) tableHandle;

        TpchTable<?> tpchTable = TpchTable.getTable(tpchTableHandle.getTableName());
        String schemaName = scaleFactorSchemaName(tpchTableHandle.getScaleFactor());

        return getTableMetadata(schemaName, tpchTable, columnNaming);
    }

    private static ConnectorTableMetadata getTableMetadata(String schemaName, TpchTable<?> tpchTable, ColumnNaming columnNaming)
    {
        ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
        for (TpchColumn<? extends TpchEntity> column : tpchTable.getColumns()) {
            columns.add(new ColumnMetadata(columnNaming.getName(column), getPrestoType(column)));
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
                    ConnectorTableMetadata tableMetadata = getTableMetadata(schemaName, tpchTable, columnNaming);
                    tableColumns.put(new SchemaTableName(schemaName, tpchTable.getTableName()), tableMetadata.getColumns());
                }
            }
        }
        return tableColumns.build();
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle, Constraint<ColumnHandle> constraint)
    {
        TpchTableHandle tpchTableHandle = (TpchTableHandle) tableHandle;
        String tableName = tpchTableHandle.getTableName();
        TpchTable<?> tpchTable = TpchTable.getTable(tableName);
        Map<TpchColumn<?>, List<Object>> columnValuesRestrictions = getColumnValuesRestrictions(tpchTable, constraint);
        Optional<TableStatisticsData> optionalTableStatisticsData = statisticsEstimator.estimateStats(tpchTable, columnValuesRestrictions, tpchTableHandle.getScaleFactor());

        Map<String, ColumnHandle> columnHandles = getColumnHandles(session, tpchTableHandle);
        return optionalTableStatisticsData
                .map(tableStatisticsData -> toTableStatistics(optionalTableStatisticsData.get(), tpchTableHandle, columnHandles))
                .orElse(TableStatistics.EMPTY_STATISTICS);
    }

    private Map<TpchColumn<?>, List<Object>> getColumnValuesRestrictions(TpchTable<?> tpchTable, Constraint<ColumnHandle> constraint)
    {
        TupleDomain<ColumnHandle> constraintSummary = constraint.getSummary();
        if (constraintSummary.isAll()) {
            return emptyMap();
        }
        else if (constraintSummary.isNone()) {
            Set<TpchColumn<?>> columns = ImmutableSet.copyOf(tpchTable.getColumns());
            return asMap(columns, key -> emptyList());
        }
        else {
            Map<ColumnHandle, Domain> domains = constraintSummary.getDomains().get();
            Optional<Domain> orderStatusDomain = Optional.ofNullable(domains.get(toColumnHandle(ORDER_STATUS)));
            Optional<Map<TpchColumn<?>, List<Object>>> allowedColumnValues = orderStatusDomain.map(domain -> {
                List<Object> allowedValues = ORDER_STATUS_VALUES.stream()
                        .filter(domain::includesNullableValue)
                        .collect(toList());
                return avoidTrivialOrderStatusRestriction(allowedValues);
            });
            return allowedColumnValues.orElse(emptyMap());
        }
    }

    private Map<TpchColumn<?>, List<Object>> avoidTrivialOrderStatusRestriction(List<Object> allowedValues)
    {
        if (allowedValues.containsAll(ORDER_STATUS_VALUES)) {
            return emptyMap();
        }
        else {
            return ImmutableMap.of(ORDER_STATUS, allowedValues);
        }
    }

    private TableStatistics toTableStatistics(TableStatisticsData tableStatisticsData, TpchTableHandle tpchTableHandle, Map<String, ColumnHandle> columnHandles)
    {
        TableStatistics.Builder builder = TableStatistics.builder()
                .setRowCount(new Estimate(tableStatisticsData.getRowCount()));
        tableStatisticsData.getColumns().forEach((columnName, stats) -> {
            TpchColumnHandle columnHandle = (TpchColumnHandle) getColumnHandle(tpchTableHandle, columnHandles, columnName);
            builder.setColumnStatistics(columnHandle, toColumnStatistics(stats, columnHandle.getType()));
        });
        return builder.build();
    }

    private ColumnHandle getColumnHandle(TpchTableHandle tpchTableHandle, Map<String, ColumnHandle> columnHandles, String columnName)
    {
        TpchTable<?> table = TpchTable.getTable(tpchTableHandle.getTableName());
        return columnHandles.get(columnNaming.getName(table.getColumn(columnName)));
    }

    private ColumnStatistics toColumnStatistics(ColumnStatisticsData stats, Type columnType)
    {
        return ColumnStatistics.builder()
                .addRange(rangeBuilder -> rangeBuilder
                        .setDistinctValuesCount(stats.getDistinctValuesCount().map(Estimate::new).orElse(Estimate.unknownValue()))
                        .setLowValue(stats.getMin().map(value -> toPrestoValue(value, columnType)))
                        .setHighValue(stats.getMax().map(value -> toPrestoValue(value, columnType)))
                        .setFraction(new Estimate((1))))
                .setNullsFraction(Estimate.zeroValue())
                .build();
    }

    private Object toPrestoValue(Object tpchValue, Type columnType)
    {
        if (columnType instanceof VarcharType) {
            return Slices.utf8Slice((String) tpchValue);
        }
        if (columnType.equals(BIGINT) || columnType.equals(INTEGER) || columnType.equals(DATE)) {
            return ((Number) tpchValue).longValue();
        }
        if (columnType.equals(DOUBLE)) {
            return ((Number) tpchValue).doubleValue();
        }
        throw new IllegalArgumentException("unsupported column type " + columnType);
    }

    public TpchColumnHandle toColumnHandle(TpchColumn<?> column)
    {
        return new TpchColumnHandle(columnNaming.getName(column), getPrestoType(column));
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        ConnectorTableMetadata tableMetadata = getTableMetadata(session, tableHandle);
        String columnName = ((TpchColumnHandle) columnHandle).getColumnName();

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

    public static Type getPrestoType(TpchColumn<?> column)
    {
        TpchColumnType tpchType = column.getType();
        switch (tpchType.getBase()) {
            case IDENTIFIER:
                return BIGINT;
            case INTEGER:
                return INTEGER;
            case DATE:
                return DATE;
            case DOUBLE:
                return DOUBLE;
            case VARCHAR:
                return createVarcharType((int) (long) tpchType.getPrecision().get());
        }
        throw new IllegalArgumentException("Unsupported type " + tpchType);
    }
}
