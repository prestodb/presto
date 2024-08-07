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
package com.facebook.plugin.arrow;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.SmallintType;
import com.facebook.presto.common.type.TimeType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.TinyintType;
import com.facebook.presto.common.type.VarbinaryType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.NotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.plugin.arrow.ArrowErrorCode.ARROW_FLIGHT_ERROR;
import static java.util.Objects.requireNonNull;

public abstract class ArrowAbstractMetadata
        implements ConnectorMetadata
{
    private static final Logger logger = Logger.get(ArrowAbstractMetadata.class);
    private final ArrowFlightConfig config;
    private final ArrowFlightClientHandler clientHandler;

    public ArrowAbstractMetadata(ArrowFlightConfig config, ArrowFlightClientHandler clientHandler)
    {
        this.config = config;
        this.clientHandler = requireNonNull(clientHandler);
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        if (!listSchemaNames(session).contains(tableName.getSchemaName())) {
            return null;
        }

        if (!listTables(session, Optional.ofNullable(tableName.getSchemaName())).contains(tableName)) {
            return null;
        }
        return new ArrowTableHandle(tableName.getSchemaName(), tableName.getTableName());
    }

    public List<Field> getColumnsList(String schema, String table, ConnectorSession connectorSession)
    {
        try {
            String dataSourceSpecificSchemaName = getDataSourceSpecificSchemaName(config, schema);
            String dataSourceSpecificTableName = getDataSourceSpecificTableName(config, table);
            ArrowFlightRequest request = getArrowFlightRequest(clientHandler.getConfig(), Optional.empty(),
                    dataSourceSpecificSchemaName, dataSourceSpecificTableName);

            FlightInfo flightInfo = clientHandler.getFlightInfo(request, connectorSession);
            List<Field> fields = flightInfo.getSchema().getFields();
            return fields;
        }
        catch (Exception e) {
            throw new ArrowException(ARROW_FLIGHT_ERROR, "The table columns could not be listed for the table " + table, e);
        }
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        Map<String, ColumnHandle> column = new HashMap<>();

        String schemaValue = ((ArrowTableHandle) tableHandle).getSchema();
        String tableValue = ((ArrowTableHandle) tableHandle).getTable();
        String dataSourceSpecificSchemaValue = getDataSourceSpecificSchemaName(config, schemaValue);
        String dataSourceSpecificTableName = getDataSourceSpecificTableName(config, tableValue);
        List<Field> columnList = getColumnsList(dataSourceSpecificSchemaValue, dataSourceSpecificTableName, session);

        for (Field field : columnList) {
            String columnName = field.getName();
            logger.debug("The value of the flight columnName is:- %s", columnName);
            switch (field.getType().getTypeID()) {
                case Int:
                    ArrowType.Int intType = (ArrowType.Int) field.getType();
                    switch (intType.getBitWidth()) {
                        case 64:
                            column.put(columnName, new ArrowColumnHandle(columnName, BigintType.BIGINT));
                            break;
                        case 32:
                            column.put(columnName, new ArrowColumnHandle(columnName, IntegerType.INTEGER));
                            break;
                        case 16:
                            column.put(columnName, new ArrowColumnHandle(columnName, SmallintType.SMALLINT));
                            break;
                        case 8:
                            column.put(columnName, new ArrowColumnHandle(columnName, TinyintType.TINYINT));
                            break;
                        default:
                            throw new ArrowException(ARROW_FLIGHT_ERROR, "Invalid bit width " + intType.getBitWidth());
                    }
                    break;
                case Binary:
                case LargeBinary:
                case FixedSizeBinary:
                    column.put(columnName, new ArrowColumnHandle(columnName, VarbinaryType.VARBINARY));
                    break;
                case Date:
                    column.put(columnName, new ArrowColumnHandle(columnName, DateType.DATE));
                    break;
                case Timestamp:
                    column.put(columnName, new ArrowColumnHandle(columnName, TimestampType.TIMESTAMP));
                    break;
                case Utf8:
                case LargeUtf8:
                    column.put(columnName, new ArrowColumnHandle(columnName, VarcharType.VARCHAR));
                    break;
                case FloatingPoint:
                    ArrowType.FloatingPoint floatingPoint = (ArrowType.FloatingPoint) field.getType();
                    switch (floatingPoint.getPrecision()) {
                        case SINGLE:
                            column.put(columnName, new ArrowColumnHandle(columnName, RealType.REAL));
                            break;
                        case DOUBLE:
                            column.put(columnName, new ArrowColumnHandle(columnName, DoubleType.DOUBLE));
                            break;
                        default:
                            throw new ArrowException(ARROW_FLIGHT_ERROR, "Invalid floating point precision " + floatingPoint.getPrecision());
                    }
                    break;
                case Decimal:
                    ArrowType.Decimal decimalType = (ArrowType.Decimal) field.getType();
                    int precision = decimalType.getPrecision();
                    int scale = decimalType.getScale();
                    column.put(columnName, new ArrowColumnHandle(columnName, DecimalType.createDecimalType(precision, scale)));
                    break;
                case Bool:
                    column.put(columnName, new ArrowColumnHandle(columnName, BooleanType.BOOLEAN));
                    break;
                case Time:
                    column.put(columnName, new ArrowColumnHandle(columnName, TimeType.TIME));
                    break;
                default:
                    throw new UnsupportedOperationException("The data type " + field.getType().getTypeID() + " is not supported.");
            }
        }
        return column;
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        ArrowTableHandle tableHandle = (ArrowTableHandle) table;

        List<ArrowColumnHandle> columns = new ArrayList<>();
        if (desiredColumns.isPresent()) {
            List<ColumnHandle> arrowColumns = new ArrayList<>(desiredColumns.get());
            columns = (List<ArrowColumnHandle>) (List<?>) arrowColumns;
        }

        ConnectorTableLayout layout = new ConnectorTableLayout(new ArrowTableLayoutHandle(tableHandle, columns, constraint.getSummary()));
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        return new ConnectorTableLayout(handle);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        List<ColumnMetadata> meta = new ArrayList<>();
        List<Field> columnList = getColumnsList(((ArrowTableHandle) table).getSchema(), ((ArrowTableHandle) table).getTable(), session);

        for (Field field : columnList) {
            String columnName = field.getName();
            switch (field.getType().getTypeID()) {
                case Int:
                    ArrowType.Int intType = (ArrowType.Int) field.getType();
                    switch (intType.getBitWidth()) {
                        case 64:
                            meta.add(new ColumnMetadata(columnName, BigintType.BIGINT));
                            break;
                        case 32:
                            meta.add(new ColumnMetadata(columnName, IntegerType.INTEGER));
                            break;
                        case 16:
                            meta.add(new ColumnMetadata(columnName, SmallintType.SMALLINT));
                            break;
                        case 8:
                            meta.add(new ColumnMetadata(columnName, TinyintType.TINYINT));
                            break;
                        default:
                            throw new ArrowException(ARROW_FLIGHT_ERROR, "Invalid bit width " + intType.getBitWidth());
                    }
                    break;
                case Binary:
                case LargeBinary:
                case FixedSizeBinary:
                    meta.add(new ColumnMetadata(columnName, VarbinaryType.VARBINARY));
                    break;
                case Date:
                    meta.add(new ColumnMetadata(columnName, DateType.DATE));
                    break;
                case Timestamp:
                    meta.add(new ColumnMetadata(columnName, TimestampType.TIMESTAMP));
                    break;
                case Utf8:
                case LargeUtf8:
                    meta.add(new ColumnMetadata(columnName, VarcharType.VARCHAR));
                    break;
                case FloatingPoint:
                    ArrowType.FloatingPoint floatingPoint = (ArrowType.FloatingPoint) field.getType();
                    switch (floatingPoint.getPrecision()) {
                        case SINGLE:
                            meta.add(new ColumnMetadata(columnName, RealType.REAL));
                            break;
                        case DOUBLE:
                            meta.add(new ColumnMetadata(columnName, DoubleType.DOUBLE));
                            break;
                        default:
                            throw new ArrowException(ARROW_FLIGHT_ERROR, "Invalid floating point precision " + floatingPoint.getPrecision());
                    }
                    break;
                case Decimal:
                    ArrowType.Decimal decimalType = (ArrowType.Decimal) field.getType();
                    int precision = decimalType.getPrecision();
                    int scale = decimalType.getScale();
                    meta.add(new ColumnMetadata(columnName, DecimalType.createDecimalType(precision, scale)));
                    break;
                case Time:
                    meta.add(new ColumnMetadata(columnName, TimeType.TIME));
                    break;
                case Bool:
                    meta.add(new ColumnMetadata(columnName, BooleanType.BOOLEAN));
                    break;
                default:
                    throw new UnsupportedOperationException("The data type " + field.getType().getTypeID() + " is not supported.");
            }
        }
        return new ConnectorTableMetadata(new SchemaTableName(((ArrowTableHandle) table).getSchema(), ((ArrowTableHandle) table).getTable()), meta);
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((ArrowColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        List<SchemaTableName> tables;
        if (prefix.getSchemaName() != null && prefix.getTableName() != null) {
            tables = ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
        }
        else {
            tables = listTables(session, Optional.of(prefix.getSchemaName()));
        }

        for (SchemaTableName tableName : tables) {
            try {
                ConnectorTableHandle tableHandle = getTableHandle(session, tableName);
                columns.put(tableName, getTableMetadata(session, tableHandle).getColumns());
            }
            catch (ClassCastException | NotFoundException e) {
                throw new ArrowException(ARROW_FLIGHT_ERROR, "The table columns could not be listed for the table " + tableName, e);
            }
            catch (Exception e) {
                throw new ArrowException(ARROW_FLIGHT_ERROR, e.getMessage(), e);
            }
        }
        return columns.build();
    }

    protected abstract ArrowFlightRequest getArrowFlightRequest(ArrowFlightConfig config, Optional<String> query, String schema, String table);

    protected abstract ArrowFlightRequest getArrowFlightRequest(ArrowFlightConfig config, String schema);

    protected abstract String getDataSourceSpecificSchemaName(ArrowFlightConfig config, String schemaName);

    protected abstract String getDataSourceSpecificTableName(ArrowFlightConfig config, String tableName);
}
