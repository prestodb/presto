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
import com.facebook.presto.common.type.Type;
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

public abstract class AbstractArrowMetadata
        implements ConnectorMetadata
{
    private static final Logger logger = Logger.get(AbstractArrowMetadata.class);
    private final ArrowFlightConfig config;
    private final ArrowFlightClientHandler clientHandler;

    public AbstractArrowMetadata(ArrowFlightConfig config, ArrowFlightClientHandler clientHandler)
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
        String dbSpecificSchemaValue = getDataSourceSpecificSchemaName(config, schemaValue);
        String dBSpecificTableName = getDataSourceSpecificTableName(config, tableValue);
        List<Field> columnList = getColumnsList(dbSpecificSchemaValue, dBSpecificTableName, session);

        for (Field field : columnList) {
            String columnName = field.getName();
            logger.debug("The value of the flight columnName is:- %s", columnName);

            ArrowColumnHandle handle;
            switch (field.getType().getTypeID()) {
                case Int:
                    ArrowType.Int intType = (ArrowType.Int) field.getType();
                    handle = createArrowColumnHandleForIntType(columnName, intType);
                    break;
                case Binary:
                case LargeBinary:
                case FixedSizeBinary:
                    handle = new ArrowColumnHandle(columnName, VarbinaryType.VARBINARY);
                    break;
                case Date:
                    handle = new ArrowColumnHandle(columnName, DateType.DATE);
                    break;
                case Timestamp:
                    handle = new ArrowColumnHandle(columnName, TimestampType.TIMESTAMP);
                    break;
                case Utf8:
                case LargeUtf8:
                    handle = new ArrowColumnHandle(columnName, VarcharType.VARCHAR);
                    break;
                case FloatingPoint:
                    ArrowType.FloatingPoint floatingPoint = (ArrowType.FloatingPoint) field.getType();
                    handle = createArrowColumnHandleForFloatingPointType(columnName, floatingPoint);
                    break;
                case Decimal:
                    ArrowType.Decimal decimalType = (ArrowType.Decimal) field.getType();
                    handle = new ArrowColumnHandle(columnName, DecimalType.createDecimalType(decimalType.getPrecision(), decimalType.getScale()));
                    break;
                case Bool:
                    handle = new ArrowColumnHandle(columnName, BooleanType.BOOLEAN);
                    break;
                case Time:
                    handle = new ArrowColumnHandle(columnName, TimeType.TIME);
                    break;
                default:
                    throw new UnsupportedOperationException("The data type " + field.getType().getTypeID() + " is not supported.");
            }
            Type type = overrideFieldType(field, handle.getColumnType());
            if (!type.equals(handle.getColumnType())) {
                handle = new ArrowColumnHandle(columnName, type);
            }
            column.put(columnName, handle);
        }
        return column;
    }

    private ArrowColumnHandle createArrowColumnHandleForIntType(String columnName, ArrowType.Int intType)
    {
        switch (intType.getBitWidth()) {
            case 64:
                return new ArrowColumnHandle(columnName, BigintType.BIGINT);
            case 32:
                return new ArrowColumnHandle(columnName, IntegerType.INTEGER);
            case 16:
                return new ArrowColumnHandle(columnName, SmallintType.SMALLINT);
            case 8:
                return new ArrowColumnHandle(columnName, TinyintType.TINYINT);
            default:
                throw new ArrowException(ARROW_FLIGHT_ERROR, "Invalid bit width " + intType.getBitWidth());
        }
    }

    private ArrowColumnHandle createArrowColumnHandleForFloatingPointType(String columnName, ArrowType.FloatingPoint floatingPoint)
    {
        switch (floatingPoint.getPrecision()) {
            case SINGLE:
                return new ArrowColumnHandle(columnName, RealType.REAL);
            case DOUBLE:
                return new ArrowColumnHandle(columnName, DoubleType.DOUBLE);
            default:
                throw new ArrowException(ARROW_FLIGHT_ERROR, "Invalid floating point precision " + floatingPoint.getPrecision());
        }
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
            ArrowType type = field.getType();

            ColumnMetadata columnMetadata;

            switch (type.getTypeID()) {
                case Int:
                    ArrowType.Int intType = (ArrowType.Int) type;
                    columnMetadata = createIntColumnMetadata(columnName, intType);
                    break;
                case Binary:
                case LargeBinary:
                case FixedSizeBinary:
                    columnMetadata = new ColumnMetadata(columnName, VarbinaryType.VARBINARY);
                    break;
                case Date:
                    columnMetadata = new ColumnMetadata(columnName, DateType.DATE);
                    break;
                case Timestamp:
                    columnMetadata = new ColumnMetadata(columnName, TimestampType.TIMESTAMP);
                    break;
                case Utf8:
                case LargeUtf8:
                    columnMetadata = new ColumnMetadata(columnName, VarcharType.VARCHAR);
                    break;
                case FloatingPoint:
                    ArrowType.FloatingPoint floatingPointType = (ArrowType.FloatingPoint) type;
                    columnMetadata = createFloatingPointColumnMetadata(columnName, floatingPointType);
                    break;
                case Decimal:
                    ArrowType.Decimal decimalType = (ArrowType.Decimal) type;
                    columnMetadata = new ColumnMetadata(columnName, DecimalType.createDecimalType(decimalType.getPrecision(), decimalType.getScale()));
                    break;
                case Time:
                    columnMetadata = new ColumnMetadata(columnName, TimeType.TIME);
                    break;
                case Bool:
                    columnMetadata = new ColumnMetadata(columnName, BooleanType.BOOLEAN);
                    break;
                default:
                    throw new UnsupportedOperationException("The data type " + type.getTypeID() + " is not supported.");
            }

            Type fieldType = overrideFieldType(field, columnMetadata.getType());
            if (!fieldType.equals(columnMetadata.getType())) {
                columnMetadata = new ColumnMetadata(columnName, fieldType);
            }
            meta.add(columnMetadata);
        }
        return new ConnectorTableMetadata(new SchemaTableName(((ArrowTableHandle) table).getSchema(), ((ArrowTableHandle) table).getTable()), meta);
    }

    private ColumnMetadata createIntColumnMetadata(String columnName, ArrowType.Int intType)
    {
        switch (intType.getBitWidth()) {
            case 64:
                return new ColumnMetadata(columnName, BigintType.BIGINT);
            case 32:
                return new ColumnMetadata(columnName, IntegerType.INTEGER);
            case 16:
                return new ColumnMetadata(columnName, SmallintType.SMALLINT);
            case 8:
                return new ColumnMetadata(columnName, TinyintType.TINYINT);
            default:
                throw new ArrowException(ARROW_FLIGHT_ERROR, "Invalid bit width " + intType.getBitWidth());
        }
    }

    private ColumnMetadata createFloatingPointColumnMetadata(String columnName, ArrowType.FloatingPoint floatingPointType)
    {
        switch (floatingPointType.getPrecision()) {
            case SINGLE:
                return new ColumnMetadata(columnName, RealType.REAL);
            case DOUBLE:
                return new ColumnMetadata(columnName, DoubleType.DOUBLE);
            default:
                throw new ArrowException(ARROW_FLIGHT_ERROR, "Invalid floating point precision " + floatingPointType.getPrecision());
        }
    }

    /**
     * Provides the field type, which can be overridden by concrete implementations
     * with their own custom type.
     *
     * @return the field type
     */
    protected Type overrideFieldType(Field field, Type type)
    {
        return type;
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
