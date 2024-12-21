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
import com.facebook.presto.spi.NotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.plugin.arrow.ArrowErrorCode.ARROW_FLIGHT_METADATA_ERROR;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public abstract class AbstractArrowMetadata
        implements ConnectorMetadata
{
    private static final Logger logger = Logger.get(AbstractArrowMetadata.class);
    private final ArrowFlightConfig config;
    private final AbstractArrowFlightClientHandler clientHandler;
    private final ArrowBlockBuilder arrowBlockBuilder;

    public AbstractArrowMetadata(ArrowFlightConfig config, AbstractArrowFlightClientHandler clientHandler, ArrowBlockBuilder arrowBlockBuilder)
    {
        this.config = requireNonNull(config, "config is null");
        this.clientHandler = requireNonNull(clientHandler, "clientHandler is null");
        this.arrowBlockBuilder = requireNonNull(arrowBlockBuilder, "arrowPageBuilder is null");
    }

    protected abstract FlightDescriptor getFlightDescriptor(String schema, String table);

    protected abstract String getDataSourceSpecificSchemaName(ArrowFlightConfig config, String schemaName);

    protected abstract String getDataSourceSpecificTableName(ArrowFlightConfig config, String tableName);

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
            FlightDescriptor flightDescriptor = getFlightDescriptor(
                    dataSourceSpecificSchemaName, dataSourceSpecificTableName);

            Schema flightSchema = clientHandler.getSchema(flightDescriptor, connectorSession);
            return flightSchema.getFields();
        }
        catch (Exception e) {
            throw new ArrowException(ARROW_FLIGHT_METADATA_ERROR, "The table columns could not be listed for the table " + table, e);
        }
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        Map<String, ColumnHandle> columnHandles = new HashMap<>();

        String schemaValue = ((ArrowTableHandle) tableHandle).getSchema();
        String tableValue = ((ArrowTableHandle) tableHandle).getTable();
        String dbSpecificSchemaValue = getDataSourceSpecificSchemaName(config, schemaValue);
        String dBSpecificTableName = getDataSourceSpecificTableName(config, tableValue);
        List<Field> columnList = getColumnsList(dbSpecificSchemaValue, dBSpecificTableName, session);

        for (Field field : columnList) {
            String columnName = field.getName();
            logger.debug("The value of the flight columnName is:- %s", columnName);

            Type type = getPrestoTypeFromArrowField(field);
            columnHandles.put(columnName, new ArrowColumnHandle(columnName, type));
        }
        return columnHandles;
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        checkArgument(table instanceof ArrowTableHandle,
                "Invalid table handle: Expected an instance of ArrowTableHandle but received %",
                table.getClass().getSimpleName());
        checkArgument(desiredColumns.orElse(Collections.emptySet()).stream().allMatch(f -> f instanceof ArrowColumnHandle),
                "Invalid column handles: Expected desired columns to be of type ArrowColumnHandle");

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
            Type fieldType = getPrestoTypeFromArrowField(field);
            meta.add(new ColumnMetadata(columnName, fieldType));
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
                throw new ArrowException(ARROW_FLIGHT_METADATA_ERROR, "The table columns could not be listed for the table " + tableName, e);
            }
            catch (Exception e) {
                throw new ArrowException(ARROW_FLIGHT_METADATA_ERROR, e.getMessage(), e);
            }
        }
        return columns.build();
    }

    private Type getPrestoTypeFromArrowField(Field field)
    {
        return arrowBlockBuilder.getPrestoTypeFromArrowField(field);
    }
}
