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
package io.prestosql.plugin.phoenix;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import io.airlift.slice.Slice;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorInsertTableHandle;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorNewTableLayout;
import io.prestosql.spi.connector.ConnectorOutputMetadata;
import io.prestosql.spi.connector.ConnectorOutputTableHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableLayout;
import io.prestosql.spi.connector.ConnectorTableLayoutHandle;
import io.prestosql.spi.connector.ConnectorTableLayoutResult;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.Constraint;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.SchemaTablePrefix;
import io.prestosql.spi.connector.TableNotFoundException;
import io.prestosql.spi.statistics.ComputedStatistics;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.RowType.Field;
import io.prestosql.spi.type.Type;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.TableProperty;

import java.io.IOException;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.plugin.phoenix.PhoenixErrorCode.PHOENIX_ERROR;
import static io.prestosql.plugin.phoenix.TypeUtils.toPrestoType;
import static io.prestosql.plugin.phoenix.TypeUtils.toSqlType;
import static io.prestosql.spi.StandardErrorCode.NOT_FOUND;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.hbase.HConstants.FOREVER;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.ARRAY_SIZE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_SIZE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DATA_TYPE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.DECIMAL_DIGITS;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_SCHEM;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TYPE_ID;
import static org.apache.phoenix.util.PhoenixRuntime.getTable;

public class PhoenixMetadata
        implements ConnectorMetadata
{
    // col name used for PK if none provided in DDL
    public static final String ROWKEY = "ROWKEY";
    // only used within Presto for DELETE
    private static final String UPDATE_ROW_ID = "PHOENIX_UPDATE_ROW_ID";
    private final PhoenixClient phoenixClient;

    private final AtomicReference<Runnable> rollbackAction = new AtomicReference<>();

    public PhoenixMetadata(PhoenixClient phoenixClient)
    {
        this.phoenixClient = requireNonNull(phoenixClient, "client is null");
    }

    // decompose the compound PK from getUpdateRowIdColumnHandle
    public static List<PhoenixColumnHandle> decomposePkColumn(List<PhoenixColumnHandle> columns)
    {
        return columns.stream().flatMap(PhoenixMetadata::decomposePk).collect(Collectors.toList());
    }

    public static Optional<PhoenixColumnHandle> getPkHandle(List<PhoenixColumnHandle> cols)
    {
        Optional<PhoenixColumnHandle> pkHandle = cols.stream().filter(PhoenixMetadata::isUpdateRowId).findFirst();
        return pkHandle;
    }

    private static Stream<PhoenixColumnHandle> decomposePk(PhoenixColumnHandle handle)
    {
        if (isUpdateRowId(handle)) {
            RowType row = (RowType) handle.getColumnType();
            return row.getFields().stream().map(field -> new PhoenixColumnHandle(field.getName().get(), field.getType()));
        }
        return Stream.of(handle);
    }

    private static boolean isUpdateRowId(PhoenixColumnHandle pHandle)
    {
        return isPkHandle(pHandle.getColumnName(), pHandle.getColumnType());
    }

    public static boolean isPkHandle(String columnName, Type columnType)
    {
        return UPDATE_ROW_ID.equals(columnName) && columnType instanceof RowType;
    }

    private static String escapeNamePattern(String name, String escape)
    {
        if ((name == null) || (escape == null)) {
            return name;
        }
        checkArgument(!escape.equals("_"), "Escape string must not be '_'");
        checkArgument(!escape.equals("%"), "Escape string must not be '%'");
        name = name.replace(escape, escape + escape);
        name = name.replace("_", escape + "_");
        name = name.replace("%", escape + "%");
        return name;
    }

    private static ResultSet getColumns(PhoenixTableHandle tableHandle, DatabaseMetaData metadata)
            throws SQLException
    {
        String escape = metadata.getSearchStringEscape();
        return metadata.getColumns(
                tableHandle.getCatalogName(),
                escapeNamePattern(tableHandle.getSchemaName(), escape),
                escapeNamePattern(tableHandle.getTableName(), escape),
                null);
    }

    public static String getFullTableName(String catalog, String schema, String table)
    {
        StringBuilder sb = new StringBuilder();
        if (!isNullOrEmpty(catalog)) {
            sb.append(catalog).append(".");
        }
        if (!isNullOrEmpty(schema)) {
            sb.append(schema).append(".");
        }
        sb.append(table);
        return sb.toString();
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.copyOf(getSchemaNames());
    }

    @Override
    public PhoenixTableHandle getTableHandle(ConnectorSession session, SchemaTableName schemaTableName)
    {
        try (PhoenixConnection connection = phoenixClient.getConnection()) {
            DatabaseMetaData metadata = connection.getMetaData();
            String schemaName = schemaTableName.getSchemaName();
            String tableName = schemaTableName.getTableName();
            if (metadata.storesUpperCaseIdentifiers()) {
                schemaName = schemaName.toUpperCase(ENGLISH);
                tableName = tableName.toUpperCase(ENGLISH);
            }
            try (ResultSet resultSet = getTables(connection, schemaName, tableName)) {
                List<PhoenixTableHandle> tableHandles = new ArrayList<>();
                while (resultSet.next()) {
                    tableHandles.add(new PhoenixTableHandle(
                            phoenixClient.getConnectorId(),
                            schemaTableName,
                            resultSet.getString("TABLE_CAT"),
                            resultSet.getString("TABLE_SCHEM"),
                            resultSet.getString("TABLE_NAME")));
                }
                if (tableHandles.isEmpty()) {
                    return null;
                }
                if (tableHandles.size() > 1) {
                    throw new PrestoException(NOT_SUPPORTED, "Multiple tables matched: " + schemaTableName);
                }
                return getOnlyElement(tableHandles);
            }
        }
        catch (SQLException e) {
            throw new PrestoException(PHOENIX_ERROR, e);
        }
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        PhoenixTableHandle tableHandle = (PhoenixTableHandle) table;
        ConnectorTableLayout layout = new ConnectorTableLayout(new PhoenixTableLayoutHandle(tableHandle, constraint.getSummary(), desiredColumns));
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
        return getTableMetadata(table, false);
    }

    private ConnectorTableMetadata getTableMetadata(ConnectorTableHandle table, boolean requiredRowKey)
    {
        PhoenixTableHandle handle = (PhoenixTableHandle) table;

        ImmutableList.Builder<ColumnMetadata> columnMetadata = ImmutableList.builder();
        for (PhoenixColumnHandle column : getColumns(handle, requiredRowKey)) {
            columnMetadata.add(column.getColumnMetadata());
        }
        return new ConnectorTableMetadata(handle.getSchemaTableName(), columnMetadata.build(), getTableProperties(handle));
    }

    public Map<String, Object> getTableProperties(PhoenixTableHandle handle)
    {
        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();

        try (PhoenixConnection pconn = phoenixClient.getConnection(); HBaseAdmin admin = pconn.getQueryServices().getAdmin()) {
            PTable table = getTable(pconn, getFullTableName(handle.getCatalogName(), handle.getSchemaName(), handle.getTableName()));

            if (table.getBucketNum() != null) {
                properties.put(PhoenixTableProperties.SALT_BUCKETS, table.getBucketNum());
            }
            if (table.isWALDisabled()) {
                properties.put(PhoenixTableProperties.DISABLE_WAL, table.isWALDisabled());
            }
            if (table.isImmutableRows()) {
                properties.put(PhoenixTableProperties.IMMUTABLE_ROWS, table.isImmutableRows());
            }

            String defaultFamilyName = table.getDefaultFamilyName() == null ? QueryConstants.DEFAULT_COLUMN_FAMILY : table.getDefaultFamilyName().getString();
            if (table.getDefaultFamilyName() != null) {
                properties.put(PhoenixTableProperties.DEFAULT_COLUMN_FAMILY, defaultFamilyName);
            }

            HTableDescriptor tableDesc = admin.getTableDescriptor(table.getPhysicalName().getBytes());

            HColumnDescriptor[] columnFamilies = tableDesc.getColumnFamilies();
            for (HColumnDescriptor columnFamily : columnFamilies) {
                if (columnFamily.getNameAsString().equals(defaultFamilyName)) {
                    if (!columnFamily.getBloomFilterType().toString().equals("NONE")) {
                        properties.put(PhoenixTableProperties.BLOOMFILTER, columnFamily.getBloomFilterType().toString());
                    }
                    if (columnFamily.getMaxVersions() != 1) {
                        properties.put(PhoenixTableProperties.VERSIONS, columnFamily.getMaxVersions());
                    }
                    if (columnFamily.getMinVersions() > 0) {
                        properties.put(PhoenixTableProperties.MIN_VERSIONS, columnFamily.getMinVersions());
                    }
                    if (!columnFamily.getCompression().toString().equals("NONE")) {
                        properties.put(PhoenixTableProperties.COMPRESSION, columnFamily.getCompression().toString());
                    }
                    if (columnFamily.getTimeToLive() < FOREVER) {
                        properties.put(PhoenixTableProperties.TTL, columnFamily.getTimeToLive());
                    }
                    break;
                }
            }
        }
        catch (IOException | SQLException e) {
            throw new PrestoException(PHOENIX_ERROR, e);
        }
        return properties.build();
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        try (PhoenixConnection connection = phoenixClient.getConnection()) {
            DatabaseMetaData metadata = connection.getMetaData();
            if (metadata.storesUpperCaseIdentifiers() && (schemaNameOrNull != null)) {
                schemaNameOrNull = schemaNameOrNull.toUpperCase(ENGLISH);
            }
            try (ResultSet resultSet = getTables(connection, schemaNameOrNull, null)) {
                ImmutableList.Builder<SchemaTableName> list = ImmutableList.builder();
                while (resultSet.next()) {
                    list.add(getSchemaTableName(resultSet));
                }
                return list.build();
            }
        }
        catch (SQLException e) {
            throw new PrestoException(PHOENIX_ERROR, e);
        }
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        PhoenixTableHandle phoenixTableHandle = (PhoenixTableHandle) tableHandle;

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        for (PhoenixColumnHandle column : getColumns(phoenixTableHandle, false)) {
            columnHandles.put(column.getColumnMetadata().getName(), column);
        }
        return columnHandles.build();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        List<SchemaTableName> tables;
        if (prefix.getTableName() != null) {
            tables = ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
        }
        else {
            tables = listTables(session, prefix.getSchemaName());
        }
        for (SchemaTableName tableName : tables) {
            try {
                PhoenixTableHandle tableHandle = getTableHandle(session, tableName);
                if (tableHandle == null) {
                    continue;
                }
                columns.put(tableName, getTableMetadata(session, tableHandle).getColumns());
            }
            catch (TableNotFoundException e) {
                // table disappeared during listing operation
            }
        }
        return columns.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((PhoenixColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName,
            Map<String, Object> properties)
    {
        StringBuilder sql = new StringBuilder()
                .append("CREATE SCHEMA ")
                .append(schemaName);

        phoenixClient.execute(sql.toString());
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName)
    {
        StringBuilder sql = new StringBuilder()
                .append("DROP SCHEMA ")
                .append(schemaName);

        phoenixClient.execute(sql.toString());
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        PhoenixTableHandle existingTable = getTableHandle(session, tableMetadata.getTable());
        if (existingTable != null && !ignoreExisting) {
            throw new IllegalArgumentException("Target table already exists: " + tableMetadata.getTable());
        }
        createTable(tableMetadata);
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        PhoenixTableHandle handle = (PhoenixTableHandle) tableHandle;
        StringBuilder sql = new StringBuilder()
                .append("DROP TABLE ")
                .append(getFullTableName(handle.getCatalogName(), handle.getSchemaName(), handle.getTableName()));

        phoenixClient.execute(sql.toString());
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
    {
        PhoenixOutputTableHandle handle = createTable(tableMetadata);
        setRollback(() -> dropTable(session, new PhoenixTableHandle(
                handle.getConnectorId(),
                new SchemaTableName(handle.getSchemaName(), handle.getTableName()),
                handle.getCatalogName(),
                handle.getSchemaName(),
                handle.getTableName())));
        return handle;
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        clearRollback();
        return Optional.empty();
    }

    private void setRollback(Runnable action)
    {
        checkState(rollbackAction.compareAndSet(null, action), "rollback action is already set");
    }

    private void clearRollback()
    {
        rollbackAction.set(null);
    }

    public void rollback()
    {
        Optional.ofNullable(rollbackAction.getAndSet(null)).ifPresent(Runnable::run);
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        ConnectorTableMetadata tableMetadata = getTableMetadata(tableHandle, true);
        return new PhoenixOutputTableHandle(
                phoenixClient.getConnectorId(),
                "",
                tableMetadata.getTable().getSchemaName(),
                tableMetadata.getTable().getTableName(),
                tableMetadata.getColumns().stream().map(ColumnMetadata::getName).collect(Collectors.toList()),
                tableMetadata.getColumns().stream().map(ColumnMetadata::getType).collect(Collectors.toList()));
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        return Optional.empty();
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column)
    {
        PhoenixTableHandle handle = (PhoenixTableHandle) tableHandle;
        StringBuilder sql = new StringBuilder()
                .append("ALTER TABLE ")
                .append(getFullTableName(handle.getCatalogName(), handle.getSchemaName(), handle.getTableName()))
                .append(" ADD ").append(column.getName()).append(" ").append(toSqlType(column.getType()));

        phoenixClient.execute(sql.toString());
    }

    @Override
    public void dropColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column)
    {
        PhoenixTableHandle handle = (PhoenixTableHandle) tableHandle;
        PhoenixColumnHandle columnHandle = (PhoenixColumnHandle) column;
        StringBuilder sql = new StringBuilder()
                .append("ALTER TABLE ")
                .append(getFullTableName(handle.getCatalogName(), handle.getSchemaName(), handle.getTableName()))
                .append(" DROP COLUMN ").append(columnHandle.getColumnName());

        phoenixClient.execute(sql.toString());
    }

    @Override
    public ColumnHandle getUpdateRowIdColumnHandle(ConnectorSession session,
            ConnectorTableHandle tableHandle)
    {
        PhoenixTableHandle pTableHandle = (PhoenixTableHandle) tableHandle;
        Map<String, PhoenixColumnHandle> namesToHandles =
                Maps.uniqueIndex(getColumns(pTableHandle, true), pHandle -> pHandle.getColumnName());
        try (PhoenixConnection connection = phoenixClient.getConnection();
                ResultSet resultSet =
                        connection.getMetaData().getPrimaryKeys(pTableHandle.getCatalogName(), pTableHandle.getSchemaName(), pTableHandle.getTableName())) {
            List<Field> fields = new ArrayList<>();
            while (resultSet.next()) {
                String columnName = resultSet.getString("COLUMN_NAME");
                PhoenixColumnHandle columnHandle = namesToHandles.get(columnName);
                if (columnHandle == null) {
                    throw new PrestoException(NOT_FOUND, "Couldn't find metadata for column: " + columnName);
                }
                fields.add(RowType.field(columnName, columnHandle.getColumnType()));
            }
            if (fields.isEmpty()) {
                throw new PrestoException(NOT_FOUND, "No PK fields found for table " + pTableHandle.getTableName());
            }
            RowType rowType = RowType.from(fields);
            return new PhoenixColumnHandle(UPDATE_ROW_ID, rowType);
        }
        catch (SQLException e) {
            throw new PrestoException(PHOENIX_ERROR, e);
        }
    }

    @Override
    public ConnectorTableHandle beginDelete(ConnectorSession session,
            ConnectorTableHandle tableHandle)
    {
        return tableHandle;
    }

    @Override
    public void finishDelete(ConnectorSession session, ConnectorTableHandle tableHandle,
            Collection<Slice> fragments)
    {
    }

    @Override
    public boolean supportsMetadataDelete(ConnectorSession session,
            ConnectorTableHandle tableHandle, ConnectorTableLayoutHandle tableLayoutHandle)
    {
        //TODO add when implemented in Phoenix
        return false;
    }

    public PhoenixOutputTableHandle createTable(ConnectorTableMetadata tableMetadata)
    {
        SchemaTableName schemaTableName = tableMetadata.getTable();
        LinkedList<ColumnMetadata> tableColumns = new LinkedList<>(tableMetadata.getColumns());
        String schema = schemaTableName.getSchemaName();
        String table = schemaTableName.getTableName();

        Map<String, Object> tableProperties = tableMetadata.getProperties();

        if (!getSchemaNames().contains(schema)) {
            throw new PrestoException(NOT_FOUND, "Schema not found: " + schema);
        }

        try (PhoenixConnection connection = phoenixClient.getConnection()) {
            boolean uppercase = connection.getMetaData().storesUpperCaseIdentifiers();
            if (uppercase) {
                schema = schema.toUpperCase(ENGLISH);
                table = table.toUpperCase(ENGLISH);
            }
            String catalog = connection.getCatalog();
            StringBuilder sql = new StringBuilder();
            Optional<Boolean> immutableRows = PhoenixTableProperties.getImmutableRows(tableProperties);
            if (immutableRows.isPresent() && immutableRows.get()) {
                sql.append("CREATE IMMUTABLE TABLE ");
            }
            else {
                sql.append("CREATE TABLE ");
            }
            sql.append(getFullTableName(catalog, schema, table)).append(" (\n ");
            ImmutableList.Builder<String> columnNames = ImmutableList.builder();
            ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();
            ImmutableList.Builder<String> columnList = ImmutableList.builder();
            Set<ColumnMetadata> pkCols = tableColumns.stream().filter(col -> PhoenixTableProperties.isPrimaryKey(col, tableProperties)).collect(Collectors.toSet());
            if (pkCols.isEmpty()) {
                // Add a rowkey when not specified in DDL
                ColumnMetadata rowKeyCol = new ColumnMetadata(ROWKEY, VARCHAR);
                tableColumns.addFirst(rowKeyCol);
                pkCols.add(rowKeyCol);
            }
            List<String> pkNames = new ArrayList<>();

            for (ColumnMetadata column : tableColumns) {
                String columnName = column.getName();
                if (uppercase) {
                    columnName = columnName.toUpperCase(ENGLISH);
                }
                columnNames.add(columnName);
                columnTypes.add(column.getType());
                String typeStatement = toSqlType(column.getType());
                if (pkCols.contains(column)) {
                    typeStatement += " not null";
                    pkNames.add(columnName);
                }
                columnList.add(new StringBuilder()
                        .append(columnName)
                        .append(" ")
                        .append(typeStatement)
                        .toString());
            }

            List<String> columns = columnList.build();
            Joiner.on(", \n ").appendTo(sql, columns);
            sql.append("\n CONSTRAINT PK PRIMARY KEY(");
            Joiner.on(", ").appendTo(sql, pkNames);
            sql.append(")\n)\n");

            ImmutableList.Builder<String> tableOptions = ImmutableList.builder();
            PhoenixTableProperties.getSaltBuckets(tableProperties).ifPresent(value -> tableOptions.add(TableProperty.SALT_BUCKETS + "=" + value));
            PhoenixTableProperties.getSplitOn(tableProperties).ifPresent(value -> tableOptions.add("SPLIT ON (" + value.replace('"', '\'') + ")"));
            PhoenixTableProperties.getDisableWal(tableProperties).ifPresent(value -> tableOptions.add(TableProperty.DISABLE_WAL + "=" + value));
            PhoenixTableProperties.getDefaultColumnFamily(tableProperties).ifPresent(value -> tableOptions.add(TableProperty.DEFAULT_COLUMN_FAMILY + "=" + value));
            PhoenixTableProperties.getBloomfilter(tableProperties).ifPresent(value -> tableOptions.add(HColumnDescriptor.BLOOMFILTER + "='" + value + "'"));
            PhoenixTableProperties.getVersions(tableProperties).ifPresent(value -> tableOptions.add(HConstants.VERSIONS + "=" + value));
            PhoenixTableProperties.getMinVersions(tableProperties).ifPresent(value -> tableOptions.add(HColumnDescriptor.MIN_VERSIONS + "=" + value));
            PhoenixTableProperties.getCompression(tableProperties).ifPresent(value -> tableOptions.add(HColumnDescriptor.COMPRESSION + "='" + value + "'"));
            PhoenixTableProperties.getTimeToLive(tableProperties).ifPresent(value -> tableOptions.add(HColumnDescriptor.TTL + "=" + value));
            Joiner.on(", \n ").appendTo(sql, tableOptions.build());

            phoenixClient.execute(connection, sql.toString());

            return new PhoenixOutputTableHandle(
                    phoenixClient.getConnectorId(),
                    catalog,
                    schema,
                    table,
                    columnNames.build(),
                    columnTypes.build());
        }
        catch (SQLException e) {
            throw new PrestoException(PHOENIX_ERROR, e);
        }
    }

    protected ResultSet getTables(PhoenixConnection connection, String schemaName, String tableName)
            throws SQLException
    {
        DatabaseMetaData metadata = connection.getMetaData();
        String escape = metadata.getSearchStringEscape();
        return metadata.getTables(
                connection.getCatalog(),
                escapeNamePattern(schemaName, escape),
                escapeNamePattern(tableName, escape),
                new String[] {
                        "TABLE", "VIEW"
                });
    }

    public List<PhoenixColumnHandle> getColumns(PhoenixTableHandle tableHandle, boolean requiredRowKey)
    {
        try (PhoenixConnection connection = phoenixClient.getConnection()) {
            try (ResultSet resultSet = getColumns(tableHandle, connection.getMetaData())) {
                List<PhoenixColumnHandle> columns = new ArrayList<>();
                boolean found = false;
                while (resultSet.next()) {
                    found = true;
                    Type columnType = toPrestoType(resultSet.getInt(DATA_TYPE), resultSet.getInt(COLUMN_SIZE), resultSet.getInt(DECIMAL_DIGITS), resultSet.getInt(ARRAY_SIZE), resultSet.getInt(TYPE_ID));
                    // skip unsupported column types
                    if (columnType != null) {
                        String columnName = resultSet.getString("COLUMN_NAME");
                        if (requiredRowKey || !ROWKEY.equals(columnName)) {
                            columns.add(new PhoenixColumnHandle(columnName, columnType));
                        }
                    }
                }
                if (!found) {
                    throw new TableNotFoundException(tableHandle.getSchemaTableName());
                }
                if (columns.isEmpty()) {
                    throw new PrestoException(NOT_SUPPORTED, "Table has no supported column types: " + tableHandle.getSchemaTableName());
                }
                return ImmutableList.copyOf(columns);
            }
        }
        catch (SQLException e) {
            throw new PrestoException(PHOENIX_ERROR, e);
        }
    }

    protected SchemaTableName getSchemaTableName(ResultSet resultSet)
            throws SQLException
    {
        return new SchemaTableName(
                resultSet.getString(TABLE_SCHEM).toLowerCase(ENGLISH),
                resultSet.getString(TABLE_NAME).toLowerCase(ENGLISH));
    }

    public Set<String> getSchemaNames()
    {
        try (PhoenixConnection connection = phoenixClient.getConnection();
                ResultSet resultSet = connection.getMetaData().getSchemas()) {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            while (resultSet.next()) {
                String schemaName = resultSet.getString(TABLE_SCHEM);
                // skip internal schemas
                if (schemaName != null && !"system".equalsIgnoreCase(schemaName)) {
                    schemaNames.add(schemaName.toLowerCase(ENGLISH));
                }
            }
            return schemaNames.build();
        }
        catch (SQLException e) {
            throw new PrestoException(PHOENIX_ERROR, e);
        }
    }
}
