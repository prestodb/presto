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
package com.facebook.presto.plugin.clickhouse;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarbinaryType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.plugin.jdbc.BaseJdbcClient;
import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.plugin.jdbc.JdbcIdentity;
import com.facebook.presto.plugin.jdbc.JdbcOutputTableHandle;
import com.facebook.presto.plugin.jdbc.JdbcTableHandle;
import com.facebook.presto.plugin.jdbc.JdbcTypeHandle;
import com.facebook.presto.plugin.jdbc.mapping.ReadMapping;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.PrestoWarning;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import jakarta.annotation.Nullable;
import jakarta.inject.Inject;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.plugin.clickhouse.ClickHouseEngineType.MERGETREE;
import static com.facebook.presto.plugin.clickhouse.ClickhouseDXLKeyWords.ORDER_BY_PROPERTY;
import static com.facebook.presto.plugin.clickhouse.StandardReadMappings.jdbcTypeToPrestoType;
import static com.facebook.presto.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static com.facebook.presto.plugin.jdbc.JdbcWarningCode.USE_OF_DEPRECATED_CONFIGURATION_PROPERTY;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.sql.ResultSetMetaData.columnNullable;
import static java.util.Collections.nCopies;
import static java.util.Locale.ENGLISH;
import static java.util.Locale.ROOT;
import static java.util.Objects.requireNonNull;

public class ClickHouseClient
        extends BaseJdbcClient
{
    private static final Logger log = Logger.get(ClickHouseClient.class);
    private static final String tempTableNamePrefix = "tmp_presto_";

    private final boolean mapStringAsVarchar;
    private final int commitBatchSize;

    @Inject
    public ClickHouseClient(JdbcConnectorId connectorId, ClickHouseConfig config, ConnectionFactory connectionFactory)
    {
        super(connectorId, config, "\"", new JdbcConnectionFactoryAdapter(connectionFactory));

        this.commitBatchSize = config.getCommitBatchSize();
        this.mapStringAsVarchar = config.isMapStringAsVarchar();
    }

    // Adapter to convert ClickHouse ConnectionFactory to JDBC ConnectionFactory
    private static class JdbcConnectionFactoryAdapter
            implements com.facebook.presto.plugin.jdbc.ConnectionFactory
    {
        private final ConnectionFactory clickHouseConnectionFactory;

        public JdbcConnectionFactoryAdapter(ConnectionFactory clickHouseConnectionFactory)
        {
            this.clickHouseConnectionFactory = requireNonNull(clickHouseConnectionFactory, "clickHouseConnectionFactory is null");
        }

        @Override
        public Connection openConnection(com.facebook.presto.plugin.jdbc.JdbcIdentity identity)
                throws SQLException
        {
            return clickHouseConnectionFactory.openConnection(identity);
        }

        @Override
        public void close()
                throws SQLException
        {
            clickHouseConnectionFactory.close();
        }
    }

    public int getCommitBatchSize()
    {
        return commitBatchSize;
    }

    @Override
    public List<JdbcColumnHandle> getColumns(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        try (Connection connection = connectionFactory.openConnection(JdbcIdentity.from(session))) {
            try (ResultSet resultSet = getColumns(tableHandle, connection.getMetaData())) {
                int allColumns = 0;
                List<JdbcColumnHandle> columns = new ArrayList<>();
                while (resultSet.next()) {
                    allColumns++;
                    // Use getObject to properly handle NULL values for COLUMN_SIZE
                    // getInt() would convert NULL to 0, causing varchar(0) issue
                    Object columnSizeObj = resultSet.getObject("COLUMN_SIZE");
                    int columnSize = (columnSizeObj != null) ? ((Number) columnSizeObj).intValue() : 0;

                    String typeName = resultSet.getString("TYPE_NAME");
                    JdbcTypeHandle typeHandle = new JdbcTypeHandle(
                            resultSet.getInt("DATA_TYPE"),
                            typeName != null ? typeName : "UNKNOWN",
                            columnSize,
                            resultSet.getInt("DECIMAL_DIGITS"));
                    Optional<ReadMapping> columnMapping = toPrestoType(session, typeHandle);
                    // skip unsupported column types
                    if (columnMapping.isPresent()) {
                        String columnName = resultSet.getString("COLUMN_NAME");
                        boolean nullable = columnNullable == resultSet.getInt("NULLABLE");
                        Optional<String> comment = Optional.ofNullable(resultSet.getString("REMARKS"));
                        columns.add(new JdbcColumnHandle(connectorId, columnName, typeHandle, columnMapping.get().getType(), nullable, comment));
                    }
                    else {
                        log.info("The ClickHouse datatype: " + typeHandle.getJdbcTypeName() + " is unsupported.");
                    }
                }
                if (columns.isEmpty()) {
                    // A table may have no supported columns. In rare cases a table might have no columns at all.
                    throw new TableNotFoundException(
                            tableHandle.getSchemaTableName(),
                            format("Table '%s' has no supported columns (all %s columns are not supported)", tableHandle.getSchemaTableName(), allColumns));
                }
                return ImmutableList.copyOf(columns);
            }
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public Optional<ReadMapping> toPrestoType(ConnectorSession session, JdbcTypeHandle typeHandle)
    {
        return jdbcTypeToPrestoType(typeHandle, mapStringAsVarchar);
    }

    protected String quoted(String name)
    {
        name = name.replace(identifierQuote, identifierQuote + identifierQuote);
        return identifierQuote + name + identifierQuote;
    }

    @Override
    protected Collection<String> listSchemas(Connection connection)
    {
        // for ClickHouse, we need to list catalogs instead of schemas
        try (ResultSet resultSet = connection.getMetaData().getCatalogs()) {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            while (resultSet.next()) {
                String schemaName = resultSet.getString("TABLE_CAT");
                // skip internal schemas
                if (!schemaName.equalsIgnoreCase("information_schema")) {
                    schemaNames.add(schemaName);
                }
            }
            return schemaNames.build();
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    public ConnectorSplitSource getSplits(ClickHouseTableLayoutHandle layoutHandle)
    {
        JdbcTableHandle tableHandle = layoutHandle.getTable();
        ClickHouseSplit clickHouseSplit = new ClickHouseSplit(
                connectorId,
                tableHandle.getCatalogName(),
                tableHandle.getSchemaName(),
                tableHandle.getTableName(),
                layoutHandle.getTupleDomain(),
                layoutHandle.getAdditionalPredicate(),
                layoutHandle.getSimpleExpression(),
                layoutHandle.getClickhouseSQL());
        return new FixedSplitSource(ImmutableList.of(clickHouseSplit));
    }

    public PreparedStatement getPreparedStatement(Connection connection, String sql)
            throws SQLException
    {
        return connection.prepareStatement(sql);
    }

    public PreparedStatement buildSql(ConnectorSession session, Connection connection, ClickHouseSplit split, List<ClickHouseColumnHandle> columnHandles)
            throws SQLException
    {
        return new QueryBuilder(identifierQuote).buildSql(
                this,
                session,
                connection,
                split.getCatalogName(),
                split.getSchemaName(),
                split.getTableName(),
                columnHandles,
                split.getTupleDomain(),
                split.getAdditionalPredicate(),
                split.getSimpleExpression(),
                split.getClickhouseSQL());
    }

    public String getIdentifierQuote()
    {
        return identifierQuote;
    }

    public Connection getConnection(JdbcIdentity identity, ClickHouseSplit split)
            throws SQLException
    {
        Connection connection = connectionFactory.openConnection(identity);
        try {
            connection.setReadOnly(true);
        }
        catch (SQLException e) {
            connection.close();
            throw e;
        }
        return connection;
    }

    public Connection getConnection(JdbcIdentity identity, JdbcOutputTableHandle handle)
            throws SQLException
    {
        return connectionFactory.openConnection(identity);
    }

    public String buildInsertSql(JdbcOutputTableHandle handle)
    {
        String columns = Joiner.on(',').join(nCopies(handle.getColumnNames().size(), "?"));
        return new StringBuilder()
                .append("INSERT INTO ")
                .append(quoted(handle.getSchemaName(), handle.getTemporaryTableName()))
                .append(" VALUES (").append(columns).append(")")
                .toString();
    }

    public JdbcTableHandle getTableHandle(ConnectorSession session, JdbcIdentity identity, SchemaTableName schemaTableName)
    {
        try (Connection connection = connectionFactory.openConnection(identity)) {
            String remoteSchema = toRemoteSchemaName(session, identity, connection, schemaTableName.getSchemaName());
            String remoteTable = toRemoteTableName(session, identity, connection, remoteSchema, schemaTableName.getTableName());
            try (ResultSet resultSet = getTables(connection, Optional.of(remoteSchema), Optional.of(remoteTable))) {
                List<JdbcTableHandle> tableHandles = new ArrayList<>();
                while (resultSet.next()) {
                    tableHandles.add(new JdbcTableHandle(
                            connectorId,
                            schemaTableName,
                            null, //"datasets",
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
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public ResultSet getTables(Connection connection, Optional<String> schemaName, Optional<String> tableName)
            throws SQLException
    {
        // Clickhouse maps their "database" to SQL catalogs and does not have schemas
        DatabaseMetaData metadata = connection.getMetaData();
        Optional<String> escape = Optional.ofNullable(metadata.getSearchStringEscape());
        return metadata.getTables(
                schemaName.orElse(null),
                null,
                escapeNamePattern(tableName, escape).orElse(null),
                null);
    }

    @Override
    protected String getTableSchemaName(ResultSet resultSet)
            throws SQLException
    {
        // ClickHouse uses TABLE_CAT for database/schema name
        return resultSet.getString("TABLE_CAT");
    }
    private static ResultSet getColumns(JdbcTableHandle tableHandle, DatabaseMetaData metadata)
            throws SQLException
    {
        // ClickHouse maps databases to catalogs, not schemas
        Optional<String> escape = Optional.ofNullable(metadata.getSearchStringEscape());
        return metadata.getColumns(
                tableHandle.getSchemaName(),  // catalog = ClickHouse database
                null,                          // schema = not used in ClickHouse
                escapeNamePattern(Optional.ofNullable(tableHandle.getTableName()), escape).orElse(null),
                null);
    }
    protected String quoted(@Nullable String schema, String table)
    {
        StringBuilder builder = new StringBuilder();
        if (!isNullOrEmpty(schema)) {
            builder.append(quoted(schema)).append(".");
        }
        builder.append(quoted(table));
        return builder.toString();
    }

    public void addColumn(JdbcIdentity identity, JdbcTableHandle handle, ColumnMetadata column)
    {
        String schema = handle.getSchemaName();
        String table = handle.getTableName();
        String columnName = column.getName();
        String sql = format(
                "ALTER TABLE %s ADD COLUMN %s",
                quoted(schema, table),
                getColumnDefinitionSql(column, columnName));

        try (Connection connection = connectionFactory.openConnection(identity)) {
            execute(connection, sql);
        }
        catch (SQLException e) {
            PrestoException exception = new PrestoException(JDBC_ERROR, e);
            exception.addSuppressed(new RuntimeException("Query: " + sql));
            throw exception;
        }
    }

    public JdbcOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        return createTemporaryTable(session, tableMetadata);
    }

    public JdbcOutputTableHandle beginInsertTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        return beginWriteTable(session, tableMetadata);
    }

    private JdbcOutputTableHandle beginWriteTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        try {
            return beginInsertTable(tableMetadata, session, generateTemporaryTableName());
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    public void dropColumn(JdbcIdentity identity, JdbcTableHandle handle, JdbcColumnHandle column)
    {
        try (Connection connection = connectionFactory.openConnection(identity)) {
            String sql = format(
                    "ALTER TABLE %s DROP COLUMN %s",
                    quoted(handle.getSchemaName(), handle.getTableName()),
                    quoted(column.getColumnName()));
            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    public void finishInsertTable(JdbcIdentity identity, JdbcOutputTableHandle handle)
    {
        String temporaryTable = quoted(handle.getSchemaName(), handle.getTemporaryTableName());
        String targetTable = quoted(handle.getSchemaName(), handle.getTableName());
        String insertSql = format("INSERT INTO %s SELECT * FROM %s", targetTable, temporaryTable);
        String cleanupSql = "DROP TABLE " + temporaryTable;

        try (Connection connection = getConnection(identity, handle)) {
            execute(connection, insertSql);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }

        try (Connection connection = getConnection(identity, handle)) {
            execute(connection, cleanupSql);
        }
        catch (SQLException e) {
            log.warn(e, "Failed to cleanup temporary table: %s", temporaryTable);
        }
    }

    public void commitCreateTable(JdbcIdentity identity, JdbcOutputTableHandle handle)
    {
        renameTable(
                identity,
                handle.getCatalogName(),
                new SchemaTableName(handle.getSchemaName(), handle.getTemporaryTableName()),
                new SchemaTableName(handle.getSchemaName(), handle.getTableName()));
    }

    public JdbcOutputTableHandle createTemporaryTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        try {
            return createTable(tableMetadata, session, generateTemporaryTableName());
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        try {
            createTable(tableMetadata, session, tableMetadata.getTable().getTableName());
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    protected String generateTemporaryTableName()
    {
        return tempTableNamePrefix + UUID.randomUUID().toString().replace("-", "");
    }

    public void abortReadConnection(Connection connection)
            throws SQLException
    {
    }

    protected void execute(Connection connection, String query)
            throws SQLException
    {
        try (Statement statement = connection.createStatement()) {
            log.debug("Execute: %s", query);
            statement.execute(query);
        }
    }

    public void renameColumn(JdbcIdentity identity, JdbcTableHandle handle, JdbcColumnHandle column, String newColumnName)
    {
        String sql = format(
                "ALTER TABLE %s RENAME COLUMN %s TO %s",
                quoted(handle.getSchemaName(), handle.getTableName()),
                quoted(column.getColumnName()),
                quoted(newColumnName));

        try (Connection connection = connectionFactory.openConnection(identity)) {
            execute(connection, sql);
        }
        catch (SQLException e) {
            PrestoException exception = new PrestoException(JDBC_ERROR, "Query: " + sql, e);
            throw exception;
        }
    }

    public JdbcOutputTableHandle beginInsertTable(ConnectorTableMetadata tableMetadata, ConnectorSession session, String tableName)
            throws SQLException
    {
        SchemaTableName schemaTableName = tableMetadata.getTable();
        JdbcIdentity identity = JdbcIdentity.from(session);
        if (!getSchemaNames(session, identity).contains(schemaTableName.getSchemaName())) {
            throw new PrestoException(NOT_FOUND, "Schema not found: " + schemaTableName.getSchemaName());
        }

        try (Connection connection = connectionFactory.openConnection(identity)) {
            boolean uppercase = connection.getMetaData().storesUpperCaseIdentifiers();
            String remoteSchema = toRemoteSchemaName(session, identity, connection, schemaTableName.getSchemaName());
            String remoteTable = toRemoteTableName(session, identity, connection, remoteSchema, schemaTableName.getTableName());
            if (uppercase) {
                tableName = tableName.toUpperCase(ENGLISH);
            }
            String catalog = connection.getCatalog();

            ImmutableList.Builder<String> columnNames = ImmutableList.builder();
            ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();
            ImmutableList.Builder<String> columnList = ImmutableList.builder();
            for (ColumnMetadata column : tableMetadata.getColumns()) {
                String columnName = column.getName();
                if (uppercase) {
                    columnName = columnName.toUpperCase(ENGLISH);
                }
                columnNames.add(columnName);
                columnTypes.add(column.getType());
                columnList.add(getColumnDefinitionSql(column, columnName));
            }

            SchemaTableName remoteTableName = new SchemaTableName(remoteSchema, tableName);
            copyTableSchema(identity, catalog, remoteSchema, schemaTableName, remoteTableName);

            return new JdbcOutputTableHandle(
                    connectorId,
                    catalog,
                    remoteSchema,
                    remoteTable,
                    columnNames.build(),
                    columnTypes.build(),
                    tableName);
        }
    }

    public JdbcOutputTableHandle createTable(ConnectorTableMetadata tableMetadata, ConnectorSession session, String tableName)
            throws SQLException
    {
        SchemaTableName schemaTableName = tableMetadata.getTable();
        JdbcIdentity identity = JdbcIdentity.from(session);
        if (!getSchemaNames(session, identity).contains(schemaTableName.getSchemaName())) {
            throw new PrestoException(NOT_FOUND, "Schema not found: " + schemaTableName.getSchemaName());
        }

        try (Connection connection = connectionFactory.openConnection(identity)) {
            boolean uppercase = connection.getMetaData().storesUpperCaseIdentifiers();
            String remoteSchema = toRemoteSchemaName(session, identity, connection, schemaTableName.getSchemaName());
            String remoteTable = toRemoteTableName(session, identity, connection, remoteSchema, schemaTableName.getTableName());
            if (uppercase && !caseSensitiveNameMatchingEnabled) {
                tableName = tableName.toUpperCase(ENGLISH);
            }
            String catalog = connection.getCatalog();

            ImmutableList.Builder<String> columnNames = ImmutableList.builder();
            ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();
            ImmutableList.Builder<String> columnList = ImmutableList.builder();
            for (ColumnMetadata column : tableMetadata.getColumns()) {
                String columnName = column.getName();
                if (uppercase && !caseSensitiveNameMatchingEnabled) {
                    columnName = columnName.toUpperCase(ENGLISH);
                }
                columnNames.add(columnName);
                columnTypes.add(column.getType());
                columnList.add(getColumnDefinitionSql(column, columnName));
            }

            RemoteTableName remoteTableName = new RemoteTableName(Optional.ofNullable(catalog), Optional.ofNullable(remoteSchema), tableName);
            String sql = createTableSql(remoteTableName, columnList.build(), tableMetadata);
            execute(connection, sql);

            return new JdbcOutputTableHandle(
                    connectorId,
                    catalog,
                    remoteSchema,
                    remoteTable,
                    columnNames.build(),
                    columnTypes.build(),
                    tableName);
        }
    }

    public void rollbackCreateTable(JdbcIdentity identity, JdbcOutputTableHandle handle)
    {
        dropTable(identity, new JdbcTableHandle(
                handle.getConnectorId(),
                new SchemaTableName(handle.getSchemaName(), handle.getTemporaryTableName()),
                handle.getCatalogName(),
                handle.getSchemaName(),
                handle.getTemporaryTableName()));
    }

    protected Map<String, String> listTablesByLowerCase(Connection connection, String remoteSchema)
    {
        try (ResultSet resultSet = getTables(connection, Optional.of(remoteSchema), Optional.empty())) {
            ImmutableMap.Builder<String, String> map = ImmutableMap.builder();
            while (resultSet.next()) {
                String tableName = resultSet.getString("TABLE_NAME");
                map.put(tableName.toLowerCase(ENGLISH), tableName);
            }
            return map.build();
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    public void dropTable(JdbcIdentity identity, JdbcTableHandle handle)
    {
        StringBuilder sql = new StringBuilder()
                .append("DROP TABLE ")
                .append(quoted(handle.getSchemaName(), handle.getTableName()));

        try (Connection connection = connectionFactory.openConnection(identity)) {
            execute(connection, sql.toString());
        }
        catch (SQLException e) {
            PrestoException exception = new PrestoException(JDBC_ERROR, e);
            exception.addSuppressed(new RuntimeException("Query: " + sql));
            throw exception;
        }
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, JdbcTableHandle handle, List<JdbcColumnHandle> columnHandles, TupleDomain<ColumnHandle> tupleDomain)
    {
        return TableStatistics.empty();
    }

    public boolean schemaExists(ConnectorSession session, JdbcIdentity identity, String schema)
    {
        return getSchemaNames(session, identity).contains(schema);
    }

    public void renameTable(JdbcIdentity identity, JdbcTableHandle handle, SchemaTableName newTable)
    {
        renameTable(identity, handle.getCatalogName(), handle.getSchemaTableName(), newTable);
    }

    public void createSchema(JdbcIdentity identity, String schemaName)
    {
        try (Connection connection = connectionFactory.openConnection(identity)) {
            execute(connection, "CREATE DATABASE " + quoted(schemaName));
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    public void dropSchema(JdbcIdentity identity, String schemaName)
    {
        try (Connection connection = connectionFactory.openConnection(identity)) {
            execute(connection, "DROP DATABASE " + quoted(schemaName));
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    protected void renameTable(JdbcIdentity identity, String catalogName, SchemaTableName oldTable, SchemaTableName newTable)
    {
        String schemaName = oldTable.getSchemaName();
        String tableName = oldTable.getTableName();
        String newSchemaName = newTable.getSchemaName();
        String newTableName = newTable.getTableName();
        String sql = format("RENAME TABLE %s TO %s",
                quoted(schemaName, tableName),
                quoted(newSchemaName, newTableName));

        try (Connection connection = connectionFactory.openConnection(identity)) {
            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    private String getColumnDefinitionSql(ColumnMetadata column, String columnName)
    {
        StringBuilder builder = new StringBuilder()
                .append(quoted(columnName))
                .append(" ");
        String columnTypeMapping = toWriteMapping(column.getType());
        if (column.isNullable()) {
            builder.append("Nullable(").append(columnTypeMapping).append(")");
        }
        else {
            builder.append(columnTypeMapping);
        }
        return builder.toString();
    }

    protected String createTableSql(RemoteTableName remoteTableName, List<String> columns, ConnectorTableMetadata tableMetadata)
    {
        ImmutableList.Builder<String> tableOptions = ImmutableList.builder();
        Map<String, Object> tableProperties = tableMetadata.getProperties();
        ClickHouseEngineType engine = ClickHouseTableProperties.getEngine(tableProperties);
        tableOptions.add("ENGINE = " + engine.getEngineType());
        if (engine == MERGETREE && formatProperty(ClickHouseTableProperties.getOrderBy(tableProperties)).equals(Optional.empty())) {
            // order_by property is required
            throw new PrestoException(INVALID_TABLE_PROPERTY,
                    format("The property of %s is required for table engine %s", ORDER_BY_PROPERTY, engine.getEngineType()));
        }
        formatProperty(ClickHouseTableProperties.getOrderBy(tableProperties)).ifPresent(value -> tableOptions.add("ORDER BY " + value));
        formatProperty(ClickHouseTableProperties.getPrimaryKey(tableProperties)).ifPresent(value -> tableOptions.add("PRIMARY KEY " + value));
        formatProperty(ClickHouseTableProperties.getPartitionBy(tableProperties)).ifPresent(value -> tableOptions.add("PARTITION BY " + value));
        ClickHouseTableProperties.getSampleBy(tableProperties).ifPresent(value -> tableOptions.add("SAMPLE BY " + value));

        return format("CREATE TABLE %s (%s) %s", quoted(remoteTableName), join(", ", columns), join(" ", tableOptions.build()));
    }

    /**
     * format property to match ClickHouse create table statement
     *
     * @param properties property will be formatted
     * @return formatted property
     */
    private Optional<String> formatProperty(List<String> properties)
    {
        if (properties == null || properties.isEmpty()) {
            return Optional.empty();
        }
        else if (properties.size() == 1) {
            // only one column
            return Optional.of(properties.get(0));
        }
        else {
            // include more than one columns
            return Optional.of("(" + String.join(",", properties) + ")");
        }
    }

    private String toWriteMapping(Type type)
    {
        if (type == BOOLEAN) {
            // ClickHouse uses UInt8 as boolean, restricted values to 0 and 1.
            return "UInt8";
        }
        if (type == TINYINT) {
            return "Int8";
        }
        if (type == SMALLINT) {
            return "Int16";
        }
        if (type == INTEGER) {
            // Map to signed Int32 for standard Presto INTEGER semantics
            return "Int32";
        }
        if (type == BIGINT) {
            // Map to signed Int64 for standard Presto BIGINT semantics
            return "Int64";
        }
        if (type.equals(REAL)) {
            return "Float32";
        }
        if (type.equals(DOUBLE)) {
            return "Float64";
        }
        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            String dataType = format("Decimal(%s, %s)", decimalType.getPrecision(), decimalType.getScale());
            return dataType;
        }
        if (type instanceof CharType || type instanceof VarcharType) {
            // The String type replaces the types VARCHAR, BLOB, CLOB, and others from other DBMSs.
            return "String";
        }
        if (type instanceof VarbinaryType) {
            return "String";
        }
        if (type == DATE) {
            return "Date";
        }
        if (type instanceof TimestampType) {
            return "DateTime64(3)";
        }
        throw new PrestoException(NOT_SUPPORTED, "Unsupported column type: " + type);
    }

    protected void copyTableSchema(JdbcIdentity identity, String catalogName, String schemaName, SchemaTableName tableName, SchemaTableName newTableName)
    {
        // ClickHouse does not support `create table tbl as select * from tbl2 where 0=1`
        // ClickHouse support the following two methods to copy schema
        // 1. create table tbl as tbl2
        // 2. create table tbl1 ENGINE=<engine> as select * from tbl2
        String oldCreateTableName = tableName.getTableName();
        String newCreateTableName = newTableName.getTableName();
        String sql = format(
                "CREATE TABLE %s AS %s ",
                quoted(schemaName, newCreateTableName),
                quoted(schemaName, oldCreateTableName));

        try (Connection connection = connectionFactory.openConnection(identity)) {
            execute(connection, sql);
        }
        catch (SQLException e) {
            PrestoException exception = new PrestoException(JDBC_ERROR, e);
            exception.addSuppressed(new RuntimeException("Query: " + sql));
            throw exception;
        }
    }

    private String quoted(RemoteTableName remoteTableName)
    {
        return quoted(
                remoteTableName.getSchemaName().orElse(null),
                remoteTableName.getTableName());
    }

    protected String toRemoteSchemaName(ConnectorSession session, JdbcIdentity identity, Connection connection, String schemaName)
    {
        requireNonNull(schemaName, "schemaName is null");

        if (caseInsensitiveNameMatching) {
            session.getWarningCollector().add(new PrestoWarning(USE_OF_DEPRECATED_CONFIGURATION_PROPERTY,
                    "'case-insensitive-name-matching' is enabled. Use of this configuration value may lead to query failures. " +
                            "Please switch to using 'case-sensitive-name-matching' for proper case sensitivity behavior."));
            try {
                Map<String, String> mapping = remoteSchemaNames.getIfPresent(identity);
                if (mapping != null && !mapping.containsKey(schemaName)) {
                    // This might be a schema that has just been created. Force reload.
                    mapping = null;
                }
                if (mapping == null) {
                    mapping = listSchemasByLowerCase(connection);
                    remoteSchemaNames.put(identity, mapping);
                }
                String remoteSchema = mapping.get(schemaName);
                if (remoteSchema != null) {
                    return remoteSchema;
                }
            }
            catch (RuntimeException e) {
                throw new PrestoException(JDBC_ERROR, "Failed to find remote schema name: " + firstNonNull(e.getMessage(), e), e);
            }
        }

        try {
            DatabaseMetaData metadata = connection.getMetaData();
            if (metadata.storesUpperCaseIdentifiers() && !caseSensitiveNameMatchingEnabled) {
                return schemaName.toUpperCase(ENGLISH);
            }
            return schemaName;
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    protected Map<String, String> listSchemasByLowerCase(Connection connection)
    {
        return listSchemas(connection).stream()
                .collect(toImmutableMap(schemaName -> schemaName.toLowerCase(ENGLISH), schemaName -> schemaName));
    }

    public String normalizeIdentifier(String identifier)
    {
        return caseSensitiveNameMatchingEnabled ? identifier : identifier.toLowerCase(ROOT);
    }
}
