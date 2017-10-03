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
package com.facebook.presto.plugin.jdbc;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.type.CharType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.base.Joiner;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;

import javax.annotation.Nullable;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static com.facebook.presto.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.CharType.createCharType;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Maps.fromProperties;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Collections.nCopies;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class BaseJdbcClient
        implements JdbcClient
{
    private static final Logger log = Logger.get(BaseJdbcClient.class);

    private static final Map<Type, String> SQL_TYPES = ImmutableMap.<Type, String>builder()
            .put(BOOLEAN, "boolean")
            .put(BIGINT, "bigint")
            .put(INTEGER, "integer")
            .put(SMALLINT, "smallint")
            .put(TINYINT, "tinyint")
            .put(DOUBLE, "double precision")
            .put(REAL, "real")
            .put(VARBINARY, "varbinary")
            .put(DATE, "date")
            .put(TIME, "time")
            .put(TIME_WITH_TIME_ZONE, "time with timezone")
            .put(TIMESTAMP, "timestamp")
            .put(TIMESTAMP_WITH_TIME_ZONE, "timestamp with timezone")
            .build();

    protected final String connectorId;
    protected final Driver driver;
    protected final String connectionUrl;
    protected final Properties connectionProperties;
    protected final String identifierQuote;

    private final LoadingCache<String, Optional<String>> schemaMappingCache;
    private final Map<String, LoadingCache<String, Optional<String>>> schemaTableMapping = new ConcurrentHashMap<>();

    public BaseJdbcClient(JdbcConnectorId connectorId, BaseJdbcConfig config, String identifierQuote, Driver driver)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.identifierQuote = requireNonNull(identifierQuote, "identifierQuote is null");
        this.driver = requireNonNull(driver, "driver is null");

        requireNonNull(config, "config is null");
        connectionUrl = config.getConnectionUrl();

        connectionProperties = new Properties();
        if (config.getConnectionUser() != null) {
            connectionProperties.setProperty("user", config.getConnectionUser());
        }
        if (config.getConnectionPassword() != null) {
            connectionProperties.setProperty("password", config.getConnectionPassword());
        }

        // generate the map of lowercased schema names to JDBC schema names
        schemaMappingCache = CacheBuilder.newBuilder().build(new CacheLoader<String, Optional<String>>()
        {
            @Override
            public Optional<String> load(String key)
                    throws Exception
            {
                for (String schemaName : getOriginalSchemas()) {
                    if (schemaName.equals(key)) {
                        return Optional.of(schemaName);
                    }

                    String schemaNameLower = schemaName.toLowerCase(ENGLISH);
                    if (schemaNameLower.equals(key)) {
                        return Optional.of(schemaName);
                    }
                }
                return Optional.empty();
            }
        });

        if (config.isPreloadSchemaTableMapping()) {
            reloadCache();
        }
    }

    /**
     * Invalidates all of the caches and reloads
     */
    protected void reloadCache()
    {
        schemaMappingCache.invalidateAll();

        // this preloads the list of schema names
        Set<String> schemas = getSchemaNames();

        // invalidate and remove from the schema table mapping all records, start from scratch
        schemaTableMapping.keySet().forEach(key -> {
            schemaTableMapping.get(key).invalidateAll();
        });
        schemaTableMapping.clear();

        for (final String schema : schemas) {
            // this preloads the list of table names for each schema
            getTableNames(schema);
        }
    }

    /**
     * Pull the list of Schema names from the RDBMS exactly as they are returned. Override if the RDBMS method to pull the schemas is different.
     *
     * @return schema names in original form
     */
    protected Set<String> getOriginalSchemas()
    {
        try (Connection connection = driver.connect(connectionUrl, connectionProperties);
                ResultSet resultSet = connection.getMetaData().getSchemas()) {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            while (resultSet.next()) {
                String schemaName = resultSet.getString("TABLE_SCHEM");
                if (!schemaName.toLowerCase(ENGLISH).equals("information_schema")) {
                    schemaNames.add(schemaName);
                }
            }
            return schemaNames.build();
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public Set<String> getSchemaNames()
    {
        ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
        Set<String> originalNames = getOriginalSchemas();
        Map<String, Optional<String>> mappedNames = new HashMap<>();
        for (String schemaName : originalNames) {
            String schemaNameLower = schemaName.toLowerCase(ENGLISH);
            schemaNames.add(schemaNameLower);
            mappedNames.put(schemaNameLower, Optional.of(schemaName));
        }

        // if someone is listing all of the schema names, throw them all into the cache as a refresh since we already spent the time pulling them from the DB
        schemaMappingCache.putAll(mappedNames);
        return schemaNames.build();
    }

    /**
     * Pull the list of Table names from the RDBMS exactly as they are returned. Override if the RDBMS method to pull the tables is different. Each {@link String} array must have
     * the table's original schema name as the first element, and the original table name as the second element.
     *
     * @param schema the schema to list the tables for, or NULL for all
     * @return the schema + table names
     */
    protected List<CaseSensitiveMappedSchemaTableName> getOriginalTablesWithSchema(Connection connection, String schema)
    {
        try (ResultSet resultSet = getTables(connection, schema, null)) {
            ImmutableList.Builder<CaseSensitiveMappedSchemaTableName> list = ImmutableList.builder();
            while (resultSet.next()) {
                String schemaName = resultSet.getString("TABLE_SCHEM");
                String tableName = resultSet.getString("TABLE_NAME");
                list.add(new CaseSensitiveMappedSchemaTableName(schemaName, tableName));
            }
            return list.build();
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public List<SchemaTableName> getTableNames(@Nullable String schema)
    {
        try (Connection connection = driver.connect(connectionUrl, connectionProperties)) {
            DatabaseMetaData metadata = connection.getMetaData();

            schema = finalizeSchemaName(metadata, schema);
            Map<String, Map<String, String>> schemaMappedNames = new HashMap<>();
            ImmutableList.Builder<SchemaTableName> tableNameList = ImmutableList.builder();
            for (CaseSensitiveMappedSchemaTableName table : getOriginalTablesWithSchema(connection, schema)) {
                Map<String, String> mappedNames = schemaMappedNames.computeIfAbsent(table.getSchemaName(), s -> new HashMap<>());
                mappedNames.put(table.getSchemaNameLower(), table.getTableName());
                tableNameList.add(new SchemaTableName(table.getSchemaNameLower(), table.getTableNameLower()));
            }
            // if someone is listing all of the table names, throw them all into the cache as a refresh since we already spent the time pulling them from the DB
            for (Map.Entry<String, Map<String, String>> entry : schemaMappedNames.entrySet()) {
                updateTableMapping(entry.getKey(), entry.getValue());
            }
            return tableNameList.build();
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    /**
     * Fill in the {@link LoadingCache} for the mapped table names for the schema provided
     *
     * @param jdbcSchema the original name of the schema as it exists in the JDBC server
     * @param tableMappings mapping of lowercase to original table names
     */
    private void updateTableMapping(String jdbcSchema, Map<String, String> tableMappings)
    {
        LoadingCache<String, Optional<String>> tableNameMapping = getTableMapping(jdbcSchema);
        Map<String, Optional<String>> tmp = new HashMap<>();
        for (Map.Entry<String, String> entry : tableMappings.entrySet()) {
            tmp.put(entry.getKey(), Optional.of(entry.getValue()));
        }
        tableNameMapping.putAll(tmp);
    }

    /**
     * Fetch the {@link LoadingCache} of table mapped names for the given schema name.
     *
     * @param jdbcSchema the original name of the schema as it exists in the JDBC server
     * @return the {@link LoadingCache} which contains the table mapped names
     */
    private LoadingCache<String, Optional<String>> getTableMapping(String jdbcSchema)
    {
        return schemaTableMapping.computeIfAbsent(jdbcSchema, (String s) ->
                CacheBuilder.newBuilder().build(new CacheLoader<String, Optional<String>>()
                {
                    @Override
                    public Optional<String> load(String key)
                            throws Exception
                    {
                        try (Connection connection = driver.connect(connectionUrl, connectionProperties)) {
                            DatabaseMetaData metadata = connection.getMetaData();
                            String jdbcSchemaName = finalizeSchemaName(metadata, jdbcSchema);

                            for (CaseSensitiveMappedSchemaTableName table : getOriginalTablesWithSchema(connection, jdbcSchemaName)) {
                                String tableName = table.getTableName();
                                if (tableName.equals(key)) {
                                    return Optional.of(tableName);
                                }
                                String tableNameLower = table.getTableNameLower();
                                if (tableNameLower.equals(key)) {
                                    return Optional.of(tableName);
                                }
                            }
                        }
                        catch (SQLException e) {
                            throw new PrestoException(JDBC_ERROR, e);
                        }
                        return Optional.empty();
                    }
                }));
    }

    /**
     * Looks up the table name given to map it to it's original, case-sensitive name in the database.
     *
     * @param jdbcSchema The database schema name
     * @param tableName The table name within the schema that needs to be mapped to it's original, or NULL if it couldn't be found in the cache
     * @return The mapped case-sensitive table name, if found, otherwise the original table name passed in
     */
    private String getMappedTableName(String jdbcSchema, String tableName)
    {
        LoadingCache<String, Optional<String>> tableNameMapping = getTableMapping(jdbcSchema);
        Optional<String> value = tableNameMapping.getUnchecked(tableName);
        if (value.isPresent()) {
            return value.get();
        }
        // If we get back an Empty value, invalidate it now so that in the future we can try and load it up again into cache, and return NULL to signify we couldn't look it up
        // At this point it is extremely likely we are trying to create the table, since it wasn't initially present and couldn't be found currently.
        tableNameMapping.invalidate(jdbcSchema);
        return null;
    }

    protected String finalizeSchemaName(DatabaseMetaData metadata, String schemaName)
            throws SQLException
    {
        if (schemaName == null) {
            return null;
        }
        if (metadata.storesUpperCaseIdentifiers()) {
            return schemaName.toUpperCase(ENGLISH);
        }
        else {
            Optional<String> value = schemaMappingCache.getUnchecked(schemaName);
            if (value.isPresent()) {
                return value.get();
            }
            // If we get back an Empty value, invalidate it now so that in the future we can try and load it up again into cache, and return the value we were given
            // At this point it is extremely likely we are trying to create the schema, since it wasn't initially present and couldn't be found currently.
            schemaMappingCache.invalidate(schemaName);
            return schemaName;
        }
    }

    protected String finalizeTableName(DatabaseMetaData metadata, String schemaName, String tableName)
            throws SQLException
    {
        if (schemaName == null || tableName == null) {
            return null;
        }
        if (metadata.storesUpperCaseIdentifiers()) {
            return tableName.toUpperCase(ENGLISH);
        }
        else {
            String value = getMappedTableName(schemaName, tableName);
            if (value == null) {
                return tableName;
            }
            // if we couldn't look up the table in the mapping cache, return the value we were given
            return value;
        }
    }

    @Nullable
    @Override
    public JdbcTableHandle getTableHandle(SchemaTableName schemaTableName)
    {
        try (Connection connection = driver.connect(connectionUrl, connectionProperties)) {
            DatabaseMetaData metadata = connection.getMetaData();

            String jdbcSchemaName = finalizeSchemaName(metadata, schemaTableName.getSchemaName());
            String jdbcTableName = finalizeTableName(metadata, jdbcSchemaName, schemaTableName.getTableName());

            if (jdbcTableName == null) {
                return null;
            }

            try (ResultSet resultSet = getTables(connection, jdbcSchemaName, jdbcTableName)) {
                List<JdbcTableHandle> tableHandles = new ArrayList<>();
                while (resultSet.next()) {
                    tableHandles.add(new JdbcTableHandle(
                            connectorId,
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
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public List<JdbcColumnHandle> getColumns(JdbcTableHandle tableHandle)
    {
        try (Connection connection = driver.connect(connectionUrl, connectionProperties)) {
            try (ResultSet resultSet = getColumns(tableHandle, connection.getMetaData())) {
                List<JdbcColumnHandle> columns = new ArrayList<>();
                boolean found = false;
                while (resultSet.next()) {
                    found = true;
                    Type columnType = toPrestoType(resultSet.getInt("DATA_TYPE"), resultSet.getInt("COLUMN_SIZE"));
                    // skip unsupported column types
                    if (columnType != null) {
                        String columnName = resultSet.getString("COLUMN_NAME");
                        columns.add(new JdbcColumnHandle(connectorId, columnName, columnType));
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
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public ConnectorSplitSource getSplits(JdbcTableLayoutHandle layoutHandle)
    {
        JdbcTableHandle tableHandle = layoutHandle.getTable();
        JdbcSplit jdbcSplit = new JdbcSplit(
                connectorId,
                tableHandle.getCatalogName(),
                tableHandle.getSchemaName(),
                tableHandle.getTableName(),
                connectionUrl,
                fromProperties(connectionProperties),
                layoutHandle.getTupleDomain());
        return new FixedSplitSource(ImmutableList.of(jdbcSplit));
    }

    @Override
    public Connection getConnection(JdbcSplit split)
            throws SQLException
    {
        Connection connection = driver.connect(split.getConnectionUrl(), toProperties(split.getConnectionProperties()));
        try {
            connection.setReadOnly(true);
        }
        catch (SQLException e) {
            connection.close();
            throw e;
        }
        return connection;
    }

    @Override
    public PreparedStatement buildSql(Connection connection, JdbcSplit split, List<JdbcColumnHandle> columnHandles)
            throws SQLException
    {
        return new QueryBuilder(identifierQuote).buildSql(
                this,
                connection,
                split.getCatalogName(),
                split.getSchemaName(),
                split.getTableName(),
                columnHandles,
                split.getTupleDomain());
    }

    @Override
    public JdbcOutputTableHandle beginCreateTable(ConnectorTableMetadata tableMetadata)
    {
        return beginWriteTable(tableMetadata);
    }

    @Override
    public JdbcOutputTableHandle beginInsertTable(ConnectorTableMetadata tableMetadata)
    {
        return beginWriteTable(tableMetadata);
    }

    private JdbcOutputTableHandle beginWriteTable(ConnectorTableMetadata tableMetadata)
    {
        SchemaTableName schemaTableName = tableMetadata.getTable();
        String schema = schemaTableName.getSchemaName();
        String table = schemaTableName.getTableName();

        if (!getSchemaNames().contains(schema)) {
            throw new PrestoException(NOT_FOUND, "Schema not found: " + schema);
        }

        try (Connection connection = driver.connect(connectionUrl, connectionProperties)) {
            DatabaseMetaData metadata = connection.getMetaData();
            boolean uppercase = metadata.storesUpperCaseIdentifiers();

            schema = finalizeSchemaName(metadata, schemaTableName.getSchemaName());
            table = finalizeTableName(metadata, table, schemaTableName.getTableName());

            String catalog = connection.getCatalog();

            String temporaryName = "tmp_presto_" + UUID.randomUUID().toString().replace("-", "");
            StringBuilder sql = new StringBuilder()
                    .append("CREATE TABLE ")
                    .append(quoted(catalog, schema, temporaryName))
                    .append(" (");
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
                columnList.add(new StringBuilder()
                        .append(quoted(columnName))
                        .append(" ")
                        .append(toSqlType(column.getType()))
                        .toString());
            }
            Joiner.on(", ").appendTo(sql, columnList.build());
            sql.append(")");

            execute(connection, sql.toString());

            return new JdbcOutputTableHandle(
                    connectorId,
                    catalog,
                    schema,
                    table,
                    columnNames.build(),
                    columnTypes.build(),
                    temporaryName,
                    connectionUrl,
                    fromProperties(connectionProperties));
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void commitCreateTable(JdbcOutputTableHandle handle)
    {
        StringBuilder sql = new StringBuilder()
                .append("ALTER TABLE ")
                .append(quoted(handle.getCatalogName(), handle.getSchemaName(), handle.getTemporaryTableName()))
                .append(" RENAME TO ")
                .append(quoted(handle.getCatalogName(), handle.getSchemaName(), handle.getTableName()));

        try (Connection connection = getConnection(handle)) {
            execute(connection, sql.toString());
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void finishInsertTable(JdbcOutputTableHandle handle)
    {
        String temporaryTable = quoted(handle.getCatalogName(), handle.getSchemaName(), handle.getTemporaryTableName());
        String targetTable = quoted(handle.getCatalogName(), handle.getSchemaName(), handle.getTableName());
        String insertSql = format("INSERT INTO %s SELECT * FROM %s", targetTable, temporaryTable);
        String cleanupSql = "DROP TABLE " + temporaryTable;

        try (Connection connection = getConnection(handle)) {
            execute(connection, insertSql);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }

        try (Connection connection = getConnection(handle)) {
            execute(connection, cleanupSql);
        }
        catch (SQLException e) {
            log.warn(e, "Failed to cleanup temporary table: %s", temporaryTable);
        }
    }

    @Override
    public void dropTable(JdbcTableHandle handle)
    {
        StringBuilder sql = new StringBuilder()
                .append("DROP TABLE ")
                .append(quoted(handle.getCatalogName(), handle.getSchemaName(), handle.getTableName()));

        try (Connection connection = driver.connect(connectionUrl, connectionProperties)) {
            execute(connection, sql.toString());
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void rollbackCreateTable(JdbcOutputTableHandle handle)
    {
        dropTable(new JdbcTableHandle(
                handle.getConnectorId(),
                new SchemaTableName(handle.getSchemaName(), handle.getTemporaryTableName()),
                handle.getCatalogName(),
                handle.getSchemaName(),
                handle.getTemporaryTableName()));
    }

    @Override
    public String buildInsertSql(JdbcOutputTableHandle handle)
    {
        String vars = Joiner.on(',').join(nCopies(handle.getColumnNames().size(), "?"));
        return new StringBuilder()
                .append("INSERT INTO ")
                .append(quoted(handle.getCatalogName(), handle.getSchemaName(), handle.getTemporaryTableName()))
                .append(" VALUES (").append(vars).append(")")
                .toString();
    }

    @Override
    public Connection getConnection(JdbcOutputTableHandle handle)
            throws SQLException
    {
        return driver.connect(handle.getConnectionUrl(), toProperties(handle.getConnectionProperties()));
    }

    @Override
    public PreparedStatement getPreparedStatement(Connection connection, String sql)
            throws SQLException
    {
        return connection.prepareStatement(sql);
    }

    protected ResultSet getTables(Connection connection, String schemaName, String tableName)
            throws SQLException
    {
        DatabaseMetaData metadata = connection.getMetaData();
        String escape = metadata.getSearchStringEscape();
        return metadata.getTables(
                connection.getCatalog(),
                escapeNamePattern(schemaName, escape),
                escapeNamePattern(tableName, escape),
                new String[] {"TABLE", "VIEW"});
    }

    protected void execute(Connection connection, String query)
            throws SQLException
    {
        try (Statement statement = connection.createStatement()) {
            log.debug("Execute: %s", query);
            statement.execute(query);
        }
    }

    protected Type toPrestoType(int jdbcType, int columnSize)
    {
        switch (jdbcType) {
            case Types.BIT:
            case Types.BOOLEAN:
                return BOOLEAN;
            case Types.TINYINT:
                return TINYINT;
            case Types.SMALLINT:
                return SMALLINT;
            case Types.INTEGER:
                return INTEGER;
            case Types.BIGINT:
                return BIGINT;
            case Types.REAL:
                return REAL;
            case Types.FLOAT:
            case Types.DOUBLE:
            case Types.NUMERIC:
            case Types.DECIMAL:
                return DOUBLE;
            case Types.CHAR:
            case Types.NCHAR:
                return createCharType(min(columnSize, CharType.MAX_LENGTH));
            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.LONGVARCHAR:
            case Types.LONGNVARCHAR:
                if (columnSize > VarcharType.MAX_LENGTH) {
                    return createUnboundedVarcharType();
                }
                return createVarcharType(columnSize);
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return VARBINARY;
            case Types.DATE:
                return DATE;
            case Types.TIME:
                return TIME;
            case Types.TIMESTAMP:
                return TIMESTAMP;
        }
        return null;
    }

    protected String toSqlType(Type type)
    {
        if (type instanceof VarcharType) {
            if (((VarcharType) type).isUnbounded()) {
                return "varchar";
            }
            return "varchar(" + ((VarcharType) type).getLength() + ")";
        }
        if (type instanceof CharType) {
            if (((CharType) type).getLength() == CharType.MAX_LENGTH) {
                return "char";
            }
            return "char(" + ((CharType) type).getLength() + ")";
        }

        String sqlType = SQL_TYPES.get(type);
        if (sqlType != null) {
            return sqlType;
        }
        throw new PrestoException(NOT_SUPPORTED, "Unsupported column type: " + type.getTypeSignature());
    }

    protected String quoted(String name)
    {
        name = name.replace(identifierQuote, identifierQuote + identifierQuote);
        return identifierQuote + name + identifierQuote;
    }

    protected String quoted(String catalog, String schema, String table)
    {
        StringBuilder sb = new StringBuilder();
        if (!isNullOrEmpty(catalog)) {
            sb.append(quoted(catalog)).append(".");
        }
        if (!isNullOrEmpty(schema)) {
            sb.append(quoted(schema)).append(".");
        }
        sb.append(quoted(table));
        return sb.toString();
    }

    protected static String escapeNamePattern(String name, String escape)
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

    private static ResultSet getColumns(JdbcTableHandle tableHandle, DatabaseMetaData metadata)
            throws SQLException
    {
        String escape = metadata.getSearchStringEscape();
        return metadata.getColumns(
                tableHandle.getCatalogName(),
                escapeNamePattern(tableHandle.getSchemaName(), escape),
                escapeNamePattern(tableHandle.getTableName(), escape),
                null);
    }

    private static Properties toProperties(Map<String, String> map)
    {
        Properties properties = new Properties();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            properties.setProperty(entry.getKey(), entry.getValue());
        }
        return properties;
    }
}
