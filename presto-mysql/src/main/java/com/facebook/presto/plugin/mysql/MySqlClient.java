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
package com.facebook.presto.plugin.mysql;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.plugin.jdbc.BaseJdbcClient;
import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.ConnectionFactory;
import com.facebook.presto.plugin.jdbc.DriverConnectionFactory;
import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.plugin.jdbc.JdbcIdentity;
import com.facebook.presto.plugin.jdbc.JdbcSplit;
import com.facebook.presto.plugin.jdbc.JdbcTableHandle;
import com.facebook.presto.plugin.jdbc.JdbcTypeHandle;
import com.facebook.presto.plugin.jdbc.QueryBuilder;
import com.facebook.presto.plugin.jdbc.mapping.ReadMapping;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.ConnectorViewDefinition;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.analyzer.ViewDefinition;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.mysql.cj.jdbc.JdbcStatement;
import com.mysql.jdbc.Driver;
import jakarta.inject.Inject;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.StandardTypes.GEOMETRY;
import static com.facebook.presto.common.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.Varchars.isVarcharType;
import static com.facebook.presto.plugin.jdbc.DriverConnectionFactory.basicConnectionProperties;
import static com.facebook.presto.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static com.facebook.presto.plugin.jdbc.QueryBuilder.quote;
import static com.facebook.presto.plugin.jdbc.mapping.StandardColumnMappings.geometryReadMapping;
import static com.facebook.presto.spi.StandardErrorCode.ALREADY_EXISTS;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.function.Function.identity;

public class MySqlClient
        extends BaseJdbcClient
{
    /**
     * Error code corresponding to code thrown when a table already exists.
     * The code is derived from the MySQL documentation.
     *
     * @see <a href="https://dev.mysql.com/doc/connector-j/en/connector-j-reference-error-sqlstates.html">MySQL documentation</a>
     */
    private static final String SQL_STATE_ER_TABLE_EXISTS_ERROR = "42S01";
    private static final JsonCodec<ViewDefinition> VIEW_CODEC = JsonCodec.jsonCodec(ViewDefinition.class);

    @Inject
    public MySqlClient(JdbcConnectorId connectorId, BaseJdbcConfig config, MySqlConfig mySqlConfig)
            throws SQLException
    {
        super(connectorId, config, "`", connectionFactory(config, mySqlConfig));
    }

    private static ConnectionFactory connectionFactory(BaseJdbcConfig config, MySqlConfig mySqlConfig)
            throws SQLException
    {
        Properties connectionProperties = basicConnectionProperties(config);
        connectionProperties.setProperty("useInformationSchema", "true");
        connectionProperties.setProperty("nullCatalogMeansCurrent", "false");
        connectionProperties.setProperty("useUnicode", "true");
        connectionProperties.setProperty("characterEncoding", "utf8");
        connectionProperties.setProperty("tinyInt1isBit", "false");
        if (mySqlConfig.isAutoReconnect()) {
            connectionProperties.setProperty("autoReconnect", String.valueOf(mySqlConfig.isAutoReconnect()));
            connectionProperties.setProperty("maxReconnects", String.valueOf(mySqlConfig.getMaxReconnects()));
        }
        if (mySqlConfig.getConnectionTimeout() != null) {
            connectionProperties.setProperty("connectTimeout", String.valueOf(mySqlConfig.getConnectionTimeout().toMillis()));
        }

        return new DriverConnectionFactory(
                new Driver(),
                config.getConnectionUrl(),
                Optional.ofNullable(config.getUserCredentialName()),
                Optional.ofNullable(config.getPasswordCredentialName()),
                connectionProperties);
    }

    @Override
    protected Collection<String> listSchemas(Connection connection)
    {
        // for MySQL, we need to list catalogs instead of schemas
        try (ResultSet resultSet = connection.getMetaData().getCatalogs()) {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            while (resultSet.next()) {
                String schemaName = resultSet.getString("TABLE_CAT");
                // skip internal schemas
                if (!listSchemasIgnoredSchemas.contains(schemaName.toLowerCase(ENGLISH))) {
                    schemaNames.add(schemaName);
                }
            }
            return schemaNames.build();
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void abortReadConnection(Connection connection)
            throws SQLException
    {
        // Abort connection before closing. Without this, the MySQL driver
        // attempts to drain the connection by reading all the results.
        connection.abort(directExecutor());
    }

    @Override
    public PreparedStatement getPreparedStatement(ConnectorSession session, Connection connection, String sql)
            throws SQLException
    {
        PreparedStatement statement = connection.prepareStatement(sql);
        if (statement.isWrapperFor(JdbcStatement.class)) {
            statement.unwrap(JdbcStatement.class).enableStreamingResults();
        }
        return statement;
    }

    @Override
    protected ResultSet getTables(Connection connection, Optional<String> schemaName, Optional<String> tableName)
            throws SQLException
    {
        // MySQL maps their "database" to SQL catalogs and does not have schemas
        DatabaseMetaData metadata = connection.getMetaData();
        Optional<String> escape = Optional.ofNullable(metadata.getSearchStringEscape());
        return metadata.getTables(
                schemaName.orElse(null),
                null,
                escapeNamePattern(tableName, escape).orElse(null),
                new String[] {"TABLE", "VIEW"});
    }

    @Override
    protected String getTableSchemaName(ResultSet resultSet)
            throws SQLException
    {
        // MySQL uses catalogs instead of schemas
        return resultSet.getString("TABLE_CAT");
    }

    @Override
    protected String toSqlType(Type type)
    {
        if (REAL.equals(type)) {
            return "float";
        }
        if (TIME_WITH_TIME_ZONE.equals(type) || TIMESTAMP_WITH_TIME_ZONE.equals(type)) {
            throw new PrestoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
        }
        if (type instanceof TimestampType) {
            // In order to preserve microsecond information for TIMESTAMP_MICROSECONDS
            return "datetime(6)";
        }
        if (VARBINARY.equals(type)) {
            return "mediumblob";
        }
        if (isVarcharType(type)) {
            VarcharType varcharType = (VarcharType) type;
            if (varcharType.isUnbounded()) {
                return "longtext";
            }
            if (varcharType.getLengthSafe() <= 255) {
                return "tinytext";
            }
            if (varcharType.getLengthSafe() <= 65535) {
                return "text";
            }
            if (varcharType.getLengthSafe() <= 16777215) {
                return "mediumtext";
            }
            return "longtext";
        }

        return super.toSqlType(type);
    }

    @Override
    public PreparedStatement buildSql(ConnectorSession session, Connection connection, JdbcSplit split, List<JdbcColumnHandle> columnHandles)
            throws SQLException
    {
        Map<String, String> columnExpressions = columnHandles.stream()
                .filter(handle -> handle.getJdbcTypeHandle().getJdbcTypeName().equalsIgnoreCase(GEOMETRY))
                .map(JdbcColumnHandle::getColumnName)
                .collect(toImmutableMap(
                        identity(),
                        columnName -> "ST_AsBinary(" + quote(identifierQuote, columnName) + ")"));

        return new QueryBuilder(identifierQuote).buildSql(
                this,
                session,
                connection,
                split.getCatalogName(),
                split.getSchemaName(),
                split.getTableName(),
                columnHandles,
                columnExpressions,
                split.getTupleDomain(),
                split.getAdditionalPredicate());
    }

    @Override
    public Optional<ReadMapping> toPrestoType(ConnectorSession session, JdbcTypeHandle typeHandle)
    {
        String typeName = typeHandle.getJdbcTypeName();

        if (typeName.equalsIgnoreCase(GEOMETRY)) {
            return Optional.of(geometryReadMapping());
        }

        return super.toPrestoType(session, typeHandle);
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        try {
            createTable(tableMetadata, session, tableMetadata.getTable().getTableName());
        }
        catch (SQLException e) {
            if (SQL_STATE_ER_TABLE_EXISTS_ERROR.equals(e.getSQLState())) {
                throw new PrestoException(ALREADY_EXISTS, e);
            }
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void renameColumn(ConnectorSession session, JdbcIdentity identity, JdbcTableHandle handle, JdbcColumnHandle jdbcColumn, String newColumnName)
    {
        try (Connection connection = connectionFactory.openConnection(identity)) {
            DatabaseMetaData metadata = connection.getMetaData();
            if (metadata.storesUpperCaseIdentifiers()) {
                newColumnName = newColumnName.toUpperCase(ENGLISH);
            }
            String sql = format(
                    "ALTER TABLE %s RENAME COLUMN %s TO %s",
                    quoted(handle.getCatalogName(), handle.getSchemaName(), handle.getTableName()),
                    quoted(jdbcColumn.getColumnName()),
                    quoted(newColumnName));
            execute(connection, sql);
        }
        catch (SQLSyntaxErrorException e) {
            // MySQL versions earlier than 8 do not support the above RENAME COLUMN syntax
            throw new PrestoException(NOT_SUPPORTED, format("Rename column not supported in catalog: '%s'", handle.getCatalogName()), e);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    protected void renameTable(JdbcIdentity identity, String catalogName, SchemaTableName oldTable, SchemaTableName newTable)
    {
        // MySQL doesn't support specifying the catalog name in a rename; by setting the
        // catalogName parameter to null it will be omitted in the alter table statement.
        super.renameTable(identity, null, oldTable, newTable);
    }

    @Override
    public String normalizeIdentifier(ConnectorSession session, String identifier)
    {
        return caseSensitiveNameMatchingEnabled ? identifier : identifier.toLowerCase(ENGLISH);
    }

    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, List<SchemaTableName> tableNames)
    {
        JdbcIdentity identity = new JdbcIdentity(session.getUser(), session.getIdentity().getExtraCredentials());
        ImmutableMap.Builder<SchemaTableName, ConnectorViewDefinition> views = ImmutableMap.builder();

        try (Connection connection = connectionFactory.openConnection(identity)) {
            for (SchemaTableName schemaTableName : tableNames) {
                String schemaName = schemaTableName.getSchemaName();
                String tableName = schemaTableName.getTableName();

                String sql = format(
                        "SELECT * FROM INFORMATION_SCHEMA.VIEWS " +
                                "WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'",
                        schemaName, tableName);

                try (Statement statement = connection.createStatement();
                        ResultSet resultSet = statement.executeQuery(sql)) {
                    while (resultSet.next()) {
                        ViewDefinition viewDefinition = getViewDefinition(resultSet, session, connectorId, schemaTableName, schemaName, tableName);

                        SchemaTableName viewName = new SchemaTableName(schemaName, tableName);
                        String viewData = VIEW_CODEC.toJson(viewDefinition);

                        String owner = resultSet.getString("DEFINER");
                        views.put(viewName, new ConnectorViewDefinition(
                                viewName,
                                Optional.of(owner),
                                viewData));
                    }
                }
            }
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
        return views.build();
    }

    private ViewDefinition getViewDefinition(ResultSet resultSet, ConnectorSession session, String connectorId, SchemaTableName schemaTableName, String schemaName, String tableName)
            throws SQLException
    {
        String owner = resultSet.getString("DEFINER");
        boolean runAsInvoker = "INVOKER".equals(resultSet.getString("SECURITY_TYPE"));
        // StatementAnalyzer can't parse sql with back ticks, so we replace them here
        String viewSql = resultSet.getString("VIEW_DEFINITION").replace('`', '"');

        List<JdbcColumnHandle> jdbcColumns = super.getColumns(session, new JdbcTableHandle(
                connectorId,
                schemaTableName,
                schemaName,
                null,
                tableName));

        List<ViewDefinition.ViewColumn> columns = jdbcColumns.stream()
                .map(MySqlClient::toViewColumn)
                .collect(toImmutableList());

        return new ViewDefinition(
                viewSql,
                Optional.of(connectorId),
                Optional.of(schemaName),
                columns,
                Optional.of(owner),
                runAsInvoker);
    }

    private static ViewDefinition.ViewColumn toViewColumn(JdbcColumnHandle jdbcColumn)
    {
        return new ViewDefinition.ViewColumn(jdbcColumn.getColumnName(), jdbcColumn.getColumnType());
    }

    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> schemaName)
    {
        JdbcIdentity identity = JdbcIdentity.from(session);
        try (Connection connection = connectionFactory.openConnection(identity)) {
            DatabaseMetaData metadata = connection.getMetaData();
            Optional<String> escape = Optional.ofNullable(metadata.getSearchStringEscape());
            try (ResultSet resultSet = metadata.getTables(
                    schemaName.orElse(null),
                    null,
                    escapeNamePattern(Optional.empty(), escape).orElse(null),
                    new String[] {"VIEW"})) {
                ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
                while (resultSet.next()) {
                    String tableName = resultSet.getString("TABLE_NAME");
                    String schema = schemaName.orElse(resultSet.getString("TABLE_SCHEM"));
                    builder.add(new SchemaTableName(
                            normalizeIdentifier(session, schema),
                            normalizeIdentifier(session, tableName)));
                }
                return builder.build();
            }
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    public List<SchemaTableName> listSchemasForViews(ConnectorSession session)
    {
        List<SchemaTableName> allTableNames = new ArrayList<>();
        JdbcIdentity identity = JdbcIdentity.from(session);
        try (Connection connection = connectionFactory.openConnection(identity)) {
            Collection<String> schemaNames = listSchemas(connection);
            for (String schema : schemaNames) {
                List<SchemaTableName> tablesNames = listViews(session, Optional.ofNullable(schema));
                allTableNames.addAll(tablesNames);
            }
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
        return allTableNames;
    }

    public void createView(ConnectorSession session, ConnectorTableMetadata viewMetadata, String viewData, boolean replace)
    {
        SchemaTableName viewName = viewMetadata.getTable();
        JdbcIdentity identity = JdbcIdentity.from(session);

        // Deserialize the Presto-internal ViewDefinition JSON to extract originalSql
        String originalSql;
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode viewDefinition = mapper.readTree(viewData);
            JsonNode originalSqlNode = viewDefinition.get("originalSql");
            if (originalSqlNode == null || originalSqlNode.isNull()) {
                throw new PrestoException(JDBC_ERROR, "Missing 'originalSql' field in view data");
            }
            originalSql = originalSqlNode.asText();
        }
        catch (JsonProcessingException e) {
            throw new PrestoException(JDBC_ERROR, "Failed to parse view data: " + e.getMessage(), e);
        }

        try (Connection connection = connectionFactory.openConnection(identity)) {
            String sql = format(
                    "%s VIEW %s AS %s",
                    replace ? "CREATE OR REPLACE" : "CREATE",
                    quoted(null, viewName.getSchemaName(), viewName.getTableName()),
                    originalSql);
            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    public void renameView(ConnectorSession session, SchemaTableName viewName, SchemaTableName newViewName)
    {
        JdbcIdentity identity = JdbcIdentity.from(session);

        try (Connection connection = connectionFactory.openConnection(identity)) {
            String sql = format(
                    "RENAME TABLE %s TO %s",
                    quoted(null, viewName.getSchemaName(), viewName.getTableName()),
                    quoted(null, newViewName.getSchemaName(), newViewName.getTableName()));
            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        JdbcIdentity identity = JdbcIdentity.from(session);

        try (Connection connection = connectionFactory.openConnection(identity)) {
            String sql = format(
                    "DROP VIEW %s",
                    quoted(null, viewName.getSchemaName(), viewName.getTableName()));
            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }
}
