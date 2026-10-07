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
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.analyzer.ViewDefinition;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.mysql.cj.jdbc.JdbcStatement;
import com.mysql.jdbc.Driver;
import jakarta.inject.Inject;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

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
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
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
    /**
     * Error codes MySQL raises when it cannot resolve a view query written for Presto: a syntax
     * error, as a catalog qualified name or a double quoted identifier gives, and a table named
     * without a schema on a connection that has no default database.
     *
     * @see <a href="https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html">MySQL documentation</a>
     */
    private static final int ER_PARSE_ERROR = 1064;
    private static final int ER_NO_DB_ERROR = 1046;
    private final JsonCodec<ViewDefinition> viewCodec;

    @Inject
    public MySqlClient(
            JdbcConnectorId connectorId,
            BaseJdbcConfig config,
            MySqlConfig mySqlConfig,
            JsonCodec<ViewDefinition> viewCodec)
            throws SQLException
    {
        super(connectorId, config, "`", connectionFactory(config, mySqlConfig));
        this.viewCodec = requireNonNull(viewCodec, "viewCodec is null");
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

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, SchemaTablePrefix prefix)
    {
        JdbcIdentity identity = JdbcIdentity.from(session);
        ImmutableMap.Builder<SchemaTableName, ConnectorViewDefinition> views = ImmutableMap.builder();

        try (Connection connection = connectionFactory.openConnection(identity)) {
            List<RemoteView> remoteViews = listRemoteViews(connection, prefix);
            if (remoteViews.isEmpty()) {
                return ImmutableMap.of();
            }
            ListMultimap<SchemaTableName, ViewDefinition.ViewColumn> columns = getViewColumns(session, connection, prefix, remoteViews);

            for (RemoteView remoteView : remoteViews) {
                List<ViewDefinition.ViewColumn> viewColumns = columns.get(remoteView.name);
                if (viewColumns.isEmpty()) {
                    // No column of the view has a type Presto can read. Leaving it out, rather than
                    // failing the whole listing, lets the lookup fall through to the table flow,
                    // which reports the view as having no supported columns.
                    continue;
                }
                SchemaTableName viewName = viewName(session, prefix, remoteView.name);
                ViewDefinition viewDefinition = new ViewDefinition(
                        remoteView.sql,
                        Optional.of(connectorId),
                        Optional.of(viewName.getSchemaName()),
                        viewColumns,
                        // an INVOKER view has no owner, as CreateViewTask records it, so the analyzer runs
                        // the view as the querying user rather than as the definer
                        remoteView.runAsInvoker ? Optional.empty() : Optional.of(remoteView.owner),
                        remoteView.runAsInvoker);

                views.put(viewName, new ConnectorViewDefinition(
                        viewName,
                        Optional.of(remoteView.owner),
                        viewCodec.toJson(viewDefinition)));
            }
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
        return views.build();
    }

    /**
     * Reads the columns of the listed views with one DatabaseMetaData.getColumns call per schema
     * holding any of them, instead of one call per view, so listing a schema costs one metadata
     * round trip however many views it holds. A call returns the columns of the tables in its
     * schema as well, which are dropped here. A prefix without a schema is not read in a single
     * call across every database, which would return every column on the server. MySQL reports
     * its databases as JDBC catalogs, so the schema goes in the catalog argument, as it does in
     * {@link #getTables}. The result is keyed by the names MySQL reports, which are the same in
     * INFORMATION_SCHEMA.VIEWS and INFORMATION_SCHEMA.COLUMNS, and keeps the ordinal order
     * getColumns returns for each view.
     */
    private ListMultimap<SchemaTableName, ViewDefinition.ViewColumn> getViewColumns(
            ConnectorSession session,
            Connection connection,
            SchemaTablePrefix prefix,
            List<RemoteView> remoteViews)
            throws SQLException
    {
        Set<SchemaTableName> viewNames = remoteViews.stream()
                .map(remoteView -> remoteView.name)
                .collect(toImmutableSet());
        Set<String> schemaNames = prefix.getSchemaName() != null
                ? ImmutableSet.of(prefix.getSchemaName())
                : viewNames.stream().map(SchemaTableName::getSchemaName).collect(toImmutableSet());

        ImmutableListMultimap.Builder<SchemaTableName, ViewDefinition.ViewColumn> columns = ImmutableListMultimap.builder();
        DatabaseMetaData metadata = connection.getMetaData();
        for (String schemaName : schemaNames) {
            try (ResultSet resultSet = getColumns(metadata, schemaName, null, prefix.getTableName())) {
                while (resultSet.next()) {
                    SchemaTableName name = new SchemaTableName(resultSet.getString("TABLE_CAT"), resultSet.getString("TABLE_NAME"));
                    if (viewNames.contains(name)) {
                        toColumnHandle(session, resultSet).ifPresent(column ->
                                columns.put(name, new ViewDefinition.ViewColumn(column.getColumnName(), column.getColumnType())));
                    }
                }
            }
        }
        return columns.build();
    }

    private SchemaTableName viewName(ConnectorSession session, SchemaTablePrefix prefix, SchemaTableName remoteName)
    {
        // A prefix naming one table is keyed by the requested name rather than the name MySQL
        // reports. MySQL compares schema and table names here under the collation of
        // INFORMATION_SCHEMA, so a row can come back in a different case than was asked for, and
        // the caller looks the view up by the name it passed in.
        if (prefix.getTableName() != null) {
            return new SchemaTableName(prefix.getSchemaName(), prefix.getTableName());
        }
        String schemaName = prefix.getSchemaName() != null ? prefix.getSchemaName() : remoteName.getSchemaName();
        return new SchemaTableName(
                normalizeIdentifier(session, schemaName),
                normalizeIdentifier(session, remoteName.getTableName()));
    }

    @Override
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
                    String schema = schemaName.orElse(resultSet.getString("TABLE_CAT"));
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

    @Override
    public void createView(ConnectorSession session, ConnectorTableMetadata viewMetadata, String viewData, boolean replace)
    {
        SchemaTableName viewName = viewMetadata.getTable();
        JdbcIdentity identity = JdbcIdentity.from(session);

        ViewDefinition viewDefinition = viewCodec.fromJson(viewData);

        try (Connection connection = connectionFactory.openConnection(identity)) {
            String schema = toRemoteSchemaName(session, identity, connection, viewName.getSchemaName());
            String view = toRemoteTableName(session, identity, connection, schema, viewName.getTableName());

            // Without an explicit DEFINER, MySQL records the connection user, so the Presto user who
            // created the view would be lost. Naming another account requires the connection user
            // to hold SET_USER_ID (SET_ANY_DEFINER from MySQL 8.2) or SUPER. The Presto user is
            // stored with the host % since Presto has no notion of the client host.
            String definer = quoted(session.getUser()) + "@" + quoted("%");
            String sql = format(
                    "%s DEFINER = %s SQL SECURITY %s VIEW %s AS %s",
                    replace ? "CREATE OR REPLACE" : "CREATE",
                    definer,
                    viewDefinition.isRunAsInvoker() ? "INVOKER" : "DEFINER",
                    quotedRemoteName(schema, view),
                    viewDefinition.getOriginalSql());
            execute(connection, sql);
        }
        catch (SQLException e) {
            // CREATE VIEW without REPLACE reports an existing view or table as ER_TABLE_EXISTS_ERROR,
            // the same way CREATE TABLE does, so there is no need to look the name up first
            if (SQL_STATE_ER_TABLE_EXISTS_ERROR.equals(e.getSQLState())) {
                throw new PrestoException(ALREADY_EXISTS, e);
            }
            // The view query is Presto SQL and goes to MySQL unchanged. Rewriting it would take the
            // Presto parser, so a query MySQL cannot resolve is rejected with what it has to look like.
            if (e.getErrorCode() == ER_PARSE_ERROR || e.getErrorCode() == ER_NO_DB_ERROR) {
                throw new PrestoException(NOT_SUPPORTED, format(
                        "The query of a view in a MySQL catalog is sent to MySQL unchanged and must be valid MySQL SQL, " +
                                "for example with each table named as schema.table, without the catalog name, " +
                                "and no identifiers quoted with double quotes. MySQL reported: %s",
                        e.getMessage()), e);
            }
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void renameView(ConnectorSession session, SchemaTableName viewName, SchemaTableName newViewName)
    {
        JdbcIdentity identity = JdbcIdentity.from(session);

        try (Connection connection = connectionFactory.openConnection(identity)) {
            String schema = toRemoteSchemaName(session, identity, connection, viewName.getSchemaName());
            String view = toRemoteTableName(session, identity, connection, schema, viewName.getTableName());
            String newSchema = toRemoteSchemaName(session, identity, connection, newViewName.getSchemaName());
            String sql = format(
                    "RENAME TABLE %s TO %s",
                    quotedRemoteName(schema, view),
                    quotedRemoteName(newSchema, newViewName.getTableName()));
            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        JdbcIdentity identity = JdbcIdentity.from(session);

        try (Connection connection = connectionFactory.openConnection(identity)) {
            String schema = toRemoteSchemaName(session, identity, connection, viewName.getSchemaName());
            String view = toRemoteTableName(session, identity, connection, schema, viewName.getTableName());
            String sql = format(
                    "DROP VIEW %s",
                    quotedRemoteName(schema, view));
            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    /**
     * Qualifies a remote object with its schema only. MySQL maps its databases to JDBC catalogs and
     * has no schemas of its own, so the remote schema name resolved from the Presto schema is
     * already the database. Adding the connection's catalog as well would build a three part name,
     * which MySQL rejects, which is also why {@link #renameTable} drops the catalog.
     */
    private String quotedRemoteName(String remoteSchema, String remoteName)
    {
        return quoted(null, remoteSchema, remoteName);
    }

    private static List<RemoteView> listRemoteViews(Connection connection, SchemaTablePrefix prefix)
            throws SQLException
    {
        try (PreparedStatement statement = connection.prepareStatement(viewsQuery(prefix))) {
            int parameterIndex = 1;
            if (prefix.getSchemaName() != null) {
                statement.setString(parameterIndex++, prefix.getSchemaName());
            }
            if (prefix.getTableName() != null) {
                statement.setString(parameterIndex, prefix.getTableName());
            }

            ImmutableList.Builder<RemoteView> remoteViews = ImmutableList.builder();
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    remoteViews.add(new RemoteView(
                            new SchemaTableName(resultSet.getString("TABLE_SCHEMA"), resultSet.getString("TABLE_NAME")),
                            // StatementAnalyzer can't parse sql with back ticks, so we replace them here
                            resultSet.getString("VIEW_DEFINITION").replace('`', '"'),
                            extractDefinerUser(resultSet.getString("DEFINER")),
                            "INVOKER".equals(resultSet.getString("SECURITY_TYPE"))));
                }
            }
            return remoteViews.build();
        }
    }

    /**
     * Builds the INFORMATION_SCHEMA.VIEWS lookup for a prefix, binding the names as parameters:
     * they arrive from user SQL and may contain quotes. A prefix with no table name, and possibly
     * no schema name either, comes from queries such as SELECT * FROM information_schema.views,
     * and is answered by this one statement, so the row count rather than the query count grows
     * with the number of views on the server. The parameters have to be bound in the same order
     * the conditions are appended here.
     */
    private static String viewsQuery(SchemaTablePrefix prefix)
    {
        String sql = "SELECT TABLE_SCHEMA, TABLE_NAME, VIEW_DEFINITION, DEFINER, SECURITY_TYPE FROM INFORMATION_SCHEMA.VIEWS";
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        if (prefix.getSchemaName() != null) {
            builder.add("TABLE_SCHEMA = ?");
        }
        if (prefix.getTableName() != null) {
            builder.add("TABLE_NAME = ?");
        }
        List<String> conditions = builder.build();
        if (conditions.isEmpty()) {
            return sql;
        }
        return sql + " WHERE " + join(" AND ", conditions);
    }

    /**
     * Returns the user part of a DEFINER as INFORMATION_SCHEMA.VIEWS reports it, an unquoted
     * user@host. The owner of a view created through Presto is the Presto user, stored with the
     * host % by {@link #createView}. A user name may itself contain an @, as an email address
     * does, but a host cannot, so the split is on the last one.
     */
    @VisibleForTesting
    static String extractDefinerUser(String definer)
    {
        int separator = definer.lastIndexOf('@');
        return separator < 0 ? definer : definer.substring(0, separator);
    }

    /**
     * A row of INFORMATION_SCHEMA.VIEWS, named as MySQL reports it.
     */
    private static final class RemoteView
    {
        private final SchemaTableName name;
        private final String sql;
        private final String owner;
        private final boolean runAsInvoker;

        private RemoteView(SchemaTableName name, String sql, String owner, boolean runAsInvoker)
        {
            this.name = requireNonNull(name, "name is null");
            this.sql = requireNonNull(sql, "sql is null");
            this.owner = requireNonNull(owner, "owner is null");
            this.runAsInvoker = runAsInvoker;
        }
    }
}
