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
package com.facebook.presto.plugin.sqlserver;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.plugin.jdbc.BaseJdbcClient;
import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.DriverConnectionFactory;
import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.plugin.jdbc.JdbcIdentity;
import com.facebook.presto.plugin.jdbc.JdbcTableHandle;
import com.facebook.presto.plugin.jdbc.JdbcTypeHandle;
import com.facebook.presto.plugin.jdbc.mapping.ReadMapping;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorViewDefinition;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.microsoft.sqlserver.jdbc.SQLServerDriver;
import jakarta.inject.Inject;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;

public class SqlServerClient
        extends BaseJdbcClient
{
    private static final Joiner DOT_JOINER = Joiner.on(".");

    @Inject
    public SqlServerClient(JdbcConnectorId connectorId, BaseJdbcConfig config)
    {
        super(connectorId, config, "\"", new DriverConnectionFactory(new SQLServerDriver(), config));
    }

    @Override
    protected void renameTable(JdbcIdentity identity, String catalogName, SchemaTableName oldTable, SchemaTableName newTable)
    {
        try (Connection connection = connectionFactory.openConnection(identity)) {
            String sql = format(
                    "sp_rename %s, %s",
                    singleQuote(catalogName, oldTable.getSchemaName(), oldTable.getTableName()),
                    singleQuote(newTable.getTableName()));
            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void renameColumn(ConnectorSession session, JdbcIdentity identity, JdbcTableHandle handle, JdbcColumnHandle jdbcColumn, String newColumnName)
    {
        try (Connection connection = connectionFactory.openConnection(identity)) {
            String sql = format(
                    "sp_rename %s, %s, 'COLUMN'",
                    singleQuote(handle.getCatalogName(), handle.getSchemaName(), handle.getTableName(), jdbcColumn.getColumnName()),
                    singleQuote(newColumnName));
            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    private static String singleQuote(String... objects)
    {
        return singleQuote(DOT_JOINER.join(objects));
    }

    private static String singleQuote(String literal)
    {
        return "\'" + literal + "\'";
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
                        "SELECT VIEW_DEFINITION FROM INFORMATION_SCHEMA.VIEWS " +
                                "WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'",
                        schemaName, tableName);

                try (Statement statement = connection.createStatement();
                        ResultSet resultSet = statement.executeQuery(sql)) {
                    if (resultSet.next()) {
                        String sqlServerViewSql = resultSet.getString(1);

                        String selectQuery = extractSelectFromCreateView(sqlServerViewSql);
                        String owner = session.getUser();

                        // Fetch column metadata for the view
                        List<String> columnJsonList = new ArrayList<>();
                        try (ResultSet columnsResultSet = connection.getMetaData().getColumns(
                                null,
                                schemaName,
                                tableName,
                                null)) {
                            while (columnsResultSet.next()) {
                                String columnName = columnsResultSet.getString("COLUMN_NAME");
                                JdbcTypeHandle typeHandle = new JdbcTypeHandle(
                                        columnsResultSet.getInt("DATA_TYPE"),
                                        columnsResultSet.getString("TYPE_NAME"),
                                        columnsResultSet.getInt("COLUMN_SIZE"),
                                        columnsResultSet.getInt("DECIMAL_DIGITS"));

                                Optional<ReadMapping> readMapping = toPrestoType(session, typeHandle);
                                if (readMapping.isPresent()) {
                                    Type prestoType = readMapping.get().getType();
                                    // Normalize column name
                                    String normalizedColumnName = normalizeIdentifier(session, columnName);
                                    // Escape for JSON
                                    String escapedColumnName = normalizedColumnName
                                            .replace("\\", "\\\\")
                                            .replace("\"", "\\\"");
                                    String escapedTypeName = prestoType.getDisplayName()
                                            .replace("\\", "\\\\")
                                            .replace("\"", "\\\"");
                                    columnJsonList.add(format(
                                            "{\"name\":\"%s\",\"type\":\"%s\"}",
                                            escapedColumnName,
                                            escapedTypeName));
                                }
                            }
                        }

                        // Escape SQL for JSON
                        String escapedSql = selectQuery
                                .replace("\\", "\\\\")
                                .replace("\"", "\\\"")
                                .replace("\n", "\\n")
                                .replace("\r", "\\r");

                        String columnsJson = String.join(",", columnJsonList);

                        // Create proper ViewDefinition JSON with all required fields
                        String prestoViewData = format(
                                "{\"originalSql\":\"%s\",\"catalog\":\"%s\",\"schema\":\"%s\",\"columns\":[%s],\"owner\":\"%s\",\"runAsInvoker\":false}",
                                escapedSql,
                                connectorId,  // Use connector ID as catalog
                                schemaName,
                                columnsJson,
                                owner);

                        views.put(schemaTableName,
                                new ConnectorViewDefinition(
                                        schemaTableName,
                                        Optional.ofNullable(owner),
                                        prestoViewData));
                    }
                }
            }
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }

        return views.build();
    }

    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> schemaName)
    {
        JdbcIdentity identity = JdbcIdentity.from(session);
        try (Connection connection = connectionFactory.openConnection(identity)) {
            try (ResultSet resultSet = getTables(connection, schemaName, Optional.empty(), new String[] {"VIEW"})) {
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
            catch (SQLException e) {
                throw new PrestoException(JDBC_ERROR, e);
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

    public void createView(ConnectorSession session, SchemaTableName viewName, String viewData, boolean replace)
    {
        JdbcIdentity identity = JdbcIdentity.from(session);
        try (Connection connection = connectionFactory.openConnection(identity)) {
            // Extract the original SQL from ViewDefinition JSON
            String viewSql = extractOriginalSql(viewData);

            // Remove catalog prefix from table references
            viewSql = removeCatalogPrefix(viewSql);
            // Remove double quotes around identifiers that Presto adds
            viewSql = removeQuotes(viewSql);

            String sql;
            if (replace) {
                try {
                    dropView(session, viewName);
                }
                catch (Exception e) {
                    // Ignore if view doesn't exist
                }
            }
            sql = format("CREATE VIEW %s.%s AS %s",
                    quoted(viewName.getSchemaName()),
                    quoted(viewName.getTableName()),
                    viewSql);
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
            String sql = format("DROP VIEW %s.%s",
                    quoted(viewName.getSchemaName()),
                    quoted(viewName.getTableName()));
            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    /**
     * Extract SELECT query from SQL Server's CREATE VIEW statement.
     * SQL Server returns: "CREATE VIEW schema.name AS SELECT ..."
     * We need to extract just: "SELECT ..."
     *
     * @param createViewSql the CREATE VIEW statement
     * @return the SELECT query portion
     */
    private String extractSelectFromCreateView(
            final String createViewSql)
    {
        int asIndex = createViewSql.toUpperCase(ENGLISH)
                .indexOf(" AS ");
        if (asIndex == -1) {
            return createViewSql.trim();
        }

        int asLength = 4;
        String selectPart = createViewSql.substring(
                asIndex + asLength).trim();
        return selectPart;
    }

    /**
     * Remove catalog prefix from table references in SQL.
     * @param sql the SQL statement to process
     * @return SQL with catalog prefixes removed
     */
    private String removeCatalogPrefix(final String sql)
    {
        String pattern = "\\b([a-zA-Z_][a-zA-Z0-9_]*)"
                + "\\.([a-zA-Z_][a-zA-Z0-9_]*)"
                + "\\.([a-zA-Z_][a-zA-Z0-9_]*)\\b";
        return sql.replaceAll(pattern, "$2.$3");
    }

    /**
     * Remove double quotes around identifiers that get added.
     * @param sql the SQL statement to process
     * @return SQL with double quotes removed from identifiers
     */
    private String removeQuotes(final String sql)
    {
        // Remove double quotes around identifiers
        // This handles cases like "count"(1) -> count(1), "avg"(age) -> avg(age)
        return sql.replaceAll("\"([a-zA-Z_][a-zA-Z0-9_]*)\"", "$1");
    }

    private String extractOriginalSql(final String viewData)
    {
        int startIndex = viewData.indexOf("\"originalSql\":\"");
        if (startIndex == -1) {
            throw new PrestoException(JDBC_ERROR, "Invalid view data: missing originalSql");
        }
        startIndex += "\"originalSql\":\"".length();

        StringBuilder sql = new StringBuilder();
        boolean escaped = false;
        for (int i = startIndex; i < viewData.length(); i++) {
            char c = viewData.charAt(i);
            if (escaped) {
                if (c == 'n') {
                    sql.append('\n');
                }
                else if (c == 't') {
                    sql.append('\t');
                }
                else if (c == 'r') {
                    sql.append('\r');
                }
                else if (c == '"' || c == '\\') {
                    sql.append(c);
                }
                else {
                    sql.append('\\').append(c);
                }
                escaped = false;
            }
            else if (c == '\\') {
                escaped = true;
            }
            else if (c == '"') {
                break;
            }
            else {
                sql.append(c);
            }
        }

        return sql.toString();
    }

    private ResultSet getTables(Connection connection, Optional<String> schemaName, Optional<String> tableName, String[] types)
            throws SQLException
    {
        DatabaseMetaData metadata = connection.getMetaData();
        Optional<String> escape = Optional.ofNullable(metadata.getSearchStringEscape());
        return metadata.getTables(
                connection.getCatalog(),
                schemaName.orElse(null),
                escapeNamePattern(tableName, escape).orElse(null),
                types);
    }
}
