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
package com.facebook.presto.plugin.redshift;

import com.amazon.redshift.jdbc.Driver;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.plugin.jdbc.BaseJdbcClient;
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
import com.facebook.presto.spi.analyzer.ViewDefinition;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.sql.tree.WithQuery;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import jakarta.inject.Inject;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.facebook.presto.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static com.facebook.presto.plugin.jdbc.mapping.ReadMapping.varbinaryReadMapping;
import static com.facebook.presto.spi.StandardErrorCode.ALREADY_EXISTS;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class RedshiftClient
        extends BaseJdbcClient
{
    private static final Logger log = Logger.get(RedshiftClient.class);
    private static final SqlParser SQL_PARSER = new SqlParser();
    private static final ParsingOptions PARSING_OPTIONS = ParsingOptions.builder().build();
    private final JsonCodec<ViewDefinition> viewCodec;

    enum ViewRewriteMode
    {
        STRIP_CATALOG, ADD_SCHEMA
    }

    @Inject
    public RedshiftClient(JdbcConnectorId connectorId, RedshiftConfig config, JsonCodec<ViewDefinition> viewCodec)
    {
        super(connectorId, config, "\"", new DriverConnectionFactory(new Driver(), config));
        this.viewCodec = requireNonNull(viewCodec, "viewCodec is null");
    }

    @Override
    public PreparedStatement getPreparedStatement(ConnectorSession session, Connection connection, String sql)
            throws SQLException
    {
        connection.setAutoCommit(false);
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setFetchSize(1000);
        return statement;
    }

    @Override
    protected void renameTable(JdbcIdentity identity, String catalogName, SchemaTableName oldTable, SchemaTableName newTable)
    {
        // Redshift does not allow qualifying the target of a rename
        try (Connection connection = connectionFactory.openConnection(identity)) {
            String sql = format(
                    "ALTER TABLE %s RENAME TO %s",
                    quoted(catalogName, oldTable.getSchemaName(), oldTable.getTableName()),
                    quoted(newTable.getTableName()));
            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    // Maps "binary varying" (Redshift driver >= 2.1.0.32) to VARBINARY.
    @Override
    public Optional<ReadMapping> toPrestoType(ConnectorSession session, JdbcTypeHandle typeHandle)
    {
        String typeName = typeHandle.getJdbcTypeName();

        if (typeName.equalsIgnoreCase("binary varying")) {
            return Optional.of(varbinaryReadMapping());
        }

        return super.toPrestoType(session, typeHandle);
    }

    @Override
    public String normalizeIdentifier(ConnectorSession session, String identifier)
    {
        return caseSensitiveNameMatchingEnabled ? identifier : identifier.toLowerCase(ENGLISH);
    }

    @Override
    public void dropTable(ConnectorSession session, JdbcIdentity identity, JdbcTableHandle handle)
    {
        try (Connection connection = connectionFactory.openConnection(identity)) {
            ResultSet resultSet = getTables(
                    connection,
                    Optional.ofNullable(handle.getSchemaName()),
                    Optional.ofNullable(handle.getTableName()),
                    new String[]{"VIEW"});

            if (resultSet.next()) {
                throw new PrestoException(NOT_SUPPORTED, format("Relation '%s' is a view, not a table", handle.getTableName()));
            }

            StringBuilder sql = new StringBuilder()
                    .append("DROP TABLE ")
                    .append(quoted(handle.getCatalogName(), handle.getSchemaName(), handle.getTableName()));

            execute(connection, sql.toString());
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    protected ResultSet getTables(Connection connection, Optional<String> schemaName, Optional<String> tableName)
            throws SQLException
    {
        return getTables(connection, schemaName, tableName, new String[] {"TABLE", "VIEW"});
    }

    private ResultSet getTables(Connection connection, Optional<String> schemaName, Optional<String> tableName, String[] tableTypes)
            throws SQLException
    {
        DatabaseMetaData metadata = connection.getMetaData();
        Optional<String> escape = Optional.ofNullable(metadata.getSearchStringEscape());
        return metadata.getTables(
                connection.getCatalog(),
                escapeNamePattern(schemaName, escape).orElse(null),
                escapeNamePattern(tableName, escape).orElse(null),
                tableTypes);
    }

    void createView(ConnectorSession session, SchemaTableName viewName, String viewData, boolean replace)
    {
        JdbcIdentity identity = JdbcIdentity.from(session);
        try (Connection connection = connectionFactory.openConnection(identity)) {
            String schema = toRemoteSchemaName(session, identity, connection, viewName.getSchemaName());
            String view = toRemoteTableName(session, identity, connection, schema, viewName.getTableName());
            String catalog = connection.getCatalog();

            if (!replace) {
                ResultSet resultSet = getTables(connection, Optional.ofNullable(schema), Optional.ofNullable(view));
                if (resultSet.next()) {
                    throw new PrestoException(ALREADY_EXISTS, format("The view/table '%s' already exists", viewName.getTableName()));
                }
            }

            String viewSql = viewCodec.fromJson(viewData).getOriginalSql();
            viewSql = rewriteViewSql(session, identity, connection, viewSql, ViewRewriteMode.STRIP_CATALOG);
            String sql = format("%s VIEW %s AS %s", replace ? "CREATE OR REPLACE" : "CREATE", quoted(catalog, schema, view), viewSql);
            log.debug("Execute: %s", sql);
            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    /**
     * Determines whether the specified object is a view.
     *
     * Metadata lookup failures are treated as views so that view
     * resolution can be attempted through {@code getViews()}.
     */
    boolean isView(ConnectorSession session, String schemaName, String tableName)
    {
        JdbcIdentity identity = JdbcIdentity.from(session);
        try (Connection connection = connectionFactory.openConnection(identity)) {
            String remoteSchema = toRemoteSchemaName(session, identity, connection, schemaName);
            String remoteTable = toRemoteTableName(session, identity, connection, remoteSchema, tableName);
            try (ResultSet rs = getTables(connection, Optional.of(remoteSchema), Optional.of(remoteTable), new String[]{"VIEW"})) {
                return rs.next();
            }
        }
        catch (SQLException e) {
            log.debug("isView metadata check failed for %s.%s, proceeding with view resolution: %s", schemaName, tableName, e.getMessage());
            return true;
        }
    }

    /**
     * Retrieves Redshift view definitions and converts them to
     * ConnectorViewDefinition instances.
     */
    Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, List<SchemaTableName> viewNames)
    {
        JdbcIdentity identity = JdbcIdentity.from(session);
        ImmutableMap.Builder<SchemaTableName, ConnectorViewDefinition> views = ImmutableMap.builder();

        try (Connection connection = connectionFactory.openConnection(identity)) {
            for (SchemaTableName schemaTableName : viewNames) {
                String schemaName = toRemoteSchemaName(session, identity, connection, schemaTableName.getSchemaName());
                String viewName = toRemoteTableName(session, identity, connection, schemaName, schemaTableName.getTableName());
                try (PreparedStatement statement = connection.prepareStatement(
                        "SELECT viewowner, definition FROM pg_views WHERE schemaname = ? AND viewname = ?")) {
                    statement.setString(1, schemaName);
                    statement.setString(2, viewName);
                    try (ResultSet resultSet = statement.executeQuery()) {
                        if (!resultSet.next()) {
                            continue;
                        }
                        String viewOwner = resultSet.getString(1);
                        String viewSql = resultSet.getString(2);
                        if (viewSql == null) {
                            log.warn("Empty view definition for %s.%s - skipping", schemaName, viewName);
                            continue;
                        }
                        viewSql = viewSql.trim();
                        if (viewSql.endsWith(";")) {
                            viewSql = viewSql.substring(0, viewSql.length() - 1);
                        }

                        viewSql = rewriteViewSql(session, identity, connection, viewSql, ViewRewriteMode.ADD_SCHEMA);

                        SchemaTableName schemaViewName = new SchemaTableName(schemaName, viewName);
                        ViewDefinition viewDefinition = buildViewDefinition(connection, session, schemaViewName, viewSql, viewOwner);
                        String viewData = viewCodec.toJson(viewDefinition);
                        views.put(schemaTableName, new ConnectorViewDefinition(schemaViewName, Optional.ofNullable(viewOwner), viewData));
                    }
                }
                catch (SQLException e) {
                    log.warn("Failed to fetch view %s: %s", schemaTableName, e.getMessage());
                }
            }
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
        return views.build();
    }

    private ViewDefinition buildViewDefinition(
            Connection connection,
            ConnectorSession session,
            SchemaTableName schemaViewName,
            String viewSql,
            String viewOwner)
            throws SQLException
    {
        String schema = schemaViewName.getSchemaName();
        String view = schemaViewName.getTableName();

        JdbcTableHandle tableHandle = new JdbcTableHandle(connectorId, schemaViewName, connection.getCatalog(), schema, view);
        List<JdbcColumnHandle> jdbcColumnHandles = super.getColumns(session, tableHandle);

        List<ViewDefinition.ViewColumn> columns = jdbcColumnHandles.stream()
                .map(jdbcColumn -> new ViewDefinition.ViewColumn(normalizeIdentifier(session, jdbcColumn.getColumnName()), jdbcColumn.getColumnType()))
                .collect(toImmutableList());

        return new ViewDefinition(
                viewSql,
                Optional.of(connectorId),
                Optional.of(schema),
                columns,
                Optional.of(viewOwner),
                false);
    }

    List<SchemaTableName> listViewsFromAllSchemas(ConnectorSession session)
    {
        List<SchemaTableName> allViewNames = new ArrayList<>();
        JdbcIdentity identity = JdbcIdentity.from(session);
        try (Connection connection = connectionFactory.openConnection(identity)) {
            for (String schema : listSchemas(connection)) {
                List<SchemaTableName> viewNames = listViews(session, identity, connection, Optional.ofNullable(schema));
                allViewNames.addAll(viewNames);
            }
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
        return allViewNames;
    }

    List<SchemaTableName> listViews(ConnectorSession session, Optional<String> schemaName)
    {
        JdbcIdentity identity = JdbcIdentity.from(session);
        try (Connection connection = connectionFactory.openConnection(identity)) {
            return listViews(session, identity, connection, schemaName);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    private List<SchemaTableName> listViews(ConnectorSession session, JdbcIdentity identity, Connection connection, Optional<String> schemaName)
            throws SQLException
    {
        Optional<String> remoteSchema = schemaName.map(schema -> toRemoteSchemaName(session, identity, connection, schema));
        try (ResultSet resultSet = getTables(connection, remoteSchema, Optional.empty(), new String[]{"VIEW"})) {
            ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
            while (resultSet.next()) {
                String tableSchema = getTableSchemaName(resultSet);
                String tableName = resultSet.getString("TABLE_NAME");
                builder.add(new SchemaTableName(normalizeIdentifier(session, tableSchema), normalizeIdentifier(session, tableName)));
            }
            return builder.build();
        }
    }

    void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        JdbcIdentity identity = JdbcIdentity.from(session);

        try (Connection connection = connectionFactory.openConnection(identity)) {
            String sql = "DROP VIEW " + (quoted(connection.getCatalog(), viewName.getSchemaName(), viewName.getTableName()));
            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    /**
     * Rewrites table references in view SQL.
     *
     * STRIP_CATALOG removes catalog qualifiers from three-part names.
     * ADD_SCHEMA qualifies unqualified table references with the current schema.
     */
    String rewriteViewSql(ConnectorSession session, JdbcIdentity identity, Connection connection, String rawSql, ViewRewriteMode mode)
            throws SQLException
    {
        requireNonNull(rawSql, "rawSql is null");

        Statement parsed;
        try {
            parsed = SQL_PARSER.createStatement(rawSql, PARSING_OPTIONS);
        }
        catch (Exception e) {
            log.warn("Failed to parse Redshift view definition, returning original view SQL. Error: %s", e.getMessage());
            return rawSql;
        }

        Set<String> cteNames = new HashSet<>();
        Set<String> tableReferences = new HashSet<>();
        Set<String> sourceCatalogs = new HashSet<>();

        new DefaultTraversalVisitor<Void, Void>()
        {
            @Override
            protected Void visitWithQuery(WithQuery node, Void context)
            {
                String cteName = node.getName().getValue();
                // Process WITH body FIRST (before adding CTE name)
                Void result = super.visitWithQuery(node, context);
                cteNames.add(cteName);
                return result;
            }

            @Override
            protected Void visitTable(Table node, Void context)
            {
                if (mode == ViewRewriteMode.ADD_SCHEMA && node.getName().getParts().size() == 1) {
                    String name = node.getName().getOriginalParts().stream().map(Object::toString).collect(Collectors.joining("."));
                    if (!cteNames.contains(name)) {
                        tableReferences.add(name);
                    }
                }
                else if (mode == ViewRewriteMode.STRIP_CATALOG && node.getName().getParts().size() == 3) {
                    // Presto's analyzer always serializes resolved table references in
                    // originalSql as fully-qualified catalog.schema.table (3 parts).
                    // Two-part or bare references are not expected here.
                    List<Identifier> parts = node.getName().getOriginalParts();
                    tableReferences.add(parts.stream().map(Object::toString).collect(Collectors.joining(".")));
                    sourceCatalogs.add(parts.get(0).getValue());
                }
                return super.visitTable(node, context);
            }
        }.process(parsed, null);

        if (tableReferences.isEmpty()) {
            // All tables are already fully qualified - return canonical form directly.
            return rawSql;
        }

        if (mode == ViewRewriteMode.STRIP_CATALOG) {
            boolean containsCrossCatalogReferences = sourceCatalogs.size() > 1 || (sourceCatalogs.size() == 1 && !sourceCatalogs.contains(connectorId));
            if (containsCrossCatalogReferences) {
                throw new PrestoException(NOT_SUPPORTED, "You can create the view for a table only if that table is in the same catalog.");
            }
        }

        String schemaName = mode == ViewRewriteMode.ADD_SCHEMA ? requireNonNull(connection.getSchema(), "schemaName is null") : null;

        String result = rawSql;
        for (String name : tableReferences) {
            if (mode == ViewRewriteMode.ADD_SCHEMA) {
                // Match unqualified table references while preserving identifier casing.
                String table = name.replaceAll("^\"|\"$", "");
                String qualified = quoted(schemaName) + '.' + quoted(toRemoteTableName(session, identity, connection, schemaName, table));
                Pattern namePattern = Pattern.compile("(?<![\\.\\w\"])\"?" + Pattern.quote(name) + "\"?(?![\\.\"])(?![\\w])");
                result = namePattern.matcher(result).replaceAll(Matcher.quoteReplacement(qualified));
            }
            else if (mode == ViewRewriteMode.STRIP_CATALOG) {
                List<String> parts = Arrays.asList(name.split("\\."));
                if (parts.size() != 3) {
                    log.warn("Expected 3-part name, got %d parts: %s", parts.size(), name);
                    continue;
                }
                String schema = toRemoteSchemaName(session, identity, connection, parts.get(1)).replaceAll("^\"|\"$", "");
                String table = toRemoteTableName(session, identity, connection, schema, parts.get(2)).replaceAll("^\"|\"$", "");
                String stripped = quoted(normalizeIdentifier(session, schema)) + '.' + quoted(normalizeIdentifier(session, table));
                result = result.replace(name, stripped);
            }
        }
        return result;
    }
}
