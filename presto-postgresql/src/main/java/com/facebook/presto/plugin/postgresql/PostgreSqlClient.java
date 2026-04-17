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
package com.facebook.presto.plugin.postgresql;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.JsonObjectMapperProvider;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.plugin.jdbc.BaseJdbcClient;
import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.DriverConnectionFactory;
import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.plugin.jdbc.JdbcIdentity;
import com.facebook.presto.plugin.jdbc.JdbcSplit;
import com.facebook.presto.plugin.jdbc.JdbcTypeHandle;
import com.facebook.presto.plugin.jdbc.QueryBuilder;
import com.facebook.presto.plugin.jdbc.mapping.ReadMapping;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.ConnectorViewDefinition;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.analyzer.ViewDefinition;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonFactoryBuilder;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import jakarta.inject.Inject;
import org.postgresql.Driver;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static com.facebook.presto.common.type.StandardTypes.GEOMETRY;
import static com.facebook.presto.common.type.StandardTypes.SPHERICAL_GEOGRAPHY;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static com.facebook.presto.plugin.jdbc.QueryBuilder.quote;
import static com.facebook.presto.plugin.jdbc.mapping.ReadMapping.createSliceReadMapping;
import static com.facebook.presto.plugin.jdbc.mapping.StandardColumnMappings.geometryReadMapping;
import static com.facebook.presto.spi.StandardErrorCode.ALREADY_EXISTS;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.fasterxml.jackson.core.JsonFactory.Feature.CANONICALIZE_FIELD_NAMES;
import static com.fasterxml.jackson.databind.SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedLongArray;
import static java.lang.Long.reverseBytes;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Locale.ENGLISH;
import static java.util.function.Function.identity;

public class PostgreSqlClient
        extends BaseJdbcClient
{
    protected final Type jsonType;
    private static final String DUPLICATE_TABLE_SQLSTATE = "42P07";
    private final Type uuidType;
    private final TypeManager typeManager;

    private static final JsonFactory JSON_FACTORY = new JsonFactoryBuilder().configure(CANONICALIZE_FIELD_NAMES, false).build();
    private static final ObjectMapper SORTED_MAPPER = new JsonObjectMapperProvider().get().configure(ORDER_MAP_ENTRIES_BY_KEYS, true);
    private static final JsonCodec<ViewDefinition> VIEW_CODEC = JsonCodec.jsonCodec(ViewDefinition.class);

    @Inject
    public PostgreSqlClient(JdbcConnectorId connectorId, BaseJdbcConfig config, TypeManager typeManager)
    {
        super(connectorId, config, "\"", new DriverConnectionFactory(new Driver(), config));
        this.jsonType = typeManager.getType(new TypeSignature(StandardTypes.JSON));
        this.uuidType = typeManager.getType(new TypeSignature(StandardTypes.UUID));
        this.typeManager = typeManager;
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
    protected ResultSet getTables(Connection connection, Optional<String> schemaName, Optional<String> tableName)
            throws SQLException
    {
        DatabaseMetaData metadata = connection.getMetaData();
        Optional<String> escape = Optional.ofNullable(metadata.getSearchStringEscape());
        return metadata.getTables(
                connection.getCatalog(),
                escapeNamePattern(schemaName, escape).orElse(null),
                escapeNamePattern(tableName, escape).orElse(null),
                new String[] {"TABLE", "VIEW", "MATERIALIZED VIEW", "FOREIGN TABLE", "PARTITIONED TABLE"});
    }

    @Override
    protected String toSqlType(Type type)
    {
        if (VARBINARY.equals(type)) {
            return "bytea";
        }

        return super.toSqlType(type);
    }

    @Override
    public PreparedStatement buildSql(ConnectorSession session, Connection connection, JdbcSplit split, List<JdbcColumnHandle> columnHandles) throws SQLException
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

        if (typeHandle.getJdbcTypeName().equals("jsonb") || typeHandle.getJdbcTypeName().equals("json")) {
            return Optional.of(jsonReadMapping());
        }

        else if (typeHandle.getJdbcTypeName().equals("uuid")) {
            return Optional.of(uuidReadMapping());
        }
        else if (typeName.equalsIgnoreCase(GEOMETRY) || typeName.equalsIgnoreCase(SPHERICAL_GEOGRAPHY)) {
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
            if (DUPLICATE_TABLE_SQLSTATE.equals(e.getSQLState())) {
                throw new PrestoException(ALREADY_EXISTS, e);
            }
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    protected void renameTable(JdbcIdentity identity, String catalogName, SchemaTableName oldTable, SchemaTableName newTable)
    {
        // PostgreSQL does not allow qualifying the target of a rename
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

    private ReadMapping jsonReadMapping()
    {
        return createSliceReadMapping(
                jsonType,
                (resultSet, columnIndex) -> jsonParse(utf8Slice(resultSet.getString(columnIndex))));
    }

    public static Slice jsonParse(Slice slice)
    {
        try (JsonParser parser = createJsonParser(JSON_FACTORY, slice)) {
            byte[] in = slice.getBytes();
            SliceOutput dynamicSliceOutput = new DynamicSliceOutput(in.length);
            SORTED_MAPPER.writeValue((OutputStream) dynamicSliceOutput, SORTED_MAPPER.readValue(parser, Object.class));
            // nextToken() returns null if the input is parsed correctly,
            // but will throw an exception if there are trailing characters.
            parser.nextToken();
            return dynamicSliceOutput.slice();
        }
        catch (Exception e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("Cannot convert '%s' to JSON", slice.toStringUtf8()));
        }
    }

    public static JsonParser createJsonParser(JsonFactory factory, Slice json)
            throws IOException
    {
        // Jackson tries to detect the character encoding automatically when using InputStream
        // so we pass an InputStreamReader instead.
        return factory.createParser(new InputStreamReader(json.getInput(), UTF_8));
    }

    private ReadMapping uuidReadMapping()
    {
        return createSliceReadMapping(
                uuidType,
                (resultSet, columnIndex) -> uuidSlice((UUID) resultSet.getObject(columnIndex)));
    }

    private static Slice uuidSlice(UUID uuid)
    {
        return wrappedLongArray(
                reverseBytes(uuid.getMostSignificantBits()),
                reverseBytes(uuid.getLeastSignificantBits()));
    }

    @Override
    public String normalizeIdentifier(ConnectorSession session, String identifier)
    {
        return caseSensitiveNameMatchingEnabled ? identifier : identifier.toLowerCase(ENGLISH);
    }

    /**
     * Get views from PostgreSQL information_schema.
     * This method retrieves view definitions for the specified schema and table names
     * and converts them to Presto ViewDefinition JSON format.
     */
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, JdbcIdentity identity, List<SchemaTableName> tableNames)
    {
        if (tableNames.isEmpty()) {
            return ImmutableMap.of();
        }

        try (Connection connection = connectionFactory.openConnection(identity)) {
            Map<SchemaTableName, ConnectorViewDefinition> views = new java.util.HashMap<>();

            // Build the query to fetch view definitions from information_schema
            StringBuilder sql = new StringBuilder(
                    "SELECT table_schema, table_name, view_definition " +
                    "FROM information_schema.views " +
                    "WHERE (table_schema, table_name) IN (");

            List<String> placeholders = new ArrayList<>();
            for (int i = 0; i < tableNames.size(); i++) {
                placeholders.add("(?, ?)");
            }
            sql.append(String.join(", ", placeholders));
            sql.append(")");

            try (PreparedStatement statement = connection.prepareStatement(sql.toString())) {
                int paramIndex = 1;
                for (SchemaTableName tableName : tableNames) {
                    String remoteSchema = toRemoteSchemaName(session, identity, connection, tableName.getSchemaName());
                    String remoteTable = toRemoteTableName(session, identity, connection, remoteSchema, tableName.getTableName());
                    statement.setString(paramIndex++, remoteSchema);
                    statement.setString(paramIndex++, remoteTable);
                }

                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        String schemaName = resultSet.getString("table_schema");
                        String tableName = resultSet.getString("table_name");
                        String viewSql = resultSet.getString("view_definition");

                        // Normalize identifiers according to PostgreSQL rules
                        schemaName = normalizeIdentifier(session, schemaName);
                        tableName = normalizeIdentifier(session, tableName);

                        // Remove trailing semicolon from PostgreSQL view definition
                        // Presto's SQL parser doesn't accept it
                        viewSql = viewSql.trim();
                        if (viewSql.endsWith(";")) {
                            viewSql = viewSql.substring(0, viewSql.length() - 1).trim();
                        }

                        SchemaTableName viewName = new SchemaTableName(schemaName, tableName);

                        // Get column information for the view
                        List<ViewDefinition.ViewColumn> columns = getViewColumns(connection, schemaName, tableName);

                        // Use session user as the view owner
                        String owner = session.getUser();

                        // Create ViewDefinition and serialize to JSON
                        // Set catalog to the connector catalog name so Presto can resolve table references
                        ViewDefinition viewDefinition = new ViewDefinition(
                                viewSql,
                                Optional.of(connectorId), // catalog - set to connector catalog
                                Optional.of(schemaName), // schema - set to view's schema
                                columns,
                                Optional.of(owner),
                                false); // runAsInvoker

                        String viewData = VIEW_CODEC.toJson(viewDefinition);

                        views.put(viewName, new ConnectorViewDefinition(
                                viewName,
                                Optional.of(owner),
                                viewData));
                    }
                }
            }

            return ImmutableMap.copyOf(views);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    /**
     * Get column information for a view from information_schema.columns
     */
    private List<ViewDefinition.ViewColumn> getViewColumns(Connection connection, String schemaName, String tableName)
            throws SQLException
    {
        String sql = "SELECT column_name, data_type, udt_name " +
                     "FROM information_schema.columns " +
                     "WHERE table_schema = ? AND table_name = ? " +
                     "ORDER BY ordinal_position";

        List<ViewDefinition.ViewColumn> columns = new ArrayList<>();

        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, schemaName);
            statement.setString(2, tableName);

            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    String columnName = resultSet.getString("column_name");
                    String dataType = resultSet.getString("data_type");
                    String udtName = resultSet.getString("udt_name");

                    // Map PostgreSQL type to Presto type
                    Type prestoType = toPrestoType(dataType, udtName);

                    columns.add(new ViewDefinition.ViewColumn(columnName, prestoType));
                }
            }
        }

        return columns;
    }

    /**
     * Map PostgreSQL data type to Presto Type
     */
    private Type toPrestoType(String dataType, String udtName)
    {
        // Handle common PostgreSQL types
        switch (dataType.toLowerCase(ENGLISH)) {
            case "integer":
            case "int":
            case "int4":
                return typeManager.getType(new TypeSignature(StandardTypes.INTEGER));
            case "bigint":
            case "int8":
                return typeManager.getType(new TypeSignature(StandardTypes.BIGINT));
            case "smallint":
            case "int2":
                return typeManager.getType(new TypeSignature(StandardTypes.SMALLINT));
            case "boolean":
            case "bool":
                return typeManager.getType(new TypeSignature(StandardTypes.BOOLEAN));
            case "real":
            case "float4":
                return typeManager.getType(new TypeSignature(StandardTypes.REAL));
            case "double precision":
            case "float8":
                return typeManager.getType(new TypeSignature(StandardTypes.DOUBLE));
            case "character varying":
            case "varchar":
            case "text":
                return typeManager.getType(new TypeSignature(StandardTypes.VARCHAR));
            case "character":
            case "char":
                return typeManager.getType(new TypeSignature(StandardTypes.CHAR));
            case "date":
                return typeManager.getType(new TypeSignature(StandardTypes.DATE));
            case "timestamp without time zone":
            case "timestamp":
                return typeManager.getType(new TypeSignature(StandardTypes.TIMESTAMP));
            case "bytea":
                return typeManager.getType(new TypeSignature(StandardTypes.VARBINARY));
            case "json":
            case "jsonb":
                return jsonType;
            case "uuid":
                return uuidType;
            default:
                // For unknown types, default to VARCHAR
                return typeManager.getType(new TypeSignature(StandardTypes.VARCHAR));
        }
    }

    /**
     * List all views in the specified schema using JDBC metadata.
     */
    public List<SchemaTableName> listViews(ConnectorSession session, JdbcIdentity identity, Optional<String> schema)
    {
        try (Connection connection = connectionFactory.openConnection(identity)) {
            Optional<String> remoteSchema = schema.map(s -> toRemoteSchemaName(session, identity, connection, s));

            try (ResultSet resultSet = getTables(connection, remoteSchema, Optional.empty(), new String[] {"VIEW"})) {
                ImmutableList.Builder<SchemaTableName> list = ImmutableList.builder();
                while (resultSet.next()) {
                    String schemaName = getTableSchemaName(resultSet);
                    String tableName = resultSet.getString("TABLE_NAME");

                    // Normalize identifiers according to PostgreSQL rules
                    schemaName = normalizeIdentifier(session, schemaName);
                    tableName = normalizeIdentifier(session, tableName);

                    list.add(new SchemaTableName(schemaName, tableName));
                }
                return list.build();
            }
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    /**
     * List all schemas that contain views.
     */
    public List<SchemaTableName> listSchemasForViews(ConnectorSession session, JdbcIdentity identity)
    {
        ImmutableList.Builder<SchemaTableName> allViews = ImmutableList.builder();

        for (String schema : getSchemaNames(session, identity)) {
            allViews.addAll(listViews(session, identity, Optional.of(schema)));
        }

        return allViews.build();
    }

    /**
     * Overloaded getTables method that accepts table types as a parameter.
     * This allows filtering by specific table types like VIEW, TABLE, etc.
     */
    protected ResultSet getTables(Connection connection, Optional<String> schemaName, Optional<String> tableName, String[] tableTypes)
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

    /**
     * Create a view in PostgreSQL.
     * Note: viewData contains the Presto ViewDefinition JSON with the original SQL.
     * We need to extract the SQL and create the PostgreSQL view directly.
     */
    public void createView(ConnectorSession session, JdbcIdentity identity, SchemaTableName viewName, String viewData, boolean replace)
    {
        try (Connection connection = connectionFactory.openConnection(identity)) {
            // viewData is the JSON string from Presto's CREATE VIEW statement
            // It contains the original SQL that we need to execute in PostgreSQL
            // Extract just the SQL portion - viewData format: {"originalSql":"SELECT ...","columns":[...],...}

            // Simple JSON parsing to extract originalSql
            String viewSql = extractOriginalSql(viewData);

            // Remove catalog prefix from table references (postgresql.schema.table -> schema.table)
            // PostgreSQL doesn't support cross-database references
            viewSql = removeCatalogPrefix(viewSql);

            String remoteSchema = toRemoteSchemaName(session, identity, connection, viewName.getSchemaName());
            String remoteTable = toRemoteTableName(session, identity, connection, remoteSchema, viewName.getTableName());

            String sql;
            if (replace) {
                sql = format("CREATE OR REPLACE VIEW %s.%s AS %s",
                        quote(identifierQuote, remoteSchema),
                        quote(identifierQuote, remoteTable),
                        viewSql);
            }
            else {
                sql = format("CREATE VIEW %s.%s AS %s",
                        quote(identifierQuote, remoteSchema),
                        quote(identifierQuote, remoteTable),
                        viewSql);
            }

            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    /**
     * Remove catalog prefix from table references in SQL.
     * Converts "catalog.schema.table" to "schema.table" for PostgreSQL compatibility.
     */
    private String removeCatalogPrefix(String sql)
    {
        // Pattern to match: word.word.word (catalog.schema.table)
        // Replace with: word.word (schema.table)
        // This is a simple regex that handles most cases
        return sql.replaceAll("\\b\\w+\\.(\\w+\\.\\w+)\\b", "$1");
    }

    /**
     * Extract the originalSql from ViewDefinition JSON string.
     * Simple extraction without full JSON parsing to avoid Type deserialization issues.
     */
    private String extractOriginalSql(String viewData)
    {
        // viewData format: {"originalSql":"SELECT ...","columns":[...],...}
        // Find the originalSql value
        int startIndex = viewData.indexOf("\"originalSql\":\"");
        if (startIndex == -1) {
            throw new PrestoException(JDBC_ERROR, "Invalid view data: missing originalSql");
        }

        startIndex += "\"originalSql\":\"".length();

        // Find the end of the SQL string (look for "," after the closing quote)
        // Need to handle escaped quotes in the SQL
        StringBuilder sql = new StringBuilder();
        boolean escaped = false;

        for (int i = startIndex; i < viewData.length(); i++) {
            char c = viewData.charAt(i);

            if (escaped) {
                // Handle escaped characters
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
                // End of originalSql value
                break;
            }
            else {
                sql.append(c);
            }
        }

        return sql.toString();
    }

    /**
     * Drop a view in PostgreSQL.
     */
    public void dropView(ConnectorSession session, JdbcIdentity identity, SchemaTableName viewName)
    {
        try (Connection connection = connectionFactory.openConnection(identity)) {
            String remoteSchema = toRemoteSchemaName(session, identity, connection, viewName.getSchemaName());
            String remoteTable = toRemoteTableName(session, identity, connection, remoteSchema, viewName.getTableName());

            String sql = format("DROP VIEW %s.%s",
                    quote(identifierQuote, remoteSchema),
                    quote(identifierQuote, remoteTable));

            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }
}
