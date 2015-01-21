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
import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.ConnectorPartitionResult;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;

import javax.annotation.Nullable;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Maps.fromProperties;
import static java.util.Collections.nCopies;
import static java.util.Locale.ENGLISH;

public class BaseJdbcClient
        implements JdbcClient
{
    private static final Logger log = Logger.get(BaseJdbcClient.class);

    private static final Map<Type, String> SQL_TYPES = ImmutableMap.<Type, String>builder()
            .put(BOOLEAN, "boolean")
            .put(BIGINT, "bigint")
            .put(DOUBLE, "double precision")
            .put(VARCHAR, "varchar")
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

    public BaseJdbcClient(JdbcConnectorId connectorId, BaseJdbcConfig config, String identifierQuote, Driver driver)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null").toString();
        this.identifierQuote = checkNotNull(identifierQuote, "identifierQuote is null");
        this.driver = checkNotNull(driver, "driver is null");

        checkNotNull(config, "config is null");
        connectionUrl = config.getConnectionUrl();

        connectionProperties = new Properties();
        if (config.getConnectionUser() != null) {
            connectionProperties.setProperty("user", config.getConnectionUser());
        }
        if (config.getConnectionPassword() != null) {
            connectionProperties.setProperty("password", config.getConnectionPassword());
        }
    }

    @Override
    public Set<String> getSchemaNames()
    {
        try (Connection connection = driver.connect(connectionUrl, connectionProperties);
                ResultSet resultSet = connection.getMetaData().getSchemas()) {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            while (resultSet.next()) {
                String schemaName = resultSet.getString("TABLE_SCHEM").toLowerCase(ENGLISH);
                // skip internal schemas
                if (!schemaName.equals("information_schema")) {
                    schemaNames.add(schemaName);
                }
            }
            return schemaNames.build();
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public List<SchemaTableName> getTableNames(@Nullable String schema)
    {
        try (Connection connection = driver.connect(connectionUrl, connectionProperties)) {
            DatabaseMetaData metadata = connection.getMetaData();
            if (metadata.storesUpperCaseIdentifiers() && (schema != null)) {
                schema = schema.toUpperCase(ENGLISH);
            }
            try (ResultSet resultSet = getTables(connection, schema, null)) {
                ImmutableList.Builder<SchemaTableName> list = ImmutableList.builder();
                while (resultSet.next()) {
                    list.add(getSchemaTableName(resultSet));
                }
                return list.build();
            }
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    @Nullable
    @Override
    public JdbcTableHandle getTableHandle(SchemaTableName schemaTableName)
    {
        try (Connection connection = driver.connect(connectionUrl, connectionProperties)) {
            DatabaseMetaData metadata = connection.getMetaData();
            String jdbcSchemaName = schemaTableName.getSchemaName();
            String jdbcTableName = schemaTableName.getTableName();
            if (metadata.storesUpperCaseIdentifiers()) {
                jdbcSchemaName = jdbcSchemaName.toUpperCase(ENGLISH);
                jdbcTableName = jdbcTableName.toUpperCase(ENGLISH);
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
            throw Throwables.propagate(e);
        }
    }

    @Override
    public List<JdbcColumnHandle> getColumns(JdbcTableHandle tableHandle)
    {
        try (Connection connection = driver.connect(connectionUrl, connectionProperties)) {
            DatabaseMetaData metadata = connection.getMetaData();
            try (ResultSet resultSet = metadata.getColumns(tableHandle.getCatalogName(), tableHandle.getSchemaName(), tableHandle.getTableName(), null)) {
                List<JdbcColumnHandle> columns = new ArrayList<>();
                boolean found = false;
                int ordinalPosition = 0;
                while (resultSet.next()) {
                    found = true;
                    Type columnType = toPrestoType(resultSet.getInt("DATA_TYPE"));
                    // skip unsupported column types
                    if (columnType != null) {
                        String columnName = resultSet.getString("COLUMN_NAME");
                        columns.add(new JdbcColumnHandle(connectorId, columnName, columnType, ordinalPosition));
                        ordinalPosition++;
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
            throw Throwables.propagate(e);
        }
    }

    @Override
    public ConnectorPartitionResult getPartitions(JdbcTableHandle jdbcTableHandle, TupleDomain<ConnectorColumnHandle> tupleDomain)
    {
        // currently we don't support partitions
        return new ConnectorPartitionResult(
                ImmutableList.<ConnectorPartition>of(new JdbcPartition(jdbcTableHandle, tupleDomain)),
                tupleDomain);
    }

    @Override
    public ConnectorSplitSource getPartitionSplits(JdbcPartition jdbcPartition)
    {
        JdbcTableHandle jdbcTableHandle = jdbcPartition.getJdbcTableHandle();
        JdbcSplit jdbcSplit = new JdbcSplit(
                connectorId,
                jdbcTableHandle.getCatalogName(),
                jdbcTableHandle.getSchemaName(),
                jdbcTableHandle.getTableName(),
                connectionUrl,
                fromProperties(connectionProperties),
                jdbcPartition.getTupleDomain());
        return new FixedSplitSource(connectorId, ImmutableList.of(jdbcSplit));
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
    public String buildSql(JdbcSplit split, List<JdbcColumnHandle> columnHandles)
    {
        return new QueryBuilder(identifierQuote).buildSql(
                split.getCatalogName(),
                split.getSchemaName(),
                split.getTableName(),
                columnHandles,
                split.getTupleDomain());
    }

    @Override
    public JdbcOutputTableHandle beginCreateTable(ConnectorTableMetadata tableMetadata)
    {
        SchemaTableName schemaTableName = tableMetadata.getTable();
        String schema = schemaTableName.getSchemaName();
        String table = schemaTableName.getTableName();

        if (!getSchemaNames().contains(schema)) {
            throw new PrestoException(NOT_FOUND, "Schema not found: " + schema);
        }

        try (Connection connection = driver.connect(connectionUrl, connectionProperties)) {
            boolean uppercase = connection.getMetaData().storesUpperCaseIdentifiers();
            if (uppercase) {
                schema = schema.toUpperCase(ENGLISH);
                table = table.toUpperCase(ENGLISH);
            }
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
                    tableMetadata.getOwner(),
                    temporaryName,
                    connectionUrl,
                    fromProperties(connectionProperties));
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void commitCreateTable(JdbcOutputTableHandle handle, Collection<Slice> fragments)
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
            throw Throwables.propagate(e);
        }
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

    protected ResultSet getTables(Connection connection, String schemaName, String tableName)
            throws SQLException
    {
        return connection.getMetaData().getTables(connection.getCatalog(), schemaName, tableName, new String[] {"TABLE"});
    }

    protected SchemaTableName getSchemaTableName(ResultSet resultSet)
            throws SQLException
    {
        return new SchemaTableName(
                resultSet.getString("TABLE_SCHEM").toLowerCase(ENGLISH),
                resultSet.getString("TABLE_NAME").toLowerCase(ENGLISH));
    }

    protected void execute(Connection connection, String query)
            throws SQLException
    {
        try (Statement statement = connection.createStatement()) {
            log.debug("Execute: %s", query);
            statement.execute(query);
        }
    }

    protected Type toPrestoType(int jdbcType)
    {
        switch (jdbcType) {
            case Types.BIT:
            case Types.BOOLEAN:
                return BOOLEAN;
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT:
                return BIGINT;
            case Types.FLOAT:
            case Types.REAL:
            case Types.DOUBLE:
            case Types.NUMERIC:
            case Types.DECIMAL:
                return DOUBLE;
            case Types.CHAR:
            case Types.NCHAR:
            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.LONGVARCHAR:
            case Types.LONGNVARCHAR:
                return VARCHAR;
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
        String sqlType = SQL_TYPES.get(type);
        if (sqlType != null) {
            return sqlType;
        }
        throw new PrestoException(NOT_SUPPORTED, "Unsuported column type: " + type.getTypeSignature());
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
            checkArgument(!isNullOrEmpty(schema), "catalog is present but schema is not");
            sb.append(quoted(catalog)).append(".");
        }
        if (!isNullOrEmpty(schema)) {
            sb.append(quoted(schema)).append(".");
        }
        sb.append(quoted(table));
        return sb.toString();
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
