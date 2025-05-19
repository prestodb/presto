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
package com.facebook.presto.plugin.db2fori;

import com.facebook.presto.plugin.jdbc.BaseJdbcClient;
import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.ColumnMapping;
import com.facebook.presto.plugin.jdbc.DriverConnectionFactory;
import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.plugin.jdbc.JdbcIdentity;
import com.facebook.presto.plugin.jdbc.JdbcTableHandle;
import com.facebook.presto.plugin.jdbc.JdbcTypeHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.google.common.collect.ImmutableList;
import com.ibm.as400.access.AS400JDBCDriver;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static com.facebook.presto.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.sql.ResultSetMetaData.columnNullable;
import static java.util.Locale.ENGLISH;

public class Db2foriClient
        extends BaseJdbcClient
{
    private static final String ENABLE_MIXED_CASE_SUPPORT = "enable-mixed-case-support";
    private final boolean enableMixedCaseSupport;
    private final boolean checkDriverCaseSupport;

    @Inject
    public Db2foriClient(JdbcConnectorId connectorId, BaseJdbcConfig config)
    {
        super(connectorId, config, "\"", new DriverConnectionFactory(new AS400JDBCDriver(), config));
        this.enableMixedCaseSupport = com.facebook.presto.common.util.ConfigUtil.getConfig(ENABLE_MIXED_CASE_SUPPORT);
        this.checkDriverCaseSupport = enableMixedCaseSupport && config.getCheckDriverCaseSupport();
    }

    @Override
    public PreparedStatement getPreparedStatement(ConnectorSession session, Connection connection, String sql) throws SQLException
    {
        connection.setAutoCommit(false);
        PreparedStatement statement = connection.prepareStatement(sql);
        return statement;
    }

    @Override
    public ResultSet getTables(Connection connection, Optional<String> schemaName, Optional<String> tableName) throws SQLException
    {
        DatabaseMetaData metadata = connection.getMetaData();
        Optional<String> escape = Optional.ofNullable(metadata.getSearchStringEscape());
        String schema = escapeNamePattern(schemaName, escape).orElse(null);
        String table = escapeNamePattern(tableName, escape).orElse(null);
        DatabaseMetaData metaData = connection.getMetaData();
        boolean uppercase = metaData.storesUpperCaseIdentifiers();
        if (uppercase && (checkDriverCaseSupport || !enableMixedCaseSupport)) {
            schema = null != schema ? schema.toUpperCase(ENGLISH) : null;
            table = null != table ? table.toUpperCase(ENGLISH) : null;
        }

        return metadata.getTables(
                connection.getCatalog(),
                schema,
                table,
                new String[] {"TABLE", "VIEW", "ALIAS"});
    }

    @Override
    protected void renameTable(JdbcIdentity identity, String catalogName, SchemaTableName oldTable, SchemaTableName newTable)
    {
        try (Connection connection = connectionFactory.openConnection(identity)) {
            if (enableMixedCaseSupport && !oldTable.getSchemaName().equals(newTable.getSchemaName())) {
                throw new PrestoException(NOT_FOUND, "Schema '" + newTable.getSchemaName() + "' does not exist");
            }
            String sql = format(
                    "RENAME TABLE %s TO %s",
                    quoted(catalogName, oldTable.getSchemaName(), oldTable.getTableName()),
                    quoted(newTable.getTableName()));
            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Nullable
    @Override
    public JdbcTableHandle getTableHandle(ConnectorSession session, JdbcIdentity identity, SchemaTableName schemaTableName)
    {
        try (Connection connection = connectionFactory.openConnection(identity)) {
            String remoteSchema = toRemoteSchemaName(identity, connection, schemaTableName.getSchemaName());
            String remoteTable = toRemoteTableName(identity, connection, remoteSchema, schemaTableName.getTableName());
            if (enableMixedCaseSupport) {
                remoteSchema = quoted(schemaTableName.getSchemaName());
                remoteTable = quoted(schemaTableName.getTableName());
            }
            try (ResultSet resultSet = getTables(connection, Optional.of(remoteSchema), Optional.of(remoteTable))) {
                List<JdbcTableHandle> tableHandles = new ArrayList<>();
                while (resultSet.next()) {
                    if (!enableMixedCaseSupport || schemaTableName.getTableName().equals(resultSet.getString("TABLE_NAME"))) {
                        tableHandles.add(new JdbcTableHandle(
                                connectorId,
                                schemaTableName,
                                resultSet.getString("TABLE_CAT"),
                                resultSet.getString("TABLE_SCHEM"),
                                resultSet.getString("TABLE_NAME"),
                                Optional.empty(),
                                Optional.of("table_alias_" + UUID.randomUUID().toString().replace("-", ""))));
                    }
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
    public List<JdbcColumnHandle> getColumns(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        try (Connection connection = connectionFactory.openConnection(JdbcIdentity.from(session))) {
            try (ResultSet resultSet = enableMixedCaseSupport ? getColumnsWithQuotes(tableHandle, connection.getMetaData()) : getColumns(tableHandle, connection.getMetaData())) {
                int allColumns = 0;
                List<JdbcColumnHandle> columns = new ArrayList<>();
                while (resultSet.next()) {
                    allColumns++;
                    if (!enableMixedCaseSupport || tableHandle.getTableName().equals(resultSet.getString("TABLE_NAME"))) {
                        JdbcTypeHandle typeHandle = new JdbcTypeHandle(
                                resultSet.getInt("DATA_TYPE"),
                                resultSet.getString("TYPE_NAME"),
                                resultSet.getInt("COLUMN_SIZE"),
                                resultSet.getInt("DECIMAL_DIGITS"),
                                Optional.empty());
                        Optional<ColumnMapping> columnMapping = toPrestoType(session, typeHandle);
                        // skip unsupported column types
                        if (columnMapping.isPresent()) {
                            String columnName = resultSet.getString("COLUMN_NAME");
                            boolean nullable = columnNullable == resultSet.getInt("NULLABLE");
                            Optional<String> comment = Optional.ofNullable(emptyToNull(resultSet.getString("REMARKS")));
                            columns.add(new JdbcColumnHandle(connectorId, columnName, typeHandle, columnMapping.get().getType(), nullable, comment, tableHandle.getTableAlias()));
                        }
                    }
                }
                if (columns.isEmpty()) {
                    // A table may have no supported columns. In rare cases (e.g. PostgreSQL) a table might have no columns at all.
                    // Throw an exception if the table has no supported columns.
                    // This can occur if all columns in the table are of unsupported types, or in rare cases, if the table has no columns at all.
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

    private ResultSet getColumnsWithQuotes(JdbcTableHandle tableHandle, DatabaseMetaData metadata)
            throws SQLException
    {
        Optional<String> escape = Optional.ofNullable(metadata.getSearchStringEscape());
        return metadata.getColumns(
                tableHandle.getCatalogName(),
                escapeNamePattern(Optional.ofNullable(quoted(tableHandle.getSchemaName())), escape).orElse(null),
                escapeNamePattern(Optional.ofNullable(quoted(tableHandle.getTableName())), escape).orElse(null),
                null);
    }

    @Override
    protected String quoted(String name)
    {
        name = !enableMixedCaseSupport ? name.toUpperCase() : name;
        return identifierQuote + name + identifierQuote;
    }
}
