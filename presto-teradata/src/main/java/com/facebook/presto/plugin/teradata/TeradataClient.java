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
package com.facebook.presto.plugin.teradata;

import com.facebook.presto.common.type.CLOBType;
import com.facebook.presto.common.util.ConfigUtil;
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
import com.teradata.jdbc.TeraDriver;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static com.facebook.presto.common.type.CLOBType.createCLOBType;
import static com.facebook.presto.common.type.CLOBType.createUnboundedCLOBType;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static com.facebook.presto.plugin.jdbc.StandardColumnMappings.blobColumnMapping;
import static com.facebook.presto.plugin.jdbc.StandardColumnMappings.clobColumnMapping;
import static com.facebook.presto.plugin.jdbc.StandardColumnMappings.varcharColumnMapping;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;

public class TeradataClient
        extends BaseJdbcClient
{
    private static final String ENABLE_MIXED_CASE_SUPPORT = "enable-mixed-case-support";
    private final boolean enableMixedCaseSupport;
    private final boolean checkDriverCaseSupport;

    @Inject
    public TeradataClient(JdbcConnectorId connectorId, BaseJdbcConfig config)
    {
        super(connectorId, config, "\"", new DriverConnectionFactory(new TeraDriver(), config));
        this.enableMixedCaseSupport = ConfigUtil.getConfig(ENABLE_MIXED_CASE_SUPPORT);
        this.checkDriverCaseSupport = config.getCheckDriverCaseSupport() && enableMixedCaseSupport;
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
    public Optional<ColumnMapping> toPrestoType(ConnectorSession session, JdbcTypeHandle typeHandle)
    {
        int columnSize = typeHandle.getColumnSize();
        switch (typeHandle.getJdbcType()) {
            case Types.BLOB:
                return Optional.of(blobColumnMapping());
            case Types.CLOB:
                if (columnSize > CLOBType.MAX_LENGTH || columnSize == 0) {
                    return Optional.of(clobColumnMapping(createUnboundedCLOBType()));
                }
                return Optional.of(clobColumnMapping(createCLOBType(columnSize)));
            case Types.VARCHAR:
                return Optional.of(varcharColumnMapping(createVarcharType(columnSize)));
        }
        return super.toPrestoType(session, typeHandle);
    }

    @javax.annotation.Nullable
    @Override
    public JdbcTableHandle getTableHandle(ConnectorSession session, JdbcIdentity identity, SchemaTableName schemaTableName)
    {
        try (Connection connection = connectionFactory.openConnection(identity)) {
            if (enableMixedCaseSupport) {
                try (ResultSet resultSet = getTables(connection, Optional.of(schemaTableName.getSchemaName()), Optional.of(schemaTableName.getTableName()))) {
                    List<JdbcTableHandle> tableHandles = new ArrayList<>();
                    while (resultSet.next()) {
                        if (schemaTableName.getSchemaName().equals(resultSet.getString("TABLE_SCHEM")) && schemaTableName.getTableName().equals(resultSet.getString("TABLE_NAME"))) {
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
            else {
                String remoteSchema = toRemoteSchemaName(identity, connection, schemaTableName.getSchemaName());
                String remoteTable = toRemoteTableName(identity, connection, remoteSchema, schemaTableName.getTableName());
                try (ResultSet resultSet = getTables(connection, Optional.of(remoteSchema), Optional.of(remoteTable))) {
                    List<JdbcTableHandle> tableHandles = new ArrayList<>();
                    while (resultSet.next()) {
                        tableHandles.add(new JdbcTableHandle(
                                connectorId,
                                schemaTableName,
                                resultSet.getString("TABLE_CAT"),
                                resultSet.getString("TABLE_SCHEM"),
                                resultSet.getString("TABLE_NAME"),
                                Optional.empty(),
                                Optional.of("table_alias_" + UUID.randomUUID().toString().replace("-", ""))));
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
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    protected void renameTable(JdbcIdentity identity, String catalogName, SchemaTableName oldTable, SchemaTableName newTable)
    {
        try (Connection connection = connectionFactory.openConnection(identity)) {
            String sql = format(
                    "RENAME TABLE %s TO %s",
                    quoted(catalogName, oldTable.getSchemaName(), oldTable.getTableName()),
                    quoted(catalogName, newTable.getSchemaName(), newTable.getTableName()));
            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void dropColumn(ConnectorSession session, JdbcIdentity identity, JdbcTableHandle handle, JdbcColumnHandle column)
    {
        try (Connection connection = connectionFactory.openConnection(identity)) {
            String sql = format(
                    "ALTER TABLE %s DROP %s",
                    quoted(handle.getCatalogName(), handle.getSchemaName(), handle.getTableName()),
                    column.getColumnName());
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
            DatabaseMetaData metadata = connection.getMetaData();
            if (metadata.storesUpperCaseIdentifiers()) {
                newColumnName = newColumnName.toUpperCase(ENGLISH);
            }
            String sql = format(
                    "ALTER TABLE %s RENAME %s TO %s",
                    quoted(handle.getCatalogName(), handle.getSchemaName(), handle.getTableName()),
                    jdbcColumn.getColumnName(),
                    newColumnName);
            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }
}
