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

import com.facebook.presto.common.util.ConfigUtil;
import com.facebook.presto.plugin.jdbc.BaseJdbcClient;
import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.DriverConnectionFactory;
import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.plugin.jdbc.JdbcIdentity;
import com.facebook.presto.plugin.jdbc.JdbcTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.base.Joiner;
import com.microsoft.sqlserver.jdbc.SQLServerDriver;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;

public class SqlServerClient
        extends BaseJdbcClient
{
    private static final String ENABLE_MIXED_CASE_SUPPORT = "enable-mixed-case-support";
    private static final Joiner DOT_JOINER = Joiner.on(".");
    private final boolean enableMixedCaseSupport;
    private final boolean checkDriverCaseSupport;

    @Inject
    public SqlServerClient(JdbcConnectorId connectorId, BaseJdbcConfig config)
    {
        super(connectorId, config, "\"", new DriverConnectionFactory(new SQLServerDriver(), config));
        this.enableMixedCaseSupport = ConfigUtil.getConfig(ENABLE_MIXED_CASE_SUPPORT);
        this.checkDriverCaseSupport = config.getCheckDriverCaseSupport() && enableMixedCaseSupport;
    }

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
                                    resultSet.getString("TABLE_NAME")));
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
}
