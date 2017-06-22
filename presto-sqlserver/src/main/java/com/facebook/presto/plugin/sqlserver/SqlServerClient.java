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

import com.facebook.presto.plugin.jdbc.BaseJdbcClient;
import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.plugin.jdbc.JdbcOutputTableHandle;
import com.facebook.presto.plugin.jdbc.JdbcSplit;
import com.facebook.presto.spi.PrestoException;
import com.microsoft.sqlserver.jdbc.SQLServerDriver;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import static com.facebook.presto.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;

public class SqlServerClient
        extends BaseJdbcClient
{
    @Inject
    public SqlServerClient(JdbcConnectorId connectorId, BaseJdbcConfig config)
            throws SQLException
    {
        super(connectorId, config, "\"", new SQLServerDriver());
        connectionProperties.setProperty("applicationName", "Presto");
        connectionProperties.setProperty("sendStringParametersAsUnicode", "false");
    }

    @Override
    public Connection getConnection(JdbcSplit split)
            throws SQLException
    {
        Connection connection = super.getConnection(split);
        try (Statement statement = connection.createStatement()) {
            statement.execute("SET QUOTED_IDENTIFIER, ANSI_NULLS, CONCAT_NULL_YIELDS_NULL, ARITHABORT ON");
        }
        catch (SQLException e) {
            connection.close();
            throw e;
        }
        return connection;
    }

    @Override
    public void commitCreateTable(JdbcOutputTableHandle handle)
    {
        try (Connection connection = getConnection(handle);
                PreparedStatement statement = connection.prepareStatement("EXEC sp_rename ?, ?")) {
            statement.setString(1, quoted(handle.getCatalogName(), handle.getSchemaName(), handle.getTemporaryTableName()));
            statement.setString(2, handle.getTableName());
            statement.executeUpdate();
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    protected String quoted(String name)
    {
        return '[' + name.replace("]", "]]") + ']';
    }
}
