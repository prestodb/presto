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
import com.facebook.presto.spi.PrestoException;
import com.microsoft.sqlserver.jdbc.SQLServerDriver;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.SQLException;

import static com.facebook.presto.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;

public class SqlServerClient
        extends BaseJdbcClient
{
    @Inject
    public SqlServerClient(JdbcConnectorId connectorId, BaseJdbcConfig config)
            throws SQLException
    {
        super(connectorId, config, "\"", new SQLServerDriver());
    }

    @Override
    public void commitCreateTable(JdbcOutputTableHandle handle)
    {
        StringBuilder sql = new StringBuilder()
                .append("sp_rename ")
                .append(singleQuote(handle.getCatalogName(), handle.getSchemaName(), handle.getTemporaryTableName()))
                .append(", ")
                .append(singleQuote(handle.getTableName()));

        try (Connection connection = getConnection(handle)) {
            execute(connection, sql.toString());
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    private static String singleQuote(String catalog, String schema, String table)
    {
        return singleQuote(catalog + "." + schema + "." + table);
    }

    private static String singleQuote(String literal)
    {
        return "\'" + literal + "\'";
    }
}
