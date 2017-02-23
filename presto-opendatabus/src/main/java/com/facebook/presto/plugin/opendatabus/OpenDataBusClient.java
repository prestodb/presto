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
package com.facebook.presto.plugin.opendatabus;

import com.facebook.presto.jdbc.PrestoDriver;
import com.facebook.presto.plugin.jdbc.BaseJdbcClient;
import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.plugin.jdbc.JdbcTableHandle;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.spi.type.Varchars;
import com.google.common.base.Throwables;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static com.facebook.presto.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Locale.ENGLISH;

public class OpenDataBusClient
        extends BaseJdbcClient
{
    @Inject
    public OpenDataBusClient(JdbcConnectorId connectorId, BaseJdbcConfig config, OpenDataBusConfig openDataBusConfig)
            throws SQLException
    {
        super(connectorId, config, "`", new PrestoDriver());
        connectionProperties.setProperty("nullCatalogMeansCurrent", "false");
        connectionProperties.setProperty("useUnicode", "true");
        connectionProperties.setProperty("characterEncoding", "utf8");
        connectionProperties.setProperty("tinyInt1isBit", "false");
        if (openDataBusConfig.isAutoReconnect()) {
            connectionProperties.setProperty("autoReconnect", String.valueOf(openDataBusConfig.isAutoReconnect()));
            connectionProperties.setProperty("maxReconnects", String.valueOf(openDataBusConfig.getMaxReconnects()));
        }
        if (openDataBusConfig.getConnectionTimeout() != null) {
            connectionProperties.setProperty("connectTimeout", String.valueOf(openDataBusConfig.getConnectionTimeout().toMillis()));
        }
    }

    @Override
    public Set<String> getSchemaNames()
    {
        System.out.println("getSchemaNames");
        // for MySQL, we need to list catalogs instead of schemas
        try (Connection connection = driver.connect(connectionUrl, connectionProperties)) {
            TreeSet schemaNames = new TreeSet<String>();
            schemaNames.add("mysql");
            return schemaNames;
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public PreparedStatement getPreparedStatement(Connection connection, String sql)
            throws SQLException
    {
        System.out.println("getPreparedStatement");
        System.out.println("SQL" + sql);
        PreparedStatement statement = connection.prepareStatement(sql);
        return statement;
    }

    @Override
    protected ResultSet getTables(Connection connection, String schemaName, String tableName)
            throws SQLException
    {
        System.out.println("getTables:" + schemaName);
        try {
            Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery("SHOW TABLES FROM mysql.reto1");
            HashMap<String, String> translate = new HashMap();
            translate.put("TABLE_NAME", "Table");
            ResultSetWrapper rsw = new ResultSetWrapper(rs, translate);
            rsw.setSchema("reto1");
            return rsw;
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public JdbcTableHandle getTableHandle(SchemaTableName schemaTableName)
    {
        try (Connection connection = driver.connect(connectionUrl, connectionProperties)) {
            String jdbcSchemaName = schemaTableName.getSchemaName();
            String jdbcTableName = schemaTableName.getTableName();

            try (ResultSet resultSet = getTables(connection, jdbcSchemaName, jdbcTableName)) {
                List<JdbcTableHandle> tableHandles = new ArrayList<>();
                while (resultSet.next()) {
                    tableHandles.add(new JdbcTableHandle(
                            connectorId,
                            schemaTableName,
                            "opendatabus",
                            ((ResultSetWrapper) resultSet).getSchema(),
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
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    protected SchemaTableName getSchemaTableName(ResultSet resultSet)
            throws SQLException
    {
        System.out.println("getSchemaTableName ------");
        System.out.println("getSchemaTableName: " + resultSet.getString("TABLE_NAME").toLowerCase(ENGLISH));
        System.out.println("getSchemaTableName: " +  ((ResultSetWrapper) resultSet).getSchema().toLowerCase(ENGLISH));
        return new SchemaTableName(
                resultSet.getString("TABLE_NAME").toLowerCase(ENGLISH),
                ((ResultSetWrapper) resultSet).getSchema().toLowerCase(ENGLISH));
    }

    @Override
    protected String toSqlType(Type type)
    {
        System.out.println("toSqlType: " + type.getDisplayName());
        if (Varchars.isVarcharType(type)) {
            VarcharType varcharType = (VarcharType) type;
            if (varcharType.getLength() <= 255) {
                return "tinytext";
            }
            if (varcharType.getLength() <= 65535) {
                return "text";
            }
            if (varcharType.getLength() <= 16777215) {
                return "mediumtext";
            }
            return "longtext";
        }

        String sqlType = super.toSqlType(type);
        switch (sqlType) {
            case "varbinary":
                return "mediumblob";
            case "time with timezone":
                return "time";
            case "timestamp":
            case "timestamp with timezone":
                return "datetime";
        }
        return sqlType;
    }
}
