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

import com.facebook.presto.plugin.jdbc.BaseJdbcClient;
import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.ConnectionFactory;
import com.facebook.presto.plugin.jdbc.DriverManagerConnectionFactory;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.plugin.jdbc.JdbcTableHandle;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;

import static com.facebook.presto.plugin.jdbc.DriverConnectionFactory.basicConnectionProperties;
import static com.facebook.presto.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.collect.Iterables.getOnlyElement;

/**
 * Implementation of TeradataClient. It describes table, schemas and columns behaviours.
 * It allows to change the QueryBuilder to a custom one as well.
 */
public class TeradataClient
        extends BaseJdbcClient
{
    static final String TERADATA_DRIVER_NAME = "com.teradata.jdbc.TeraDriver";
    private boolean usePreparedStatement;
    @Inject
    public TeradataClient(JdbcConnectorId connectorId, BaseJdbcConfig config, TeradataConfig teradataConfig)
            throws SQLException
    {
        /*
            We use an empty string as identifierQuote parameter, to avoid using quotes when creating queries
            The following properties are already set to BaseJdbcClient, via properties injection:
            - connectionProperties.setProperty("user", teradataConfig.getUser());
            - connectionProperties.setProperty("url", teradataConfig.getUrl());
            - connectionProperties.setProperty("password", teradataConfig.getPassword());
         */
        super(connectorId, config, "", connectionFactory(config, teradataConfig));
        this.usePreparedStatement = teradataConfig.isUsePreparedStatement();
    }

    private static ConnectionFactory connectionFactory(BaseJdbcConfig config, TeradataConfig teradataConfig)
            throws SQLException
    {
        Properties connectionProperties = basicConnectionProperties(config);
        return new DriverManagerConnectionFactory(TERADATA_DRIVER_NAME, config.getConnectionUrl(), connectionProperties);
    }

    @Override
    public Set<String> getSchemaNames()
    {
        try (Connection connection = connectionFactory.openConnection(); ResultSet resultSet = connection.getMetaData().getSchemas()) {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            while (resultSet.next()) {
                String schemaName = resultSet.getString(1).toLowerCase(Locale.ENGLISH);
                schemaNames.add(schemaName);
            }
            return schemaNames.build();
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    protected ResultSet getTables(Connection connection, String schemaName,
                                  String tableName) throws SQLException
    {
        //We filter just VIEW, TABLE and SYNONYM. For more table types: connection.getMetaData().getTableTypes()
        return connection.getMetaData().getTables(null, schemaName, tableName, new String[] {"VIEW", "TABLE", "SYNONYM"});
    }

    @Nullable
    @Override
    public JdbcTableHandle getTableHandle(SchemaTableName schemaTableName)
    {
        try (Connection connection = connectionFactory.openConnection()) {
            DatabaseMetaData metadata = connection.getMetaData();
            String jdbcSchemaName = schemaTableName.getSchemaName();
            String jdbcTableName = schemaTableName.getTableName();
            if (metadata.storesUpperCaseIdentifiers()) {
                jdbcSchemaName = jdbcSchemaName.toUpperCase();
                jdbcTableName = jdbcTableName.toUpperCase();
            }
            ResultSet resultSet = getTables(connection, jdbcSchemaName, jdbcTableName);
            List<JdbcTableHandle> tableHandles = new ArrayList<JdbcTableHandle>();

            while (resultSet.next()) {
                int columns = resultSet.getMetaData().getColumnCount();
                tableHandles.add(
                        new JdbcTableHandle(
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
                throw new PrestoException(NOT_SUPPORTED,
                        "Multiple tables matched: " + schemaTableName);
            }
            return getOnlyElement(tableHandles);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<SchemaTableName> getTableNames(@Nullable String schema)
    {
        try (Connection connection = connectionFactory.openConnection()) {
            DatabaseMetaData metadata = connection.getMetaData();
            if (metadata.storesUpperCaseIdentifiers() && (schema != null)) {
                schema = schema.toUpperCase();
            }
            ResultSet resultSet = getTables(connection, schema, null);
            ImmutableList.Builder<SchemaTableName> list = ImmutableList.builder();
            while (resultSet.next()) {
                list.add(getSchemaTableName(resultSet));
            }
            return list.build();
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected SchemaTableName getSchemaTableName(ResultSet resultSet)
            throws SQLException
    {
        String tableSchema = resultSet.getString("TABLE_SCHEM");
        String tableName = resultSet.getString("TABLE_NAME");
        if (tableSchema != null) {
            tableSchema = tableSchema.toLowerCase(Locale.ENGLISH);
        }
        if (tableName != null) {
            tableName = tableName.toLowerCase(Locale.ENGLISH);
        }
        return new SchemaTableName(tableSchema, tableName);
    }

    @Override
    protected void execute(Connection connection, String query)
            throws SQLException
    {
        if (!this.usePreparedStatement) {
            super.execute(connection, rewriteQuery(query));
        }
        else {
            super.getPreparedStatement(connection, rewriteQuery(query)).execute();
        }
    }

    @VisibleForTesting
    protected String rewriteQuery(String query)
    {
        return query;
    }
}
