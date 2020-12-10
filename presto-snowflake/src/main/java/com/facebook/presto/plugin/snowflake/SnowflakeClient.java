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
package com.facebook.presto.plugin.snowflake;

import com.facebook.presto.common.type.Decimals;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.plugin.jdbc.BaseJdbcClient;
import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.DriverConnectionFactory;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.plugin.jdbc.JdbcIdentity;
import com.facebook.presto.plugin.jdbc.JdbcSplit;
import com.facebook.presto.plugin.jdbc.JdbcTypeHandle;
import com.facebook.presto.plugin.jdbc.ReadMapping;
import com.facebook.presto.spi.ConnectorSession;
import net.snowflake.client.jdbc.SnowflakeDriver;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Optional;

import static com.facebook.presto.common.type.DecimalType.createDecimalType;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.plugin.jdbc.StandardReadMappings.bigintReadMapping;
import static com.facebook.presto.plugin.jdbc.StandardReadMappings.decimalReadMapping;
import static com.facebook.presto.plugin.jdbc.StandardReadMappings.doubleReadMapping;
import static com.facebook.presto.plugin.jdbc.StandardReadMappings.realReadMapping;
import static com.facebook.presto.plugin.jdbc.StandardReadMappings.smallintReadMapping;
import static com.facebook.presto.plugin.jdbc.StandardReadMappings.varcharReadMapping;

public class SnowflakeClient
        extends BaseJdbcClient
{
    private static final int FETCH_SIZE = 10000;

    @Inject
    public SnowflakeClient(JdbcConnectorId connectorId, BaseJdbcConfig config)
    {
        super(connectorId, config, "\"", new DriverConnectionFactory(new SnowflakeDriver(), config));
    }

    @Override
    public Connection getConnection(JdbcIdentity identity, JdbcSplit split)
            throws SQLException
    {
        Connection connection = connectionFactory.openConnection(identity);
        try {
            connection.setReadOnly(false);
        }
        catch (SQLException e) {
            connection.close();
            throw e;
        }
        return connection;
    }

    private String[] getTableTypes()
    {
        return new String[] {"TABLE", "VIEW"};
    }

    @Override
    protected ResultSet getTables(Connection connection, Optional<String> schemaName, Optional<String> tableName)
            throws SQLException
    {
        DatabaseMetaData metadata = connection.getMetaData();
        String escape = metadata.getSearchStringEscape();
        return metadata.getTables(
                connection.getCatalog(),
                escapeNamePattern(schemaName, Optional.of(escape)).orElse(null),
                escapeNamePattern(tableName, Optional.of(escape)).orElse(null),
                getTableTypes());
    }

    @Override
    public PreparedStatement getPreparedStatement(Connection connection, String sql)
            throws SQLException
    {
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setFetchSize(FETCH_SIZE);
        return statement;
    }

    @Override
    protected String generateTemporaryTableName()
    {
        return "presto_tmp_" + System.nanoTime();
    }

    @Override
    public Optional<ReadMapping> toPrestoType(ConnectorSession session, JdbcTypeHandle typeHandle)
    {
        int columnSize = typeHandle.getColumnSize();

        switch (typeHandle.getJdbcType()) {
            case Types.CLOB:
                return Optional.of(varcharReadMapping(createUnboundedVarcharType()));
            case Types.SMALLINT:
                return Optional.of(smallintReadMapping());
            case Types.FLOAT:
            case Types.DOUBLE:
                return Optional.of(doubleReadMapping());
            case Types.REAL:
                return Optional.of(realReadMapping());
            case Types.NUMERIC:
                int precision = columnSize == 0 ? Decimals.MAX_PRECISION : columnSize;
                int scale = typeHandle.getDecimalDigits();

                if (scale == 0) {
                    return Optional.of(bigintReadMapping());
                }

                return Optional.of(decimalReadMapping(createDecimalType(precision, scale)));
            case Types.LONGVARCHAR:
                if (columnSize > VarcharType.MAX_LENGTH || columnSize == 0) {
                    return Optional.of(varcharReadMapping(createUnboundedVarcharType()));
                }
                return Optional.of(varcharReadMapping(createVarcharType(columnSize)));
            case Types.VARCHAR:
                return Optional.of(varcharReadMapping(createVarcharType(columnSize)));
        }
        return super.toPrestoType(session, typeHandle);
    }
}
