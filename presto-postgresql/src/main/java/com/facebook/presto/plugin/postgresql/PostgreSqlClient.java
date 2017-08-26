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

import com.facebook.presto.plugin.jdbc.BaseJdbcClient;
import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.plugin.jdbc.JdbcOutputTableHandle;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.SmallintType;
import com.facebook.presto.spi.type.TimeType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import org.postgresql.Driver;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

public class PostgreSqlClient
        extends BaseJdbcClient
{
    private static final Map<String, Type> PG_ARRAY_TYPE_TO_ELEMENT_TYPE =
            ImmutableMap.<String, Type>builder()
                    .put("_bool", BooleanType.BOOLEAN)
                    .put("_bit", BooleanType.BOOLEAN)
                    .put("_int8", BigintType.BIGINT)
                    .put("_int4", IntegerType.INTEGER)
                    .put("_int2", SmallintType.SMALLINT)
                    .put("_text", VarcharType.createUnboundedVarcharType())
                    .put("_bytea", VarbinaryType.VARBINARY)
                    .put("_float4", DoubleType.DOUBLE)
                    .put("_float8", DoubleType.DOUBLE)
                    .put("_timestamp", TimestampType.TIMESTAMP)
                    .put("_date", DateType.DATE)
                    .put("_time", TimeType.TIME)
                    .put("_numeric", DoubleType.DOUBLE)
                    .build();

    @Inject
    public PostgreSqlClient(JdbcConnectorId connectorId, BaseJdbcConfig config)
            throws SQLException
    {
        super(connectorId, config, "\"", new Driver());
    }

    @Override
    public void commitCreateTable(JdbcOutputTableHandle handle)
    {
        // PostgreSQL does not allow qualifying the target of a rename
        StringBuilder sql = new StringBuilder()
                .append("ALTER TABLE ")
                .append(quoted(handle.getCatalogName(), handle.getSchemaName(), handle.getTemporaryTableName()))
                .append(" RENAME TO ")
                .append(quoted(handle.getTableName()));

        try (Connection connection = getConnection(handle)) {
            execute(connection, sql.toString());
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public PreparedStatement getPreparedStatement(Connection connection, String sql)
            throws SQLException
    {
        connection.setAutoCommit(false);
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setFetchSize(1000);
        return statement;
    }

    @Override
    protected ResultSet getTables(Connection connection, String schemaName, String tableName)
            throws SQLException
    {
        DatabaseMetaData metadata = connection.getMetaData();
        String escape = metadata.getSearchStringEscape();
        return metadata.getTables(
                connection.getCatalog(),
                escapeNamePattern(schemaName, escape),
                escapeNamePattern(tableName, escape),
                new String[] {"TABLE", "VIEW", "MATERIALIZED VIEW", "FOREIGN TABLE"});
    }

    @Override
    protected Type toPrestoType(int dataType, int columnSize, String typeName)
    {
        if ("_char".equals(typeName) || "_varchar".equals(typeName)) {
            return new ArrayType(VarcharType.createVarcharType(columnSize));
        }
        Type elementType = PG_ARRAY_TYPE_TO_ELEMENT_TYPE.get(typeName);
        if (elementType != null) {
            return new ArrayType(elementType);
        }
        return super.toPrestoType(dataType, columnSize, typeName);
    }
}
