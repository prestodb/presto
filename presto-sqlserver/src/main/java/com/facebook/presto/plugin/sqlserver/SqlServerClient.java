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

import com.facebook.presto.common.type.Decimals;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.plugin.jdbc.BaseJdbcClient;
import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.DriverConnectionFactory;
import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.plugin.jdbc.JdbcIdentity;
import com.facebook.presto.plugin.jdbc.JdbcTableHandle;
import com.facebook.presto.plugin.jdbc.JdbcTypeHandle;
import com.facebook.presto.plugin.jdbc.ReadMapping;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.base.Joiner;
import com.microsoft.sqlserver.jdbc.SQLServerDriver;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Optional;

import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DecimalType.createDecimalType;
import static com.facebook.presto.common.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.common.type.Varchars.isVarcharType;
import static com.facebook.presto.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static com.facebook.presto.plugin.jdbc.StandardReadMappings.bigintReadMapping;
import static com.facebook.presto.plugin.jdbc.StandardReadMappings.decimalReadMapping;
import static com.facebook.presto.plugin.jdbc.StandardReadMappings.doubleReadMapping;
import static com.facebook.presto.plugin.jdbc.StandardReadMappings.realReadMapping;
import static com.facebook.presto.plugin.jdbc.StandardReadMappings.smallintReadMapping;
import static com.facebook.presto.plugin.jdbc.StandardReadMappings.varcharReadMapping;
import static java.lang.String.format;

public class SqlServerClient
        extends BaseJdbcClient
{
    private static final Joiner DOT_JOINER = Joiner.on(".");

    @Inject
    public SqlServerClient(JdbcConnectorId connectorId, BaseJdbcConfig config)
    {
        super(connectorId, config, "\"", new DriverConnectionFactory(new SQLServerDriver(), config));
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
    public void renameColumn(JdbcIdentity identity, JdbcTableHandle handle, JdbcColumnHandle jdbcColumn, String newColumnName)
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
        return "'" + literal + "'";
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

    @Override
    protected String toSqlType(Type type)
    {
        if (BOOLEAN.equals(type)) {
            return "bit";
        }
        if (TIME_WITH_TIME_ZONE.equals(type) || TIMESTAMP_WITH_TIME_ZONE.equals(type)) {
            return "datetimeoffset";
        }
        if (TIMESTAMP.equals(type)) {
            return "datetime";
        }
        if (isVarcharType(type)) {
            VarcharType varcharType = (VarcharType) type;
            if (varcharType.isUnbounded() || varcharType.getLengthSafe() > 4000) {
                return "nvarchar(max)";
            }
            else {
                return "nvarchar(" + varcharType.getLengthSafe() + ")";
            }
        }

        return super.toSqlType(type);
    }
}
