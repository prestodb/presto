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
import com.facebook.presto.plugin.jdbc.DriverConnectionFactory;
import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.plugin.jdbc.JdbcTableHandle;
import com.facebook.presto.plugin.jdbc.JdbcTypeHandle;
import com.facebook.presto.plugin.jdbc.ReadMapping;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.CharType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.microsoft.sqlserver.jdbc.ISQLServerResultSet;
import com.microsoft.sqlserver.jdbc.SQLServerDriver;
import microsoft.sql.DateTimeOffset;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static com.facebook.presto.plugin.jdbc.ReadMapping.longReadMapping;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.Chars.isCharType;
import static com.facebook.presto.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static java.lang.String.format;

public class SqlServerClient
        extends BaseJdbcClient
{
    private static final Joiner DOT_JOINER = Joiner.on(".");

    private static final int MAX_CHAR_LENGTH = 8000;

    private static final Map<Type, String> MSSQL_TYPES = ImmutableMap.<Type, String>builder()
            .put(BOOLEAN, "bit")
            .put(BIGINT, "bigint")
            .put(INTEGER, "int")
            .put(SMALLINT, "smallint")
            .put(TINYINT, "tinyint")
            .put(REAL, "float(24)")
            .put(DOUBLE, "float(53)")
            .put(VARBINARY, "varbinary(max)")
            .put(DATE, "date")
            .put(TIME, "time")
            .put(TIMESTAMP, "datetime2")
            .put(TIMESTAMP_WITH_TIME_ZONE, "datetimeoffset")
            .build();

    @Inject
    public SqlServerClient(JdbcConnectorId connectorId, BaseJdbcConfig config)
    {
        super(connectorId, config, "\"", new DriverConnectionFactory(new SQLServerDriver(), config));
    }

    @Override
    protected void renameTable(String catalogName, SchemaTableName oldTable, SchemaTableName newTable)
    {
        try (Connection connection = connectionFactory.openConnection()) {
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
    public void renameColumn(JdbcTableHandle handle, JdbcColumnHandle jdbcColumn, String newColumnName)
    {
        try (Connection connection = connectionFactory.openConnection()) {
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

    @Override
    protected String toSqlType(Type type)
    {
        if (isVarcharType(type)) {
            VarcharType varcharType = (VarcharType) type;
            if (varcharType.isUnbounded() || varcharType.getLengthSafe() > MAX_CHAR_LENGTH) {
                return "varchar(max)";
            }
            return "varchar(" + varcharType.getLengthSafe() + ")";
        }

        if (isCharType(type)) {
            CharType charType = (CharType) type;
            if (charType.getLength() <= MAX_CHAR_LENGTH) {
                return "char(" + charType.getLength() + ")";
            }
            throw new PrestoException(NOT_SUPPORTED, "Length of char type cannot exceed " + MAX_CHAR_LENGTH);
        }

        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            return format("decimal(%s, %s)", decimalType.getPrecision(), decimalType.getScale());
        }

        String sqlType = MSSQL_TYPES.get(type);
        if (sqlType != null) {
            return sqlType;
        }
        throw new PrestoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
    }

    @Override
    public Optional<ReadMapping> toPrestoType(ConnectorSession session, JdbcTypeHandle typeHandle)
    {
        if (typeHandle.getJdbcType() == microsoft.sql.Types.DATETIMEOFFSET) {
            return Optional.of(dateTimeOffsetReadMapping());
        }
        return super.toPrestoType(session, typeHandle);
    }

    private static ReadMapping dateTimeOffsetReadMapping()
    {
        return longReadMapping(TIMESTAMP_WITH_TIME_ZONE, (resultSet, columnIndex) -> {
            DateTimeOffset dto = resultSet.unwrap(ISQLServerResultSet.class).getDateTimeOffset(columnIndex);
            return packDateTimeWithZone(dto.getTimestamp().getTime(), dto.getMinutesOffset());
        });
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
