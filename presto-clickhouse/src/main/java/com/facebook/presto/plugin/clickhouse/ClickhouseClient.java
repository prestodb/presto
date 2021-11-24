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
package com.facebook.presto.plugin.clickhouse;

import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.plugin.jdbc.BaseJdbcClient;
import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.ConnectionFactory;
import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.plugin.jdbc.JdbcErrorCode;
import com.facebook.presto.plugin.jdbc.JdbcIdentity;
import com.facebook.presto.plugin.jdbc.JdbcSplit;
import com.facebook.presto.plugin.jdbc.JdbcTypeHandle;
import com.facebook.presto.plugin.jdbc.QueryBuilder;
import com.facebook.presto.plugin.jdbc.ReadMapping;
import com.facebook.presto.plugin.jdbc.StandardReadMappings;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.StandardErrorCode;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ClickhouseClient
        extends BaseJdbcClient
{
    private static final int FETCH_SIZE = 1000;

    private final int numberDefaultScale;

    @Inject
    public ClickhouseClient(JdbcConnectorId connectorId,
                            BaseJdbcConfig config,
                            ClickhouseConfig clickhouseConfig,
                            ConnectionFactory connectionFactory)
    {
        super(connectorId, config, "", connectionFactory);
        requireNonNull(clickhouseConfig, "clickhouse config is null");
        this.numberDefaultScale = clickhouseConfig.getNumberDefaultScale();
    }

    private String[] getTableTypes()
    {
        return new String[]{"TABLE", "VIEW"};
    }

    protected ResultSet getTables(Connection connection, Optional<String> schemaName, Optional<String> tableName)
            throws SQLException
    {
        DatabaseMetaData metadata = connection.getMetaData();
        String escape = metadata.getSearchStringEscape();
        return metadata.getTables(connection.getCatalog(),
                escapeNamePattern(schemaName, Optional.of(escape)).orElse(null),
                escapeNamePattern(tableName, Optional.of(escape)).orElse(null),
                getTableTypes());
    }

    public PreparedStatement getPreparedStatement(Connection connection, String sql) throws SQLException
    {
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setFetchSize(FETCH_SIZE);
        return statement;
    }

    /**
     *
     * clickhouse sql no quote to delimit fields or schema or table
     * @author zhzhenqin@163.com
     *
     * @param session
     * @param connection
     * @param split
     * @param columnHandles
     * @return
     * @throws SQLException
     */
    @Override
    public PreparedStatement buildSql(ConnectorSession session, Connection connection, JdbcSplit split, List<JdbcColumnHandle> columnHandles)
            throws SQLException
    {
        return new QueryBuilder(identifierQuote).buildSql(
                this,
                session,
                connection,
                "",
                split.getSchemaName(),
                split.getTableName(),
                columnHandles,
                split.getTupleDomain(),
                split.getAdditionalPredicate());
    }

    protected String generateTemporaryTableName()
    {
        return "presto_tmp_" + System.nanoTime();
    }

    protected void renameTable(JdbcIdentity identity, String catalogName, SchemaTableName oldTable, SchemaTableName newTable)
    {
        if (!oldTable.getSchemaName().equalsIgnoreCase(newTable.getSchemaName())) {
            throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "Table rename across schemas is not supported in Oracle");
        }
        String newTableName = newTable.getTableName().toUpperCase(Locale.ENGLISH);
        String oldTableName = oldTable.getTableName().toUpperCase(Locale.ENGLISH);
        String sql = String.format("ALTER TABLE %s RENAME TO %s",
                new Object[]{quoted(catalogName, oldTable.getSchemaName(), oldTableName), quoted(newTableName)});
        try (Connection connection = this.connectionFactory.openConnection(identity)) {
            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new PrestoException(JdbcErrorCode.JDBC_ERROR, e);
        }
    }

    public Optional<ReadMapping> toPrestoType(ConnectorSession session, JdbcTypeHandle typeHandle)
    {
        int precision;
        int scale;
        int columnSize = typeHandle.getColumnSize();
        switch (typeHandle.getJdbcType()) {
            case Types.CLOB:
                return Optional.of(StandardReadMappings.varcharReadMapping(VarcharType.createUnboundedVarcharType()));
            case Types.SMALLINT:
                return Optional.of(StandardReadMappings.smallintReadMapping());
            case Types.FLOAT:
            case Types.DOUBLE:
                return Optional.of(StandardReadMappings.doubleReadMapping());
            case Types.REAL:
                return Optional.of(StandardReadMappings.realReadMapping());
            case Types.NUMERIC:
                precision = (columnSize == 0) ? 38 : columnSize;
                scale = typeHandle.getDecimalDigits();
                if (scale == 0) {
                    return Optional.of(StandardReadMappings.bigintReadMapping());
                }
                if (scale < 0 || scale > precision) {
                    return Optional.of(StandardReadMappings.decimalReadMapping(DecimalType.createDecimalType(precision, this.numberDefaultScale)));
                }
                return Optional.of(StandardReadMappings.decimalReadMapping(DecimalType.createDecimalType(precision, scale)));
            case Types.LONGVARCHAR:
                if (columnSize > 2147483646 || columnSize == 0) {
                    return Optional.of(StandardReadMappings.varcharReadMapping(VarcharType.createUnboundedVarcharType()));
                }
                return Optional.of(StandardReadMappings.varcharReadMapping(VarcharType.createVarcharType(columnSize)));
            case Types.VARCHAR:
                return Optional.of(StandardReadMappings.varcharReadMapping(VarcharType.createVarcharType(columnSize)));
        }
        return super.toPrestoType(session, typeHandle);
    }
}
