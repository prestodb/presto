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
package com.facebook.presto.plugin.oracle;

import com.facebook.presto.plugin.jdbc.BaseJdbcClient;
import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.ConnectionFactory;
import com.facebook.presto.plugin.jdbc.DriverConnectionFactory;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.plugin.jdbc.JdbcTypeHandle;
import com.facebook.presto.plugin.jdbc.ReadMapping;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.type.CharType;
import com.facebook.presto.spi.type.Decimals;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableSet;
import oracle.jdbc.OracleDriver;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Locale;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static com.facebook.presto.plugin.jdbc.DriverConnectionFactory.basicConnectionProperties;
import static com.facebook.presto.plugin.jdbc.StandardReadMappings.bigintReadMapping;
import static com.facebook.presto.plugin.jdbc.StandardReadMappings.booleanReadMapping;
import static com.facebook.presto.plugin.jdbc.StandardReadMappings.charReadMapping;
import static com.facebook.presto.plugin.jdbc.StandardReadMappings.dateReadMapping;
import static com.facebook.presto.plugin.jdbc.StandardReadMappings.decimalReadMapping;
import static com.facebook.presto.plugin.jdbc.StandardReadMappings.doubleReadMapping;
import static com.facebook.presto.plugin.jdbc.StandardReadMappings.integerReadMapping;
import static com.facebook.presto.plugin.jdbc.StandardReadMappings.realReadMapping;
import static com.facebook.presto.plugin.jdbc.StandardReadMappings.smallintReadMapping;
import static com.facebook.presto.plugin.jdbc.StandardReadMappings.timeReadMapping;
import static com.facebook.presto.plugin.jdbc.StandardReadMappings.timestampReadMapping;
import static com.facebook.presto.plugin.jdbc.StandardReadMappings.tinyintReadMapping;
import static com.facebook.presto.plugin.jdbc.StandardReadMappings.varbinaryReadMapping;
import static com.facebook.presto.plugin.jdbc.StandardReadMappings.varcharReadMapping;
import static com.facebook.presto.spi.type.CharType.createCharType;
import static com.facebook.presto.spi.type.DecimalType.createDecimalType;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static java.lang.Math.max;
import static java.lang.Math.min;
//import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;

public class OracleClient
        extends BaseJdbcClient
{
    @Inject
    public OracleClient(JdbcConnectorId connectorId, BaseJdbcConfig config,
                        com.facebook.presto.plugin.oracle.OracleConfig oracleConfig) throws SQLException
    {
        super(connectorId, config, "", connectionFactory(config, oracleConfig));
    }

    private static ConnectionFactory connectionFactory(BaseJdbcConfig config, OracleConfig oracleConfig)
            throws SQLException
    {
        Properties connectionProperties = basicConnectionProperties(config);
        connectionProperties.setProperty("nullCatalogMeansCurrent", "false");
        connectionProperties.setProperty("useUnicode", "true");
        connectionProperties.setProperty("characterEncoding", "utf8");
        connectionProperties.setProperty("tinyInt1isBit", "false");
        connectionProperties.setProperty("defaultRowPrefetch", oracleConfig.getDefaultRowPreFetch());
        connectionProperties.setProperty("defaultRowPrefetch", oracleConfig.getDefaultRowPreFetch());

        if (oracleConfig.isIncludeSynonyms()) {
            connectionProperties.setProperty("includeSynonyms", String.valueOf(oracleConfig.isIncludeSynonyms()));
        }

        if (oracleConfig.getConnectionTimeout() != null) {
            connectionProperties.setProperty("connectTimeout", String.valueOf(oracleConfig.getConnectionTimeout().toMillis()));
        }

        return new DriverConnectionFactory(new OracleDriver(), config.getConnectionUrl(), connectionProperties);
    }

    @Override
    public Set<String> getSchemaNames()
    {
        try (Connection connection = connectionFactory.openConnection();
                ResultSet resultSet = connection.getMetaData().getSchemas()) {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            while (resultSet.next()) {
                String schemaName = resultSet.getString(1).toLowerCase(Locale.ENGLISH);
                schemaNames.add(schemaName);
            }
            return schemaNames.build();
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    protected ResultSet getTables(Connection connection, String schemaName,
            String tableName) throws SQLException
    {
        // Here we put TABLE and SYNONYM when the table schema is another user schema
        return connection.getMetaData().getTables(null, schemaName, tableName,
                new String[] {"TABLE", "SYNONYM", "VIEW" });
    }

    @Override
    public Optional<ReadMapping> toPrestoType(ConnectorSession session, JdbcTypeHandle typeHandle)
    {
        int columnSize = typeHandle.getColumnSize();
        switch (typeHandle.getJdbcType()) {
            case Types.BIT:
            case Types.BOOLEAN:
                return Optional.of(booleanReadMapping());
            case Types.TINYINT:
                return Optional.of(tinyintReadMapping());
            case Types.SMALLINT:
                return Optional.of(smallintReadMapping());
            case Types.INTEGER:
                return Optional.of(integerReadMapping());
            case Types.BIGINT:
                return Optional.of(bigintReadMapping());
            case Types.REAL:
                return Optional.of(realReadMapping());
            case Types.NUMERIC:
                return Optional.of(decimalReadMapping(createDecimalType(columnSize == 0 ? 38 : columnSize, max(typeHandle.getDecimalDigits(), 0))));
            case Types.DECIMAL:
                int decimalDigits = typeHandle.getDecimalDigits();
                int precision = columnSize + max(-decimalDigits, 0); // Map decimal(p, -s) (negative scale) to decimal(p+s, 0).
                if (precision > Decimals.MAX_PRECISION) {
                    return Optional.empty();
                }
                return Optional.of(decimalReadMapping(createDecimalType(precision, max(decimalDigits, 0))));
            case Types.FLOAT:
            case Types.DOUBLE:
                return Optional.of(doubleReadMapping());
            case Types.CHAR:
            case Types.NCHAR:
                // TODO this is wrong, we're going to construct malformed Slice representation if source > charLength
                int charLength = min(columnSize, CharType.MAX_LENGTH);
                return Optional.of(charReadMapping(createCharType(charLength)));
            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.LONGVARCHAR:
            case Types.LONGNVARCHAR:
                if (columnSize > VarcharType.MAX_LENGTH || columnSize == 0) {
                    return Optional.of(varcharReadMapping(createUnboundedVarcharType()));
                }
                return Optional.of(varcharReadMapping(createVarcharType(columnSize)));
            case Types.BLOB:
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return Optional.of(varbinaryReadMapping());
            case Types.DATE:
                return Optional.of(dateReadMapping());
            case Types.TIME:
                return Optional.of(timeReadMapping());
            case Types.TIMESTAMP:
                return Optional.of(timestampReadMapping());
        }
        return Optional.empty();
    }
}
