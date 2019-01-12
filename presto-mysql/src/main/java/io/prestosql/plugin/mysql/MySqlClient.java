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
package io.prestosql.plugin.mysql;

import com.google.common.collect.ImmutableSet;
import com.mysql.jdbc.Driver;
import com.mysql.jdbc.Statement;
import io.prestosql.plugin.jdbc.BaseJdbcClient;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.DriverConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcConnectorId;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Set;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.prestosql.plugin.jdbc.DriverConnectionFactory.basicConnectionProperties;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.Varchars.isVarcharType;
import static java.util.Locale.ENGLISH;

public class MySqlClient
        extends BaseJdbcClient
{
    @Inject
    public MySqlClient(JdbcConnectorId connectorId, BaseJdbcConfig config, MySqlConfig mySqlConfig)
            throws SQLException
    {
        super(connectorId, config, "`", connectionFactory(config, mySqlConfig));
    }

    private static ConnectionFactory connectionFactory(BaseJdbcConfig config, MySqlConfig mySqlConfig)
            throws SQLException
    {
        Properties connectionProperties = basicConnectionProperties(config);
        connectionProperties.setProperty("useInformationSchema", "true");
        connectionProperties.setProperty("nullCatalogMeansCurrent", "false");
        connectionProperties.setProperty("useUnicode", "true");
        connectionProperties.setProperty("characterEncoding", "utf8");
        connectionProperties.setProperty("tinyInt1isBit", "false");
        if (mySqlConfig.isAutoReconnect()) {
            connectionProperties.setProperty("autoReconnect", String.valueOf(mySqlConfig.isAutoReconnect()));
            connectionProperties.setProperty("maxReconnects", String.valueOf(mySqlConfig.getMaxReconnects()));
        }
        if (mySqlConfig.getConnectionTimeout() != null) {
            connectionProperties.setProperty("connectTimeout", String.valueOf(mySqlConfig.getConnectionTimeout().toMillis()));
        }

        return new DriverConnectionFactory(new Driver(), config.getConnectionUrl(), connectionProperties);
    }

    @Override
    public Set<String> getSchemaNames()
    {
        // for MySQL, we need to list catalogs instead of schemas
        try (Connection connection = connectionFactory.openConnection();
                ResultSet resultSet = connection.getMetaData().getCatalogs()) {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            while (resultSet.next()) {
                String schemaName = resultSet.getString("TABLE_CAT").toLowerCase(ENGLISH);
                // skip internal schemas
                if (!schemaName.equals("information_schema") && !schemaName.equals("mysql")) {
                    schemaNames.add(schemaName);
                }
            }
            return schemaNames.build();
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void abortReadConnection(Connection connection)
            throws SQLException
    {
        // Abort connection before closing. Without this, the MySQL driver
        // attempts to drain the connection by reading all the results.
        connection.abort(directExecutor());
    }

    @Override
    public PreparedStatement getPreparedStatement(Connection connection, String sql)
            throws SQLException
    {
        PreparedStatement statement = connection.prepareStatement(sql);
        if (statement.isWrapperFor(Statement.class)) {
            statement.unwrap(Statement.class).enableStreamingResults();
        }
        return statement;
    }

    @Override
    protected ResultSet getTables(Connection connection, String schemaName, String tableName)
            throws SQLException
    {
        // MySQL maps their "database" to SQL catalogs and does not have schemas
        DatabaseMetaData metadata = connection.getMetaData();
        String escape = metadata.getSearchStringEscape();
        return metadata.getTables(
                schemaName,
                null,
                escapeNamePattern(tableName, escape),
                new String[] {"TABLE", "VIEW"});
    }

    @Override
    protected SchemaTableName getSchemaTableName(ResultSet resultSet)
            throws SQLException
    {
        // MySQL uses catalogs instead of schemas
        return new SchemaTableName(
                resultSet.getString("TABLE_CAT").toLowerCase(ENGLISH),
                resultSet.getString("TABLE_NAME").toLowerCase(ENGLISH));
    }

    @Override
    protected String toSqlType(Type type)
    {
        if (REAL.equals(type)) {
            return "float";
        }
        if (TIME_WITH_TIME_ZONE.equals(type) || TIMESTAMP_WITH_TIME_ZONE.equals(type)) {
            throw new PrestoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
        }
        if (TIMESTAMP.equals(type)) {
            return "datetime";
        }
        if (VARBINARY.equals(type)) {
            return "mediumblob";
        }
        if (isVarcharType(type)) {
            VarcharType varcharType = (VarcharType) type;
            if (varcharType.isUnbounded()) {
                return "longtext";
            }
            if (varcharType.getLengthSafe() <= 255) {
                return "tinytext";
            }
            if (varcharType.getLengthSafe() <= 65535) {
                return "text";
            }
            if (varcharType.getLengthSafe() <= 16777215) {
                return "mediumtext";
            }
            return "longtext";
        }

        return super.toSqlType(type);
    }
}
