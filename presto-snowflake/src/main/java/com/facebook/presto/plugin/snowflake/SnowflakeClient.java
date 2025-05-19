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

import com.facebook.presto.common.util.ConfigUtil;
import com.facebook.presto.plugin.jdbc.BaseJdbcClient;
import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.ColumnMapping;
import com.facebook.presto.plugin.jdbc.DriverConnectionFactory;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.plugin.jdbc.JdbcTypeHandle;
import com.facebook.presto.plugin.jdbc.StandardColumnMappings;
import com.facebook.presto.spi.ConnectorSession;
import net.snowflake.client.jdbc.SnowflakeDriver;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Optional;

import static com.facebook.presto.common.constant.ConfigConstants.ENABLE_MIXED_CASE_SUPPORT;

public class SnowflakeClient
        extends BaseJdbcClient
{
    private final boolean enableMixedCaseSupport;
    @Inject
    public SnowflakeClient(JdbcConnectorId connectorId, BaseJdbcConfig config)
    {
        super(connectorId, config, "\"", new DriverConnectionFactory(new SnowflakeDriver(), config));
        this.enableMixedCaseSupport = ConfigUtil.getConfig(ENABLE_MIXED_CASE_SUPPORT);
    }

    @Override
    public PreparedStatement getPreparedStatement(ConnectorSession session, Connection connection, String sql)
            throws SQLException
    {
        connection.setAutoCommit(false);
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setFetchSize(1000);
        return statement;
    }

    public Optional<ColumnMapping> toPrestoType(ConnectorSession session, JdbcTypeHandle typeHandle)
    {
        String typeName = typeHandle.getJdbcTypeName();
        if (typeName.equalsIgnoreCase("binary")) {
            return Optional.of(StandardColumnMappings.varbinaryColumnMapping());
        }
        return super.toPrestoType(session, typeHandle);
    }

    @Override
    protected String quoted(String name)
    {
        if (!enableMixedCaseSupport) {
            return name;
        }
        name = name.replace(identifierQuote, identifierQuote + identifierQuote);
        return identifierQuote + name + identifierQuote;
    }
}
