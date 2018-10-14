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
package com.facebook.presto.plugin.jdbc;

import com.facebook.presto.spi.PrestoException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import static com.facebook.presto.plugin.jdbc.JdbcErrorCode.DRIVER_NOT_FOUND;
import static java.util.Objects.requireNonNull;

public class DriverManagerConnectionFactory
        implements ConnectionFactory
{
    private final String connectionUrl;
    private final Properties connectionProperties;

    public DriverManagerConnectionFactory(String driverName, BaseJdbcConfig config)
    {
        this(driverName, config.getConnectionUrl(), basicConnectionProperties(config));
    }

    public static Properties basicConnectionProperties(BaseJdbcConfig config)
    {
        Properties connectionProperties = new Properties();
        if (config.getConnectionUser() != null) {
            connectionProperties.setProperty("user", config.getConnectionUser());
        }
        if (config.getConnectionPassword() != null) {
            connectionProperties.setProperty("password", config.getConnectionPassword());
        }
        return connectionProperties;
    }

    public DriverManagerConnectionFactory(String driverName, String connectionUrl, Properties connectionProperties)
    {
        try {
            Class.forName(driverName);
        }
        catch (ClassNotFoundException e) {
            throw new PrestoException(DRIVER_NOT_FOUND, driverName + " not found");
        }

        this.connectionUrl = requireNonNull(connectionUrl, "connectionUrl is null");
        this.connectionProperties = new Properties();
        this.connectionProperties.putAll(requireNonNull(connectionProperties, "basicConnectionProperties is null"));
    }

    @Override
    public Connection openConnection()
            throws SQLException
    {
        return DriverManager.getConnection(connectionUrl, connectionProperties);
    }
}
