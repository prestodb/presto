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

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class DriverConnectionFactory
        implements ConnectionFactory
{
    private final Driver driver;
    private final String connectionUrl;
    private final Properties connectionProperties;
    private final Optional<String> userCredentialName;
    private final Optional<String> passwordCredentialName;

    public DriverConnectionFactory(Driver driver, BaseJdbcConfig config)
    {
        this(
                driver,
                config.getConnectionUrl(),
                Optional.ofNullable(config.getUserCredentialName()),
                Optional.ofNullable(config.getPasswordCredentialName()),
                basicConnectionProperties(config));
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

    public DriverConnectionFactory(Driver driver, String connectionUrl, Optional<String> userCredentialName, Optional<String> passwordCredentialName, Properties connectionProperties)
    {
        this.driver = requireNonNull(driver, "driver is null");
        this.connectionUrl = requireNonNull(connectionUrl, "connectionUrl is null");
        this.connectionProperties = new Properties();
        this.connectionProperties.putAll(requireNonNull(connectionProperties, "connectionProperties is null"));
        this.userCredentialName = requireNonNull(userCredentialName, "userCredentialName is null");
        this.passwordCredentialName = requireNonNull(passwordCredentialName, "passwordCredentialName is null");
    }

    @Override
    public Connection openConnection(JdbcIdentity identity)
            throws SQLException
    {
        Properties updatedConnectionProperties;
        if (userCredentialName.isPresent() || passwordCredentialName.isPresent()) {
            updatedConnectionProperties = new Properties();
            updatedConnectionProperties.putAll(connectionProperties);
            userCredentialName.ifPresent(credentialName -> setConnectionProperty(updatedConnectionProperties, identity.getExtraCredentials(), credentialName, "user"));
            passwordCredentialName.ifPresent(credentialName -> setConnectionProperty(updatedConnectionProperties, identity.getExtraCredentials(), credentialName, "password"));
        }
        else {
            updatedConnectionProperties = connectionProperties;
        }

        Connection connection = driver.connect(connectionUrl, updatedConnectionProperties);
        checkState(connection != null, "Driver returned null connection");
        return connection;
    }

    private static void setConnectionProperty(Properties connectionProperties, Map<String, String> extraCredentials, String credentialName, String propertyName)
    {
        String value = extraCredentials.get(credentialName);
        if (value != null) {
            connectionProperties.setProperty(propertyName, value);
        }
    }
}
