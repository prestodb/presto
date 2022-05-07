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

import ru.yandex.clickhouse.ClickHouseDriver;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class DriverConnectionFactory
        implements ConnectionFactory
{
    private final ClickHouseDriver driver;
    private final String connectionUrl;
    private final Properties connectionProperties;
    private final Optional<String> userCredential;
    private final Optional<String> passwordCredential;

    public DriverConnectionFactory(ClickHouseDriver driver, ClickHouseConfig config)
    {
        this(
                driver,
                config.getConnectionUrl(),
                Optional.ofNullable(config.getUserCredential()),
                Optional.ofNullable(config.getPasswordCredential()),
                basicConnectionProperties(config));
    }

    public static Properties basicConnectionProperties(ClickHouseConfig config)
    {
        Properties connectionProperties = new Properties();
        if (config.getConnectionUser() != null) {
            connectionProperties.setProperty("user", config.getConnectionUser());
        }
        if (config.getConnectionPassword() != null) {
            connectionProperties.setProperty("password", config.getConnectionPassword());
        }
        connectionProperties.setProperty("useInformationSchema", "true");
        connectionProperties.setProperty("nullCatalogMeansCurrent", "false");
        connectionProperties.setProperty("useUnicode", "true");
        connectionProperties.setProperty("characterEncoding", "utf8");
        connectionProperties.setProperty("tinyInt1isBit", "false");
        return connectionProperties;
    }

    public DriverConnectionFactory(ClickHouseDriver driver, String connectionUrl, Optional<String> userCredential, Optional<String> passwordCredential, Properties connectionProperties)
    {
        this.driver = requireNonNull(driver, "clickHouseDriver is null");
        this.connectionUrl = requireNonNull(connectionUrl, "connectionUrl is null");
        this.connectionProperties = new Properties();
        this.connectionProperties.putAll(requireNonNull(connectionProperties, "connectionProperties is null"));
        this.userCredential = requireNonNull(userCredential, "userCredentialName is null");
        this.passwordCredential = requireNonNull(passwordCredential, "passwordCredentialName is null");
    }

    @Override
    public Connection openConnection(ClickHouseIdentity identity)
            throws SQLException
    {
        Properties updatedConnectionProperties;
        if (userCredential.isPresent() || passwordCredential.isPresent()) {
            updatedConnectionProperties = new Properties();
            updatedConnectionProperties.putAll(connectionProperties);
            userCredential.ifPresent(credentialName -> setConnectionProperty(updatedConnectionProperties, identity.getExtraCredentials(), credentialName, "user"));
            passwordCredential.ifPresent(credentialName -> setConnectionProperty(updatedConnectionProperties, identity.getExtraCredentials(), credentialName, "password"));
        }
        else {
            updatedConnectionProperties = connectionProperties;
        }

        Connection connection = driver.connect(connectionUrl, updatedConnectionProperties);
        checkState(connection != null, "ClickHouseDriver returned null connection");
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
