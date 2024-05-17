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
package com.facebook.presto.iceberg.jdbc;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;

import java.util.Optional;

public class JdbcConfig
{
    private String serverUrl;
    private String user;
    private String password;
    private String driverClass;

    @Config("iceberg.jdbc.connection-url")
    @ConfigDescription("The URL to connect to the database server")
    public JdbcConfig setServerUrl(String serverUrl)
    {
        this.serverUrl = serverUrl;
        return this;
    }

    public Optional<String> getServerUrl()
    {
        return Optional.ofNullable(serverUrl);
    }

    @Config("iceberg.jdbc.user")
    @ConfigDescription("The username to connect to the database server")
    public JdbcConfig setUser(String user)
    {
        this.user = user;
        return this;
    }

    public Optional<String> getUser()
    {
        return Optional.ofNullable(user);
    }

    @Config("iceberg.jdbc.password")
    @ConfigDescription("The password to connect to the database server")
    public JdbcConfig setPassword(String password)
    {
        this.password = password;
        return this;
    }

    public Optional<String> getPassword()
    {
        return Optional.ofNullable(password);
    }

    @Config("iceberg.jdbc.driver-class")
    @ConfigDescription("The driver-class to connect to the database server")
    public JdbcConfig setDriverClass(String driverClass)
    {
        this.driverClass = driverClass;
        return this;
    }

    public Optional<String> getDriverClass()
    {
        return Optional.ofNullable(driverClass);
    }
}
