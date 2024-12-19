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
package com.facebook.plugin.arrow;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigSecuritySensitive;

public class TestingArrowFlightConfig
{
    private String host;
    private String database;
    private String username;
    private String password;
    private String name;
    private Integer port;
    private Boolean ssl;

    public String getDataSourceHost()
    {
        return host;
    }

    public String getDataSourceDatabase()
    {
        return database;
    }

    public String getDataSourceUsername()
    {
        return username;
    }

    public String getDataSourcePassword()
    {
        return password;
    }

    public String getDataSourceName()
    {
        return name;
    }

    public Integer getDataSourcePort()
    {
        return port;
    }

    public Boolean getDataSourceSSL()
    {
        return ssl;
    }

    @Config("data-source.host")
    public TestingArrowFlightConfig setDataSourceHost(String host)
    {
        this.host = host;
        return this;
    }

    @Config("data-source.database")
    public TestingArrowFlightConfig setDataSourceDatabase(String database)
    {
        this.database = database;
        return this;
    }

    @Config("data-source.username")
    public TestingArrowFlightConfig setDataSourceUsername(String username)
    {
        this.username = username;
        return this;
    }

    @Config("data-source.password")
    @ConfigSecuritySensitive
    public TestingArrowFlightConfig setDataSourcePassword(String password)
    {
        this.password = password;
        return this;
    }

    @Config("data-source.name")
    public TestingArrowFlightConfig setDataSourceName(String name)
    {
        this.name = name;
        return this;
    }

    @Config("data-source.port")
    public TestingArrowFlightConfig setDataSourcePort(Integer port)
    {
        this.port = port;
        return this;
    }

    @Config("data-source.ssl")
    public TestingArrowFlightConfig setDataSourceSSL(Boolean ssl)
    {
        this.ssl = ssl;
        return this;
    }
}
