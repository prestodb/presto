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
    private String host; // non-static field
    private String database; // non-static field
    private String username; // non-static field
    private String password; // non-static field
    private String name; // non-static field
    private Integer port; // non-static field
    private Boolean ssl;

    public String getDataSourceHost()
    { // non-static getter
        return host;
    }

    public String getDataSourceDatabase()
    { // non-static getter
        return database;
    }

    public String getDataSourceUsername()
    { // non-static getter
        return username;
    }

    public String getDataSourcePassword()
    { // non-static getter
        return password;
    }

    public String getDataSourceName()
    { // non-static getter
        return name;
    }

    public Integer getDataSourcePort()
    { // non-static getter
        return port;
    }

    public Boolean getDataSourceSSL()
    { // non-static getter
        return ssl;
    }

    @Config("data-source.host")
    public TestingArrowFlightConfig setDataSourceHost(String host)
    { // non-static setter
        this.host = host;
        return this;
    }

    @Config("data-source.database")
    public TestingArrowFlightConfig setDataSourceDatabase(String database)
    { // non-static setter
        this.database = database;
        return this;
    }

    @Config("data-source.username")
    public TestingArrowFlightConfig setDataSourceUsername(String username)
    { // non-static setter
        this.username = username;
        return this;
    }

    @Config("data-source.password")
    @ConfigSecuritySensitive
    public TestingArrowFlightConfig setDataSourcePassword(String password)
    { // non-static setter
        this.password = password;
        return this;
    }

    @Config("data-source.name")
    public TestingArrowFlightConfig setDataSourceName(String name)
    { // non-static setter
        this.name = name;
        return this;
    }

    @Config("data-source.port")
    public TestingArrowFlightConfig setDataSourcePort(Integer port)
    { // non-static setter
        this.port = port;
        return this;
    }

    @Config("data-source.ssl")
    public TestingArrowFlightConfig setDataSourceSSL(Boolean ssl)
    { // non-static setter
        this.ssl = ssl;
        return this;
    }
}
