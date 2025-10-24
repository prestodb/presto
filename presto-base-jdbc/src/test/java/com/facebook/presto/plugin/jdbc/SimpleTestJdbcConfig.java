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

import com.facebook.airlift.configuration.Config;
import jakarta.validation.constraints.NotNull;

/**
 * Simple JDBC configuration that doesn't extend BaseJdbcConfig.
 * This demonstrates that JDBC connectors can implement their own
 * configuration mechanisms without depending on BaseJdbcConfig.
 */
public class SimpleTestJdbcConfig
{
    private String jdbcUrl;
    private String username;
    private String password;

    @NotNull
    public String getJdbcUrl()
    {
        return jdbcUrl;
    }

    @Config("jdbc-url")
    public SimpleTestJdbcConfig setJdbcUrl(String jdbcUrl)
    {
        this.jdbcUrl = jdbcUrl;
        return this;
    }

    public String getUsername()
    {
        return username;
    }

    @Config("username")
    public SimpleTestJdbcConfig setUsername(String username)
    {
        this.username = username;
        return this;
    }

    public String getPassword()
    {
        return password;
    }

    @Config("password")
    public SimpleTestJdbcConfig setPassword(String password)
    {
        this.password = password;
        return this;
    }
}
