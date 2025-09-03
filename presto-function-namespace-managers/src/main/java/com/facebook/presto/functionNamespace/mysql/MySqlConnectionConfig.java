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
package com.facebook.presto.functionNamespace.mysql;

import com.facebook.airlift.configuration.Config;
import jakarta.validation.constraints.NotNull;

public class MySqlConnectionConfig
{
    private String jdbcDriverName = "com.mysql.jdbc.Driver";

    private String databaseUrl;

    @NotNull
    public String getDatabaseUrl()
    {
        return databaseUrl;
    }

    @Config("database-url")
    public MySqlConnectionConfig setDatabaseUrl(String databaseUrl)
    {
        this.databaseUrl = databaseUrl;
        return this;
    }

    public String getJdbcDriverName()
    {
        return jdbcDriverName;
    }

    @Config("database-driver-name")
    public MySqlConnectionConfig setJdbcDriverName(String jdbcDriverName)
    {
        this.jdbcDriverName = jdbcDriverName;
        return this;
    }
}
