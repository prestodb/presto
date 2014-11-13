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

import java.util.concurrent.TimeUnit;

import io.airlift.configuration.Config;
import io.airlift.units.Duration;

import javax.validation.constraints.NotNull;

public class BaseJdbcConfig
{
    private String connectionUrl;
    private String connectionUser;
    private String connectionPassword;
    public static final String DEFAULT_VALUE = "NA";
    private Duration jdbcReloadSubtableInterval = new Duration(5, TimeUnit.MINUTES);
    private String jdbcSubTableConnectionDB = DEFAULT_VALUE;
    private String jdbcSubTableConnectionTable = DEFAULT_VALUE;
    private boolean jdbcSubTableAllocator = false;
    private boolean jdbcSubTableEnable = false;

    @NotNull
    public String getConnectionUrl()
    {
        return connectionUrl;
    }

    @Config("connection-url")
    public BaseJdbcConfig setConnectionUrl(String connectionUrl)
    {
        this.connectionUrl = connectionUrl;
        return this;
    }

    public String getConnectionUser()
    {
        return connectionUser;
    }

    @Config("connection-user")
    public BaseJdbcConfig setConnectionUser(String connectionUser)
    {
        this.connectionUser = connectionUser;
        return this;
    }

    public String getConnectionPassword()
    {
        return connectionPassword;
    }

    @Config("connection-password")
    public BaseJdbcConfig setConnectionPassword(String connectionPassword)
    {
        this.connectionPassword = connectionPassword;
        return this;
    }

    public Duration getJdbcReloadSubtableInterval()
    {
        return jdbcReloadSubtableInterval;
    }

    @Config("jdbc-reload-subtable-interval")
    public BaseJdbcConfig setJdbcReloadSubtableInterval(Duration jdbcReloadSubtableInterval)
    {
        this.jdbcReloadSubtableInterval = jdbcReloadSubtableInterval;
        return this;
    }

    public String getJdbcSubTableConnectionDB()
    {
        return jdbcSubTableConnectionDB;
    }

    @Config("jdbc-sub-table-connection-db")
    public BaseJdbcConfig setJdbcSubTableConnectionDB(String jdbcSubTableConnectionDB)
    {
        this.jdbcSubTableConnectionDB = jdbcSubTableConnectionDB;
        return this;
    }

    public String getJdbcSubTableConnectionTable()
    {
        return jdbcSubTableConnectionTable;
    }

    @Config("jdbc-sub-table-connection-table")
    public BaseJdbcConfig setJdbcSubTableConnectionTable(String jdbcSubTableConnectionTable)
    {
        this.jdbcSubTableConnectionTable = jdbcSubTableConnectionTable;
        return this;
    }

    public boolean getJdbcSubTableAllocator()
    {
        return jdbcSubTableAllocator;
    }

    @Config("jdbc-sub-table-allocator-enable")
    public BaseJdbcConfig setJdbcSubTableAllocator(boolean allocator)
    {
        this.jdbcSubTableAllocator = allocator;
        return this;
    }

    public boolean getJdbcSubTableEnable()
    {
        return jdbcSubTableEnable;
    }

    @Config("jdbc-sub-table-enable")
    public BaseJdbcConfig setJdbcSubTableEnable(boolean jdbcSubTableEnable)
    {
        this.jdbcSubTableEnable = jdbcSubTableEnable;
        return this;
    }
}
