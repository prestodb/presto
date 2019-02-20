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
package com.facebook.presto.password.jdbc;

import com.google.common.base.Preconditions;
import io.airlift.configuration.Config;
import io.airlift.units.Duration;
import org.apache.commons.lang3.StringUtils;

import java.util.concurrent.TimeUnit;

import static java.util.Locale.ENGLISH;

public class JdbcConfig
{
    private Duration jdbcAuthCacheTtl = new Duration(1, TimeUnit.HOURS);

    private ConnectionType jdbcAuthConnectionType;
    private String jdbcAuthUrl;
    private String jdbcAuthUser;
    private String jdbcAuthPassword;
    private String jdbcAuthSchema;
    private String jdbcAuthTable;
    private int poolInitialSize = 1;
    private int poolMinIdle = 5;
    private int poolMaxIdle = 10;
    private int maxOpenPreparedStatements = 100;

    public Duration getJdbcAuthCacheTtl()
    {
        return jdbcAuthCacheTtl;
    }

    public enum ConnectionType
    {
        postgresql, mysql;
    }

    @Config("jdbc.auth.cache-ttl")
    public JdbcConfig setJdbcAuthCacheTtl(Duration jdbcAuthCacheTtl)
    {
        this.jdbcAuthCacheTtl = jdbcAuthCacheTtl;
        return this;
    }

    public String getJdbcAuthUrl()
    {
        return jdbcAuthUrl;
    }

    @Config("jdbc.auth.url")
    public JdbcConfig setJdbcAuthUrl(String jdbcAuthUrl)
    {
        this.jdbcAuthUrl = jdbcAuthUrl;
        return this;
    }

    public String getJdbcAuthUser()
    {
        return jdbcAuthUser;
    }

    @Config("jdbc.auth.user")
    public JdbcConfig setJdbcAuthUser(String jdbcAuthUser)
    {
        this.jdbcAuthUser = jdbcAuthUser;
        return this;
    }

    public String getJdbcAuthPassword()
    {
        return jdbcAuthPassword;
    }

    @Config("jdbc.auth.password")
    public JdbcConfig setJdbcAuthPassword(String jdbcAuthPassword)
    {
        this.jdbcAuthPassword = jdbcAuthPassword;
        return this;
    }

    public ConnectionType getJdbcAuthConnectionType()
    {
        return jdbcAuthConnectionType;
    }

    @Config("jdbc.auth.type")
    public JdbcConfig setJdbcAuthConnectionType(String jdbcAuthConnectionType)
    {
        Preconditions.checkArgument(StringUtils.isNotEmpty(jdbcAuthConnectionType), "jdbcAuthConnectionType is a mandatory parameter");
        this.jdbcAuthConnectionType = ConnectionType.valueOf(jdbcAuthConnectionType.toLowerCase(ENGLISH));
        return this;
    }

    public String getJdbcAuthSchema()
    {
        return jdbcAuthSchema;
    }

    @Config("jdbc.auth.schema")
    public JdbcConfig setJdbcAuthSchema(String jdbcAuthSchema)
    {
        this.jdbcAuthSchema = jdbcAuthSchema;
        return this;
    }

    public String getJdbcAuthTable()
    {
        return jdbcAuthTable;
    }

    @Config("jdbc.auth.table")
    public JdbcConfig setJdbcAuthTable(String jdbcAuthTable)
    {
        this.jdbcAuthTable = jdbcAuthTable;
        return this;
    }

    public int getMaxOpenPreparedStatements()
    {
        return maxOpenPreparedStatements;
    }

    @Config("jdbc.max.open.statements")
    public JdbcConfig setMaxOpenPreparedStatements(int maxOpenPreparedStatements)
    {
        this.maxOpenPreparedStatements = maxOpenPreparedStatements;
        return this;
    }

    public int getPoolInitialSize()
    {
        return poolInitialSize;
    }

    @Config("jdbc.pool.initial.size")
    public JdbcConfig setPoolInitialSize(int poolInitialSize)
    {
        this.poolInitialSize = poolInitialSize;
        return this;
    }

    public int getPoolMinIdle()
    {
        return poolMinIdle;
    }

    @Config("jdbc.pool.min.idle")
    public JdbcConfig setPoolMinIdle(int poolMinIdle)
    {
        this.poolMinIdle = poolMinIdle;
        return this;
    }

    public int getPoolMaxIdle()
    {
        return poolMaxIdle;
    }

    @Config("jdbc.pool.max.idle")
    public JdbcConfig setPoolMaxIdle(int poolMaxIdle)
    {
        this.poolMaxIdle = poolMaxIdle;
        return this;
    }
}
