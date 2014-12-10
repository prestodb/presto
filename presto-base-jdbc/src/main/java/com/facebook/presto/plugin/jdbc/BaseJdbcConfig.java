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
import javax.validation.constraints.Min;

public class BaseJdbcConfig
{
    private String connectionUrl;
    private String connectionUser;
    private String connectionPassword;
    private boolean connectionAutoReconnect = true;
    private int connectionMaxReconnects = 3;
    private Duration connectionTimeout = new Duration(3, TimeUnit.SECONDS);

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

    public boolean isConnectionAutoReconnect()
    {
        return connectionAutoReconnect;
    }

    @Config("connection-autoReconnect")
    public BaseJdbcConfig setConnectionAutoReconnect(boolean connectionAutoReconnect)
    {
        this.connectionAutoReconnect = connectionAutoReconnect;
        return this;
    }

    @Min(1)
    public int getConnectionMaxReconnects()
    {
        return connectionMaxReconnects;
    }

    @Config("connection-maxReconnects")
    public BaseJdbcConfig setConnectionMaxReconnects(int connectionMaxReconnects)
    {
        this.connectionMaxReconnects = connectionMaxReconnects;
        return this;
    }

    public Duration getConnectionTimeout()
    {
        return connectionTimeout;
    }

    @Config("connection-timeout")
    public BaseJdbcConfig setConnectionTimeout(Duration connectionTimeout)
    {
        this.connectionTimeout = connectionTimeout;
        return this;
    }
}
