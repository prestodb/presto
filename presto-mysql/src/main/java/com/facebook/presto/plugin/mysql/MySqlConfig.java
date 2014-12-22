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
package com.facebook.presto.plugin.mysql;

import java.util.concurrent.TimeUnit;

import io.airlift.configuration.Config;
import io.airlift.units.Duration;

import javax.validation.constraints.Min;

public class MySqlConfig
{
    private boolean connectionAutoReconnect = true;
    private int connectionMaxReconnects = 3;
    private Duration connectionTimeout = new Duration(3, TimeUnit.SECONDS);

    public boolean isConnectionAutoReconnect()
    {
        return connectionAutoReconnect;
    }

    @Config("mysql-connection-auto-reconnect")
    public MySqlConfig setConnectionAutoReconnect(boolean connectionAutoReconnect)
    {
        this.connectionAutoReconnect = connectionAutoReconnect;
        return this;
    }

    @Min(1)
    public int getConnectionMaxReconnects()
    {
        return connectionMaxReconnects;
    }

    @Config("mysql-connection-max-reconnects")
    public MySqlConfig setConnectionMaxReconnects(int connectionMaxReconnects)
    {
        this.connectionMaxReconnects = connectionMaxReconnects;
        return this;
    }

    public Duration getConnectionTimeout()
    {
        return connectionTimeout;
    }

    @Config("mysql-connection-timeout")
    public MySqlConfig setConnectionTimeout(Duration connectionTimeout)
    {
        this.connectionTimeout = connectionTimeout;
        return this;
    }
}
