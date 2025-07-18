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

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

/**
 * Default implementation of TableLocationProvider that uses the connection URL
 * from BaseJdbcConfig as the table location. If connection URL is not provided,
 * returns null to allow connectors to handle table location differently.
 */
public class DefaultTableLocationProvider
        implements TableLocationProvider
{
    private final String connectionUrl;

    @Inject
    public DefaultTableLocationProvider(BaseJdbcConfig baseJdbcConfig)
    {
        requireNonNull(baseJdbcConfig, "baseJdbcConfig is null");
        this.connectionUrl = baseJdbcConfig.getConnectionUrl();
    }

    @Override
    public String getTableLocation()
    {
        return connectionUrl; // Can be null if connection-url is not configured
    }
}
