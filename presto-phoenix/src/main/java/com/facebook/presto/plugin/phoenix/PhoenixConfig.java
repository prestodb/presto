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
package com.facebook.presto.plugin.phoenix;

import io.airlift.configuration.Config;

import javax.validation.constraints.NotNull;

import java.util.Properties;

public class PhoenixConfig
{
    private Properties connectionProperties = new Properties();
    private String connectionUrl;

    @NotNull
    public String getConnectionUrl()
    {
        return connectionUrl;
    }

    @Config("connection-url")
    public PhoenixConfig setConnectionUrl(String connectionUrl)
    {
        this.connectionUrl = connectionUrl;
        return this;
    }

    @NotNull
    public Properties getConnectionProperties()
    {
        return connectionProperties;
    }

    @Config("connection-properties")
    public PhoenixConfig setConnectionProperties(String properties)
    {
        final String[] entries = properties.split(";");
        for (int i = 0; i < entries.length; ++i) {
            final String entry = entries[i];
            if (entry.length() > 0) {
                final int index = entry.indexOf('=');
                if (index > 0) {
                    final String name = entry.substring(0, index);
                    final String value = entry.substring(index + 1);
                    connectionProperties.setProperty(name, value);
                }
                if (index <= 0) {
                    connectionProperties.setProperty(entry, "");
                }
            }
        }
        return this;
    }
}
