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
package com.facebook.presto.plugin.oracle;

import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import io.airlift.configuration.Config;
import io.airlift.units.Duration;

import javax.validation.constraints.Min;

import java.util.concurrent.TimeUnit;

public class OracleConfig
            extends BaseJdbcConfig
{
    public static final String DEFAULT_ROW_PRE_FETCH = "10000";

    private boolean includeSynonyms = true;
    private boolean autoReconnect = true;
    private int maxReconnects = 3;

    private Duration connectionTimeout = new Duration(10, TimeUnit.SECONDS);

    public String getDefaultRowPreFetch()
    {
        return defaultRowPreFetch;
    }

    @Config("oracle.defaultRowPrefetch")
    public void setDefaultRowPreFetch(String defaultRowPreFetch)
    {
        this.defaultRowPreFetch = defaultRowPreFetch;
    }

    private String defaultRowPreFetch = DEFAULT_ROW_PRE_FETCH;

    public Duration getConnectionTimeout()
    {
        return connectionTimeout;
    }

    @Config("oracle.connection-timeout")
    public OracleConfig setConnectionTimeout(Duration connectionTimeout)
    {
        this.connectionTimeout = connectionTimeout;
        return this;
    }

    public boolean isIncludeSynonyms()
    {
        return includeSynonyms;
    }

    @Config("oracle.include-synonyms")
    public OracleConfig setIncludeSynonyms(boolean includeSynonyms)
    {
        this.includeSynonyms = includeSynonyms;
        return this;
    }

    public boolean isAutoReconnect()
    {
        return autoReconnect;
    }

    @Config("oracle.auto-reconnect")
    public OracleConfig setAutoReconnect(boolean autoReconnect)
    {
        this.autoReconnect = autoReconnect;
        return this;
    }

    @Min(1)
    public int getMaxReconnects()
    {
        return maxReconnects;
    }

    @Config("oracle.max-reconnects")
    public OracleConfig setMaxReconnects(int maxReconnects)
    {
        this.maxReconnects = maxReconnects;
        return this;
    }
}
