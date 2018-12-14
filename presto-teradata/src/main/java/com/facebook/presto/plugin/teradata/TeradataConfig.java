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
package com.facebook.presto.plugin.teradata;

import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import io.airlift.configuration.Config;
import io.airlift.units.Duration;

import java.util.concurrent.TimeUnit;

/**
 * To get custom properties in order to connect to the database.
 * User, password and URL parameters are provided by BaseJdbcClient; and are not required.
 * If there is another custom configuration it should be put here.
 */
public class TeradataConfig
        extends BaseJdbcConfig
{
    public static final String DEFAULT_ROW_PRE_FETCH = "10000";

    private boolean includeSynonyms = true;
    private boolean usePreparedStatement = true;

    private Duration connectionTimeout = new Duration(10, TimeUnit.SECONDS);

    public String getDefaultRowPreFetch()
    {
        return defaultRowPreFetch;
    }

    @Config("teradata.defaultRowPrefetch")
    public void setDefaultRowPreFetch(String defaultRowPreFetch)
    {
        this.defaultRowPreFetch = defaultRowPreFetch;
    }

    private String defaultRowPreFetch = DEFAULT_ROW_PRE_FETCH;

    public Duration getConnectionTimeout()
    {
        return connectionTimeout;
    }

    @Config("teradata.connection-timeout")
    public TeradataConfig setConnectionTimeout(Duration connectionTimeout)
    {
        this.connectionTimeout = connectionTimeout;
        return this;
    }

    public boolean isIncludeSynonyms()
    {
        return includeSynonyms;
    }

    @Config("teradata.include-synonyms")
    public TeradataConfig setIncludeSynonyms(boolean includeSynonyms)
    {
        this.includeSynonyms = includeSynonyms;
        return this;
    }

    public boolean isUsePreparedStatement()
    {
        return usePreparedStatement;
    }

    @Config("teradata.use-preparedstatement")
    public TeradataConfig setUsePreparedStatement(boolean usePreparedStatement)
    {
        this.usePreparedStatement = usePreparedStatement;
        return this;
    }
}
