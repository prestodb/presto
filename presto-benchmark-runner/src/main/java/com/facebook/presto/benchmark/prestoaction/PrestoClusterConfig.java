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
package com.facebook.presto.benchmark.prestoaction;

import com.facebook.airlift.configuration.Config;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.NotNull;

import static java.util.concurrent.TimeUnit.MINUTES;

public class PrestoClusterConfig
{
    private String jdbcUrl;
    private Duration queryTimeout = new Duration(60, MINUTES);

    @NotNull
    public String getJdbcUrl()
    {
        return jdbcUrl;
    }

    @Config("jdbc-url")
    public PrestoClusterConfig setJdbcUrl(String jdbcUrl)
    {
        this.jdbcUrl = jdbcUrl;
        return this;
    }

    @MinDuration("1s")
    public Duration getQueryTimeout()
    {
        return queryTimeout;
    }

    @Config("query-timeout")
    public PrestoClusterConfig setQueryTimeout(Duration queryTimeout)
    {
        this.queryTimeout = queryTimeout;
        return this;
    }
}
