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
package com.facebook.presto.verifier.prestoaction;

import com.facebook.airlift.configuration.Config;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import static java.util.concurrent.TimeUnit.MINUTES;

public class PrestoActionConfig
        implements PrestoAddress
{
    private String host;
    private int jdbcPort;
    private Optional<Integer> httpPort = Optional.empty();
    private Map<String, String> jdbcUrlParameters = ImmutableMap.of();
    private Duration queryTimeout = new Duration(60, MINUTES);

    @Override
    @NotNull
    public String getHost()
    {
        return host;
    }

    @Config("host")
    public PrestoActionConfig setHost(String host)
    {
        this.host = host;
        return this;
    }

    @Override
    @Min(0)
    @Max(65535)
    public int getJdbcPort()
    {
        return jdbcPort;
    }

    @Config("jdbc-port")
    public PrestoActionConfig setJdbcPort(int jdbcPort)
    {
        this.jdbcPort = jdbcPort;
        return this;
    }

    @Override
    @NotNull
    public Optional<Integer> getHttpPort()
    {
        return httpPort;
    }

    @Config("http-port")
    public PrestoActionConfig setHttpPort(Integer httpPort)
    {
        this.httpPort = Optional.ofNullable(httpPort);
        return this;
    }

    @Override
    @NotNull
    public Map<String, String> getJdbcUrlParameters()
    {
        return jdbcUrlParameters;
    }

    @Config("jdbc-url-parameters")
    public PrestoActionConfig setJdbcUrlParameters(String jdbcUrlParameters)
    {
        if (jdbcUrlParameters == null) {
            return this;
        }

        try {
            this.jdbcUrlParameters = new ObjectMapper().readValue(jdbcUrlParameters, new TypeReference<Map<String, String>>() {});
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        return this;
    }

    @MinDuration("1s")
    public Duration getQueryTimeout()
    {
        return queryTimeout;
    }

    @Config("query-timeout")
    public PrestoActionConfig setQueryTimeout(Duration queryTimeout)
    {
        this.queryTimeout = queryTimeout;
        return this;
    }
}
