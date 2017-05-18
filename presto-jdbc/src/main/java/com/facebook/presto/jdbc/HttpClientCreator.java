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
package com.facebook.presto.jdbc;

import com.google.common.collect.ImmutableSet;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.HttpRequestFilter;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.http.client.jetty.JettyIoPool;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;

import static java.util.Objects.requireNonNull;

public class HttpClientCreator
{
    private final String userAgent;
    private final JettyIoPool jettyIoPool;
    private final Map<String, BiConsumer<HttpClientConfig, String>> configSetters;

    public HttpClientCreator(
            String userAgent,
            JettyIoPool jettyIoPool,
            Map<String, BiConsumer<HttpClientConfig, String>> configSetters)
    {
        this.userAgent = requireNonNull(userAgent, "userAgent is null");
        this.jettyIoPool = requireNonNull(jettyIoPool, "jettyIoPool is null");
        this.configSetters = requireNonNull(configSetters, "configSetters is null");
    }

    private Set<? extends HttpRequestFilter> getFilters()
    {
        return ImmutableSet.of(new UserAgentRequestFilter(userAgent));
    }

    private HttpClientConfig getClientConfig(HttpClientConfig config)
    {
        for (Map.Entry<String, BiConsumer<HttpClientConfig, String>> entry : configSetters.entrySet()) {
            entry.getValue().accept(config, entry.getKey());
        }
        return config;
    }

    public HttpClient create(HttpClientConfig config)
    {
        return new JettyHttpClient(getClientConfig(config), jettyIoPool, getFilters());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(userAgent, jettyIoPool, configSetters.keySet());
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this) {
            return true;
        }

        if (obj == null || !obj.getClass().equals(this.getClass())) {
            return false;
        }

        HttpClientCreator other = (HttpClientCreator) obj;

        return Objects.equals(this.userAgent, other.userAgent) &&
                Objects.equals(jettyIoPool, other.jettyIoPool) &&
                Objects.equals(this.configSetters.keySet(), other.configSetters.keySet());
    }
}
