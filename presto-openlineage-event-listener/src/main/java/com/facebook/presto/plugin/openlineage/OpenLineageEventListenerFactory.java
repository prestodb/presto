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
package com.facebook.presto.plugin.openlineage;

import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.spi.eventlistener.EventListenerFactory;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClient;
import io.openlineage.client.transports.ConsoleTransport;
import io.openlineage.client.transports.HttpConfig;
import io.openlineage.client.transports.HttpSslContextConfig;
import io.openlineage.client.transports.HttpTransport;
import io.openlineage.client.transports.TokenProvider;
import io.openlineage.client.transports.Transport;

import java.net.URI;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class OpenLineageEventListenerFactory
        implements EventListenerFactory
{
    private static final URI PRODUCER_URI = URI.create("https://github.com/prestodb/presto/plugin/presto-openlineage-event-listener");

    @Override
    public String getName()
    {
        return "openlineage-event-listener";
    }

    @Override
    public EventListener create(Map<String, String> config)
    {
        requireNonNull(config, "config is null");

        OpenLineageEventListenerConfig listenerConfig = new OpenLineageEventListenerConfig(config);
        OpenLineageTransportConfig transportConfig = new OpenLineageTransportConfig(config);
        Transport transport = buildTransport(transportConfig, config);

        String[] disabledFacets = listenerConfig.getDisabledFacets().stream()
                .map(OpenLineagePrestoFacet::asText)
                .toArray(String[]::new);

        OpenLineageClient client = OpenLineageClient.builder()
                .transport(transport)
                .disableFacets(disabledFacets)
                .build();

        OpenLineage openLineage = new OpenLineage(PRODUCER_URI);
        return new OpenLineageEventListener(openLineage, client, listenerConfig);
    }

    private static Transport buildTransport(OpenLineageTransportConfig transportConfig, Map<String, String> config)
    {
        switch (transportConfig.getTransport()) {
            case CONSOLE:
                return new ConsoleTransport();
            case HTTP:
                return buildHttpTransport(new OpenLineageHttpTransportConfig(config));
            default:
                throw new IllegalArgumentException("Unsupported transport type: " + transportConfig.getTransport());
        }
    }

    private static HttpTransport buildHttpTransport(OpenLineageHttpTransportConfig config)
    {
        TokenProvider tokenProvider = config.getApiKey()
                .map(key -> (TokenProvider) () -> "Bearer " + key)
                .orElse(null);

        OpenLineageHttpTransportConfig.Compression configCompression = config.getCompression();
        HttpConfig.Compression httpCompression = configCompression == OpenLineageHttpTransportConfig.Compression.GZIP
                ? HttpConfig.Compression.GZIP
                : null;

        return new HttpTransport(
                new HttpConfig(
                        config.getUrl(),
                        config.getEndpoint(),
                        (int) config.getTimeoutMillis(),
                        tokenProvider,
                        config.getUrlParams(),
                        config.getHeaders(),
                        httpCompression,
                        new HttpSslContextConfig()));
    }
}
