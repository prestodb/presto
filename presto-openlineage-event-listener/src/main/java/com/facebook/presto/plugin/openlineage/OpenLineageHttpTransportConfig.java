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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class OpenLineageHttpTransportConfig
{
    public enum Compression
    {
        NONE, GZIP
    }

    private URI url;
    private String endpoint;
    private Optional<String> apiKey = Optional.empty();
    private long timeoutMillis = TimeUnit.SECONDS.toMillis(5);
    private Map<String, String> headers = new HashMap<>();
    private Map<String, String> urlParams = new HashMap<>();
    private Compression compression = Compression.NONE;

    public OpenLineageHttpTransportConfig()
    {
    }

    public OpenLineageHttpTransportConfig(Map<String, String> config)
    {
        requireNonNull(config, "config is null");
        String urlStr = config.get("openlineage-event-listener.transport.url");
        if (urlStr != null) {
            try {
                this.url = new URI(urlStr);
            }
            catch (URISyntaxException e) {
                throw new IllegalArgumentException("Invalid transport URL: " + urlStr, e);
            }
        }
        this.endpoint = config.get("openlineage-event-listener.transport.endpoint");
        String apiKeyStr = config.get("openlineage-event-listener.transport.api-key");
        this.apiKey = Optional.ofNullable(apiKeyStr);

        String timeoutStr = config.get("openlineage-event-listener.transport.timeout");
        if (timeoutStr != null) {
            this.timeoutMillis = parseDurationToMillis(timeoutStr);
        }

        String headersStr = config.get("openlineage-event-listener.transport.headers");
        if (headersStr != null && !headersStr.isEmpty()) {
            this.headers = parseKeyValuePairs(headersStr, "headers");
        }

        String urlParamsStr = config.get("openlineage-event-listener.transport.url-params");
        if (urlParamsStr != null && !urlParamsStr.isEmpty()) {
            this.urlParams = parseKeyValuePairs(urlParamsStr, "url-params");
        }

        String compressionStr = config.get("openlineage-event-listener.transport.compression");
        if (compressionStr != null) {
            this.compression = Compression.valueOf(compressionStr.toUpperCase());
        }
    }

    public URI getUrl()
    {
        return url;
    }

    public OpenLineageHttpTransportConfig setUrl(URI url)
    {
        this.url = url;
        return this;
    }

    public String getEndpoint()
    {
        return endpoint;
    }

    public OpenLineageHttpTransportConfig setEndpoint(String endpoint)
    {
        this.endpoint = endpoint;
        return this;
    }

    public Optional<String> getApiKey()
    {
        return apiKey;
    }

    public OpenLineageHttpTransportConfig setApiKey(String apiKey)
    {
        this.apiKey = Optional.ofNullable(apiKey);
        return this;
    }

    public long getTimeoutMillis()
    {
        return timeoutMillis;
    }

    public OpenLineageHttpTransportConfig setTimeoutMillis(long timeoutMillis)
    {
        this.timeoutMillis = timeoutMillis;
        return this;
    }

    public Map<String, String> getHeaders()
    {
        return headers;
    }

    public OpenLineageHttpTransportConfig setHeaders(Map<String, String> headers)
    {
        this.headers = headers;
        return this;
    }

    public Map<String, String> getUrlParams()
    {
        return urlParams;
    }

    public OpenLineageHttpTransportConfig setUrlParams(Map<String, String> urlParams)
    {
        this.urlParams = urlParams;
        return this;
    }

    public Compression getCompression()
    {
        return compression;
    }

    public OpenLineageHttpTransportConfig setCompression(Compression compression)
    {
        this.compression = compression;
        return this;
    }

    private static Map<String, String> parseKeyValuePairs(String input, String propertyName)
    {
        Map<String, String> result = new HashMap<>();
        String[] pairs = input.split(",");
        for (String pair : pairs) {
            String[] parts = pair.split(":", 2);
            if (parts.length != 2) {
                throw new IllegalArgumentException(format(
                        "Cannot parse %s from property; value provided was %s, " +
                                "expected format is \"key1:value1,key2:value2,...\"",
                        propertyName, input));
            }
            result.put(parts[0].trim(), parts[1].trim());
        }
        return result;
    }

    private static long parseDurationToMillis(String duration)
    {
        duration = duration.trim().toLowerCase();
        if (duration.endsWith("ms")) {
            return Long.parseLong(duration.substring(0, duration.length() - 2));
        }
        else if (duration.endsWith("s")) {
            return TimeUnit.SECONDS.toMillis(Long.parseLong(duration.substring(0, duration.length() - 1)));
        }
        else if (duration.endsWith("m")) {
            return TimeUnit.MINUTES.toMillis(Long.parseLong(duration.substring(0, duration.length() - 1)));
        }
        else if (duration.endsWith("h")) {
            return TimeUnit.HOURS.toMillis(Long.parseLong(duration.substring(0, duration.length() - 1)));
        }
        return Long.parseLong(duration);
    }
}
