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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.jetty.JettyIoPool;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.BiConsumer;

import static com.facebook.presto.jdbc.ConnectionProperties.SSL;
import static com.facebook.presto.jdbc.ConnectionProperties.SSL_ENABLED;
import static com.facebook.presto.jdbc.ConnectionProperties.SSL_TRUST_STORE_PASSWORD;
import static com.facebook.presto.jdbc.ConnectionProperties.SSL_TRUST_STORE_PATH;
import static com.facebook.presto.jdbc.ConnectionProperties.SSL_TRUST_STORE_PWD;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.airlift.http.client.HttpUriBuilder.uriBuilder;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Parses and extracts parameters from a Presto JDBC URL.
 */
final class PrestoConnectionConfig
{
    private static final String JDBC_URL_START = "jdbc:";

    private static final Splitter QUERY_SPLITTER = Splitter.on('&').omitEmptyStrings();
    private static final Splitter ARG_SPLITTER = Splitter.on('=').limit(2);

    private final HostAndPort address;
    private final URI uri;
    private final Properties connectionProperties;

    private String catalog;
    private String schema;

    private final boolean useSecureConnection;

    private static final Map<ConnectionProperty, BiConsumer<HttpClientConfig, String>> CLIENT_SETTERS = ImmutableMap.of(
            SSL_TRUST_STORE_PATH, HttpClientConfig::setTrustStorePath,
            SSL_TRUST_STORE_PASSWORD, HttpClientConfig::setTrustStorePassword,
            SSL_TRUST_STORE_PWD, HttpClientConfig::setTrustStorePassword);

    public PrestoConnectionConfig(String url, Properties driverProperties)
            throws SQLException
    {
        this(parseDriverUrl(url), driverProperties);
    }

    private PrestoConnectionConfig(URI uri, Properties driverProperties)
            throws SQLException
    {
        this.uri = requireNonNull(uri, "uri is null");
        address = HostAndPort.fromParts(uri.getHost(), uri.getPort());
        connectionProperties = mergeConnectionProperties(uri, driverProperties);

        validateConnectionProperties(connectionProperties);

        useSecureConnection = SSL.getValue(connectionProperties).get() == SSL_ENABLED;

        initCatalogAndSchema();
    }

    public URI getJdbcUri()
    {
        return uri;
    }

    public String getSchema()
    {
        return schema;
    }

    public String getCatalog()
    {
        return catalog;
    }

    public URI getHttpUri()
    {
        return buildHttpUri();
    }

    public Properties getConnectionProperties()
    {
        return connectionProperties;
    }

    public HttpClientCreator getCreator(String userAgent, JettyIoPool jettyIoPool)
            throws SQLException
    {
        return new HttpClientCreator(userAgent, jettyIoPool, getConfigSetters());
    }

    private Map<String, BiConsumer<HttpClientConfig, String>> getConfigSetters()
            throws SQLException
    {
        ImmutableMap.Builder<String, BiConsumer<HttpClientConfig, String>> result = ImmutableMap.builder();
        for (String key : connectionProperties.stringPropertyNames()) {
            ConnectionProperty property = ConnectionProperties.forKey(key);
            String value = connectionProperties.getProperty(key);

            if (property.isRequired(connectionProperties) && value == null) {
                throw new SQLException(format("Missing required property %s", key));
            }

            BiConsumer<HttpClientConfig, String> setter = CLIENT_SETTERS.get(property);
            if (setter != null) {
                result.put(value, setter);
            }
        }

        return result.build();
    }

    private static Map<String, String> parseParameters(String query)
    {
        Map<String, String> result = new HashMap<>();

        if (query != null) {
            Iterable<String> queryArgs = QUERY_SPLITTER.split(query);
            for (String queryArg : queryArgs) {
                List<String> parts = ARG_SPLITTER.splitToList(queryArg);
                result.put(parts.get(0), parts.get(1));
            }
        }

        return result;
    }

    private static URI parseDriverUrl(String url)
            throws SQLException
    {
        URI uri;
        try {
            uri = new URI(url.substring(JDBC_URL_START.length()));
        }
        catch (URISyntaxException e) {
            throw new SQLException("Invalid JDBC URL: " + url, e);
        }
        if (isNullOrEmpty(uri.getHost())) {
            throw new SQLException("No host specified: " + url);
        }
        if (uri.getPort() == -1) {
            throw new SQLException("No port number specified: " + url);
        }
        if ((uri.getPort() < 1) || (uri.getPort() > 65535)) {
            throw new SQLException("Invalid port number: " + url);
        }
        return uri;
    }

    private URI buildHttpUri()
    {
        String scheme = useSecureConnection ? "https" : "http";

        return uriBuilder()
                .scheme(scheme)
                .host(address.getHostText()).port(address.getPort())
                .build();
    }

    private void initCatalogAndSchema()
            throws SQLException
    {
        String path = uri.getPath();
        if (isNullOrEmpty(uri.getPath()) || path.equals("/")) {
            return;
        }

        // remove first slash
        if (!path.startsWith("/")) {
            throw new SQLException("Path does not start with a slash: " + uri);
        }
        path = path.substring(1);

        List<String> parts = Splitter.on("/").splitToList(path);
        // remove last item due to a trailing slash
        if (parts.get(parts.size() - 1).isEmpty()) {
            parts = parts.subList(0, parts.size() - 1);
        }

        if (parts.size() > 2) {
            throw new SQLException("Invalid path segments in URL: " + uri);
        }

        if (parts.get(0).isEmpty()) {
            throw new SQLException("Catalog name is empty: " + uri);
        }
        catalog = parts.get(0);

        if (parts.size() > 1) {
            if (parts.get(1).isEmpty()) {
                throw new SQLException("Schema name is empty: " + uri);
            }
            schema = parts.get(1);
        }
    }

    private static Properties mergeConnectionProperties(URI uri, Properties driverProperties)
            throws SQLException
    {
        Properties result = new Properties();
        result.putAll(ConnectionProperties.getDefaults());
        result.putAll(driverProperties);
        result.putAll(parseParameters(uri.getQuery()));
        return result;
    }

    private static void validateConnectionProperties(Properties connectionProperties)
            throws SQLException
    {
        for (String propertyName : connectionProperties.stringPropertyNames()) {
            if (ConnectionProperties.forKey(propertyName) == null) {
                throw new SQLException(format("Unrecognized connection property '%s'", propertyName));
            }
        }

        for (ConnectionProperty property : ConnectionProperties.allOf()) {
            property.validate(connectionProperties);
        }
    }
}
