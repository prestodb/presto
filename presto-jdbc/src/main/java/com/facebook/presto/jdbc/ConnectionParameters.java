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
import com.google.common.net.HostAndPort;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Strings.isNullOrEmpty;
import static io.airlift.http.client.HttpUriBuilder.uriBuilder;

/**
 * Container for parameters of a JDBC connection. The class is also responsible for parsing Presto JDBC URL.
 */
final class ConnectionParameters
{
    private static final String JDBC_URL_START = "jdbc:";

    private final AtomicReference<String> catalog = new AtomicReference<>();
    private final AtomicReference<String> schema = new AtomicReference<>();

    private static final Splitter QUERY_SPLITTER = Splitter.on('&').omitEmptyStrings();
    private static final Splitter ARG_SPLITTER = Splitter.on('=').limit(2);

    private final HostAndPort address;
    private final URI uri;

    private final boolean useSSL;

    ConnectionParameters(final String url)
            throws SQLException
    {
        this(parseDriverUrl(url));
    }

    ConnectionParameters(final URI uri)
            throws SQLException
    {
        this.uri = uri;
        this.address = HostAndPort.fromParts(uri.getHost(), uri.getPort());

        final Map<String, String> params = parseParameters(uri.getQuery());
        this.useSSL = Boolean.parseBoolean(params.get("useSSL"));

        if (!isNullOrEmpty(uri.getPath())) {
            setCatalogAndSchema();
        }
    }

    URI getURI()
    {
        return uri;
    }

    String getSchema()
    {
        return schema.get();
    }

    void setSchema(final String schema)
    {
        this.schema.set(schema);
    }

    String getCatalog()
    {
        return catalog.get();
    }

    void setCatalog(final String catalog)
    {
        this.catalog.set(catalog);
    }

    URI getHttpUri()
    {
        return buildHttpUri();
    }

    private Map<String, String> parseParameters(String query)
    {
        final Map<String, String> result = new HashMap<>();

        if (query != null) {
            Iterable<String> queryArgs = QUERY_SPLITTER.split(query);
            for (String queryArg : queryArgs) {
                final List<String> parts = ARG_SPLITTER.splitToList(queryArg);
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
        final String scheme = (address.getPort() == 443 || useSSL) ? "https" : "http";

        return uriBuilder()
                .scheme(scheme)
                .host(address.getHostText()).port(address.getPort())
                .build();
    }

    private void setCatalogAndSchema()
            throws SQLException
    {
        String path = uri.getPath();
        if (path.equals("/")) {
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
        catalog.set(parts.get(0));

        if (parts.size() > 1) {
            if (parts.get(1).isEmpty()) {
                throw new SQLException("Schema name is empty: " + uri);
            }
            schema.set(parts.get(1));
        }
    }
}
