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

import com.facebook.presto.client.ClientException;
import com.google.common.base.Splitter;
import com.google.common.collect.Maps;
import com.google.common.net.HostAndPort;
import okhttp3.OkHttpClient;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;

import static com.facebook.presto.client.KerberosUtil.defaultCredentialCachePath;
import static com.facebook.presto.client.OkHttpUtil.basicAuth;
import static com.facebook.presto.client.OkHttpUtil.setupCookieJar;
import static com.facebook.presto.client.OkHttpUtil.setupHttpProxy;
import static com.facebook.presto.client.OkHttpUtil.setupKerberos;
import static com.facebook.presto.client.OkHttpUtil.setupSocksProxy;
import static com.facebook.presto.client.OkHttpUtil.setupSsl;
import static com.facebook.presto.client.OkHttpUtil.tokenAuth;
import static com.facebook.presto.jdbc.ConnectionProperties.ACCESS_TOKEN;
import static com.facebook.presto.jdbc.ConnectionProperties.APPLICATION_NAME_PREFIX;
import static com.facebook.presto.jdbc.ConnectionProperties.HTTP_PROXY;
import static com.facebook.presto.jdbc.ConnectionProperties.KERBEROS_CONFIG_PATH;
import static com.facebook.presto.jdbc.ConnectionProperties.KERBEROS_CREDENTIAL_CACHE_PATH;
import static com.facebook.presto.jdbc.ConnectionProperties.KERBEROS_KEYTAB_PATH;
import static com.facebook.presto.jdbc.ConnectionProperties.KERBEROS_PRINCIPAL;
import static com.facebook.presto.jdbc.ConnectionProperties.KERBEROS_REMOTE_SERVICE_NAME;
import static com.facebook.presto.jdbc.ConnectionProperties.KERBEROS_USE_CANONICAL_HOSTNAME;
import static com.facebook.presto.jdbc.ConnectionProperties.PASSWORD;
import static com.facebook.presto.jdbc.ConnectionProperties.SOCKS_PROXY;
import static com.facebook.presto.jdbc.ConnectionProperties.SSL;
import static com.facebook.presto.jdbc.ConnectionProperties.SSL_KEY_STORE_PASSWORD;
import static com.facebook.presto.jdbc.ConnectionProperties.SSL_KEY_STORE_PATH;
import static com.facebook.presto.jdbc.ConnectionProperties.SSL_TRUST_STORE_PASSWORD;
import static com.facebook.presto.jdbc.ConnectionProperties.SSL_TRUST_STORE_PATH;
import static com.facebook.presto.jdbc.ConnectionProperties.USER;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Parses and extracts parameters from a Presto JDBC URL.
 */
final class PrestoDriverUri
{
    private static final String JDBC_URL_START = "jdbc:";

    private static final Splitter QUERY_SPLITTER = Splitter.on('&').omitEmptyStrings();
    private static final Splitter ARG_SPLITTER = Splitter.on('=').limit(2);

    private final HostAndPort address;
    private final URI uri;

    private final Properties properties;

    private String catalog;
    private String schema;

    private final boolean useSecureConnection;

    public PrestoDriverUri(String url, Properties driverProperties)
            throws SQLException
    {
        this(parseDriverUrl(url), driverProperties);
    }

    private PrestoDriverUri(URI uri, Properties driverProperties)
            throws SQLException
    {
        this.uri = requireNonNull(uri, "uri is null");
        address = HostAndPort.fromParts(uri.getHost(), uri.getPort());
        properties = mergeConnectionProperties(uri, driverProperties);

        validateConnectionProperties(properties);

        // enable SSL by default for standard port
        useSecureConnection = SSL.getValue(properties).orElse(uri.getPort() == 443);

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

    public String getUser()
            throws SQLException
    {
        return USER.getRequiredValue(properties);
    }

    public Optional<String> getApplicationNamePrefix()
            throws SQLException
    {
        return APPLICATION_NAME_PREFIX.getValue(properties);
    }

    public Properties getProperties()
    {
        return properties;
    }

    public void setupClient(OkHttpClient.Builder builder)
            throws SQLException
    {
        try {
            setupCookieJar(builder);
            setupSocksProxy(builder, SOCKS_PROXY.getValue(properties));
            setupHttpProxy(builder, HTTP_PROXY.getValue(properties));

            // TODO: fix Tempto to allow empty passwords
            String password = PASSWORD.getValue(properties).orElse("");
            if (!password.isEmpty() && !password.equals("***empty***")) {
                if (!useSecureConnection) {
                    throw new SQLException("Authentication using username/password requires SSL to be enabled");
                }
                builder.addInterceptor(basicAuth(getUser(), password));
            }

            if (useSecureConnection) {
                setupSsl(
                        builder,
                        SSL_KEY_STORE_PATH.getValue(properties),
                        SSL_KEY_STORE_PASSWORD.getValue(properties),
                        SSL_TRUST_STORE_PATH.getValue(properties),
                        SSL_TRUST_STORE_PASSWORD.getValue(properties));
            }

            if (KERBEROS_REMOTE_SERVICE_NAME.getValue(properties).isPresent()) {
                if (!useSecureConnection) {
                    throw new SQLException("Authentication using Kerberos requires SSL to be enabled");
                }
                setupKerberos(
                        builder,
                        KERBEROS_REMOTE_SERVICE_NAME.getRequiredValue(properties),
                        KERBEROS_USE_CANONICAL_HOSTNAME.getRequiredValue(properties),
                        KERBEROS_PRINCIPAL.getValue(properties),
                        KERBEROS_CONFIG_PATH.getValue(properties),
                        KERBEROS_KEYTAB_PATH.getValue(properties),
                        Optional.ofNullable(KERBEROS_CREDENTIAL_CACHE_PATH.getValue(properties)
                                .orElseGet(() -> defaultCredentialCachePath().map(File::new).orElse(null))));
            }

            if (ACCESS_TOKEN.getValue(properties).isPresent()) {
                if (!useSecureConnection) {
                    throw new SQLException("Authentication using an access token requires SSL to be enabled");
                }
                builder.addInterceptor(tokenAuth(ACCESS_TOKEN.getValue(properties).get()));
            }
        }
        catch (ClientException e) {
            throw new SQLException(e.getMessage(), e);
        }
        catch (RuntimeException e) {
            throw new SQLException("Error setting up connection", e);
        }
    }

    private static Map<String, String> parseParameters(String query)
            throws SQLException
    {
        Map<String, String> result = new HashMap<>();

        if (query != null) {
            Iterable<String> queryArgs = QUERY_SPLITTER.split(query);
            for (String queryArg : queryArgs) {
                List<String> parts = ARG_SPLITTER.splitToList(queryArg);
                if (result.put(parts.get(0), parts.get(1)) != null) {
                    throw new SQLException(format("Connection property '%s' is in URL multiple times", parts.get(0)));
                }
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
        try {
            return new URI(scheme, null, address.getHost(), address.getPort(), null, null, null);
        }
        catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
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
        Map<String, String> defaults = ConnectionProperties.getDefaults();
        Map<String, String> urlProperties = parseParameters(uri.getQuery());
        Map<String, String> suppliedProperties = Maps.fromProperties(driverProperties);

        for (String key : urlProperties.keySet()) {
            if (suppliedProperties.containsKey(key)) {
                throw new SQLException(format("Connection property '%s' is both in the URL and an argument", key));
            }
        }

        Properties result = new Properties();
        setProperties(result, defaults);
        setProperties(result, urlProperties);
        setProperties(result, suppliedProperties);
        return result;
    }

    private static void setProperties(Properties properties, Map<String, String> values)
    {
        for (Entry<String, String> entry : values.entrySet()) {
            properties.setProperty(entry.getKey(), entry.getValue());
        }
    }

    private static void validateConnectionProperties(Properties connectionProperties)
            throws SQLException
    {
        for (String propertyName : connectionProperties.stringPropertyNames()) {
            if (ConnectionProperties.forKey(propertyName) == null) {
                throw new SQLException(format("Unrecognized connection property '%s'", propertyName));
            }
        }

        for (ConnectionProperty<?> property : ConnectionProperties.allProperties()) {
            property.validate(connectionProperties);
        }
    }
}
