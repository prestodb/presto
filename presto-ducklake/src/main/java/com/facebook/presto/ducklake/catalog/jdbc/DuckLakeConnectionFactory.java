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
package com.facebook.presto.ducklake.catalog.jdbc;

import com.facebook.presto.ducklake.DuckLakeCatalogConfig;
import jakarta.inject.Inject;
import org.postgresql.Driver;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Properties;

import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

/**
 * Builds JDBC connections to the PostgreSQL database that stores a DuckLake catalog. Kept as a
 * plain class, rather than a {@code javax.sql.DataSource}, so that a future SQLite or MySQL
 * backend can supply a different implementation. Never opens a connection eagerly: connecting is
 * deferred to {@link #openConnection()} so that constructing this class (and the {@link
 * com.facebook.presto.ducklake.catalog.DuckLakeCatalog} that wraps it) never fails even when the
 * configured database is unreachable.
 */
public class DuckLakeConnectionFactory
{
    private final String connectionUrl;
    private final String connectionUser;
    private final String connectionPassword;

    @Inject
    public DuckLakeConnectionFactory(DuckLakeCatalogConfig config)
    {
        requireNonNull(config, "config is null");
        this.connectionUrl = requireNonNull(config.getConnectionUrl(), "connectionUrl is null");
        this.connectionUser = config.getConnectionUser();
        this.connectionPassword = config.getConnectionPassword();
    }

    public Connection openConnection()
            throws SQLException
    {
        Properties properties = new Properties();
        if (connectionUser != null) {
            properties.setProperty("user", connectionUser);
        }
        if (connectionPassword != null) {
            properties.setProperty("password", connectionPassword);
        }
        return new Driver().connect(connectionUrl, properties);
    }

    /**
     * Returns {@link #connectionUrl} with any {@code password} query parameter stripped, for use
     * in error messages.
     */
    public String getSanitizedConnectionUrl()
    {
        return sanitizeUrl(connectionUrl);
    }

    private static String sanitizeUrl(String url)
    {
        int queryIndex = url.indexOf('?');
        if (queryIndex < 0) {
            return url;
        }
        String base = url.substring(0, queryIndex);
        String query = url.substring(queryIndex + 1);
        String filteredQuery = Arrays.stream(query.split("&"))
                .filter(parameter -> !parameter.toLowerCase(ENGLISH).startsWith("password="))
                .collect(joining("&"));
        return filteredQuery.isEmpty() ? base : base + "?" + filteredQuery;
    }
}
