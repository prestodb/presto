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
package com.facebook.presto.ducklake;

import com.facebook.presto.ducklake.catalog.jdbc.DuckLakeConnectionFactory;
import com.facebook.presto.ducklake.catalog.jdbc.JdbcDuckLakeCatalog;
import com.google.common.io.Resources;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * A PostgreSQL 14 server (see {@link TestingPostgreSqlServer}) loaded with the DuckLake catalog
 * fixture at {@code src/test/resources/ducklake/catalog.sql}, with the fixture's {@code
 * data_path} metadata entry rewritten to the absolute path of the sibling {@code data} resource
 * directory so that resolved schema and table paths point at the real Parquet files on disk.
 */
public class TestingDuckLakeCatalog
        implements Closeable
{
    private final TestingPostgreSqlServer server;
    private final String dataPath;
    private final DuckLakeCatalogConfig catalogConfig;

    public TestingDuckLakeCatalog()
    {
        server = TestingPostgreSqlServer.create();
        try {
            Path catalogSqlFile = resourceToPath("ducklake/catalog.sql");
            Path dataDirectory = catalogSqlFile.getParent().resolve("data");
            dataPath = dataDirectory.toAbsolutePath() + "/";

            try (Connection connection = openConnection()) {
                executeStatements(connection, catalogSqlFile);
                rewriteDataPath(connection, dataPath);
            }
        }
        catch (RuntimeException | IOException | SQLException e) {
            server.close();
            throw new RuntimeException("Failed to set up the DuckLake catalog fixture", e);
        }

        catalogConfig = new DuckLakeCatalogConfig()
                .setConnectionUrl(server.getJdbcUrl())
                .setConnectionUser(server.getUser())
                .setConnectionPassword(server.getPassword())
                .setSchema("public");
    }

    public String getJdbcUrl()
    {
        return server.getJdbcUrl();
    }

    public String getUser()
    {
        return server.getUser();
    }

    public String getPassword()
    {
        return server.getPassword();
    }

    public String getDataPath()
    {
        return dataPath;
    }

    public DuckLakeCatalogConfig getCatalogConfig()
    {
        return catalogConfig;
    }

    public Connection openConnection()
            throws SQLException
    {
        return DriverManager.getConnection(server.getJdbcUrl(), server.getUser(), server.getPassword());
    }

    public JdbcDuckLakeCatalog createCatalog()
    {
        return new JdbcDuckLakeCatalog(new DuckLakeConnectionFactory(catalogConfig), catalogConfig);
    }

    @Override
    public void close()
    {
        server.close();
    }

    private static Path resourceToPath(String resourceName)
    {
        URL resourceUrl = Resources.getResource(resourceName);
        try {
            return Paths.get(resourceUrl.toURI());
        }
        catch (URISyntaxException e) {
            throw new UncheckedIOException(new IOException(e));
        }
    }

    private static void executeStatements(Connection connection, Path dumpFile)
            throws IOException, SQLException
    {
        List<String> lines = Files.readAllLines(dumpFile, UTF_8);
        StringBuilder statement = new StringBuilder();
        try (Statement jdbcStatement = connection.createStatement()) {
            for (String line : lines) {
                String trimmedLine = line.trim();
                if (trimmedLine.isEmpty() || trimmedLine.startsWith("--")) {
                    continue;
                }
                if (statement.length() > 0) {
                    statement.append('\n');
                }
                statement.append(line);
                if (trimmedLine.endsWith(";")) {
                    jdbcStatement.execute(statement.toString());
                    statement.setLength(0);
                }
            }
        }
    }

    private static void rewriteDataPath(Connection connection, String dataPath)
            throws SQLException
    {
        requireNonNull(dataPath, "dataPath is null");
        try (PreparedStatement statement = connection.prepareStatement("UPDATE public.ducklake_metadata SET value = ? WHERE key = 'data_path'")) {
            statement.setString(1, dataPath);
            int updated = statement.executeUpdate();
            if (updated != 1) {
                throw new IllegalStateException("Expected exactly one data_path row, updated " + updated);
            }
        }
    }
}
