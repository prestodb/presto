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
package com.facebook.presto.plugin.clp.mockdb;

import com.facebook.presto.plugin.clp.mockdb.table.ArchivesTableRows;
import com.facebook.presto.plugin.clp.mockdb.table.ColumnMetadataTableRows;
import com.facebook.presto.plugin.clp.mockdb.table.DatasetsTableRows;
import com.google.common.collect.ImmutableList;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.facebook.presto.plugin.clp.metadata.ClpMySqlMetadataProvider.COLUMN_METADATA_TABLE_COLUMN_NAME;
import static com.facebook.presto.plugin.clp.metadata.ClpMySqlMetadataProvider.COLUMN_METADATA_TABLE_COLUMN_TYPE;
import static com.facebook.presto.plugin.clp.metadata.ClpMySqlMetadataProvider.COLUMN_METADATA_TABLE_SUFFIX;
import static com.facebook.presto.plugin.clp.metadata.ClpMySqlMetadataProvider.DATASETS_TABLE_COLUMN_ARCHIVE_STORAGE_DIRECTORY;
import static com.facebook.presto.plugin.clp.metadata.ClpMySqlMetadataProvider.DATASETS_TABLE_COLUMN_NAME;
import static com.facebook.presto.plugin.clp.metadata.ClpMySqlMetadataProvider.DATASETS_TABLE_SUFFIX;
import static com.facebook.presto.plugin.clp.mockdb.table.ArchivesTableRows.COLUMN_BEGIN_TIMESTAMP;
import static com.facebook.presto.plugin.clp.mockdb.table.ArchivesTableRows.COLUMN_END_TIMESTAMP;
import static com.facebook.presto.plugin.clp.mockdb.table.ArchivesTableRows.COLUMN_ID;
import static com.facebook.presto.plugin.clp.mockdb.table.ArchivesTableRows.COLUMN_PAGINATION_ID;
import static com.facebook.presto.plugin.clp.split.ClpMySqlSplitProvider.ARCHIVES_TABLE_SUFFIX;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.fail;

/**
 * File-backed H2 mock metadata database for CLP tests. Uses the same schema as the CLP package.
 * Provides a builder-driven setup and a single-call teardown that drops all objects and deletes
 * files.
 */
public class ClpMockMetadataDatabase
{
    private static final String MOCK_METADATA_DB_DEFAULT_USERNAME = "sa";
    private static final String MOCK_METADATA_DB_DEFAULT_PASSWORD = "";

    private static final String MOCK_METADATA_DB_DEFAULT_TABLE_PREFIX = "clp_";

    private static final String MOCK_METADATA_DB_URL_TEMPLATE = "jdbc:h2:file:%s;MODE=MySQL;DATABASE_TO_UPPER=FALSE";

    private String url;
    private String archiveStorageDirectory;
    private String username;
    private String password;
    private String tablePrefix;

    /**
     * Creates a new builder instance for constructing {@link ClpMockMetadataDatabase}.
     *
     * @return a new {@link Builder}
     */
    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * Tears down the mock database by dropping all objects (tables, views, etc.) and deleting the
     * backing database file. Any exceptions during cleanup will cause the test to fail.
     */
    public void teardown()
    {
        try (Connection connection = DriverManager.getConnection(url, username, password); Statement stmt = connection.createStatement()) {
            stmt.execute("DROP ALL OBJECTS DELETE FILES");
        }
        catch (SQLException e) {
            fail(e.getMessage());
        }
    }

    public String getUrl()
    {
        return url;
    }

    public String getUsername()
    {
        return username;
    }

    public String getPassword()
    {
        return password;
    }

    public String getTablePrefix()
    {
        return tablePrefix;
    }

    public void addTableToDatasetsTableIfNotExist(List<String> tableNames)
    {
        try (Connection connection = DriverManager.getConnection(url, username, password)) {
            ImmutableList.Builder<String> repeatedArchiveStorageDirectory = ImmutableList.builder();
            for (String tableName : tableNames) {
                createArchivesTableIfNotExist(connection, tableName);
                createColumnMetadataTableIfNotExist(connection, tableName);
                repeatedArchiveStorageDirectory.add(archiveStorageDirectory);
            }
            DatasetsTableRows datasetsTableRows = new DatasetsTableRows(tableNames, repeatedArchiveStorageDirectory.build());
            datasetsTableRows.insertToTable(connection, tablePrefix);
        }
        catch (SQLException e) {
            fail(e.getMessage());
        }
    }

    public void addColumnMetadata(Map<String, ColumnMetadataTableRows> clpFields)
    {
        try (Connection connection = DriverManager.getConnection(url, username, password)) {
            for (Map.Entry<String, ColumnMetadataTableRows> entry : clpFields.entrySet()) {
                entry.getValue().insertToTable(connection, tablePrefix, entry.getKey());
            }
        }
        catch (SQLException e) {
            fail(e.getMessage());
        }
    }

    public void addSplits(Map<String, ArchivesTableRows> splits)
    {
        try (Connection connection = DriverManager.getConnection(url, username, password)) {
            for (Map.Entry<String, ArchivesTableRows> entry : splits.entrySet()) {
                entry.getValue().insertToTable(connection, tablePrefix, entry.getKey());
            }
        }
        catch (SQLException e) {
            fail(e.getMessage());
        }
    }

    private ClpMockMetadataDatabase()
    {
    }

    private void createArchivesTableIfNotExist(Connection connection, String tableName)
    {
        final String createTableSql = format(
                "CREATE TABLE IF NOT EXISTS `%s` (" +
                        "`%s` BIGINT AUTO_INCREMENT PRIMARY KEY, " +
                        "`%s` VARCHAR(64) NOT NULL, " +
                        "`%s` BIGINT, " +
                        "`%s` BIGINT)",
                format("%s%s%s", tablePrefix, tableName, ARCHIVES_TABLE_SUFFIX),
                COLUMN_PAGINATION_ID,
                COLUMN_ID,
                COLUMN_BEGIN_TIMESTAMP,
                COLUMN_END_TIMESTAMP);
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(createTableSql);
        }
        catch (SQLException e) {
            fail(e.getMessage());
        }
    }

    private void createColumnMetadataTableIfNotExist(Connection connection, String tableName)
    {
        String createTableSql = format(
                "CREATE TABLE IF NOT EXISTS %s (" +
                        "`%s` VARCHAR(512) NOT NULL, " +
                        "`%s` TINYINT NOT NULL, " +
                        "PRIMARY KEY (`%s`, `%s`))",
                format("%s%s%s", tablePrefix, tableName, COLUMN_METADATA_TABLE_SUFFIX),
                COLUMN_METADATA_TABLE_COLUMN_NAME,
                COLUMN_METADATA_TABLE_COLUMN_TYPE,
                COLUMN_METADATA_TABLE_COLUMN_NAME,
                COLUMN_METADATA_TABLE_COLUMN_TYPE);
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(createTableSql);
        }
        catch (SQLException e) {
            fail(e.getMessage());
        }
    }

    private void createDatasetsTableIfNotExist()
    {
        final String createTableSql = format(
                "CREATE TABLE IF NOT EXISTS %s (%s VARCHAR(255) PRIMARY KEY, %s VARCHAR(4096) NOT NULL)",
                format("%s%s", tablePrefix, DATASETS_TABLE_SUFFIX),
                DATASETS_TABLE_COLUMN_NAME,
                DATASETS_TABLE_COLUMN_ARCHIVE_STORAGE_DIRECTORY);
        try (Connection connection = DriverManager.getConnection(url, username, password); Statement stmt = connection.createStatement()) {
            stmt.execute(createTableSql);
        }
        catch (SQLException e) {
            fail(e.getMessage());
        }
    }

    public static final class Builder
    {
        private String url;
        private String archiveStorageDirectory;
        private String username;
        private String password;
        private String tablePrefix;

        private Builder()
        {
            setDatabaseUrl(format("/tmp/%s", UUID.randomUUID()));
            setUsername(MOCK_METADATA_DB_DEFAULT_USERNAME);
            setPassword(MOCK_METADATA_DB_DEFAULT_PASSWORD);
            setTablePrefix(MOCK_METADATA_DB_DEFAULT_TABLE_PREFIX);
        }

        public Builder setDatabaseUrl(String databaseFilePath)
        {
            this.url = format(MOCK_METADATA_DB_URL_TEMPLATE, databaseFilePath);
            return this;
        }

        public Builder setArchiveStorageDirectory(String archiveStorageDirectory)
        {
            this.archiveStorageDirectory = archiveStorageDirectory;
            return this;
        }

        public Builder setUsername(String username)
        {
            this.username = username;
            return this;
        }

        public Builder setPassword(String password)
        {
            this.password = password;
            return this;
        }

        public Builder setTablePrefix(String tablePrefix)
        {
            this.tablePrefix = tablePrefix;
            return this;
        }

        /**
         * Builds and returns the configured {@link ClpMockMetadataDatabase} instance.
         *
         * @return the constructed {@link ClpMockMetadataDatabase}
         */
        public ClpMockMetadataDatabase build()
        {
            validate();
            ClpMockMetadataDatabase mockMetadataDatabase = new ClpMockMetadataDatabase();
            mockMetadataDatabase.url = this.url;
            mockMetadataDatabase.archiveStorageDirectory = this.archiveStorageDirectory;
            mockMetadataDatabase.username = this.username;
            mockMetadataDatabase.password = this.password;
            mockMetadataDatabase.tablePrefix = this.tablePrefix;

            mockMetadataDatabase.createDatasetsTableIfNotExist();
            return mockMetadataDatabase;
        }

        /**
         * Validates that all required parameters have been set and the datasets table has been
         * created.
         */
        private void validate()
        {
            requireNonNull(url, "url is null");
            requireNonNull(archiveStorageDirectory, "archiveStorageDirectory is null");
            requireNonNull(username, "username is null");
            requireNonNull(password, "password is null");
            requireNonNull(tablePrefix, "tablePrefix is null");
        }
    }
}
