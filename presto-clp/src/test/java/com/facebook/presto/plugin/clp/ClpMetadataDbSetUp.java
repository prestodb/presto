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
package com.facebook.presto.plugin.clp;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.plugin.clp.metadata.ClpMetadataProvider;
import com.facebook.presto.plugin.clp.metadata.ClpMySqlMetadataProvider;
import com.facebook.presto.plugin.clp.metadata.ClpSchemaTreeNodeType;
import com.facebook.presto.plugin.clp.split.ClpMySqlSplitProvider;
import org.apache.commons.io.FileUtils;
import org.apache.commons.math3.util.Pair;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.plugin.clp.metadata.ClpMySqlMetadataProvider.COLUMN_METADATA_TABLE_COLUMN_NAME;
import static com.facebook.presto.plugin.clp.metadata.ClpMySqlMetadataProvider.COLUMN_METADATA_TABLE_COLUMN_TYPE;
import static com.facebook.presto.plugin.clp.metadata.ClpMySqlMetadataProvider.DATASETS_TABLE_COLUMN_ARCHIVE_STORAGE_DIRECTORY;
import static com.facebook.presto.plugin.clp.metadata.ClpMySqlMetadataProvider.DATASETS_TABLE_COLUMN_NAME;
import static com.facebook.presto.plugin.clp.metadata.ClpMySqlMetadataProvider.DATASETS_TABLE_SUFFIX;
import static com.facebook.presto.plugin.clp.split.ClpMySqlSplitProvider.ARCHIVES_TABLE_COLUMN_ID;
import static com.facebook.presto.plugin.clp.split.ClpMySqlSplitProvider.ARCHIVES_TABLE_SUFFIX;
import static java.lang.String.format;
import static java.util.UUID.randomUUID;
import static org.testng.Assert.fail;

public final class ClpMetadataDbSetUp
{
    public static final String METADATA_DB_PASSWORD = "";
    public static final String METADATA_DB_TABLE_PREFIX = "clp_";
    public static final String METADATA_DB_URL_TEMPLATE = "jdbc:h2:file:%s;MODE=MySQL;DATABASE_TO_UPPER=FALSE";
    public static final String METADATA_DB_USER = "sa";
    public static final String ARCHIVES_STORAGE_DIRECTORY_BASE = "/tmp/archives/";

    private static final Logger log = Logger.get(ClpMetadataDbSetUp.class);
    private static final String DATASETS_TABLE_NAME = METADATA_DB_TABLE_PREFIX + DATASETS_TABLE_SUFFIX;
    private static final String ARCHIVES_TABLE_COLUMN_BEGIN_TIMESTAMP = "begin_timestamp";
    private static final String ARCHIVES_TABLE_COLUMN_PAGINATION_ID = "pagination_id";
    private static final String ARCHIVES_TABLE_COLUMN_END_TIMESTAMP = "end_timestamp";

    private ClpMetadataDbSetUp()
    {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    public static DbHandle getDbHandle(String dbName)
    {
        return new DbHandle(format("/tmp/presto-clp-test-%s/%s", randomUUID(), dbName));
    }

    public static ClpMetadata setupMetadata(DbHandle dbHandle, Map<String, List<Pair<String, ClpSchemaTreeNodeType>>> clpFields)
    {
        final String metadataDbUrl = format(METADATA_DB_URL_TEMPLATE, dbHandle.dbPath);
        final String columnMetadataTableSuffix = "_column_metadata";

        try (Connection conn = DriverManager.getConnection(metadataDbUrl, METADATA_DB_USER, METADATA_DB_PASSWORD); Statement stmt = conn.createStatement()) {
            createDatasetsTable(stmt);

            for (Map.Entry<String, List<Pair<String, ClpSchemaTreeNodeType>>> entry : clpFields.entrySet()) {
                String tableName = entry.getKey();
                String columnMetadataTableName = METADATA_DB_TABLE_PREFIX + tableName + columnMetadataTableSuffix;
                String createColumnMetadataSQL = format(
                        "CREATE TABLE IF NOT EXISTS %s (" +
                                " %s VARCHAR(512) NOT NULL," +
                                " %s TINYINT NOT NULL," +
                                " PRIMARY KEY (%s, %s))",
                        columnMetadataTableName,
                        COLUMN_METADATA_TABLE_COLUMN_NAME,
                        COLUMN_METADATA_TABLE_COLUMN_TYPE,
                        COLUMN_METADATA_TABLE_COLUMN_NAME,
                        COLUMN_METADATA_TABLE_COLUMN_TYPE);
                String insertColumnMetadataSQL = format(
                        "INSERT INTO %s (%s, %s) VALUES (?, ?)",
                        columnMetadataTableName,
                        COLUMN_METADATA_TABLE_COLUMN_NAME,
                        COLUMN_METADATA_TABLE_COLUMN_TYPE);

                stmt.execute(createColumnMetadataSQL);
                updateDatasetsTable(conn, tableName);

                try (PreparedStatement pstmt = conn.prepareStatement(insertColumnMetadataSQL)) {
                    for (Pair<String, ClpSchemaTreeNodeType> record : entry.getValue()) {
                        pstmt.setString(1, record.getFirst());
                        pstmt.setByte(2, record.getSecond().getType());
                        pstmt.addBatch();
                    }
                    pstmt.executeBatch();
                }
            }
        }
        catch (SQLException e) {
            fail(e.getMessage());
        }

        ClpConfig config = new ClpConfig()
                .setPolymorphicTypeEnabled(true)
                .setMetadataDbUrl(metadataDbUrl)
                .setMetadataDbUser(METADATA_DB_USER)
                .setMetadataDbPassword(METADATA_DB_PASSWORD)
                .setMetadataTablePrefix(METADATA_DB_TABLE_PREFIX);
        ClpMetadataProvider metadataProvider = new ClpMySqlMetadataProvider(config);
        return new ClpMetadata(config, metadataProvider);
    }

    public static ClpMySqlSplitProvider setupSplit(DbHandle dbHandle, Map<String, List<ArchivesTableRow>> splits)
    {
        final String metadataDbUrl = format(METADATA_DB_URL_TEMPLATE, dbHandle.dbPath);
        final String archiveTableFormat = METADATA_DB_TABLE_PREFIX + "%s" + ARCHIVES_TABLE_SUFFIX;

        try (Connection conn = DriverManager.getConnection(metadataDbUrl, METADATA_DB_USER, METADATA_DB_PASSWORD); Statement stmt = conn.createStatement()) {
            createDatasetsTable(stmt);

            // Create and populate archive tables
            for (Map.Entry<String, List<ArchivesTableRow>> tableSplits : splits.entrySet()) {
                String tableName = tableSplits.getKey();
                updateDatasetsTable(conn, tableName);

                String archiveTableName = format(archiveTableFormat, tableSplits.getKey());
                String createArchiveTableSQL = format(
                        "CREATE TABLE IF NOT EXISTS %s (" +
                                "%s BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY, " +
                                "%s VARCHAR(64) NOT NULL, " +
                                "%s BIGINT, " +
                                "%s BIGINT)",
                        archiveTableName,
                        ARCHIVES_TABLE_COLUMN_PAGINATION_ID,
                        ARCHIVES_TABLE_COLUMN_ID,
                        ARCHIVES_TABLE_COLUMN_BEGIN_TIMESTAMP,
                        ARCHIVES_TABLE_COLUMN_END_TIMESTAMP);

                stmt.execute(createArchiveTableSQL);

                String insertArchiveTableSQL = format(
                        "INSERT INTO %s (%s, %s, %s) VALUES (?, ?, ?)",
                        archiveTableName,
                        ARCHIVES_TABLE_COLUMN_ID,
                        ARCHIVES_TABLE_COLUMN_BEGIN_TIMESTAMP,
                        ARCHIVES_TABLE_COLUMN_END_TIMESTAMP);
                try (PreparedStatement pstmt = conn.prepareStatement(insertArchiveTableSQL)) {
                    for (ArchivesTableRow split : tableSplits.getValue()) {
                        pstmt.setString(1, split.id);
                        pstmt.setLong(2, split.beginTimestamp);
                        pstmt.setLong(3, split.endTimestamp);
                        pstmt.addBatch();
                    }
                    pstmt.executeBatch();
                }
            }
        }
        catch (SQLException e) {
            fail(e.getMessage());
        }

        return new ClpMySqlSplitProvider(
                new ClpConfig()
                        .setPolymorphicTypeEnabled(true)
                        .setMetadataDbUrl(metadataDbUrl)
                        .setMetadataDbUser(METADATA_DB_USER)
                        .setMetadataDbPassword(METADATA_DB_PASSWORD)
                        .setMetadataTablePrefix(METADATA_DB_TABLE_PREFIX));
    }

    public static void tearDown(DbHandle dbHandle)
    {
        File dir = new File(dbHandle.dbPath).getParentFile();
        if (dir.exists()) {
            try {
                FileUtils.deleteDirectory(dir);
                log.info("Deleted database dir" + dir.getAbsolutePath());
            }
            catch (IOException e) {
                log.warn("Failed to delete directory " + dir + ": " + e.getMessage());
            }
        }
    }

    private static void createDatasetsTable(Statement stmt)
            throws SQLException
    {
        final String createDatasetsTableSql = format(
                "CREATE TABLE IF NOT EXISTS %s (%s VARCHAR(255) PRIMARY KEY, %s VARCHAR(4096) NOT NULL)",
                DATASETS_TABLE_NAME,
                DATASETS_TABLE_COLUMN_NAME,
                DATASETS_TABLE_COLUMN_ARCHIVE_STORAGE_DIRECTORY);
        stmt.execute(createDatasetsTableSql);
    }

    private static void updateDatasetsTable(Connection conn, String tableName)
            throws SQLException
    {
        final String insertDatasetsTableSql = format(
                "INSERT INTO %s (%s, %s) VALUES (?, ?)",
                DATASETS_TABLE_NAME,
                DATASETS_TABLE_COLUMN_NAME,
                DATASETS_TABLE_COLUMN_ARCHIVE_STORAGE_DIRECTORY);
        try (PreparedStatement pstmt = conn.prepareStatement(insertDatasetsTableSql)) {
            pstmt.setString(1, tableName);
            pstmt.setString(2, ARCHIVES_STORAGE_DIRECTORY_BASE + tableName);
            pstmt.executeUpdate();
        }
    }

    static final class DbHandle
    {
        private final String dbPath;

        DbHandle(String dbPath)
        {
            this.dbPath = dbPath;
        }
    }

    static final class ArchivesTableRow
    {
        private final String id;
        private final long beginTimestamp;
        private final long endTimestamp;

        ArchivesTableRow(String id, long beginTimestamp, long endTimestamp)
        {
            this.id = id;
            this.beginTimestamp = beginTimestamp;
            this.endTimestamp = endTimestamp;
        }

        public String getId()
        {
            return id;
        }

        public long getBeginTimestamp()
        {
            return beginTimestamp;
        }

        public long getEndTimestamp()
        {
            return endTimestamp;
        }
    }
}
