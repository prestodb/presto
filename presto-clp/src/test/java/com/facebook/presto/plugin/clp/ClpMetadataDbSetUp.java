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
import com.facebook.presto.plugin.clp.metadata.ClpNodeType;
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
import java.util.UUID;

import static org.testng.Assert.fail;

public final class ClpMetadataDbSetUp
{
    private static final Logger log = Logger.get(ClpMetadataDbSetUp.class);

    public static final String metadataDbUrlTemplate =
            "jdbc:h2:file:%s;MODE=MySQL;DATABASE_TO_UPPER=FALSE";
    public static final String metadataDbTablePrefix = "clp_";
    public static final String metadataDbUser = "sa";
    public static final String metadataDbPassword = "";
    private static final String datasetsTableName = metadataDbTablePrefix + "datasets";

    public static final class DbHandle
    {
        DbHandle(String dbPath)
        {
            this.dbPath = dbPath;
        }
        public String dbPath;
    }

    private ClpMetadataDbSetUp()
    {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    public static DbHandle getDbHandle(String dbName)
    {
        return new DbHandle(String.format("/tmp/presto-clp-test-%s/%s", UUID.randomUUID(), dbName));
    }

    public static ClpMetadata setupMetadata(DbHandle dbHandle, Map<String, List<Pair<String, ClpNodeType>>> clpFields)
    {
        final String metadataDbUrl = String.format(metadataDbUrlTemplate, dbHandle.dbPath);
        final String columnMetadataTableSuffix = "_column_metadata";

        try (Connection conn = DriverManager.getConnection(metadataDbUrl, metadataDbUser, metadataDbPassword);
                Statement stmt = conn.createStatement()) {
            createDatasetsTable(stmt);

            for (Map.Entry<String, List<Pair<String, ClpNodeType>>> entry : clpFields.entrySet()) {
                String tableName = entry.getKey();
                String columnMetadataTableName = metadataDbTablePrefix + tableName + columnMetadataTableSuffix;
                String createColumnMetadataSQL = String.format(
                        "CREATE TABLE IF NOT EXISTS %s (" +
                                " name VARCHAR(512) NOT NULL," +
                                " type TINYINT NOT NULL," +
                                " PRIMARY KEY (name, type))", columnMetadataTableName);
                String insertColumnMetadataSQL = String.format(
                        "INSERT INTO %s (name, type) VALUES (?, ?)", columnMetadataTableName);
                stmt.execute(createColumnMetadataSQL);

                updateDatasetsTable(conn, tableName);

                try (PreparedStatement pstmt = conn.prepareStatement(insertColumnMetadataSQL)) {
                    for (Pair<String, ClpNodeType> record : entry.getValue()) {
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

        ClpConfig config = new ClpConfig().setPolymorphicTypeEnabled(true)
                .setMetadataDbUrl(metadataDbUrl)
                .setMetadataDbUser(metadataDbUser)
                .setMetadataDbPassword(metadataDbPassword)
                .setMetadataTablePrefix(metadataDbTablePrefix);
        ClpMetadataProvider metadataProvider = new ClpMySqlMetadataProvider(config);
        return new ClpMetadata(config, metadataProvider);
    }

    public static ClpMySqlSplitProvider setupSplit(DbHandle dbHandle, Map<String, List<String>> splits)
    {
        final String metadataDbUrl = String.format(metadataDbUrlTemplate, dbHandle.dbPath);
        final String archiveTableSuffix = "_archives";
        final String archiveTableFormat = metadataDbTablePrefix + "%s" + archiveTableSuffix;

        try (Connection conn = DriverManager.getConnection(metadataDbUrl, metadataDbUser, metadataDbPassword);
                Statement stmt = conn.createStatement()) {
            createDatasetsTable(stmt);

            // Create and populate archive tables
            for (Map.Entry<String, List<String>> tableSplits : splits.entrySet()) {
                String tableName = tableSplits.getKey();
                updateDatasetsTable(conn, tableName);

                String archiveTableName = String.format(archiveTableFormat, tableSplits.getKey());
                String createArchiveTableSQL = String.format(
                        "CREATE TABLE IF NOT EXISTS %s (" +
                                "pagination_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY, " +
                                "id VARCHAR(64) NOT NULL" +
                                ")",
                        archiveTableName);
                stmt.execute(createArchiveTableSQL);

                String insertArchiveTableSQL = String.format("INSERT INTO %s (id) VALUES (?)", archiveTableName);
                try (PreparedStatement pstmt = conn.prepareStatement(insertArchiveTableSQL)) {
                    for (String splitPath : tableSplits.getValue()) {
                        pstmt.setString(1, splitPath);
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
                        .setMetadataDbUser(metadataDbUser)
                        .setMetadataDbPassword(metadataDbPassword)
                        .setMetadataTablePrefix(metadataDbTablePrefix));
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

    private static void createDatasetsTable(Statement stmt) throws SQLException
    {
        final String createDatasetTableSQL = String.format(
                "CREATE TABLE IF NOT EXISTS %s (" +
                        " name VARCHAR(255) PRIMARY KEY," +
                        " archive_storage_type VARCHAR(4096) NOT NULL," +
                        " archive_storage_directory VARCHAR(4096) NOT NULL)", datasetsTableName);
        stmt.execute(createDatasetTableSQL);
    }

    private static void updateDatasetsTable(Connection conn, String tableName) throws SQLException
    {
        final String insertDatasetTableSQL = String.format(
                "INSERT INTO %s (name, archive_storage_type, archive_storage_directory) VALUES (?, ?, ?)", datasetsTableName);
        try (PreparedStatement pstmt = conn.prepareStatement(insertDatasetTableSQL)) {
            pstmt.setString(1, tableName);
            pstmt.setString(2, "fs");
            pstmt.setString(3, "/tmp/archives/" + tableName);
            pstmt.executeUpdate();
        }
    }
}
