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
package com.facebook.presto.plugin.clp.split;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.plugin.clp.ClpConfig;
import com.facebook.presto.plugin.clp.ClpSplit;
import com.facebook.presto.plugin.clp.ClpTableLayoutHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ClpMySqlSplitProvider
        implements ClpSplitProvider
{
    private static final Logger log = Logger.get(ClpMySqlSplitProvider.class);

    private static final String ARCHIVE_TABLE_SUFFIX = "_archives";
    private static final String TABLE_METADATA_TABLE_SUFFIX = "table_metadata";
    private static final String QUERY_SELECT_ARCHIVE_IDS = "SELECT archive_id FROM %s%s" + ARCHIVE_TABLE_SUFFIX;
    private static final String QUERY_SELECT_TABLE_METADATA = "SELECT table_path FROM %s" + TABLE_METADATA_TABLE_SUFFIX + " WHERE table_name = '%s'";

    private final ClpConfig config;

    public ClpMySqlSplitProvider(ClpConfig config)
    {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        }
        catch (ClassNotFoundException e) {
            log.error(e, "Failed to load MySQL JDBC driver");
            throw new RuntimeException("MySQL JDBC driver not found", e);
        }
        this.config = config;
    }

    private Connection getConnection() throws SQLException
    {
        Connection connection = DriverManager.getConnection(config.getMetadataDbUrl(), config.getMetadataDbUser(), config.getMetadataDbPassword());
        String dbName = config.getMetadataDbName();
        if (dbName != null && !dbName.isEmpty()) {
            connection.createStatement().execute("USE " + dbName);
        }
        return connection;
    }

    @Override
    public List<ClpSplit> listSplits(ClpTableLayoutHandle clpTableLayoutHandle)
    {
        List<ClpSplit> splits = new ArrayList<>();
        SchemaTableName tableSchemaName = clpTableLayoutHandle.getTable().getSchemaTableName();
        String tableName = tableSchemaName.getTableName();

        String tablePathQuery = String.format(QUERY_SELECT_TABLE_METADATA, config.getMetadataTablePrefix(), tableName);
        String archivePathQuery = String.format(QUERY_SELECT_ARCHIVE_IDS, config.getMetadataTablePrefix(), tableName);

        try (Connection connection = getConnection()) {
            // Fetch table path
            String tablePath;
            try (PreparedStatement statement = connection.prepareStatement(tablePathQuery);
                    ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    log.error("Table metadata not found for table: %s", tableName);
                    return ImmutableList.of();
                }
                tablePath = resultSet.getString("table_path");
            }

            if (tablePath == null || tablePath.isEmpty()) {
                log.error("Table path is null for table: %s", tableName);
                return ImmutableList.of();
            }

            // Fetch archive IDs and create splits
            try (PreparedStatement statement = connection.prepareStatement(archivePathQuery);
                    ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    final String archiveId = resultSet.getString("archive_id");
                    final String archivePath = tablePath + "/" + archiveId;
                    splits.add(new ClpSplit(tableSchemaName, archivePath, clpTableLayoutHandle.getQuery()));
                }
            }
        }
        catch (SQLException e) {
            log.error("Database error while processing splits for %s: %s", tableName, e);
        }

        return splits;
    }
}
