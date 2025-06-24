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
package com.facebook.presto.plugin.clp.metadata;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.plugin.clp.ClpColumnHandle;
import com.facebook.presto.plugin.clp.ClpConfig;
import com.facebook.presto.plugin.clp.ClpTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static java.lang.String.format;

public class ClpMySqlMetadataProvider
        implements ClpMetadataProvider
{
    // Column names
    public static final String COLUMN_METADATA_TABLE_COLUMN_NAME = "name";
    public static final String COLUMN_METADATA_TABLE_COLUMN_TYPE = "type";
    public static final String DATASETS_TABLE_COLUMN_NAME = "name";
    public static final String DATASETS_TABLE_COLUMN_ARCHIVE_STORAGE_DIRECTORY = "archive_storage_directory";

    // Table suffixes
    public static final String COLUMN_METADATA_TABLE_SUFFIX = "_column_metadata";
    public static final String DATASETS_TABLE_SUFFIX = "datasets";

    // SQL templates
    private static final String SQL_SELECT_COLUMN_METADATA_TEMPLATE = "SELECT * FROM `%s%s" + COLUMN_METADATA_TABLE_SUFFIX + "`";
    private static final String SQL_SELECT_DATASETS_TEMPLATE = format(
            "SELECT `%s`, `%s` FROM `%%s%s`",
            DATASETS_TABLE_COLUMN_NAME,
            DATASETS_TABLE_COLUMN_ARCHIVE_STORAGE_DIRECTORY,
            DATASETS_TABLE_SUFFIX);

    private static final Logger log = Logger.get(ClpMySqlMetadataProvider.class);

    private final ClpConfig config;

    @Inject
    public ClpMySqlMetadataProvider(ClpConfig config)
    {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        }
        catch (ClassNotFoundException e) {
            log.error(e, "Failed to load MySQL JDBC driver");
            throw new RuntimeException("MySQL JDBC driver not found", e);
        }
        this.config = config;
    }

    @Override
    public List<ClpColumnHandle> listColumnHandles(SchemaTableName schemaTableName)
    {
        String query = format(SQL_SELECT_COLUMN_METADATA_TEMPLATE, config.getMetadataTablePrefix(), schemaTableName.getTableName());
        ClpSchemaTree schemaTree = new ClpSchemaTree(config.isPolymorphicTypeEnabled());
        try (Connection connection = getConnection(); PreparedStatement statement = connection.prepareStatement(query)) {
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    schemaTree.addColumn(
                            resultSet.getString(COLUMN_METADATA_TABLE_COLUMN_NAME),
                            resultSet.getByte(COLUMN_METADATA_TABLE_COLUMN_TYPE));
                }
            }
        }
        catch (SQLException e) {
            log.warn("Failed to load table schema for %s: %s", schemaTableName.getTableName(), e);
        }
        return schemaTree.collectColumnHandles();
    }

    @Override
    public List<ClpTableHandle> listTableHandles(String schemaName)
    {
        ImmutableList.Builder<ClpTableHandle> tableHandles = new ImmutableList.Builder<>();
        String query = format(SQL_SELECT_DATASETS_TEMPLATE, config.getMetadataTablePrefix());
        try (Connection connection = getConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(query)) {
            while (resultSet.next()) {
                String tableName = resultSet.getString(DATASETS_TABLE_COLUMN_NAME);
                String archiveStorageDirectory = resultSet.getString(DATASETS_TABLE_COLUMN_ARCHIVE_STORAGE_DIRECTORY);
                if (isValidIdentifier(tableName) && archiveStorageDirectory != null && !archiveStorageDirectory.isEmpty()) {
                    tableHandles.add(new ClpTableHandle(new SchemaTableName(schemaName, tableName), archiveStorageDirectory));
                }
                else {
                    log.warn("Ignoring invalid table name found in metadata: %s", tableName);
                }
            }
        }
        catch (SQLException e) {
            log.warn("Failed to load table names: %s", e);
        }
        return tableHandles.build();
    }

    private Connection getConnection()
            throws SQLException
    {
        Connection connection = DriverManager.getConnection(config.getMetadataDbUrl(), config.getMetadataDbUser(), config.getMetadataDbPassword());
        String dbName = config.getMetadataDbName();
        if (dbName != null && !dbName.isEmpty()) {
            connection.createStatement().execute(format("USE `%s`", dbName));
        }
        return connection;
    }

    private boolean isValidIdentifier(String identifier)
    {
        return identifier != null && ClpConfig.SAFE_SQL_TABLE_NAME_PATTERN.matcher(identifier).matches();
    }
}
