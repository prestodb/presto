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
import com.facebook.presto.ducklake.catalog.DuckLakeCatalog;
import com.facebook.presto.ducklake.catalog.DuckLakeColumnRow;
import com.facebook.presto.ducklake.catalog.DuckLakeDataFile;
import com.facebook.presto.ducklake.catalog.DuckLakeDeleteFile;
import com.facebook.presto.ducklake.catalog.DuckLakeFileColumnStats;
import com.facebook.presto.ducklake.catalog.DuckLakeInlinedRowSource;
import com.facebook.presto.ducklake.catalog.DuckLakeInlinedTable;
import com.facebook.presto.ducklake.catalog.DuckLakePartitionField;
import com.facebook.presto.ducklake.catalog.DuckLakeSchema;
import com.facebook.presto.ducklake.catalog.DuckLakeSnapshot;
import com.facebook.presto.ducklake.catalog.DuckLakeTable;
import com.facebook.presto.ducklake.catalog.DuckLakeTableColumnStats;
import com.facebook.presto.ducklake.catalog.DuckLakeTableStats;
import com.facebook.presto.ducklake.catalog.SnapshotPredicate;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import jakarta.inject.Inject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static com.facebook.presto.ducklake.DuckLakeErrorCode.DUCKLAKE_CATALOG_ERROR;
import static com.facebook.presto.ducklake.DuckLakeErrorCode.DUCKLAKE_INVALID_METADATA;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

/**
 * Implements {@link DuckLakeCatalog} by querying the {@code ducklake_*} metadata tables of a
 * PostgreSQL database directly, over JDBC. Every query is a prepared statement, with the schema
 * name taken from {@link DuckLakeCatalogConfig#getSchema()} prefixed onto each table name; the
 * schema name is trusted configuration, while every value is bound as a {@code ?} parameter.
 * Opens a fresh connection per call and never caches one, other than the {@code data_path} global
 * metadata value, which is immutable for the lifetime of a lake and is loaded lazily on first
 * use.
 */
public class JdbcDuckLakeCatalog
        implements DuckLakeCatalog
{
    private static final String SNAPSHOT_COLUMNS = "s.snapshot_id, s.snapshot_time, s.schema_version, c.author, c.commit_message, c.changes_made";

    private final DuckLakeConnectionFactory connectionFactory;
    private final String catalogSchema;

    private volatile String dataPath;

    @Inject
    public JdbcDuckLakeCatalog(DuckLakeConnectionFactory connectionFactory, DuckLakeCatalogConfig config)
    {
        this.connectionFactory = requireNonNull(connectionFactory, "connectionFactory is null");
        this.catalogSchema = requireNonNull(config, "config is null").getSchema();
    }

    @Override
    public long getLatestSnapshotId()
    {
        return execute(connection -> {
            String sql = "SELECT MAX(snapshot_id) AS latest_snapshot_id FROM " + qualify("ducklake_snapshot");
            try (PreparedStatement statement = connection.prepareStatement(sql);
                    ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                long latestSnapshotId = resultSet.getLong("latest_snapshot_id");
                if (resultSet.wasNull()) {
                    throw new PrestoException(DUCKLAKE_INVALID_METADATA, "DuckLake catalog has no snapshots");
                }
                return latestSnapshotId;
            }
        });
    }

    @Override
    public Optional<DuckLakeSnapshot> getSnapshot(long snapshotId)
    {
        return execute(connection -> {
            String sql = "SELECT " + SNAPSHOT_COLUMNS + " FROM " + snapshotJoin() + " WHERE s.snapshot_id = ?";
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setLong(1, snapshotId);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (!resultSet.next()) {
                        return Optional.empty();
                    }
                    return Optional.of(mapSnapshot(resultSet));
                }
            }
        });
    }

    @Override
    public Optional<DuckLakeSnapshot> getSnapshotAtOrBefore(Instant time)
    {
        requireNonNull(time, "time is null");
        return execute(connection -> {
            String sql = "SELECT " + SNAPSHOT_COLUMNS + " FROM " + snapshotJoin() +
                    " WHERE s.snapshot_time <= ? ORDER BY s.snapshot_time DESC, s.snapshot_id DESC LIMIT 1";
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setObject(1, OffsetDateTime.ofInstant(time, ZoneOffset.UTC));
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (!resultSet.next()) {
                        return Optional.empty();
                    }
                    return Optional.of(mapSnapshot(resultSet));
                }
            }
        });
    }

    @Override
    public List<DuckLakeSnapshot> listSnapshots()
    {
        return execute(connection -> {
            String sql = "SELECT " + SNAPSHOT_COLUMNS + " FROM " + snapshotJoin() + " ORDER BY s.snapshot_id";
            try (PreparedStatement statement = connection.prepareStatement(sql);
                    ResultSet resultSet = statement.executeQuery()) {
                ImmutableList.Builder<DuckLakeSnapshot> snapshots = ImmutableList.builder();
                while (resultSet.next()) {
                    snapshots.add(mapSnapshot(resultSet));
                }
                return snapshots.build();
            }
        });
    }

    @Override
    public List<DuckLakeSchema> listSchemas(long snapshotId)
    {
        return execute(connection -> {
            SnapshotPredicate predicate = new SnapshotPredicate("s", snapshotId);
            String sql = "SELECT schema_id, schema_name, path, path_is_relative FROM " + qualify("ducklake_schema") + " s" +
                    " WHERE " + predicate.sql() + " ORDER BY schema_name";
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                bindParameters(statement, 1, predicate.parameters());
                try (ResultSet resultSet = statement.executeQuery()) {
                    String resolvedDataPath = getDataPath(connection);
                    ImmutableList.Builder<DuckLakeSchema> schemas = ImmutableList.builder();
                    while (resultSet.next()) {
                        schemas.add(mapSchema(resultSet, resolvedDataPath));
                    }
                    return schemas.build();
                }
            }
        });
    }

    @Override
    public Optional<DuckLakeSchema> getSchema(long snapshotId, String schemaName)
    {
        requireNonNull(schemaName, "schemaName is null");
        return execute(connection -> {
            SnapshotPredicate predicate = new SnapshotPredicate("s", snapshotId);
            String sql = "SELECT schema_id, schema_name, path, path_is_relative FROM " + qualify("ducklake_schema") + " s" +
                    " WHERE " + predicate.sql() + " AND s.schema_name = ?";
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                int index = bindParameters(statement, 1, predicate.parameters());
                statement.setString(index, schemaName);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (!resultSet.next()) {
                        return Optional.empty();
                    }
                    return Optional.of(mapSchema(resultSet, getDataPath(connection)));
                }
            }
        });
    }

    @Override
    public List<DuckLakeTable> listTables(long snapshotId, DuckLakeSchema schema)
    {
        requireNonNull(schema, "schema is null");
        return execute(connection -> {
            SnapshotPredicate predicate = new SnapshotPredicate("t", snapshotId);
            String sql = "SELECT table_id, schema_id, table_name, path, path_is_relative FROM " + qualify("ducklake_table") + " t" +
                    " WHERE t.schema_id = ? AND " + predicate.sql() + " ORDER BY table_name";
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setLong(1, schema.getSchemaId());
                bindParameters(statement, 2, predicate.parameters());
                try (ResultSet resultSet = statement.executeQuery()) {
                    ImmutableList.Builder<DuckLakeTable> tables = ImmutableList.builder();
                    while (resultSet.next()) {
                        tables.add(mapTable(resultSet, schema));
                    }
                    return tables.build();
                }
            }
        });
    }

    @Override
    public Optional<DuckLakeTable> getTable(long snapshotId, DuckLakeSchema schema, String tableName)
    {
        requireNonNull(schema, "schema is null");
        requireNonNull(tableName, "tableName is null");
        return execute(connection -> {
            SnapshotPredicate predicate = new SnapshotPredicate("t", snapshotId);
            String sql = "SELECT table_id, schema_id, table_name, path, path_is_relative FROM " + qualify("ducklake_table") + " t" +
                    " WHERE t.schema_id = ? AND " + predicate.sql() + " AND t.table_name = ?";
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setLong(1, schema.getSchemaId());
                int index = bindParameters(statement, 2, predicate.parameters());
                statement.setString(index, tableName);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (!resultSet.next()) {
                        return Optional.empty();
                    }
                    return Optional.of(mapTable(resultSet, schema));
                }
            }
        });
    }

    @Override
    public List<DuckLakeColumnRow> listColumns(long snapshotId, long tableId)
    {
        return execute(connection -> {
            SnapshotPredicate predicate = new SnapshotPredicate("c", snapshotId);
            String sql = "SELECT column_id, column_order, column_name, column_type, initial_default, nulls_allowed, parent_column" +
                    " FROM " + qualify("ducklake_column") + " c" +
                    " WHERE c.table_id = ? AND " + predicate.sql() + " ORDER BY column_order";
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setLong(1, tableId);
                bindParameters(statement, 2, predicate.parameters());
                try (ResultSet resultSet = statement.executeQuery()) {
                    ImmutableList.Builder<DuckLakeColumnRow> columns = ImmutableList.builder();
                    while (resultSet.next()) {
                        columns.add(mapColumn(resultSet));
                    }
                    return columns.build();
                }
            }
        });
    }

    @Override
    public List<DuckLakePartitionField> listPartitionFields(long snapshotId, long tableId)
    {
        return execute(connection -> {
            SnapshotPredicate predicate = new SnapshotPredicate("p", snapshotId);
            String sql = "SELECT c.partition_key_index, c.column_id, c.transform" +
                    " FROM " + qualify("ducklake_partition_info") + " p" +
                    " JOIN " + qualify("ducklake_partition_column") + " c ON c.partition_id = p.partition_id AND c.table_id = p.table_id" +
                    " WHERE p.table_id = ? AND " + predicate.sql() +
                    " ORDER BY c.partition_key_index";
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setLong(1, tableId);
                bindParameters(statement, 2, predicate.parameters());
                try (ResultSet resultSet = statement.executeQuery()) {
                    ImmutableList.Builder<DuckLakePartitionField> fields = ImmutableList.builder();
                    while (resultSet.next()) {
                        fields.add(mapPartitionField(resultSet));
                    }
                    return fields.build();
                }
            }
        });
    }

    @Override
    public List<DuckLakeDataFile> listDataFiles(long snapshotId, DuckLakeTable table)
    {
        requireNonNull(table, "table is null");
        return execute(connection -> {
            List<DataFileRow> fileRows = queryDataFileRows(connection, snapshotId, table);
            if (fileRows.isEmpty()) {
                return ImmutableList.of();
            }

            Map<Long, Map<Integer, Optional<String>>> partitionValuesByFile = queryFilePartitionValues(connection, snapshotId, table.getTableId());
            Map<Long, Map<Long, DuckLakeFileColumnStats>> columnStatsByFile = queryFileColumnStats(connection, snapshotId, table.getTableId());

            ImmutableList.Builder<DuckLakeDataFile> dataFiles = ImmutableList.builder();
            for (DataFileRow row : fileRows) {
                dataFiles.add(new DuckLakeDataFile(
                        row.dataFileId,
                        row.path,
                        row.fileFormat,
                        row.recordCount,
                        row.fileSizeBytes,
                        row.footerSize,
                        row.rowIdStart,
                        row.partitionId,
                        row.encryptionKey,
                        row.mappingId,
                        row.partialMax,
                        row.deleteFile,
                        ImmutableMap.copyOf(partitionValuesByFile.getOrDefault(row.dataFileId, ImmutableMap.of())),
                        ImmutableMap.copyOf(columnStatsByFile.getOrDefault(row.dataFileId, ImmutableMap.of()))));
            }
            return dataFiles.build();
        });
    }

    @Override
    public Optional<DuckLakeTableStats> getTableStats(long tableId)
    {
        return execute(connection -> {
            String sql = "SELECT record_count, file_size_bytes FROM " + qualify("ducklake_table_stats") + " WHERE table_id = ?";
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setLong(1, tableId);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (!resultSet.next()) {
                        return Optional.empty();
                    }
                    long recordCount = resultSet.getLong("record_count");
                    long fileSizeBytes = resultSet.getLong("file_size_bytes");
                    return Optional.of(new DuckLakeTableStats(recordCount, fileSizeBytes));
                }
            }
        });
    }

    @Override
    public List<DuckLakeTableColumnStats> listTableColumnStats(long tableId)
    {
        return execute(connection -> {
            String sql = "SELECT column_id, contains_null, contains_nan, min_value, max_value" +
                    " FROM " + qualify("ducklake_table_column_stats") + " WHERE table_id = ? ORDER BY column_id";
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setLong(1, tableId);
                try (ResultSet resultSet = statement.executeQuery()) {
                    ImmutableList.Builder<DuckLakeTableColumnStats> stats = ImmutableList.builder();
                    while (resultSet.next()) {
                        stats.add(mapTableColumnStats(resultSet));
                    }
                    return stats.build();
                }
            }
        });
    }

    @Override
    public List<DuckLakeInlinedTable> listInlinedTables(long tableId)
    {
        return execute(connection -> {
            String sql = "SELECT table_name, schema_version FROM " + qualify("ducklake_inlined_data_tables") + " WHERE table_id = ? ORDER BY schema_version";
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setLong(1, tableId);
                try (ResultSet resultSet = statement.executeQuery()) {
                    ImmutableList.Builder<DuckLakeInlinedTable> tables = ImmutableList.builder();
                    while (resultSet.next()) {
                        tables.add(new DuckLakeInlinedTable(resultSet.getString("table_name"), resultSet.getLong("schema_version")));
                    }
                    return tables.build();
                }
            }
        });
    }

    @Override
    public DuckLakeInlinedRowSource openInlinedRows(long snapshotId, DuckLakeInlinedTable inlinedTable, List<String> columnNames)
    {
        requireNonNull(inlinedTable, "inlinedTable is null");
        requireNonNull(columnNames, "columnNames is null");
        validateIdentifier(inlinedTable.getTableName());
        columnNames.forEach(JdbcDuckLakeCatalog::validateIdentifier);

        Connection connection = null;
        try {
            connection = connectionFactory.openConnection();
            connection.setAutoCommit(false);
            SnapshotPredicate predicate = new SnapshotPredicate("t", snapshotId);
            String selectList = columnNames.stream()
                    .map(columnName -> "\"" + columnName + "\"")
                    .collect(joining(", "));
            String sql = "SELECT row_id" + (selectList.isEmpty() ? "" : ", " + selectList) +
                    " FROM " + qualify("\"" + inlinedTable.getTableName() + "\"") + " t" +
                    " WHERE " + predicate.sql() +
                    " ORDER BY row_id";
            PreparedStatement statement = connection.prepareStatement(sql);
            try {
                statement.setFetchSize(1000);
                bindParameters(statement, 1, predicate.parameters());
                ResultSet resultSet = statement.executeQuery();
                return new JdbcInlinedRowSource(connection, statement, resultSet);
            }
            catch (SQLException e) {
                statement.close();
                throw e;
            }
        }
        catch (SQLException e) {
            closeQuietly(connection);
            throw new PrestoException(
                    DUCKLAKE_CATALOG_ERROR,
                    format("Failed to query DuckLake catalog at %s: %s", connectionFactory.getSanitizedConnectionUrl(), e.getMessage()),
                    e);
        }
    }

    private String snapshotJoin()
    {
        return qualify("ducklake_snapshot") + " s LEFT JOIN " + qualify("ducklake_snapshot_changes") + " c ON c.snapshot_id = s.snapshot_id";
    }

    private String qualify(String tableName)
    {
        return catalogSchema + "." + tableName;
    }

    /**
     * Returns the global {@code data_path} metadata value, loading and caching it on first use.
     * Reuses {@code connection} for the lookup when the value is not yet cached, rather than
     * opening a second connection.
     */
    private String getDataPath(Connection connection)
            throws SQLException
    {
        String cachedDataPath = dataPath;
        if (cachedDataPath != null) {
            return cachedDataPath;
        }
        String sql = "SELECT value FROM " + qualify("ducklake_metadata") + " WHERE key = 'data_path' AND scope IS NULL";
        try (PreparedStatement statement = connection.prepareStatement(sql);
                ResultSet resultSet = statement.executeQuery()) {
            if (!resultSet.next()) {
                throw new PrestoException(DUCKLAKE_INVALID_METADATA, "DuckLake catalog is missing the data_path metadata entry");
            }
            String loadedDataPath = resultSet.getString("value");
            dataPath = loadedDataPath;
            return loadedDataPath;
        }
    }

    private static DuckLakeSnapshot mapSnapshot(ResultSet resultSet)
            throws SQLException
    {
        long snapshotId = resultSet.getLong("snapshot_id");
        Instant snapshotTime = resultSet.getObject("snapshot_time", OffsetDateTime.class).toInstant();
        long schemaVersion = resultSet.getLong("schema_version");
        Optional<String> author = Optional.ofNullable(resultSet.getString("author"));
        Optional<String> commitMessage = Optional.ofNullable(resultSet.getString("commit_message"));
        Optional<String> changesMade = Optional.ofNullable(resultSet.getString("changes_made"));
        return new DuckLakeSnapshot(snapshotId, snapshotTime, schemaVersion, author, commitMessage, changesMade);
    }

    private static DuckLakeSchema mapSchema(ResultSet resultSet, String resolvedDataPath)
            throws SQLException
    {
        long schemaId = resultSet.getLong("schema_id");
        String schemaName = resultSet.getString("schema_name");
        String path = resultSet.getString("path");
        boolean pathIsRelative = resultSet.getBoolean("path_is_relative");
        String resolvedPath = pathIsRelative ? resolvedDataPath + path : path;
        return new DuckLakeSchema(schemaId, schemaName, resolvedPath);
    }

    private static DuckLakeTable mapTable(ResultSet resultSet, DuckLakeSchema schema)
            throws SQLException
    {
        long tableId = resultSet.getLong("table_id");
        long schemaId = resultSet.getLong("schema_id");
        String tableName = resultSet.getString("table_name");
        String path = resultSet.getString("path");
        boolean pathIsRelative = resultSet.getBoolean("path_is_relative");
        String resolvedPath;
        if (path == null) {
            resolvedPath = schema.getPath();
        }
        else {
            resolvedPath = pathIsRelative ? schema.getPath() + path : path;
        }
        return new DuckLakeTable(tableId, schemaId, tableName, resolvedPath);
    }

    private static DuckLakeColumnRow mapColumn(ResultSet resultSet)
            throws SQLException
    {
        long columnId = resultSet.getLong("column_id");
        long columnOrder = resultSet.getLong("column_order");
        String columnName = resultSet.getString("column_name");
        String columnType = resultSet.getString("column_type");
        Optional<String> initialDefault = Optional.ofNullable(resultSet.getString("initial_default"));
        boolean nullsAllowed = resultSet.getBoolean("nulls_allowed");
        long parentColumn = resultSet.getLong("parent_column");
        OptionalLong parentColumnId = resultSet.wasNull() ? OptionalLong.empty() : OptionalLong.of(parentColumn);
        return new DuckLakeColumnRow(columnId, columnOrder, columnName, columnType, initialDefault, nullsAllowed, parentColumnId);
    }

    private static DuckLakePartitionField mapPartitionField(ResultSet resultSet)
            throws SQLException
    {
        int partitionKeyIndex = (int) resultSet.getLong("partition_key_index");
        long columnId = resultSet.getLong("column_id");
        String transform = resultSet.getString("transform");
        return new DuckLakePartitionField(partitionKeyIndex, columnId, transform);
    }

    private static DuckLakeTableColumnStats mapTableColumnStats(ResultSet resultSet)
            throws SQLException
    {
        long columnId = resultSet.getLong("column_id");
        Optional<Boolean> containsNull = getOptionalBoolean(resultSet, "contains_null");
        Optional<Boolean> containsNan = getOptionalBoolean(resultSet, "contains_nan");
        Optional<String> minValue = Optional.ofNullable(resultSet.getString("min_value"));
        Optional<String> maxValue = Optional.ofNullable(resultSet.getString("max_value"));
        return new DuckLakeTableColumnStats(columnId, containsNull, containsNan, minValue, maxValue);
    }

    /**
     * Runs the data-file query described in {@link #listDataFiles}: {@code ducklake_data_file}
     * LEFT JOINed to the {@code ducklake_delete_file} row (if any) visible at {@code snapshotId}
     * for the same {@code data_file_id}, ordered by {@code file_order} then {@code data_file_id}.
     * Returns everything but the partition values and column stats, which are queried separately
     * and merged in by the caller.
     */
    private List<DataFileRow> queryDataFileRows(Connection connection, long snapshotId, DuckLakeTable table)
            throws SQLException
    {
        SnapshotPredicate deletePredicate = new SnapshotPredicate("d", snapshotId);
        SnapshotPredicate dataPredicate = new SnapshotPredicate("data", snapshotId);
        String sql = "SELECT data.data_file_id, data.path, data.path_is_relative, data.file_format, data.record_count," +
                " data.file_size_bytes, data.footer_size, data.row_id_start, data.partition_id, data.encryption_key," +
                " data.mapping_id, data.partial_max, del.delete_file_id, del.path AS delete_path," +
                " del.path_is_relative AS delete_path_is_relative, del.format AS delete_format, del.delete_count," +
                " del.file_size_bytes AS delete_file_size_bytes, del.footer_size AS delete_footer_size," +
                " del.encryption_key AS delete_encryption_key, del.partial_max AS delete_partial_max" +
                " FROM " + qualify("ducklake_data_file") + " data" +
                " LEFT JOIN (" +
                " SELECT * FROM " + qualify("ducklake_delete_file") + " d" +
                " WHERE d.table_id = ? AND " + deletePredicate.sql() +
                " ) del ON del.data_file_id = data.data_file_id" +
                " WHERE data.table_id = ? AND " + dataPredicate.sql() +
                " ORDER BY data.file_order, data.data_file_id";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            int index = 1;
            statement.setLong(index, table.getTableId());
            index++;
            index = bindParameters(statement, index, deletePredicate.parameters());
            statement.setLong(index, table.getTableId());
            index++;
            bindParameters(statement, index, dataPredicate.parameters());
            try (ResultSet resultSet = statement.executeQuery()) {
                ImmutableList.Builder<DataFileRow> rows = ImmutableList.builder();
                while (resultSet.next()) {
                    rows.add(mapDataFileRow(resultSet, table));
                }
                return rows.build();
            }
        }
    }

    private static DataFileRow mapDataFileRow(ResultSet resultSet, DuckLakeTable table)
            throws SQLException
    {
        long dataFileId = resultSet.getLong("data_file_id");
        String path = resolvePath(resultSet.getString("path"), resultSet.getBoolean("path_is_relative"), table.getPath());
        String fileFormat = resultSet.getString("file_format");
        long recordCount = resultSet.getLong("record_count");
        long fileSizeBytes = resultSet.getLong("file_size_bytes");
        OptionalLong footerSize = getOptionalLong(resultSet, "footer_size");
        OptionalLong rowIdStart = getOptionalLong(resultSet, "row_id_start");
        OptionalLong partitionId = getOptionalLong(resultSet, "partition_id");
        Optional<String> encryptionKey = Optional.ofNullable(resultSet.getString("encryption_key"));
        OptionalLong mappingId = getOptionalLong(resultSet, "mapping_id");
        OptionalLong partialMax = getOptionalLong(resultSet, "partial_max");
        Optional<DuckLakeDeleteFile> deleteFile = mapDeleteFile(resultSet, table);
        return new DataFileRow(dataFileId, path, fileFormat, recordCount, fileSizeBytes, footerSize, rowIdStart,
                partitionId, encryptionKey, mappingId, partialMax, deleteFile);
    }

    private static Optional<DuckLakeDeleteFile> mapDeleteFile(ResultSet resultSet, DuckLakeTable table)
            throws SQLException
    {
        long deleteFileId = resultSet.getLong("delete_file_id");
        if (resultSet.wasNull()) {
            return Optional.empty();
        }
        String path = resolvePath(resultSet.getString("delete_path"), resultSet.getBoolean("delete_path_is_relative"), table.getPath());
        String format = resultSet.getString("delete_format");
        long deleteCount = resultSet.getLong("delete_count");
        long fileSizeBytes = resultSet.getLong("delete_file_size_bytes");
        OptionalLong footerSize = getOptionalLong(resultSet, "delete_footer_size");
        Optional<String> encryptionKey = Optional.ofNullable(resultSet.getString("delete_encryption_key"));
        OptionalLong partialMax = getOptionalLong(resultSet, "delete_partial_max");
        return Optional.of(new DuckLakeDeleteFile(deleteFileId, path, format, deleteCount, fileSizeBytes, footerSize, encryptionKey, partialMax));
    }

    /**
     * Resolves a {@code path}/{@code path_is_relative} pair the same way as the extension's
     * {@code FromRelativePath}: a relative path is joined onto {@code tablePath} (which the caller
     * has already resolved to an absolute location), an absolute path is used unchanged.
     */
    private static String resolvePath(String path, boolean pathIsRelative, String tablePath)
    {
        return pathIsRelative ? tablePath + path : path;
    }

    /**
     * Scoped the same way as {@link #queryDataFileRows}: joined to {@code ducklake_data_file} on
     * {@code data_file_id} and filtered by the same {@code table_id} and snapshot-visibility
     * predicate, so only the partition values of files {@code listDataFiles} is about to return are
     * fetched, rather than every partition value the table has ever had.
     */
    private Map<Long, Map<Integer, Optional<String>>> queryFilePartitionValues(Connection connection, long snapshotId, long tableId)
            throws SQLException
    {
        SnapshotPredicate predicate = new SnapshotPredicate("data", snapshotId);
        String sql = "SELECT v.data_file_id, v.partition_key_index, v.partition_value" +
                " FROM " + qualify("ducklake_file_partition_value") + " v" +
                " JOIN " + qualify("ducklake_data_file") + " data ON data.data_file_id = v.data_file_id" +
                " WHERE data.table_id = ? AND " + predicate.sql();
        Map<Long, Map<Integer, Optional<String>>> partitionValuesByFile = new HashMap<>();
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setLong(1, tableId);
            bindParameters(statement, 2, predicate.parameters());
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    long dataFileId = resultSet.getLong("data_file_id");
                    int partitionKeyIndex = (int) resultSet.getLong("partition_key_index");
                    Optional<String> partitionValue = Optional.ofNullable(resultSet.getString("partition_value"));
                    partitionValuesByFile.computeIfAbsent(dataFileId, key -> new HashMap<>()).put(partitionKeyIndex, partitionValue);
                }
            }
        }
        return partitionValuesByFile;
    }

    /**
     * Scoped the same way as {@link #queryFilePartitionValues}: joined to {@code
     * ducklake_data_file} and filtered by {@code table_id} and snapshot visibility, so only the
     * column stats of files {@code listDataFiles} is about to return are fetched.
     */
    private Map<Long, Map<Long, DuckLakeFileColumnStats>> queryFileColumnStats(Connection connection, long snapshotId, long tableId)
            throws SQLException
    {
        SnapshotPredicate predicate = new SnapshotPredicate("data", snapshotId);
        String sql = "SELECT s.data_file_id, s.column_id, s.column_size_bytes, s.value_count, s.null_count, s.min_value, s.max_value, s.contains_nan" +
                " FROM " + qualify("ducklake_file_column_stats") + " s" +
                " JOIN " + qualify("ducklake_data_file") + " data ON data.data_file_id = s.data_file_id" +
                " WHERE data.table_id = ? AND " + predicate.sql();
        Map<Long, Map<Long, DuckLakeFileColumnStats>> statsByFile = new HashMap<>();
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setLong(1, tableId);
            bindParameters(statement, 2, predicate.parameters());
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    long dataFileId = resultSet.getLong("data_file_id");
                    DuckLakeFileColumnStats stats = mapFileColumnStats(resultSet);
                    statsByFile.computeIfAbsent(dataFileId, key -> new HashMap<>()).put(stats.getColumnId(), stats);
                }
            }
        }
        return statsByFile;
    }

    private static DuckLakeFileColumnStats mapFileColumnStats(ResultSet resultSet)
            throws SQLException
    {
        long columnId = resultSet.getLong("column_id");
        OptionalLong columnSizeBytes = getOptionalLong(resultSet, "column_size_bytes");
        OptionalLong valueCount = getOptionalLong(resultSet, "value_count");
        OptionalLong nullCount = getOptionalLong(resultSet, "null_count");
        Optional<String> minValue = Optional.ofNullable(resultSet.getString("min_value"));
        Optional<String> maxValue = Optional.ofNullable(resultSet.getString("max_value"));
        Optional<Boolean> containsNan = getOptionalBoolean(resultSet, "contains_nan");
        return new DuckLakeFileColumnStats(columnId, columnSizeBytes, valueCount, nullCount, minValue, maxValue, containsNan);
    }

    private static OptionalLong getOptionalLong(ResultSet resultSet, String columnName)
            throws SQLException
    {
        long value = resultSet.getLong(columnName);
        return resultSet.wasNull() ? OptionalLong.empty() : OptionalLong.of(value);
    }

    private static Optional<Boolean> getOptionalBoolean(ResultSet resultSet, String columnName)
            throws SQLException
    {
        boolean value = resultSet.getBoolean(columnName);
        return resultSet.wasNull() ? Optional.empty() : Optional.of(value);
    }

    private static void validateIdentifier(String identifier)
    {
        if (identifier.contains("\"")) {
            throw new IllegalArgumentException("DuckLake identifier must not contain a double quote: " + identifier);
        }
    }

    private static void closeQuietly(Connection connection)
    {
        if (connection == null) {
            return;
        }
        try {
            connection.close();
        }
        catch (SQLException ignored) {
        }
    }

    private static int bindParameters(PreparedStatement statement, int startIndex, List<Object> parameters)
            throws SQLException
    {
        int index = startIndex;
        for (Object parameter : parameters) {
            statement.setObject(index, parameter);
            index++;
        }
        return index;
    }

    private <T> T execute(SqlFunction<T> function)
    {
        try (Connection connection = connectionFactory.openConnection()) {
            return function.apply(connection);
        }
        catch (SQLException e) {
            throw new PrestoException(
                    DUCKLAKE_CATALOG_ERROR,
                    format("Failed to query DuckLake catalog at %s: %s", connectionFactory.getSanitizedConnectionUrl(), e.getMessage()),
                    e);
        }
    }

    private interface SqlFunction<T>
    {
        T apply(Connection connection)
                throws SQLException;
    }

    /**
     * The scalar columns of one {@code ducklake_data_file} row (with its resolved delete file, if
     * any) read by {@link #queryDataFileRows}, before the partition values and column stats
     * (queried and merged in separately by {@link #listDataFiles}) are attached.
     */
    private static final class DataFileRow
    {
        final long dataFileId;
        final String path;
        final String fileFormat;
        final long recordCount;
        final long fileSizeBytes;
        final OptionalLong footerSize;
        final OptionalLong rowIdStart;
        final OptionalLong partitionId;
        final Optional<String> encryptionKey;
        final OptionalLong mappingId;
        final OptionalLong partialMax;
        final Optional<DuckLakeDeleteFile> deleteFile;

        DataFileRow(
                long dataFileId,
                String path,
                String fileFormat,
                long recordCount,
                long fileSizeBytes,
                OptionalLong footerSize,
                OptionalLong rowIdStart,
                OptionalLong partitionId,
                Optional<String> encryptionKey,
                OptionalLong mappingId,
                OptionalLong partialMax,
                Optional<DuckLakeDeleteFile> deleteFile)
        {
            this.dataFileId = dataFileId;
            this.path = path;
            this.fileFormat = fileFormat;
            this.recordCount = recordCount;
            this.fileSizeBytes = fileSizeBytes;
            this.footerSize = footerSize;
            this.rowIdStart = rowIdStart;
            this.partitionId = partitionId;
            this.encryptionKey = encryptionKey;
            this.mappingId = mappingId;
            this.partialMax = partialMax;
            this.deleteFile = deleteFile;
        }
    }
}
