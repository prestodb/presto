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
package com.facebook.presto.delta;

import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.StandardErrorCode;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.TableNotFoundException;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.utils.CloseableIterator;
import jakarta.inject.Inject;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.delta.DeltaTable.DataFormat.PARQUET;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Locale.US;
import static java.util.Objects.requireNonNull;

/**
 * Class to interact with Delta lake table APIs.
 */
public class DeltaClient
{
    private static final String TABLE_NOT_FOUND_ERROR_TEMPLATE = "Delta table (%s.%s) no longer exists.";
    private final HdfsEnvironment hdfsEnvironment;

    @Inject
    public DeltaClient(HdfsEnvironment hdfsEnvironment)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
    }

    /**
     * Load the delta table.
     *
     * @param session                     Current user session
     * @param schemaTableName             Schema and table name referred to as in the query
     * @param tableLocation               Location of the Delta table on storage
     * @param snapshotId                  Id of the snapshot to read from the Delta table
     * @param snapshotAsOfTimestampMillis Latest snapshot as of given timestamp
     * @return If the table is found return {@link DeltaTable}.
     */
    public Optional<DeltaTable> getTable(
            DeltaConfig config,
            ConnectorSession session,
            SchemaTableName schemaTableName,
            String tableLocation,
            Optional<Long> snapshotId,
            Optional<Long> snapshotAsOfTimestampMillis)
    {
        Path location = new Path(tableLocation);
        Optional<Engine> deltaEngine = loadDeltaEngine(session, location, schemaTableName);
        if (!deltaEngine.isPresent()) {
            return Optional.empty();
        }

        Table deltaTable = loadDeltaTable(location.toString(), deltaEngine.get());
        Snapshot snapshot = getSnapshot(deltaTable, deltaEngine.get(), schemaTableName, snapshotId,
                snapshotAsOfTimestampMillis);
        return Optional.of(new DeltaTable(
                schemaTableName.getSchemaName(),
                schemaTableName.getTableName(),
                tableLocation,
                Optional.of(snapshot.getVersion()), // lock the snapshot version
                getSchema(config, schemaTableName, snapshot),
                getColumnMappingMode(snapshot)));
    }

    /**
     * Reads the {@code delta.columnMapping.mode} table property from the
     * snapshot's metadata. Falls back to {@link DeltaTable#COLUMN_MAPPING_MODE_NONE}
     * when the property is absent (older tables, or column-mapping never
     * enabled). The Delta protocol uses lower-case strings, so we normalize
     * to lower case for stable equality on the wire.
     */
    private static String getColumnMappingMode(Snapshot snapshot)
    {
        if (!(snapshot instanceof SnapshotImpl)) {
            return DeltaTable.COLUMN_MAPPING_MODE_NONE;
        }
        Map<String, String> configuration =
                ((SnapshotImpl) snapshot).getMetadata().getConfiguration();
        String mode = configuration.get("delta.columnMapping.mode");
        if (mode == null || mode.isEmpty()) {
            return DeltaTable.COLUMN_MAPPING_MODE_NONE;
        }
        return mode.toLowerCase(US);
    }

    private Snapshot getSnapshot(
            Table deltaTable,
            Engine deltaEngine,
            SchemaTableName schemaTableName,
            Optional<Long> snapshotId,
            Optional<Long> snapshotAsOfTimestampMillis)
    {
        // Fetch the snapshot info for given snapshot version. If no snapshot version is given, get the latest snapshot info.
        // Lock the snapshot version here and use it later in the rest of the query (such as fetching file list etc.).
        // If we don't lock the snapshot version here, the query may end up with schema from one version and data files from another
        // version when the underlying delta table is changing while the query is running.
        Snapshot snapshot;
        if (snapshotId.isPresent()) {
            snapshot = getSnapshotById(deltaTable, deltaEngine, snapshotId.get(), schemaTableName);
        }
        else if (snapshotAsOfTimestampMillis.isPresent()) {
            snapshot = getSnapshotAsOfTimestamp(deltaTable, deltaEngine,
                    snapshotAsOfTimestampMillis.get(), schemaTableName);
        }
        else {
            try {
                snapshot = deltaTable.getLatestSnapshot(deltaEngine); // get the latest snapshot
            }
            catch (TableNotFoundException e) {
                throw new PrestoException(StandardErrorCode.NOT_FOUND,
                        format("Could not move to latest snapshot on table '%s.%s'", schemaTableName.getSchemaName(),
                                schemaTableName.getTableName()), e);
            }
        }

        if (snapshot instanceof SnapshotImpl) {
            String format = ((SnapshotImpl) snapshot).getMetadata().getFormat().getProvider();
            if (!PARQUET.name().equalsIgnoreCase(format)) {
                throw new PrestoException(DeltaErrorCode.DELTA_UNSUPPORTED_DATA_FORMAT,
                        format("Delta table %s has unsupported data format: %s. Only the Parquet data format is supported", schemaTableName, format));
            }
        }
        return snapshot;
    }

    /**
     * Get the list of files corresponding to the given Delta table.
     *
     * @return Closeable iterator of files. It is responsibility of the caller to close the iterator.
     */
    public CloseableIterator<FilteredColumnarBatch> listFiles(ConnectorSession session, DeltaTable deltaTable)
    {
        requireNonNull(deltaTable, "deltaTable is null");
        checkArgument(deltaTable.getSnapshotId().isPresent(), "Snapshot id is missing from the Delta table");
        Optional<Engine> deltaEngine = loadDeltaEngine(session,
                new Path(deltaTable.getTableLocation()),
                new SchemaTableName(deltaTable.getSchemaName(), deltaTable.getTableName()));
        if (!deltaEngine.isPresent()) {
            throw new PrestoException(DeltaErrorCode.DELTA_ERROR_LOADING_METADATA,
                    format("Could not obtain Delta engine in '%s'", deltaTable.getTableLocation()));
        }
        Table sourceTable = loadDeltaTable(deltaTable.getTableLocation(), deltaEngine.get());

        if (!deltaTable.getSnapshotId().isPresent()) {
            throw new PrestoException(DeltaErrorCode.DELTA_ERROR_LOADING_SNAPSHOT, "Could not obtain snapshot id");
        }

        try {
            return sourceTable.getSnapshotAsOfVersion(deltaEngine.get(),
                            deltaTable.getSnapshotId().get()).getScanBuilder().build()
                    .getScanFiles(deltaEngine.get());
        }
        catch (TableNotFoundException e) {
            throw new PrestoException(StandardErrorCode.NOT_FOUND,
                    format("Delta table not found in '%s'", deltaTable.getTableLocation()), e);
        }
    }

    private Optional<Engine> loadDeltaEngine(ConnectorSession session, Path tableLocation,
                                                       SchemaTableName schemaTableName)
    {
        try {
            HdfsContext hdfsContext = new HdfsContext(
                    session,
                    schemaTableName.getSchemaName(),
                    schemaTableName.getTableName(),
                    tableLocation.toString(),
                    false);
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(hdfsContext, tableLocation);
            if (!fileSystem.isDirectory(tableLocation)) {
                return Optional.empty();
            }
            return Optional.of(DefaultEngine.create(fileSystem.getConf()));
        }
        catch (IOException ioException) {
            throw new PrestoException(DeltaErrorCode.DELTA_ERROR_LOADING_METADATA,
                    "Failed to load Delta table: " + ioException.getMessage(), ioException);
        }
    }

    private Table loadDeltaTable(String tableLocation, Engine deltaEngine)
    {
        return Table.forPath(deltaEngine, tableLocation);
    }

    private static Snapshot getSnapshotById(Table deltaTable, Engine deltaEngine, long snapshotId, SchemaTableName schemaTableName)
    {
        try {
            return deltaTable.getSnapshotAsOfVersion(deltaEngine, snapshotId);
        }
        catch (IllegalArgumentException exception) {
            throw new PrestoException(
                    StandardErrorCode.NOT_FOUND,
                    format("Snapshot version %d does not exist in Delta table '%s'.", snapshotId, schemaTableName),
                    exception);
        }
        catch (TableNotFoundException e) {
            throw new PrestoException(StandardErrorCode.NOT_FOUND,
                    format(TABLE_NOT_FOUND_ERROR_TEMPLATE, schemaTableName.getSchemaName(),
                            schemaTableName.getTableName()));
        }
    }

    private static Snapshot getSnapshotAsOfTimestamp(Table deltaTable, Engine deltaEngine,
                                                     long snapshotAsOfTimestampMillis, SchemaTableName schemaTableName)
    {
        try {
            return deltaTable.getSnapshotAsOfTimestamp(deltaEngine, snapshotAsOfTimestampMillis);
        }
        catch (IllegalArgumentException exception) {
            throw new PrestoException(
                    StandardErrorCode.NOT_FOUND,
                    format(
                            "There is no snapshot exists in Delta table '%s' that is created on or before '%s'",
                            schemaTableName,
                            Instant.ofEpochMilli(snapshotAsOfTimestampMillis)),
                    exception);
        }
        catch (TableNotFoundException e) {
            throw new PrestoException(StandardErrorCode.NOT_FOUND,
                    format(TABLE_NOT_FOUND_ERROR_TEMPLATE, schemaTableName.getSchemaName(),
                            schemaTableName.getTableName()));
        }
    }

    /**
     * Utility method that returns the columns in given Delta metadata. Returned columns include regular and partition types.
     * Data type from Delta is mapped to appropriate Presto data type.
     */
    private static List<DeltaColumn> getSchema(DeltaConfig config, SchemaTableName tableName, Snapshot snapshot)
    {
        Set<String> partitionColumns = snapshot.getPartitionColumnNames().stream()
                .map(columnName -> config.isCaseSensitivePartitionsEnabled() ? columnName : columnName.toLowerCase(US))
                .collect(Collectors.toSet());
        return snapshot.getSchema().fields().stream()
                .map(field -> {
                    String columnName = config.isCaseSensitivePartitionsEnabled() ? field.getName() :
                            field.getName().toLowerCase(US);
                    TypeSignature prestoType = DeltaTypeUtils.convertDeltaDataTypePrestoDataType(tableName,
                            columnName, field.getDataType());
                    TypeSignature physicalType = DeltaTypeUtils.convertDeltaDataTypePrestoPhysicalType(tableName,
                            columnName, field.getDataType());
                    return new DeltaColumn(
                            DeltaColumnMetadataUtil.getColumnIdFromMetadata(field.getMetadata()),
                            DeltaColumnMetadataUtil.getPhysicalNameFromMetadata(field.getMetadata()),
                            columnName,
                            prestoType,
                            physicalType,
                            field.isNullable(),
                            partitionColumns.contains(columnName));
                }).collect(Collectors.toList());
    }
}
