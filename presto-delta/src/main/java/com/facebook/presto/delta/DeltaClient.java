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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.StandardErrorCode;
import io.delta.kernel.Scan;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.TableNotFoundException;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.ScanImpl;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.utils.CloseableIterator;
import jakarta.inject.Inject;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
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
    private static final Logger log = Logger.get(DeltaClient.class);
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
                getSchema(config, schemaTableName, deltaEngine.get(), snapshot)));
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

    /**
     * Returns all active file entries in the snapshot with per-file statistics parsed from
     * {@code add.stats}. Separate from {@link #listFiles} — no predicate pushdown, stats only.
     */
    public List<DeltaFileEntry> listFileEntries(ConnectorSession session, DeltaTable deltaTable)
    {
        requireNonNull(deltaTable, "deltaTable is null");
        checkArgument(deltaTable.getSnapshotId().isPresent(), "Snapshot id is missing from the Delta table");

        Optional<Engine> deltaEngineOpt = loadDeltaEngine(session,
                new Path(deltaTable.getTableLocation()),
                new SchemaTableName(deltaTable.getSchemaName(), deltaTable.getTableName()));
        if (!deltaEngineOpt.isPresent()) {
            throw new PrestoException(DeltaErrorCode.DELTA_ERROR_LOADING_METADATA,
                    format("Could not obtain Delta engine in '%s'", deltaTable.getTableLocation()));
        }
        Engine deltaEngine = deltaEngineOpt.get();
        Table sourceTable = loadDeltaTable(deltaTable.getTableLocation(), deltaEngine);

        List<DeltaFileEntry> result = new ArrayList<>();
        // getScanFiles(engine, includeStats=true) is only on internal ScanImpl, not the public
        // Scan interface. Cast is unavoidable in Kernel 4.0.0.
        // TODO: remove once Kernel exposes this on the public Scan interface.

        Scan rawScan = sourceTable
                .getSnapshotAsOfVersion(deltaEngine, deltaTable.getSnapshotId().get())
                .getScanBuilder()
                .build();
        if (!(rawScan instanceof ScanImpl)) {
            throw new PrestoException(DeltaErrorCode.DELTA_ERROR_LOADING_METADATA, format(
                    "Cannot read Delta file statistics: expected ScanImpl but got %s. "
                    + "Delta Kernel may have changed its internal API — remove the ScanImpl cast "
                    + "in DeltaClient.listFileEntries() and use the public stats API instead.",
                    rawScan.getClass().getName()));
        }
        ScanImpl scan = (ScanImpl) rawScan;
        try (CloseableIterator<FilteredColumnarBatch> scanBatches =
                scan.getScanFiles(deltaEngine, true /* includeStats */)) {
            while (scanBatches.hasNext()) {
                FilteredColumnarBatch batch = scanBatches.next();
                try (CloseableIterator<Row> rows = batch.getRows()) {
                    while (rows.hasNext()) {
                        result.add(toFileEntry(rows.next()));
                    }
                }
            }
        }
        catch (TableNotFoundException e) {
            throw new PrestoException(StandardErrorCode.NOT_FOUND,
                    format("Delta table not found in '%s'", deltaTable.getTableLocation()), e);
        }
        catch (IOException e) {
            throw new UncheckedIOException("Could not close scan file iterator", e);
        }
        return result;
    }

    /** Converts a scan-file {@link Row} to a {@link DeltaFileEntry}. Stats errors are swallowed. */
    private static DeltaFileEntry toFileEntry(Row row)
    {
        io.delta.kernel.utils.FileStatus fileStatus = InternalScanFileUtils.getAddFileStatus(row);
        Map<String, String> partitionValues = InternalScanFileUtils.getPartitionValues(row);

        Optional<DeltaJsonFileStatistics> stats = Optional.empty();
        try {
            Row addFileRow = row.getStruct(InternalScanFileUtils.ADD_FILE_ORDINAL);
            if (!addFileRow.isNullAt(InternalScanFileUtils.ADD_FILE_STATS_ORDINAL)) {
                String statsJson = addFileRow.getString(InternalScanFileUtils.ADD_FILE_STATS_ORDINAL);
                stats = DeltaJsonFileStatistics.create(statsJson);
            }
        }
        catch (Exception e) {
            log.debug("Could not read stats for file %s, skipping: %s", fileStatus.getPath(), e.getMessage());
        }

        return new DeltaFileEntry(
                fileStatus.getPath(),
                fileStatus.getSize(),
                fileStatus.getModificationTime(),
                partitionValues,
                stats);
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
    private static List<DeltaColumn> getSchema(DeltaConfig config, SchemaTableName tableName, Engine deltaEngine,
                                               Snapshot snapshot)
    {
        // Read partition columns from snapshot metaData — reliable even when no data files exist.
        Set<String> partitionColNames = new HashSet<>(
                ((SnapshotImpl) snapshot).getPartitionColumnNames());

        return snapshot.getSchema().fields().stream()
                .map(field -> {
                    String columnName = config.isCaseSensitivePartitionsEnabled() ? field.getName() :
                            field.getName().toLowerCase(US);
                    TypeSignature prestoType = DeltaTypeUtils.convertDeltaDataTypePrestoDataType(tableName,
                            columnName, field.getDataType());
                    boolean isPartition = partitionColNames.stream()
                            .anyMatch(p -> p.equalsIgnoreCase(columnName));
                    return new DeltaColumn(
                            DeltaColumnMetadataUtil.getColumnIdFromMetadata(field.getMetadata()),
                            DeltaColumnMetadataUtil.getPhysicalNameFromMetadata(field.getMetadata()),
                            columnName,
                            prestoType,
                            field.isNullable(),
                            isPartition);
                }).collect(Collectors.toList());
    }
}
