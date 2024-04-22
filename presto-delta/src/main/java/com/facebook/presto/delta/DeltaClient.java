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
import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.TableNotFoundException;
import io.delta.kernel.client.TableClient;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.client.DefaultTableClient;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.utils.CloseableIterator;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.inject.Inject;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.delta.DeltaErrorCode.DELTA_UNSUPPORTED_DATA_FORMAT;
import static com.facebook.presto.delta.DeltaTable.DataFormat.PARQUET;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
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
        Optional<TableClient> deltaTableClient = loadDeltaTableClient(session, location,
                schemaTableName);
        if (!deltaTableClient.isPresent()) {
            return Optional.empty();
        }

        Table deltaTable = loadDeltaTable(location.toString(), deltaTableClient.get());
        // Fetch the snapshot info for given snapshot version. If no snapshot version is given, get the latest snapshot info.
        // Lock the snapshot version here and use it later in the rest of the query (such as fetching file list etc.).
        // If we don't lock the snapshot version here, the query may end up with schema from one version and data files from another
        // version when the underlying delta table is changing while the query is running.
        Snapshot snapshot;
        if (snapshotId.isPresent()) {
            snapshot = getSnapshotById(deltaTable, deltaTableClient.get(), snapshotId.get(), schemaTableName);
        }
        else if (snapshotAsOfTimestampMillis.isPresent()) {
            snapshot = getSnapshotAsOfTimestamp(deltaTable, deltaTableClient.get(),
                    snapshotAsOfTimestampMillis.get(), schemaTableName);
        }
        else {
            try {
                snapshot = deltaTable.getLatestSnapshot(deltaTableClient.get()); // get the latest snapshot
            }
            catch (TableNotFoundException e) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR,
                        format("Could not move to latest snapshot on table '%s.%s'", schemaTableName.getSchemaName(),
                                schemaTableName.getTableName()));
            }
        }

        if (snapshot instanceof SnapshotImpl) {
            String format = ((SnapshotImpl) snapshot).getMetadata().getFormat().getProvider();
            if (!PARQUET.name().equalsIgnoreCase(format)) {
                throw new PrestoException(DELTA_UNSUPPORTED_DATA_FORMAT,
                        format("Delta table %s has unsupported data format: %s. Currently only Parquet data format is supported", schemaTableName, format));
            }
        }

        return Optional.of(new DeltaTable(
                schemaTableName.getSchemaName(),
                schemaTableName.getTableName(),
                tableLocation,
                Optional.of(snapshot.getVersion(deltaTableClient.get())), // lock the snapshot version
                getSchema(config, schemaTableName, deltaTable, deltaTableClient.get())));
    }

    /**
     * Get the list of files corresponding to the given Delta table.
     *
     * @return Closeable iterator of files. It is responsibility of the caller to close the iterator.
     */
    public CloseableIterator<FilteredColumnarBatch> listFiles(ConnectorSession session, DeltaTable deltaTable)
    {
        Optional<TableClient> deltaTableClient = loadDeltaTableClient(session,
                new Path(deltaTable.getTableLocation()),
                new SchemaTableName(deltaTable.getSchemaName(), deltaTable.getTableName()));
        if (!deltaTableClient.isPresent()) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR,
                    format("Could not obtain Delta table client in '%s'", deltaTable.getTableLocation()));
        }
        checkArgument(deltaTable.getSnapshotId().isPresent(), "Snapshot id is missing from the Delta table");
        Table sourceTable = loadDeltaTable(deltaTable.getTableLocation(), deltaTableClient.get());

        try {
            return sourceTable.getLatestSnapshot(deltaTableClient.get()).getScanBuilder(deltaTableClient.get()).build()
                    .getScanFiles(deltaTableClient.get());
        }
        catch (TableNotFoundException e) {
            throw new PrestoException(NOT_FOUND,
                    format("Delta table not found in '%s'", deltaTable.getTableLocation()));
        }
    }

    private Optional<TableClient> loadDeltaTableClient(ConnectorSession session, Path tableLocation,
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
            return Optional.of(DefaultTableClient.create(fileSystem.getConf()));
        }
        catch (IOException ioException) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to load Delta table: " + ioException.getMessage(), ioException);
        }
    }

    private Table loadDeltaTable(String tableLocation, TableClient deltaTableClient)
    {
        try {
            return Table.forPath(deltaTableClient, tableLocation);
        }
        catch (TableNotFoundException e) {
            throw new PrestoException(NOT_FOUND,
                    format("Could not obtain Delta table client in '%s'", tableLocation));
        }
    }

    private static Snapshot getSnapshotById(Table deltaTable, TableClient deltaTableClient, long snapshotId, SchemaTableName schemaTableName)
    {
        try {
            return deltaTable.getLatestSnapshot(deltaTableClient);
        }
        catch (IllegalArgumentException exception) {
            throw new PrestoException(
                    NOT_FOUND,
                    format("Snapshot version %d does not exist in Delta table '%s'.", snapshotId, schemaTableName),
                    exception);
        }
        catch (TableNotFoundException e) {
            throw new PrestoException(NOT_FOUND,
                    format(TABLE_NOT_FOUND_ERROR_TEMPLATE, schemaTableName.getSchemaName(),
                            schemaTableName.getTableName()));
        }
    }

    private static Snapshot getSnapshotAsOfTimestamp(Table deltaTable, TableClient deltaTableClient,
                                                     long snapshotAsOfTimestampMillis, SchemaTableName schemaTableName)
    {
        try {
            return deltaTable.getLatestSnapshot(deltaTableClient);
        }
        catch (IllegalArgumentException exception) {
            throw new PrestoException(
                    NOT_FOUND,
                    format(
                            "There is no snapshot exists in Delta table '%s' that is created on or before '%s'",
                            schemaTableName,
                            Instant.ofEpochMilli(snapshotAsOfTimestampMillis)),
                    exception);
        }
        catch (TableNotFoundException e) {
            throw new PrestoException(NOT_FOUND,
                    format(TABLE_NOT_FOUND_ERROR_TEMPLATE, schemaTableName.getSchemaName(),
                            schemaTableName.getTableName()));
        }
    }

    /**
     * Utility method that returns the columns in given Delta metadata. Returned columns include regular and partition types.
     * Data type from Delta is mapped to appropriate Presto data type.
     */
    private static List<DeltaColumn> getSchema(DeltaConfig config, SchemaTableName tableName, Table deltaTable,
                                               TableClient deltaTableClient)
    {
        try {
            Snapshot latestSnapshot = deltaTable.getLatestSnapshot(deltaTableClient);
            CloseableIterator<FilteredColumnarBatch> columnBatches = latestSnapshot.getScanBuilder(deltaTableClient)
                    .build().getScanFiles(deltaTableClient);
            Row row = null;
            while (columnBatches.hasNext()) {
                CloseableIterator<Row> rows = columnBatches.next().getRows();
                if (rows.hasNext()) {
                    row = rows.next();
                    break;
                }
            }
            Map<String, String> partitionValues = row != null ?
                    InternalScanFileUtils.getPartitionValues(row) : new HashMap<>(0);
            return latestSnapshot.getSchema(deltaTableClient).fields().stream()
                    .map(field -> {
                        String columnName = config.isCaseSensitivePartitionsEnabled() ? field.getName() :
                                field.getName().toLowerCase(US);
                        TypeSignature prestoType = DeltaTypeUtils.convertDeltaDataTypePrestoDataType(tableName,
                                columnName, field.getDataType());
                        return new DeltaColumn(
                                columnName,
                                prestoType,
                                field.isNullable(),
                                partitionValues.containsKey(columnName));
                    }).collect(Collectors.toList());
        }
        catch (TableNotFoundException e) {
            throw new PrestoException(NOT_FOUND,
                    format(TABLE_NOT_FOUND_ERROR_TEMPLATE, tableName.getSchemaName(), tableName.getTableName()));
        }
    }
}
