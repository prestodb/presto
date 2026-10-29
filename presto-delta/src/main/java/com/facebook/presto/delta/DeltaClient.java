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
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.StandardErrorCode;
import io.delta.kernel.Scan;
import io.delta.kernel.ScanBuilder;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.clustering.ClusteringColumnInfo;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.TableNotFoundException;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.utils.CloseableIterator;
import jakarta.inject.Inject;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
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
    private static final Logger logger = Logger.get(DeltaExpressionUtils.class);
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
        if (deltaEngine.isEmpty()) {
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
                getSchema(config, schemaTableName, snapshot)));
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

        String format = snapshot.getMetadata().getFormat().getProvider();
        if (!PARQUET.name().equalsIgnoreCase(format)) {
            throw new PrestoException(DeltaErrorCode.DELTA_UNSUPPORTED_DATA_FORMAT,
                    format("Delta table %s has unsupported data format: %s. Only the Parquet data format is supported", schemaTableName, format));
        }
        return snapshot;
    }

    /**
     * Get the list of files corresponding to the given Delta table.
     *
     * @return Closeable iterator of files. It is responsibility of the caller to close the iterator.
     */
    public CloseableIterator<FilteredColumnarBatch> listFiles(ConnectorSession session, DeltaConfig deltaConfig,
            DeltaTableLayoutHandle deltaTableHandle, TypeManager typeManager)
    {
        DeltaTable deltaTable = deltaTableHandle.getTable().getDeltaTable();
        requireNonNull(deltaTable, "deltaTable is null");
        checkArgument(deltaTable.getSnapshotId().isPresent(), "Snapshot id is missing from the Delta table");
        Optional<Engine> deltaEngine = loadDeltaEngine(session,
                new Path(deltaTable.getTableLocation()),
                new SchemaTableName(deltaTable.getSchemaName(), deltaTable.getTableName()));
        if (deltaEngine.isEmpty()) {
            throw new PrestoException(DeltaErrorCode.DELTA_ERROR_LOADING_METADATA,
                    format("Could not obtain Delta engine in '%s'", deltaTable.getTableLocation()));
        }
        Table sourceTable = loadDeltaTable(deltaTable.getTableLocation(), deltaEngine.get());

        if (deltaTable.getSnapshotId().isEmpty()) {
            throw new PrestoException(DeltaErrorCode.DELTA_ERROR_LOADING_SNAPSHOT, "Could not obtain snapshot id");
        }

        Scan scan = null;
        try {
            boolean enablePushdown = DeltaSessionProperties.isDeltaKernelPredicatePushdownEnabled(session) == null ?
                    deltaConfig.isKernelPredicatePushdownEnabled() : DeltaSessionProperties.isDeltaKernelPredicatePushdownEnabled(session);
            ScanBuilder scanBuilder = sourceTable.getSnapshotAsOfVersion(deltaEngine.get(),
                    deltaTable.getSnapshotId().get()).getScanBuilder();
            if (enablePushdown) {
                try {
                    TupleDomain<DeltaColumnHandle> tupleDomain = deltaTableHandle.getPredicate();
                    Optional<List<TupleDomain.ColumnDomain<DeltaColumnHandle>>> columnDomains = tupleDomain.getColumnDomains();
                    ScanFilter.Builder scanFilterBuilder = new ScanFilter.Builder(typeManager);
                    columnDomains.ifPresent(columnDomainList -> columnDomainList.forEach((scanFilterBuilder::withDomain)));
                    Optional<Predicate> predicate = scanFilterBuilder.build().getPredicate();
                    logger.debug("Pushdown enabled: true - Predicate pushdown: " + (predicate.isPresent() ? predicate.get().toString() : " - "));
                    scan = predicate.isPresent() ?
                            scanBuilder.withFilter(predicate.get()).build() : scanBuilder.build();
                }
                catch (UnsupportedOperationException e) {
                    logger.debug("Pushdown enabled: true - Skipping predicate pushdown: " + e.getMessage());
                    scan = scanBuilder.build();
                }
            }
            else {
                logger.debug("Pushdown enabled: false");
                scan = scanBuilder.build();
            }

            return scan.getScanFiles(deltaEngine.get());
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
        // Extract partition columns directly from metadata without scanning files
        List<String> partitionColumns = snapshot.getPartitionColumnNames();

        // Extract clustering columns if no partitioning
        Set<String> clusterColumns = new HashSet<>(0);
        if (partitionColumns.isEmpty()) {
            // check for Liquid Clustering as it will only be active if the data is not partitioned
            Optional<List<ClusteringColumnInfo>> optionalClusteringColumnInfoList = snapshot.getClusteringColumnInfos();
            if (optionalClusteringColumnInfoList.isPresent()) {
                List<ClusteringColumnInfo> clusteringColumnInfoList = optionalClusteringColumnInfoList.get();
                for (ClusteringColumnInfo clusteringColumnInfo : clusteringColumnInfoList) {
                    String columnName = clusteringColumnInfo.getLogicalColumn().getNames()[0];
                    clusterColumns.add(columnName);
                }
            }
        }

        return snapshot.getSchema().fields().stream()
                .map(field -> {
                    String columnName = config.isCaseSensitivePartitionsEnabled() ? field.getName() :
                            field.getName().toLowerCase(US);
                    TypeSignature prestoType = DeltaTypeUtils.convertDeltaDataTypePrestoDataType(tableName,
                            columnName, field.getDataType());
                    // this is needed as column mapping + partitioning generate non hive stile partitions, so
                    // we must treat them as regular columns.
                    // (https://docs.databricks.com/aws/en/tables/partitions#delta-lake-and-parquet-partitioning-compatibility)
                    // previous code does also treat them as regular columns (because with column mapping enabled
                    // the physical name was returned from the delta kernel library for partition columns, which
                    // won't match logical name, so partitionColumns.contains(columnName) would always be false
                    // treating them as regular columns)
                    // Partition pruning of partitioned columns with column mapping enabled for those columns has
                    // to be added as a separate task.
                    // for now, we do this in order to keep the old behavior
                    String physicalName = DeltaColumnMetadataUtil.getPhysicalNameFromMetadata(field.getMetadata());
                    String hivePartitioningColumnName = physicalName == null ? columnName : physicalName;
                    return new DeltaColumn(
                            DeltaColumnMetadataUtil.getColumnIdFromMetadata(field.getMetadata()),
                            physicalName,
                            columnName,
                            prestoType,
                            field.isNullable(),
                            partitionColumns.contains(hivePartitioningColumnName),
                            clusterColumns.contains(columnName));
                }).collect(Collectors.toList());
    }
}
