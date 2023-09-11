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
package com.facebook.presto.iceberg;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveWrittenPartitions;
import com.facebook.presto.hive.metastore.HiveColumnStatistics;
import com.facebook.presto.hive.metastore.MetastoreUtil;
import com.facebook.presto.hive.metastore.Statistics;
import com.facebook.presto.iceberg.changelog.ChangelogUtil;
import com.facebook.presto.iceberg.samples.SampleUtil;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorNewTableLayout;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorOutputMetadata;
import com.facebook.presto.spi.statistics.ColumnStatisticMetadata;
import com.facebook.presto.spi.statistics.ColumnStatistics;
import com.facebook.presto.spi.statistics.ComputedStatistics;
import com.facebook.presto.spi.statistics.DoubleRange;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.statistics.TableStatisticType;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.facebook.presto.spi.statistics.TableStatisticsMetadata;
import com.google.common.base.VerifyException;
import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.iceberg.IcebergColumnHandle.primitiveIcebergColumnHandle;
import static com.facebook.presto.iceberg.IcebergSchemaProperties.LOCATION_PROPERTY;
import static com.facebook.presto.iceberg.IcebergSessionProperties.useSampleStatistics;
import static com.facebook.presto.iceberg.IcebergTableProperties.FILE_FORMAT_PROPERTY;
import static com.facebook.presto.iceberg.IcebergTableProperties.FORMAT_VERSION;
import static com.facebook.presto.iceberg.IcebergTableProperties.PARTITIONING_PROPERTY;
import static com.facebook.presto.iceberg.IcebergUtil.getColumns;
import static com.facebook.presto.iceberg.IcebergUtil.getFileFormat;
import static com.facebook.presto.iceberg.IcebergUtil.getTableComment;
import static com.facebook.presto.iceberg.IcebergUtil.resolveSnapshotIdByName;
import static com.facebook.presto.iceberg.PartitionFields.toPartitionFields;
import static com.facebook.presto.iceberg.TableType.CHANGELOG;
import static com.facebook.presto.iceberg.TableType.SAMPLES;
import static com.facebook.presto.iceberg.TypeConverter.toIcebergType;
import static com.facebook.presto.iceberg.TypeConverter.toPrestoType;
import static com.facebook.presto.iceberg.changelog.ChangelogUtil.getPrimaryKeyType;
import static com.facebook.presto.iceberg.changelog.ChangelogUtil.getRowTypeFromSchema;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.NUMBER_OF_DISTINCT_VALUES;
import static com.facebook.presto.spi.statistics.TableStatisticType.ROW_COUNT;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public abstract class IcebergAbstractMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(IcebergAbstractMetadata.class);

    protected final TypeManager typeManager;
    protected final JsonCodec<CommitTaskData> commitTaskCodec;

    protected final HdfsEnvironment hdfsEnvironment;

    protected final Map<String, Optional<Long>> snapshotIds = new ConcurrentHashMap<>();

    protected final Cache<IcebergTableHandle, TableStatistics> statsCache;

    private boolean useSampleForAnalyze;
    protected Transaction transaction;

    public IcebergAbstractMetadata(TypeManager typeManager, JsonCodec<CommitTaskData> commitTaskCodec, HdfsEnvironment hdfsEnvironment, Cache<IcebergTableHandle, TableStatistics> statsCache)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.commitTaskCodec = requireNonNull(commitTaskCodec, "commitTaskCodec is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.statsCache = requireNonNull(statsCache);
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        IcebergTableHandle handle = (IcebergTableHandle) table;
        ConnectorTableLayout layout = new ConnectorTableLayout(new IcebergTableLayoutHandle(handle, constraint.getSummary()));
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        return new ConnectorTableLayout(handle);
    }

    protected Optional<SystemTable> getIcebergSystemTable(SchemaTableName tableName, org.apache.iceberg.Table table)
    {
        IcebergTableName name = IcebergTableName.from(tableName.getTableName());
        SchemaTableName systemTableName = new SchemaTableName(tableName.getSchemaName(), name.getTableNameWithType());
        Optional<Long> snapshotId = resolveSnapshotIdByName(table, name);

        switch (name.getTableType()) {
            case CHANGELOG:
            case DATA:
            case SAMPLES:
                break;
            case HISTORY:
                if (name.getSnapshotId().isPresent()) {
                    throw new PrestoException(NOT_SUPPORTED, "Snapshot ID not supported for history table: " + systemTableName);
                }
                // make sure Hadoop FileSystem initialized in classloader-safe context
                table.refresh();
                return Optional.of(new HistoryTable(systemTableName, table));
            case SNAPSHOTS:
                if (name.getSnapshotId().isPresent()) {
                    throw new PrestoException(NOT_SUPPORTED, "Snapshot ID not supported for snapshots table: " + systemTableName);
                }
                table.refresh();
                return Optional.of(new SnapshotsTable(systemTableName, typeManager, table));
            case PARTITIONS:
                return Optional.of(new PartitionTable(systemTableName, typeManager, table, snapshotId));
            case MANIFESTS:
                return Optional.of(new ManifestsTable(systemTableName, table, snapshotId));
            case FILES:
                return Optional.of(new FilesTable(systemTableName, table, snapshotId, typeManager));
            case PROPERTIES:
                return Optional.of(new PropertiesTable(systemTableName, table));
        }
        return Optional.empty();
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        IcebergTableHandle ith = (IcebergTableHandle) table;
        return getTableMetaWithType(session, ith.getSchemaTableNameWithType());
    }

    protected abstract Table getIcebergTable(ConnectorSession session, SchemaTableName table);

    private ConnectorTableMetadata getTableMetaWithType(ConnectorSession session, SchemaTableName table)
    {
        IcebergTableName itn = IcebergTableName.from(table.getTableName());
        if (itn.isSystemTable()) {
            throw new TableNotFoundException(table);
        }
        ConnectorTableMetadata tableMeta = getTableMetadata(session, new SchemaTableName(table.getSchemaName(), itn.getTableName()));
        if (itn.getTableType() == CHANGELOG) {
            String primaryKeyColumn = IcebergTableProperties.getSampleTablePrimaryKey(tableMeta.getProperties());
            return ChangelogUtil.getChangelogTableMeta(table, getPrimaryKeyType(tableMeta, primaryKeyColumn), typeManager, tableMeta.getColumns());
        }
        else if (itn.getTableType() == SAMPLES) {
            return new ConnectorTableMetadata(new SchemaTableName(table.getSchemaName(), itn.getTableNameWithType()), tableMeta.getColumns(), tableMeta.getProperties(), tableMeta.getComment(), tableMeta.getTableConstraints());
        }
        return tableMeta;
    }

    protected ConnectorTableMetadata getTableMetadata(ConnectorSession session, SchemaTableName table)
    {
        org.apache.iceberg.Table icebergTable = getIcebergTable(session, table);
        List<ColumnMetadata> columns = getColumnMetadatas(icebergTable);
        return new ConnectorTableMetadata(table, columns, createMetadataProperties(icebergTable), getTableComment(icebergTable));
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        List<SchemaTableName> tables = prefix.getTableName() != null ? singletonList(prefix.toSchemaTableName()) : listTables(session, Optional.ofNullable(prefix.getSchemaName()));

        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName table : tables) {
            try {
                columns.put(table, getTableMetaWithType(session, table).getColumns());
            }
            catch (TableNotFoundException e) {
                log.warn(String.format("table disappeared during listing operation: %s", e.getMessage()));
            }
            catch (UnknownTableTypeException e) {
                log.warn(String.format("%s: Unknown table type of table %s", e.getMessage(), table.getTableName()));
            }
        }
        return columns.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        IcebergColumnHandle column = (IcebergColumnHandle) columnHandle;
        return ColumnMetadata.builder()
                .setName(column.getName())
                .setType(column.getType())
                .setComment(column.getComment())
                .setHidden(false)
                .build();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle icebergHandle = (IcebergTableHandle) tableHandle;
        Schema schema;
        Table icebergTable = getIcebergTable(session, icebergHandle.getSchemaTableName());
        if (icebergHandle.getTableType() == CHANGELOG) {
            String primaryKeyColumn = IcebergTableProperties.getSampleTablePrimaryKey((Map) icebergTable.properties());
            schema = ChangelogUtil.changelogTableSchema(getPrimaryKeyType(icebergTable, primaryKeyColumn), getRowTypeFromSchema(icebergTable.schema(), typeManager));
        }
        else {
            schema = icebergTable.schema();
        }

        return getColumns(schema, typeManager).stream()
                .collect(toImmutableMap(IcebergColumnHandle::getName, identity()));
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        Optional<ConnectorNewTableLayout> layout = getNewTableLayout(session, tableMetadata);
        finishCreateTable(session, beginCreateTable(session, tableMetadata, layout), ImmutableList.of(), ImmutableList.of());
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        return finishInsert(session, (IcebergWritableTableHandle) tableHandle, fragments, computedStatistics);
    }

    protected ConnectorInsertTableHandle beginIcebergTableInsert(ConnectorSession session, IcebergTableHandle table, org.apache.iceberg.Table icebergTable, HdfsEnvironment env)
    {
        org.apache.iceberg.Table insertTable;
        switch (table.getTableType()) {
            case DATA:
                insertTable = icebergTable;
                break;
            case SAMPLES:
                insertTable = SampleUtil.getSampleTableFromActual(icebergTable, table.getSchemaName(), env, session);
                break;
            default:
                throw new PrestoException(NOT_SUPPORTED, "can only write to data or samples table");
        }
        transaction = insertTable.newTransaction();

        return new IcebergWritableTableHandle(
                table.getSchemaName(),
                table.getTableName(),
                SchemaParser.toJson(insertTable.schema()),
                PartitionSpecParser.toJson(insertTable.spec()),
                getColumns(insertTable.schema(), typeManager),
                insertTable.location(),
                getFileFormat(icebergTable),
                insertTable.properties());
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        if (fragments.isEmpty()) {
            transaction.commitTransaction();
            return Optional.empty();
        }

        IcebergWritableTableHandle table = (IcebergWritableTableHandle) insertHandle;
        org.apache.iceberg.Table icebergTable = transaction.table();

        List<CommitTaskData> commitTasks = fragments.stream()
                .map(slice -> commitTaskCodec.fromJson(slice.getBytes()))
                .collect(toImmutableList());

        Type[] partitionColumnTypes = icebergTable.spec().fields().stream()
                .map(field -> field.transform().getResultType(
                        icebergTable.schema().findType(field.sourceId())))
                .toArray(Type[]::new);

        AppendFiles appendFiles = transaction.newFastAppend();
        for (CommitTaskData task : commitTasks) {
            DataFiles.Builder builder = DataFiles.builder(icebergTable.spec())
                    .withPath(task.getPath())
                    .withFileSizeInBytes(task.getFileSizeInBytes())
                    .withFormat(table.getFileFormat())
                    .withMetrics(task.getMetrics().metrics());

            if (!icebergTable.spec().fields().isEmpty()) {
                String partitionDataJson = task.getPartitionDataJson()
                        .orElseThrow(() -> new VerifyException("No partition data for partitioned table"));
                builder.withPartition(PartitionData.fromJson(partitionDataJson, partitionColumnTypes));
            }

            appendFiles.appendFile(builder.build());
        }

        appendFiles.commit();
        transaction.commitTransaction();

        return Optional.of(new HiveWrittenPartitions(commitTasks.stream()
                .map(CommitTaskData::getPath)
                .collect(toImmutableList())));
    }

    @Override
    public ColumnHandle getUpdateRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return primitiveIcebergColumnHandle(0, "$row_id", BIGINT, Optional.empty());
    }

    @Override
    public ConnectorTableHandle beginDelete(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle table = (IcebergTableHandle) tableHandle;
        Table icebergTable = getIcebergTable(session, table.getSchemaTableName());
        transaction = icebergTable.newTransaction();
        return table;
    }

    /**
     * Finish delete query
     *
     * @param fragments all fragments returned by {@link com.facebook.presto.spi.UpdatablePageSource#finish()}
     */
    @Override
    public void finishDelete(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<Slice> fragments)
    {
        return;
    }

    /**
     * @return whether delete without table scan is supported
     */
    @Override
    public boolean supportsMetadataDelete(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<ConnectorTableLayoutHandle> tableLayoutHandle)
    {
        return tableLayoutHandle.map(x -> ((IcebergTableLayoutHandle) x).getTupleDomain().equals(TupleDomain.all())).orElse(true);
    }

    /**
     * Delete the provided table layout
     *
     * @return number of rows deleted, or null for unknown
     */
    public OptionalLong metadataDelete(ConnectorSession session, ConnectorTableHandle tableHandle, ConnectorTableLayoutHandle tableLayoutHandle)
    {
        IcebergTableLayoutHandle layoutHandle = (IcebergTableLayoutHandle) tableLayoutHandle;
        if (!layoutHandle.getTupleDomain().equals(TupleDomain.all())) {
            throw new PrestoException(INVALID_ARGUMENTS, "iceberg metadata delete doesn't support predicates");
        }
        IcebergTableHandle handle = (IcebergTableHandle) tableHandle;
        Table icebergTable = getIcebergTable(session, handle.getSchemaTableName());
        if (handle.getTableType().equals(SAMPLES)) {
            icebergTable = SampleUtil.getSampleTableFromActual(icebergTable, handle.getSchemaName(), hdfsEnvironment, session);
        }
        transaction = icebergTable.newTransaction();
        DeleteFiles deletes = transaction.newDelete();
        AtomicLong rowsDeleted = new AtomicLong(0L);
        try (CloseableIterable<FileScanTask> files = icebergTable.newScan().planFiles()) {
            files.forEach(t -> {
                deletes.deleteFile(t.file());
                rowsDeleted.addAndGet(t.estimatedRowsCount());
            });
        }
        catch (IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "failed to scan files for delete", e);
        }
        deletes.commit();
        transaction.commitTransaction();
        return OptionalLong.of(rowsDeleted.get());
    }

    protected List<ColumnMetadata> getColumnMetadatas(org.apache.iceberg.Table table)
    {
        return table.schema().columns().stream()
                .map(column -> ColumnMetadata.builder()
                        .setName(column.name())
                        .setType(toPrestoType(column.type(), typeManager))
                        .setComment(Optional.ofNullable(column.doc()))
                        .setHidden(false)
                        .build())
                .collect(toImmutableList());
    }

    protected ImmutableMap<String, Object> createMetadataProperties(org.apache.iceberg.Table icebergTable)
    {
        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
        properties.put(FILE_FORMAT_PROPERTY, getFileFormat(icebergTable));

        int formatVersion = ((BaseTable) icebergTable).operations().current().formatVersion();
        properties.put(FORMAT_VERSION, String.valueOf(formatVersion));

        if (!icebergTable.spec().fields().isEmpty()) {
            properties.put(PARTITIONING_PROPERTY, toPartitionFields(icebergTable.spec()));
        }

        if (!icebergTable.location().isEmpty()) {
            properties.put(LOCATION_PROPERTY, icebergTable.location());
        }
        return properties.build();
    }

    protected static Schema toIcebergSchema(List<ColumnMetadata> columns)
    {
        List<Types.NestedField> icebergColumns = new ArrayList<>();
        for (ColumnMetadata column : columns) {
            if (!column.isHidden()) {
                int index = icebergColumns.size();
                Type type = toIcebergType(column.getType());
                Types.NestedField field = column.isNullable()
                        ? Types.NestedField.optional(index, column.getName(), type, column.getComment())
                        : Types.NestedField.required(index, column.getName(), type, column.getComment());
                icebergColumns.add(field);
            }
        }
        Type icebergSchema = Types.StructType.of(icebergColumns);
        AtomicInteger nextFieldId = new AtomicInteger(1);
        icebergSchema = TypeUtil.assignFreshIds(icebergSchema, nextFieldId::getAndIncrement);
        return new Schema(icebergSchema.asStructType().fields());
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<ConnectorTableLayoutHandle> tableLayoutHandle, List<ColumnHandle> columnHandles, Constraint<ColumnHandle> constraint)
    {
        IcebergTableHandle handle = (IcebergTableHandle) tableHandle;
        Table icebergTable = getIcebergTable(session, handle.getSchemaTableName());
        if (handle.getTableType() == SAMPLES) {
            icebergTable = SampleUtil.getSampleTableFromActual(icebergTable, handle.getSchemaName(), hdfsEnvironment, session);
        }
        Table finalIcebergTable = icebergTable;
        return Optional.ofNullable(statsCache.getIfPresent(tableHandle)).orElseGet(() -> TableStatisticsMaker.getTableStatistics(typeManager, constraint, handle, finalIcebergTable, session, hdfsEnvironment));
    }

    /**
     * Returns a table handle for the specified table name, or null if the connector does not contain the table.
     * The returned table handle can contain information in analyzeProperties.
     */
    public ConnectorTableHandle getTableHandleForStatisticsCollection(ConnectorSession session, SchemaTableName tableName, Map<String, Object> analyzeProperties)
    {
        if (useSampleStatistics(session)) {
            IcebergTableName itn = IcebergTableName.from(tableName.getTableName());
            org.apache.iceberg.Table table = getIcebergTable(session, tableName);
            boolean isSampleExist = SampleUtil.sampleTableExists(table, tableName.getSchemaName(), hdfsEnvironment, session);
            if (isSampleExist) {
                useSampleForAnalyze = true;
                table = SampleUtil.getSampleTableFromActual(table, tableName.getSchemaName(), hdfsEnvironment, session);
                Optional<Long> snapshotId = resolveSnapshotIdByName(table, itn);
                return new IcebergTableHandle(
                        tableName.getSchemaName(),
                        tableName.getTableName(),
                        SAMPLES,
                        snapshotId,
                        TupleDomain.all());
            }
            else {
                return getTableHandle(session, tableName);
            }
        }
        return getTableHandle(session, tableName);
    }

    /**
     * Describe statistics that must be collected during a statistics collection
     */
    @Override
    public TableStatisticsMetadata getStatisticsCollectionMetadata(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        Set<ColumnStatisticMetadata> columnStatistics = tableMetadata.getColumns().stream()
                .filter(column -> !column.isHidden())
                .map(meta -> this.getColumnStatisticMetadata(session, meta))
                .flatMap(List::stream)
                .collect(toImmutableSet());

        Set<TableStatisticType> tableStatistics = ImmutableSet.of(ROW_COUNT);
        return new TableStatisticsMetadata(columnStatistics, tableStatistics, Collections.emptyList());
    }

    private List<ColumnStatisticMetadata> getColumnStatisticMetadata(ConnectorSession session, ColumnMetadata col)
    {
        return MetastoreUtil.getSupportedColumnStatistics(col.getType()).stream().map(x ->
        {
            if (useSampleForAnalyze && useSampleStatistics(session) && x.equals(NUMBER_OF_DISTINCT_VALUES)) {
                return x.getColumnStatisticMetadataWithCustomFunction(col.getName(), "ndv_estimator");
            }
            else {
                return x.getColumnStatisticMetadata(col.getName());
            }
        }).collect(Collectors.toList());
    }

    /**
     * Begin statistics collection
     */
    @Override
    public ConnectorTableHandle beginStatisticsCollection(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return tableHandle;
    }

    /**
     * Finish statistics collection
     */
    public void finishStatisticsCollection(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<ComputedStatistics> computedStatistics)
    {
        TableStatistics.Builder builder = TableStatistics.builder();
        Map<String, ColumnHandle> columns = getColumnHandles(session, tableHandle);
        Map<String, com.facebook.presto.common.type.Type> types = columns.values().stream()
                .map(c -> (IcebergColumnHandle) c)
                .collect(toImmutableMap(IcebergColumnHandle::getName, IcebergColumnHandle::getType));
        computedStatistics.forEach(stat -> {
            AtomicLong rowCount = new AtomicLong(0);
            stat.getTableStatistics().forEach((key, value) -> {
                if (key.equals(ROW_COUNT)) {
                    verify(!value.isNull(0), "row count must not be nul");
                    if (useSampleForAnalyze) {
                        Table table = getIcebergTable(session, ((IcebergTableHandle) tableHandle).getSchemaTableName());
                        Map<String, String> tableProperties = table.properties();
                        verify(tableProperties.containsKey("sample.processed_count"),
                                "when using sample for Analyze, the sample table should have necessary information (sample.processed_count) to get row count");
                        int deletedRows = Integer.parseInt(tableProperties.getOrDefault("sample.deleted_count", "0"));
                        int rowCountFromSampleProperty = Integer.parseInt(tableProperties.get("sample.processed_count")) - deletedRows;
                        rowCount.set(rowCountFromSampleProperty);
                    }
                    else {
                        rowCount.set(value.getLong(0));
                    }
                    builder.setRowCount(Estimate.of(rowCount.get()));
                }
            });
            Map<String, HiveColumnStatistics> columnStats =
                    Statistics.fromComputedStatistics(session, DateTimeZone.UTC, stat.getColumnStatistics(), types, rowCount.get());
            columnStats.forEach((key, value) -> {
                IcebergColumnHandle ch = (IcebergColumnHandle) columns.get(key);
                ColumnStatistics.Builder colBuilder = ColumnStatistics.builder();
                value.getNullsCount().ifPresent(nullCount -> colBuilder.setNullsFraction(Estimate.of((double) nullCount / rowCount.get())));
                value.getDistinctValuesCount().ifPresent(ndvs -> colBuilder.setDistinctValuesCount(Estimate.of(ndvs)));
                value.getTotalSizeInBytes().ifPresent(totalDataSize -> colBuilder.setDataSize(Estimate.of((double) totalDataSize / rowCount.get())));
                if (isRangeSupported(ch.getType())) {
                    colBuilder.setRange(createRange(ch.getType(), value));
                }
                builder.setColumnStatistics(ch, colBuilder.build());
            });
        });
        statsCache.put((IcebergTableHandle) tableHandle, builder.build());
    }

    private static Optional<DoubleRange> createRange(com.facebook.presto.common.type.Type type, HiveColumnStatistics statistics)
    {
        if (type.equals(BIGINT) || type.equals(INTEGER) || type.equals(SMALLINT) || type.equals(TINYINT)) {
            return statistics.getIntegerStatistics()
                    .filter(is -> is.getMin().isPresent())
                    .filter(is -> is.getMax().isPresent())
                    .map(is -> new DoubleRange(is.getMin().getAsLong(), is.getMax().getAsLong()));
        }
        if (type.equals(DOUBLE) || type.equals(REAL)) {
            return statistics.getDoubleStatistics()
                    .filter(ds -> ds.getMin().isPresent())
                    .filter(ds -> ds.getMax().isPresent())
                    .map(ds -> new DoubleRange(ds.getMin().getAsDouble(), ds.getMax().getAsDouble()));
        }
        if (type.equals(DATE)) {
            return statistics.getDateStatistics()
                    .filter(ds -> ds.getMin().isPresent())
                    .filter(ds -> ds.getMax().isPresent())
                    .map(ds -> new DoubleRange(ds.getMin().get().toEpochDay(), ds.getMax().get().toEpochDay()));
        }
        if (type instanceof DecimalType) {
            return statistics.getDecimalStatistics()
                    .filter(ds -> ds.getMin().isPresent())
                    .filter(ds -> ds.getMax().isPresent())
                    .map(ds -> new DoubleRange(ds.getMin().get().doubleValue(), ds.getMax().get().doubleValue()));
        }
        throw new IllegalArgumentException("Unexpected type: " + type);
    }

    private static boolean isRangeSupported(com.facebook.presto.common.type.Type type)
    {
        return type.equals(TINYINT)
                || type.equals(SMALLINT)
                || type.equals(INTEGER)
                || type.equals(BIGINT)
                || type.equals(REAL)
                || type.equals(DOUBLE)
                || type.equals(DATE)
                || type instanceof DecimalType;
    }

    public void rollback()
    {
        // TODO: cleanup open transaction
    }
}
