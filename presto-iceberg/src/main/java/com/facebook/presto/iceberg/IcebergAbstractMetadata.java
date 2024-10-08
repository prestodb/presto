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
import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.SqlTimestampWithTimeZone;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.TimestampWithTimeZoneType;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.hive.HivePartition;
import com.facebook.presto.hive.HiveWrittenPartitions;
import com.facebook.presto.hive.NodeVersion;
import com.facebook.presto.iceberg.changelog.ChangelogUtil;
import com.facebook.presto.iceberg.statistics.StatisticsFileCache;
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
import com.facebook.presto.spi.DiscretePredicates;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorOutputMetadata;
import com.facebook.presto.spi.connector.ConnectorTableVersion;
import com.facebook.presto.spi.connector.ConnectorTableVersion.VersionOperator;
import com.facebook.presto.spi.connector.ConnectorTableVersion.VersionType;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.FilterStatsCalculatorService;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.facebook.presto.spi.statistics.ColumnStatisticMetadata;
import com.facebook.presto.spi.statistics.ComputedStatistics;
import com.facebook.presto.spi.statistics.TableStatisticType;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.facebook.presto.spi.statistics.TableStatisticsMetadata;
import com.google.common.base.Functions;
import com.google.common.base.Predicates;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import io.airlift.slice.Slice;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.MetricsModes.None;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.CharSequenceSet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.hive.MetadataUtils.getCombinedRemainingPredicate;
import static com.facebook.presto.hive.MetadataUtils.getDiscretePredicates;
import static com.facebook.presto.hive.MetadataUtils.getPredicate;
import static com.facebook.presto.hive.MetadataUtils.getSubfieldPredicate;
import static com.facebook.presto.iceberg.ExpressionConverter.toIcebergExpression;
import static com.facebook.presto.iceberg.IcebergColumnHandle.DATA_SEQUENCE_NUMBER_COLUMN_HANDLE;
import static com.facebook.presto.iceberg.IcebergColumnHandle.DATA_SEQUENCE_NUMBER_COLUMN_METADATA;
import static com.facebook.presto.iceberg.IcebergColumnHandle.PATH_COLUMN_HANDLE;
import static com.facebook.presto.iceberg.IcebergColumnHandle.PATH_COLUMN_METADATA;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_INVALID_SNAPSHOT_ID;
import static com.facebook.presto.iceberg.IcebergMetadataColumn.DATA_SEQUENCE_NUMBER;
import static com.facebook.presto.iceberg.IcebergMetadataColumn.FILE_PATH;
import static com.facebook.presto.iceberg.IcebergPartitionType.ALL;
import static com.facebook.presto.iceberg.IcebergSessionProperties.getCompressionCodec;
import static com.facebook.presto.iceberg.IcebergSessionProperties.isPushdownFilterEnabled;
import static com.facebook.presto.iceberg.IcebergTableProperties.DELETE_MODE;
import static com.facebook.presto.iceberg.IcebergTableProperties.FILE_FORMAT_PROPERTY;
import static com.facebook.presto.iceberg.IcebergTableProperties.FORMAT_VERSION;
import static com.facebook.presto.iceberg.IcebergTableProperties.LOCATION_PROPERTY;
import static com.facebook.presto.iceberg.IcebergTableProperties.METADATA_DELETE_AFTER_COMMIT;
import static com.facebook.presto.iceberg.IcebergTableProperties.METADATA_PREVIOUS_VERSIONS_MAX;
import static com.facebook.presto.iceberg.IcebergTableProperties.METRICS_MAX_INFERRED_COLUMN;
import static com.facebook.presto.iceberg.IcebergTableProperties.PARTITIONING_PROPERTY;
import static com.facebook.presto.iceberg.IcebergTableType.CHANGELOG;
import static com.facebook.presto.iceberg.IcebergTableType.DATA;
import static com.facebook.presto.iceberg.IcebergTableType.EQUALITY_DELETES;
import static com.facebook.presto.iceberg.IcebergUtil.MIN_FORMAT_VERSION_FOR_DELETE;
import static com.facebook.presto.iceberg.IcebergUtil.getColumns;
import static com.facebook.presto.iceberg.IcebergUtil.getDeleteMode;
import static com.facebook.presto.iceberg.IcebergUtil.getFileFormat;
import static com.facebook.presto.iceberg.IcebergUtil.getPartitionFields;
import static com.facebook.presto.iceberg.IcebergUtil.getPartitionKeyColumnHandles;
import static com.facebook.presto.iceberg.IcebergUtil.getPartitionSpecsIncludingValidData;
import static com.facebook.presto.iceberg.IcebergUtil.getPartitions;
import static com.facebook.presto.iceberg.IcebergUtil.getSnapshotIdTimeOperator;
import static com.facebook.presto.iceberg.IcebergUtil.getTableComment;
import static com.facebook.presto.iceberg.IcebergUtil.resolveSnapshotIdByName;
import static com.facebook.presto.iceberg.IcebergUtil.toHiveColumns;
import static com.facebook.presto.iceberg.IcebergUtil.tryGetLocation;
import static com.facebook.presto.iceberg.IcebergUtil.tryGetProperties;
import static com.facebook.presto.iceberg.IcebergUtil.tryGetSchema;
import static com.facebook.presto.iceberg.IcebergUtil.validateTableMode;
import static com.facebook.presto.iceberg.IcebergUtil.verifyTypeSupported;
import static com.facebook.presto.iceberg.PartitionFields.getPartitionColumnName;
import static com.facebook.presto.iceberg.PartitionFields.getTransformTerm;
import static com.facebook.presto.iceberg.PartitionFields.toPartitionFields;
import static com.facebook.presto.iceberg.PartitionSpecConverter.toPrestoPartitionSpec;
import static com.facebook.presto.iceberg.SchemaConverter.toPrestoSchema;
import static com.facebook.presto.iceberg.TableStatisticsMaker.getSupportedColumnStatistics;
import static com.facebook.presto.iceberg.TypeConverter.toIcebergType;
import static com.facebook.presto.iceberg.TypeConverter.toPrestoType;
import static com.facebook.presto.iceberg.changelog.ChangelogUtil.getRowTypeFromColumnMeta;
import static com.facebook.presto.iceberg.optimizer.IcebergPlanOptimizer.getEnforcedColumns;
import static com.facebook.presto.iceberg.util.StatisticsUtil.calculateBaseTableStatistics;
import static com.facebook.presto.iceberg.util.StatisticsUtil.calculateStatisticsConsideringLayout;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.statistics.TableStatisticType.ROW_COUNT;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.MetadataColumns.ROW_POSITION;
import static org.apache.iceberg.SnapshotSummary.DELETED_RECORDS_PROP;
import static org.apache.iceberg.SnapshotSummary.REMOVED_EQ_DELETES_PROP;
import static org.apache.iceberg.SnapshotSummary.REMOVED_POS_DELETES_PROP;

public abstract class IcebergAbstractMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(IcebergAbstractMetadata.class);

    protected final TypeManager typeManager;
    protected final JsonCodec<CommitTaskData> commitTaskCodec;
    protected final NodeVersion nodeVersion;
    protected final RowExpressionService rowExpressionService;
    protected final FilterStatsCalculatorService filterStatsCalculatorService;
    protected Transaction transaction;
    protected final StatisticsFileCache statisticsFileCache;

    private final StandardFunctionResolution functionResolution;
    private final ConcurrentMap<SchemaTableName, Table> icebergTables = new ConcurrentHashMap<>();

    public IcebergAbstractMetadata(
            TypeManager typeManager,
            StandardFunctionResolution functionResolution,
            RowExpressionService rowExpressionService,
            JsonCodec<CommitTaskData> commitTaskCodec,
            NodeVersion nodeVersion,
            FilterStatsCalculatorService filterStatsCalculatorService,
            StatisticsFileCache statisticsFileCache)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.commitTaskCodec = requireNonNull(commitTaskCodec, "commitTaskCodec is null");
        this.functionResolution = requireNonNull(functionResolution, "functionResolution is null");
        this.rowExpressionService = requireNonNull(rowExpressionService, "rowExpressionService is null");
        this.nodeVersion = requireNonNull(nodeVersion, "nodeVersion is null");
        this.filterStatsCalculatorService = requireNonNull(filterStatsCalculatorService, "filterStatsCalculatorService is null");
        this.statisticsFileCache = requireNonNull(statisticsFileCache, "statisticsFileCache is null");
    }

    protected final Table getIcebergTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        return icebergTables.computeIfAbsent(
                schemaTableName,
                ignored -> getRawIcebergTable(session, schemaTableName));
    }

    protected abstract Table getRawIcebergTable(ConnectorSession session, SchemaTableName schemaTableName);

    protected abstract boolean tableExists(ConnectorSession session, SchemaTableName schemaTableName);

    public abstract void registerTable(ConnectorSession clientSession, SchemaTableName schemaTableName, Path metadataLocation);

    public abstract void unregisterTable(ConnectorSession clientSession, SchemaTableName schemaTableName);

    /**
     * This class implements the default implementation for getTableLayoutForConstraint which will be used in the case of a Java Worker
     */
    @Override
    public ConnectorTableLayoutResult getTableLayoutForConstraint(
            ConnectorSession session,
            ConnectorTableHandle table,
            Constraint<ColumnHandle> constraint,
            Optional<Set<ColumnHandle>> desiredColumns)
    {
        Map<String, IcebergColumnHandle> predicateColumns = constraint.getSummary().getDomains().get().keySet().stream()
                .map(IcebergColumnHandle.class::cast)
                .collect(toImmutableMap(IcebergColumnHandle::getName, Functions.identity()));

        IcebergTableHandle handle = (IcebergTableHandle) table;
        Table icebergTable = getIcebergTable(session, handle.getSchemaTableName());

        List<IcebergColumnHandle> partitionColumns = getPartitionKeyColumnHandles(handle, icebergTable, typeManager);
        TupleDomain<ColumnHandle> partitionColumnPredicate = TupleDomain.withColumnDomains(Maps.filterKeys(constraint.getSummary().getDomains().get(), Predicates.in(partitionColumns)));
        Optional<Set<IcebergColumnHandle>> requestedColumns = desiredColumns.map(columns -> columns.stream().map(column -> (IcebergColumnHandle) column).collect(toImmutableSet()));

        List<HivePartition> partitions;
        if (handle.getIcebergTableName().getTableType() == CHANGELOG ||
                handle.getIcebergTableName().getTableType() == EQUALITY_DELETES) {
            partitions = ImmutableList.of(new HivePartition(handle.getSchemaTableName()));
        }
        else {
            partitions = getPartitions(
                    typeManager,
                    handle,
                    icebergTable,
                    constraint,
                    partitionColumns);
        }

        ConnectorTableLayout layout = getTableLayout(
                session,
                new IcebergTableLayoutHandle.Builder()
                        .setPartitionColumns(ImmutableList.copyOf(partitionColumns))
                        .setDataColumns(toHiveColumns(icebergTable.schema().columns()))
                        .setDomainPredicate(constraint.getSummary().simplify().transform(IcebergAbstractMetadata::toSubfield))
                        .setRemainingPredicate(TRUE_CONSTANT)
                        .setPredicateColumns(predicateColumns)
                        .setRequestedColumns(requestedColumns)
                        .setPushdownFilterEnabled(isPushdownFilterEnabled(session))
                        .setPartitionColumnPredicate(partitionColumnPredicate.simplify())
                        .setPartitions(Optional.ofNullable(partitions.size() == 0 ? null : partitions))
                        .setTable(handle)
                        .build());
        return new ConnectorTableLayoutResult(layout, constraint.getSummary());
    }

    public static Subfield toSubfield(ColumnHandle columnHandle)
    {
        return new Subfield(((IcebergColumnHandle) columnHandle).getName(), ImmutableList.of());
    }

    protected static boolean isEntireColumn(Subfield subfield)
    {
        return subfield.getPath().isEmpty();
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        IcebergTableLayoutHandle icebergTableLayoutHandle = (IcebergTableLayoutHandle) handle;

        IcebergTableHandle tableHandle = icebergTableLayoutHandle.getTable();
        if (!tableExists(session, tableHandle.getSchemaTableName())) {
            return null;
        }

        Table icebergTable = getIcebergTable(session, tableHandle.getSchemaTableName());
        validateTableMode(session, icebergTable);
        List<ColumnHandle> partitionColumns = ImmutableList.copyOf(icebergTableLayoutHandle.getPartitionColumns());
        Optional<List<HivePartition>> partitions = icebergTableLayoutHandle.getPartitions();
        Optional<DiscretePredicates> discretePredicates = partitions.flatMap(parts -> getDiscretePredicates(partitionColumns, parts));
        if (!isPushdownFilterEnabled(session)) {
            return new ConnectorTableLayout(
                    icebergTableLayoutHandle,
                    Optional.empty(),
                    partitions.isPresent() ? icebergTableLayoutHandle.getPartitionColumnPredicate() : TupleDomain.none(),
                    Optional.empty(),
                    Optional.empty(),
                    discretePredicates,
                    ImmutableList.of(),
                    Optional.empty());
        }

        Map<String, ColumnHandle> predicateColumns = icebergTableLayoutHandle.getPredicateColumns().entrySet()
                .stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        Optional<TupleDomain<ColumnHandle>> predicate = partitions.map(parts -> getPredicate(icebergTableLayoutHandle, partitionColumns, parts, predicateColumns));
        // capture subfields from domainPredicate to add to remainingPredicate
        // so those filters don't get lost
        Map<String, com.facebook.presto.common.type.Type> columnTypes = getColumns(icebergTable.schema(), icebergTable.spec(), typeManager).stream()
                .collect(toImmutableMap(IcebergColumnHandle::getName, icebergColumnHandle -> getColumnMetadata(session, tableHandle, icebergColumnHandle).getType()));

        RowExpression subfieldPredicate = getSubfieldPredicate(session, icebergTableLayoutHandle, columnTypes, functionResolution, rowExpressionService);

        // combine subfieldPredicate with remainingPredicate
        RowExpression combinedRemainingPredicate = getCombinedRemainingPredicate(icebergTableLayoutHandle, subfieldPredicate);

        return predicate.map(pred -> new ConnectorTableLayout(
                        icebergTableLayoutHandle,
                        Optional.empty(),
                        pred,
                        Optional.empty(),
                        Optional.empty(),
                        discretePredicates,
                        ImmutableList.of(),
                        Optional.of(combinedRemainingPredicate)))
                .orElseGet(() -> new ConnectorTableLayout(
                        icebergTableLayoutHandle,
                        Optional.empty(),
                        TupleDomain.none(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableList.of(),
                        Optional.empty()));
    }

    protected Optional<SystemTable> getIcebergSystemTable(SchemaTableName tableName, Table table)
    {
        IcebergTableName name = IcebergTableName.from(tableName.getTableName());
        SchemaTableName systemTableName = new SchemaTableName(tableName.getSchemaName(), name.getTableNameWithType());
        Optional<Long> snapshotId = resolveSnapshotIdByName(table, name);

        switch (name.getTableType()) {
            case CHANGELOG:
            case DATA:
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
            case REFS:
                return Optional.of(new RefsTable(systemTableName, table));
        }
        return Optional.empty();
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        IcebergTableHandle icebergTableHandle = (IcebergTableHandle) table;
        return getTableMetadata(session, icebergTableHandle.getSchemaTableName(), icebergTableHandle.getIcebergTableName());
    }

    protected ConnectorTableMetadata getTableMetadata(ConnectorSession session, SchemaTableName table, IcebergTableName icebergTableName)
    {
        Table icebergTable = getIcebergTable(session, new SchemaTableName(table.getSchemaName(), icebergTableName.getTableName()));
        ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
        columns.addAll(getColumnMetadatas(icebergTable));
        if (icebergTableName.getTableType() == CHANGELOG) {
            return ChangelogUtil.getChangelogTableMeta(table, typeManager, columns.build());
        }
        else {
            columns.add(PATH_COLUMN_METADATA);
            columns.add(DATA_SEQUENCE_NUMBER_COLUMN_METADATA);
        }
        return new ConnectorTableMetadata(table, columns.build(), createMetadataProperties(icebergTable), getTableComment(icebergTable));
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        List<SchemaTableName> tables = prefix.getTableName() != null ? singletonList(prefix.toSchemaTableName()) : listTables(session, Optional.ofNullable(prefix.getSchemaName()));

        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName table : tables) {
            try {
                IcebergTableName tableName = IcebergTableName.from(table.getTableName());
                if (!tableName.isSystemTable()) {
                    columns.put(table, getTableMetadata(session, table, tableName).getColumns());
                }
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
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        Optional<ConnectorNewTableLayout> layout = getNewTableLayout(session, tableMetadata);
        finishCreateTable(session, beginCreateTable(session, tableMetadata, layout), ImmutableList.of(), ImmutableList.of());
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        return finishWrite(session, (IcebergOutputTableHandle) tableHandle, fragments, computedStatistics);
    }

    protected ConnectorInsertTableHandle beginIcebergTableInsert(ConnectorSession session, IcebergTableHandle table, Table icebergTable)
    {
        transaction = icebergTable.newTransaction();

        return new IcebergInsertTableHandle(
                table.getSchemaName(),
                table.getIcebergTableName(),
                toPrestoSchema(icebergTable.schema(), typeManager),
                toPrestoPartitionSpec(icebergTable.spec(), typeManager),
                getColumns(icebergTable.schema(), icebergTable.spec(), typeManager),
                icebergTable.location(),
                getFileFormat(icebergTable),
                getCompressionCodec(session),
                icebergTable.properties());
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        return finishWrite(session, (IcebergInsertTableHandle) insertHandle, fragments, computedStatistics);
    }

    private Optional<ConnectorOutputMetadata> finishWrite(ConnectorSession session, IcebergWritableTableHandle writableTableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        if (fragments.isEmpty()) {
            transaction.commitTransaction();
            return Optional.empty();
        }

        Table icebergTable = transaction.table();

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
                    .withFormat(FileFormat.fromString(writableTableHandle.getFileFormat().name()))
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
    public ColumnHandle getDeleteRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return IcebergColumnHandle.create(ROW_POSITION, typeManager, IcebergColumnHandle.ColumnType.REGULAR);
    }

    @Override
    public boolean isLegacyGetLayoutSupported(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return !isPushdownFilterEnabled(session);
    }

    protected List<ColumnMetadata> getColumnMetadatas(Table table)
    {
        Map<String, List<String>> partitionFields = getPartitionFields(table.spec(), ALL);
        return table.schema().columns().stream()
                .map(column -> ColumnMetadata.builder()
                        .setName(column.name())
                        .setType(toPrestoType(column.type(), typeManager))
                        .setComment(Optional.ofNullable(column.doc()))
                        .setHidden(false)
                        .setExtraInfo(Optional.ofNullable(
                                partitionFields.containsKey(column.name()) ?
                                        columnExtraInfo(partitionFields.get(column.name())) :
                                        null))
                        .build())
                .collect(toImmutableList());
    }

    private static String columnExtraInfo(List<String> partitionTransforms)
    {
        if (partitionTransforms.size() == 1 && partitionTransforms.get(0).equals("identity")) {
            return "partition key";
        }

        return "partition by " + partitionTransforms.stream().collect(Collectors.joining(", "));
    }

    protected ImmutableMap<String, Object> createMetadataProperties(Table icebergTable)
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

        properties.put(DELETE_MODE, IcebergUtil.getDeleteMode(icebergTable));
        properties.put(METADATA_PREVIOUS_VERSIONS_MAX, IcebergUtil.getMetadataPreviousVersionsMax(icebergTable));
        properties.put(METADATA_DELETE_AFTER_COMMIT, IcebergUtil.isMetadataDeleteAfterCommit(icebergTable));
        properties.put(METRICS_MAX_INFERRED_COLUMN, IcebergUtil.getMetricsMaxInferredColumn(icebergTable));

        return properties.build();
    }

    public static Schema toIcebergSchema(List<ColumnMetadata> columns)
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
    public ConnectorTableHandle getTableHandleForStatisticsCollection(ConnectorSession session, SchemaTableName tableName, Map<String, Object> analyzeProperties)
    {
        return getTableHandle(session, tableName);
    }

    @Override
    public TableStatisticsMetadata getStatisticsCollectionMetadata(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        Table table = getIcebergTable(session, tableMetadata.getTable());
        MetricsConfig metricsConfig = MetricsConfig.forTable(table);
        Set<ColumnStatisticMetadata> columnStatistics = tableMetadata.getColumns().stream()
                .filter(column -> !column.isHidden() && metricsConfig.columnMode(column.getName()) != None.get())
                .flatMap(meta -> getSupportedColumnStatistics(meta.getName(), meta.getType()).stream())
                .collect(toImmutableSet());

        Set<TableStatisticType> tableStatistics = ImmutableSet.of(ROW_COUNT);
        return new TableStatisticsMetadata(columnStatistics, tableStatistics, Collections.emptyList());
    }

    @Override
    public ConnectorTableHandle beginStatisticsCollection(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return tableHandle;
    }

    @Override
    public void finishStatisticsCollection(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<ComputedStatistics> computedStatistics)
    {
        IcebergTableHandle icebergTableHandle = (IcebergTableHandle) tableHandle;
        Table icebergTable = getIcebergTable(session, icebergTableHandle.getSchemaTableName());
        TableStatisticsMaker.writeTableStatistics(nodeVersion, typeManager, icebergTableHandle, icebergTable, session, computedStatistics);
    }

    public void rollback()
    {
        // TODO: cleanup open transaction
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column)
    {
        if (!column.isNullable()) {
            throw new PrestoException(NOT_SUPPORTED, "This connector does not support add column with non null");
        }

        Type columnType = toIcebergType(column.getType());

        if (columnType.equals(Types.TimestampType.withZone())) {
            throw new PrestoException(NOT_SUPPORTED, format("Iceberg column type %s is not supported", columnType));
        }

        IcebergTableHandle handle = (IcebergTableHandle) tableHandle;
        verify(handle.getIcebergTableName().getTableType() == DATA, "only the data table can have columns added");
        Table icebergTable = getIcebergTable(session, handle.getSchemaTableName());
        Transaction transaction = icebergTable.newTransaction();
        transaction.updateSchema().addColumn(column.getName(), columnType, column.getComment()).commit();
        if (column.getProperties().containsKey(PARTITIONING_PROPERTY)) {
            String transform = (String) column.getProperties().get(PARTITIONING_PROPERTY);
            transaction.updateSpec().addField(getPartitionColumnName(column.getName(), transform),
                    getTransformTerm(column.getName(), transform)).commit();
        }
        transaction.commitTransaction();
    }

    @Override
    public void dropColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column)
    {
        IcebergTableHandle icebergTableHandle = (IcebergTableHandle) tableHandle;
        IcebergColumnHandle handle = (IcebergColumnHandle) column;
        verify(icebergTableHandle.getIcebergTableName().getTableType() == DATA, "only the data table can have columns dropped");
        Table icebergTable = getIcebergTable(session, icebergTableHandle.getSchemaTableName());

        // Currently drop partition column used in any partition specs of a table would introduce some problems in Iceberg.
        // So we explicitly disallow dropping partition columns until Iceberg fix this problem.
        // See https://github.com/apache/iceberg/issues/4563
        boolean shouldNotDropPartitionColumn = icebergTable.specs().values().stream()
                .flatMap(partitionSpec -> partitionSpec.fields().stream())
                .anyMatch(field -> field.sourceId() == handle.getId());
        if (shouldNotDropPartitionColumn) {
            throw new PrestoException(NOT_SUPPORTED, "This connector does not support dropping columns which exist in any of the table's partition specs");
        }

        icebergTable.updateSchema().deleteColumn(handle.getName()).commit();
    }

    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle source, String target)
    {
        IcebergTableHandle icebergTableHandle = (IcebergTableHandle) tableHandle;
        verify(icebergTableHandle.getIcebergTableName().getTableType() == DATA, "only the data table can have columns renamed");
        IcebergColumnHandle columnHandle = (IcebergColumnHandle) source;
        Table icebergTable = getIcebergTable(session, icebergTableHandle.getSchemaTableName());
        Transaction transaction = icebergTable.newTransaction();
        transaction.updateSchema().renameColumn(columnHandle.getName(), target).commit();
        if (icebergTable.spec().fields().stream().map(PartitionField::sourceId).anyMatch(sourceId -> sourceId == columnHandle.getId())) {
            transaction.updateSpec().renameField(columnHandle.getName(), target).commit();
        }
        transaction.commitTransaction();
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle table = (IcebergTableHandle) tableHandle;
        verify(table.getIcebergTableName().getTableType() == DATA, "only the data table can have data inserted");
        Table icebergTable = getIcebergTable(session, table.getSchemaTableName());
        validateTableMode(session, icebergTable);

        verifyTypeSupported(icebergTable.schema());

        return beginIcebergTableInsert(session, table, icebergTable);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle table = (IcebergTableHandle) tableHandle;
        Table icebergTable = getIcebergTable(session, table.getSchemaTableName());
        Schema schema;
        if (table.getIcebergTableName().getTableType() == CHANGELOG) {
            schema = ChangelogUtil.changelogTableSchema(getRowTypeFromColumnMeta(getColumnMetadatas(icebergTable)));
        }
        else {
            schema = icebergTable.schema();
        }

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        for (IcebergColumnHandle columnHandle : getColumns(schema, icebergTable.spec(), typeManager)) {
            columnHandles.put(columnHandle.getName(), columnHandle);
        }
        if (table.getIcebergTableName().getTableType() != CHANGELOG) {
            columnHandles.put(FILE_PATH.getColumnName(), PATH_COLUMN_HANDLE);
            columnHandles.put(DATA_SEQUENCE_NUMBER.getColumnName(), DATA_SEQUENCE_NUMBER_COLUMN_HANDLE);
        }
        return columnHandles.build();
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<ConnectorTableLayoutHandle> tableLayoutHandle, List<ColumnHandle> columnHandles, Constraint<ColumnHandle> constraint)
    {
        TableStatistics baseStatistics = calculateBaseTableStatistics(this, typeManager, session, statisticsFileCache, (IcebergTableHandle) tableHandle, tableLayoutHandle, columnHandles, constraint);
        return calculateStatisticsConsideringLayout(filterStatsCalculatorService, rowExpressionService, baseStatistics, session, tableLayoutHandle);
    }

    @Override
    public IcebergTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        return getTableHandle(session, tableName, Optional.empty());
    }

    @Override
    public IcebergTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName, Optional<ConnectorTableVersion> tableVersion)
    {
        IcebergTableName name = IcebergTableName.from(tableName.getTableName());
        verify(name.getTableType() == DATA || name.getTableType() == CHANGELOG || name.getTableType() == EQUALITY_DELETES, "Wrong table type: " + name.getTableType());

        if (!tableExists(session, tableName)) {
            return null;
        }

        // use a new schema table name that omits the table type
        Table table = getIcebergTable(session, new SchemaTableName(tableName.getSchemaName(), name.getTableName()));

        Optional<Long> tableSnapshotId = tableVersion
                .map(version -> {
                    long tableVersionSnapshotId = getSnapshotIdForTableVersion(table, version);
                    return Optional.of(tableVersionSnapshotId);
                })
                .orElseGet(() -> resolveSnapshotIdByName(table, name));

        // Get Iceberg tables schema, properties, and location with missing
        // filesystem metadata will fail.
        // See https://github.com/prestodb/presto/pull/21181
        Optional<Schema> tableSchema = tryGetSchema(table);
        Optional<String> tableSchemaJson = tableSchema.map(SchemaParser::toJson);

        return new IcebergTableHandle(
                tableName.getSchemaName(),
                new IcebergTableName(name.getTableName(), name.getTableType(), tableSnapshotId, name.getChangelogEndSnapshot()),
                name.getSnapshotId().isPresent(),
                tryGetLocation(table),
                tryGetProperties(table),
                tableSchemaJson,
                Optional.empty(),
                Optional.empty());
    }

    @Override
    public Optional<SystemTable> getSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        IcebergTableName name = IcebergTableName.from(tableName.getTableName());
        if (name.getTableType() == DATA || name.getTableType() == CHANGELOG) {
            return Optional.empty();
        }
        SchemaTableName icebergTableName = new SchemaTableName(tableName.getSchemaName(), name.getTableName());
        if (!tableExists(session, icebergTableName)) {
            return Optional.empty();
        }

        Table icebergTable = getIcebergTable(session, icebergTableName);

        if (name.getSnapshotId().isPresent() && icebergTable.snapshot(name.getSnapshotId().get()) == null) {
            throw new PrestoException(ICEBERG_INVALID_SNAPSHOT_ID, format("Invalid snapshot [%s] for table: %s", name.getSnapshotId().get(), icebergTable));
        }

        return getIcebergSystemTable(tableName, icebergTable);
    }

    @Override
    public void truncateTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle handle = (IcebergTableHandle) tableHandle;
        Table icebergTable = getIcebergTable(session, handle.getSchemaTableName());
        removeScanFiles(icebergTable, TupleDomain.all());
    }

    @Override
    public ConnectorTableHandle beginDelete(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle handle = (IcebergTableHandle) tableHandle;
        Table icebergTable = getIcebergTable(session, handle.getSchemaTableName());

        if (handle.isSnapshotSpecified()) {
            throw new PrestoException(NOT_SUPPORTED, "This connector do not allow delete data at specified snapshot");
        }

        int formatVersion = ((BaseTable) icebergTable).operations().current().formatVersion();
        if (formatVersion < MIN_FORMAT_VERSION_FOR_DELETE) {
            throw new PrestoException(NOT_SUPPORTED, format("This connector only supports delete where one or more partitions are deleted entirely for table versions older than %d", MIN_FORMAT_VERSION_FOR_DELETE));
        }

        if (getDeleteMode(icebergTable) == RowLevelOperationMode.COPY_ON_WRITE) {
            throw new PrestoException(NOT_SUPPORTED, "This connector only supports delete where one or more partitions are deleted entirely. Configure delete_mode table property to allow row level deletions.");
        }

        validateTableMode(session, icebergTable);
        transaction = icebergTable.newTransaction();

        return handle;
    }

    @Override
    public void finishDelete(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<Slice> fragments)
    {
        IcebergTableHandle handle = (IcebergTableHandle) tableHandle;
        Table icebergTable = getIcebergTable(session, handle.getSchemaTableName());

        RowDelta rowDelta = transaction.newRowDelta();

        List<CommitTaskData> commitTasks = fragments.stream()
                .map(slice -> commitTaskCodec.fromJson(slice.getBytes()))
                .collect(toImmutableList());

        CharSequenceSet referencedDataFiles = CharSequenceSet.empty();

        for (CommitTaskData task : commitTasks) {
            PartitionSpec spec = icebergTable.specs().get(task.getPartitionSpecId());
            FileMetadata.Builder builder = FileMetadata.deleteFileBuilder(spec)
                    .ofPositionDeletes()
                    .withPath(task.getPath())
                    .withFileSizeInBytes(task.getFileSizeInBytes())
                    .withFormat(FileFormat.fromString(task.getFileFormat().name()))
                    .withMetrics(task.getMetrics().metrics());

            if (!spec.fields().isEmpty()) {
                String partitionDataJson = task.getPartitionDataJson()
                        .orElseThrow(() -> new VerifyException("No partition data for partitioned table"));
                Type[] partitionColumnTypes = spec.fields().stream()
                        .map(field -> field.transform().getResultType(
                                spec.schema().findType(field.sourceId())))
                        .toArray(Type[]::new);
                builder.withPartition(PartitionData.fromJson(partitionDataJson, partitionColumnTypes));
            }
            rowDelta.addDeletes(builder.build());
            if (task.getReferencedDataFile().isPresent()) {
                referencedDataFiles.add(task.getReferencedDataFile().get());
            }
        }

        if (!referencedDataFiles.isEmpty()) {
            rowDelta.validateDataFilesExist(referencedDataFiles);
        }

        rowDelta.commit();
        transaction.commitTransaction();
    }

    @Override
    public boolean supportsMetadataDelete(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<ConnectorTableLayoutHandle> tableLayoutHandle)
    {
        IcebergTableHandle handle = (IcebergTableHandle) tableHandle;
        if (handle.isSnapshotSpecified()) {
            return false;
        }

        if (!tableLayoutHandle.isPresent()) {
            return true;
        }

        // Allow metadata delete for range filters on partition columns.
        IcebergTableLayoutHandle layoutHandle = (IcebergTableLayoutHandle) tableLayoutHandle.get();

        // Do not support metadata delete if existing predicate on non-entire columns
        Optional<Set<Subfield>> subFields = layoutHandle.getDomainPredicate().getDomains()
                .map(subfieldDomainMap -> subfieldDomainMap.keySet().stream()
                        .filter(subfield -> !isEntireColumn(subfield))
                        .collect(Collectors.toSet()));
        if (subFields.isPresent() && !subFields.get().isEmpty()) {
            return false;
        }

        TupleDomain<IcebergColumnHandle> domainPredicate = layoutHandle.getDomainPredicate()
                .transform(Subfield::getRootName)
                .transform(layoutHandle.getPredicateColumns()::get)
                .transform(IcebergColumnHandle.class::cast);

        if (domainPredicate.isAll()) {
            return true;
        }

        // Get partition specs that really need to be checked
        Table icebergTable = getIcebergTable(session, handle.getSchemaTableName());
        Set<Integer> partitionSpecIds = getPartitionSpecsIncludingValidData(icebergTable, handle.getIcebergTableName().getSnapshotId());

        Set<Integer> enforcedColumnIds = getEnforcedColumns(icebergTable, partitionSpecIds, domainPredicate, session)
                .stream()
                .map(IcebergColumnHandle::getId)
                .collect(toImmutableSet());

        Set<Integer> predicateColumnIds = domainPredicate.getDomains().get().keySet().stream()
                .map(IcebergColumnHandle.class::cast)
                .map(IcebergColumnHandle::getId)
                .collect(toImmutableSet());

        return Objects.equals(enforcedColumnIds, predicateColumnIds);
    }

    @Override
    public OptionalLong metadataDelete(ConnectorSession session, ConnectorTableHandle tableHandle, ConnectorTableLayoutHandle tableLayoutHandle)
    {
        IcebergTableHandle handle = (IcebergTableHandle) tableHandle;
        IcebergTableLayoutHandle layoutHandle = (IcebergTableLayoutHandle) tableLayoutHandle;

        Table icebergTable;
        try {
            icebergTable = getIcebergTable(session, handle.getSchemaTableName());
        }
        catch (Exception e) {
            throw new TableNotFoundException(handle.getSchemaTableName());
        }

        TupleDomain<IcebergColumnHandle> domainPredicate = layoutHandle.getValidPredicate();
        return removeScanFiles(icebergTable, domainPredicate);
    }

    /**
     * Deletes all the files for a specific predicate
     *
     * @return the number of rows deleted from all files
     */
    private OptionalLong removeScanFiles(Table icebergTable, TupleDomain<IcebergColumnHandle> predicate)
    {
        transaction = icebergTable.newTransaction();
        DeleteFiles deleteFiles = transaction.newDelete()
                .deleteFromRowFilter(toIcebergExpression(predicate));
        deleteFiles.commit();
        transaction.commitTransaction();

        Map<String, String> summary = icebergTable.currentSnapshot().summary();
        long deletedRecords = Long.parseLong(summary.getOrDefault(DELETED_RECORDS_PROP, "0"));
        long removedPositionDeletes = Long.parseLong(summary.getOrDefault(REMOVED_POS_DELETES_PROP, "0"));
        long removedEqualityDeletes = Long.parseLong(summary.getOrDefault(REMOVED_EQ_DELETES_PROP, "0"));
        // Removed rows count is inaccurate when existing equality delete files
        return OptionalLong.of(deletedRecords - removedPositionDeletes - removedEqualityDeletes);
    }

    private static long getSnapshotIdForTableVersion(Table table, ConnectorTableVersion tableVersion)
    {
        if (tableVersion.getVersionType() == VersionType.TIMESTAMP) {
            if (tableVersion.getVersionExpressionType() instanceof TimestampWithTimeZoneType) {
                long millisUtc = new SqlTimestampWithTimeZone((long) tableVersion.getTableVersion()).getMillisUtc();
                return getSnapshotIdTimeOperator(table, millisUtc, tableVersion.getVersionOperator());
            }
            else if (tableVersion.getVersionExpressionType() instanceof TimestampType) {
                long timestampValue = (long) tableVersion.getTableVersion();
                long millisUtc = ((TimestampType) tableVersion.getVersionExpressionType()).getPrecision().toMillis(timestampValue);
                return getSnapshotIdTimeOperator(table, millisUtc, tableVersion.getVersionOperator());
            }
            throw new PrestoException(NOT_SUPPORTED, "Unsupported table version expression type: " + tableVersion.getVersionExpressionType());
        }
        if (tableVersion.getVersionType() == VersionType.VERSION) {
            long snapshotId;
            if (tableVersion.getVersionExpressionType() instanceof BigintType) {
                snapshotId = (long) tableVersion.getTableVersion();
                if (table.snapshot(snapshotId) == null) {
                    throw new PrestoException(ICEBERG_INVALID_SNAPSHOT_ID, "Iceberg snapshot ID does not exists: " + snapshotId);
                }
                if (tableVersion.getVersionOperator() == VersionOperator.EQUAL) { // AS OF Case
                    return snapshotId;
                }
                else { // BEFORE Case
                    return getSnapshotIdTimeOperator(table, table.snapshot(snapshotId).timestampMillis(), VersionOperator.LESS_THAN);
                }
            }
            else if (tableVersion.getVersionExpressionType() instanceof VarcharType) {
                String branchOrTagName = ((Slice) tableVersion.getTableVersion()).toStringUtf8();
                if (!table.refs().containsKey(branchOrTagName)) {
                    throw new PrestoException(ICEBERG_INVALID_SNAPSHOT_ID, "Could not find Iceberg table branch or tag: " + branchOrTagName);
                }
                return table.refs().get(branchOrTagName).snapshotId();
            }
            throw new PrestoException(NOT_SUPPORTED, "Unsupported table version expression type: " + tableVersion.getVersionExpressionType());
        }
        throw new PrestoException(NOT_SUPPORTED, "Unsupported table version type: " + tableVersion.getVersionType());
    }
}
