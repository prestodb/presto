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
import com.facebook.presto.common.type.TimestampWithTimeZoneType;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.HivePartition;
import com.facebook.presto.hive.HiveWrittenPartitions;
import com.facebook.presto.hive.NodeVersion;
import com.facebook.presto.iceberg.changelog.ChangelogUtil;
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
import com.facebook.presto.spi.function.StandardFunctionResolution;
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
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.hive.MetadataUtils.createPredicate;
import static com.facebook.presto.hive.MetadataUtils.getCombinedRemainingPredicate;
import static com.facebook.presto.hive.MetadataUtils.getDiscretePredicates;
import static com.facebook.presto.hive.MetadataUtils.getPredicate;
import static com.facebook.presto.hive.MetadataUtils.getSubfieldPredicate;
import static com.facebook.presto.iceberg.ExpressionConverter.toIcebergExpression;
import static com.facebook.presto.iceberg.IcebergColumnHandle.DATA_SEQUENCE_NUMBER_COLUMN_HANDLE;
import static com.facebook.presto.iceberg.IcebergColumnHandle.DATA_SEQUENCE_NUMBER_COLUMN_METADATA;
import static com.facebook.presto.iceberg.IcebergColumnHandle.PATH_COLUMN_HANDLE;
import static com.facebook.presto.iceberg.IcebergColumnHandle.PATH_COLUMN_METADATA;
import static com.facebook.presto.iceberg.IcebergColumnHandle.primitiveIcebergColumnHandle;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_INVALID_SNAPSHOT_ID;
import static com.facebook.presto.iceberg.IcebergMetadataColumn.DATA_SEQUENCE_NUMBER;
import static com.facebook.presto.iceberg.IcebergMetadataColumn.FILE_PATH;
import static com.facebook.presto.iceberg.IcebergSessionProperties.isPushdownFilterEnabled;
import static com.facebook.presto.iceberg.IcebergTableProperties.FILE_FORMAT_PROPERTY;
import static com.facebook.presto.iceberg.IcebergTableProperties.FORMAT_VERSION;
import static com.facebook.presto.iceberg.IcebergTableProperties.LOCATION_PROPERTY;
import static com.facebook.presto.iceberg.IcebergTableProperties.PARTITIONING_PROPERTY;
import static com.facebook.presto.iceberg.IcebergTableType.CHANGELOG;
import static com.facebook.presto.iceberg.IcebergTableType.DATA;
import static com.facebook.presto.iceberg.IcebergTableType.EQUALITY_DELETES;
import static com.facebook.presto.iceberg.IcebergUtil.getColumns;
import static com.facebook.presto.iceberg.IcebergUtil.getFileFormat;
import static com.facebook.presto.iceberg.IcebergUtil.getPartitionKeyColumnHandles;
import static com.facebook.presto.iceberg.IcebergUtil.getSnapshotIdAsOfTime;
import static com.facebook.presto.iceberg.IcebergUtil.getTableComment;
import static com.facebook.presto.iceberg.IcebergUtil.resolveSnapshotIdByName;
import static com.facebook.presto.iceberg.IcebergUtil.toHiveColumns;
import static com.facebook.presto.iceberg.IcebergUtil.tryGetSchema;
import static com.facebook.presto.iceberg.IcebergUtil.validateTableMode;
import static com.facebook.presto.iceberg.PartitionFields.getPartitionColumnName;
import static com.facebook.presto.iceberg.PartitionFields.getTransformTerm;
import static com.facebook.presto.iceberg.PartitionFields.toPartitionFields;
import static com.facebook.presto.iceberg.TableStatisticsMaker.getSupportedColumnStatistics;
import static com.facebook.presto.iceberg.TypeConverter.toIcebergType;
import static com.facebook.presto.iceberg.TypeConverter.toPrestoType;
import static com.facebook.presto.iceberg.changelog.ChangelogUtil.getRowTypeFromColumnMeta;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.statistics.TableStatisticType.ROW_COUNT;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

public abstract class IcebergAbstractMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(IcebergAbstractMetadata.class);

    protected final TypeManager typeManager;
    protected final JsonCodec<CommitTaskData> commitTaskCodec;
    protected final NodeVersion nodeVersion;
    protected final RowExpressionService rowExpressionService;
    private final StandardFunctionResolution functionResolution;

    protected Transaction transaction;

    public IcebergAbstractMetadata(
            TypeManager typeManager,
            StandardFunctionResolution functionResolution,
            RowExpressionService rowExpressionService,
            JsonCodec<CommitTaskData> commitTaskCodec,
            NodeVersion nodeVersion)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.commitTaskCodec = requireNonNull(commitTaskCodec, "commitTaskCodec is null");
        this.functionResolution = requireNonNull(functionResolution, "functionResolution is null");
        this.rowExpressionService = requireNonNull(rowExpressionService, "rowExpressionService is null");
        this.nodeVersion = requireNonNull(nodeVersion, "nodeVersion is null");
    }

    protected abstract Table getIcebergTable(ConnectorSession session, SchemaTableName schemaTableName);

    protected abstract boolean tableExists(ConnectorSession session, SchemaTableName schemaTableName);

    protected abstract void registerTable(ConnectorSession clientSession, SchemaTableName schemaTableName, Path metadataLocation);

    protected abstract void unregisterTable(ConnectorSession clientSession, SchemaTableName schemaTableName);

    /**
     * This class implements the default implementation for getTableLayouts which will be used in the case of a Java Worker
     */
    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(
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

        TupleDomain<ColumnHandle> partitionColumnPredicate = TupleDomain.withColumnDomains(Maps.filterKeys(constraint.getSummary().getDomains().get(), Predicates.in(getPartitionKeyColumnHandles(icebergTable, typeManager))));
        Optional<Set<IcebergColumnHandle>> requestedColumns = desiredColumns.map(columns -> columns.stream().map(column -> (IcebergColumnHandle) column).collect(toImmutableSet()));

        ConnectorTableLayout layout = getTableLayout(
                session,
                new IcebergTableLayoutHandle.Builder()
                        .setPartitionColumns(ImmutableList.copyOf(getPartitionKeyColumnHandles(icebergTable, typeManager)))
                        .setDataColumns(toHiveColumns(icebergTable.schema().columns()))
                        .setDomainPredicate(constraint.getSummary().transform(IcebergAbstractMetadata::toSubfield))
                        .setRemainingPredicate(TRUE_CONSTANT)
                        .setPredicateColumns(predicateColumns)
                        .setRequestedColumns(requestedColumns)
                        .setPushdownFilterEnabled(isPushdownFilterEnabled(session))
                        .setPartitionColumnPredicate(partitionColumnPredicate)
                        .setPartitions(Optional.empty())
                        .setTable(handle)
                        .build());
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
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

        if (!isPushdownFilterEnabled(session)) {
            return new ConnectorTableLayout(handle);
        }

        if (!icebergTableLayoutHandle.getPartitions().isPresent()) {
            return new ConnectorTableLayout(
                    icebergTableLayoutHandle,
                    Optional.empty(),
                    TupleDomain.none(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    ImmutableList.of(),
                    Optional.empty());
        }
        List<ColumnHandle> partitionColumns = ImmutableList.copyOf(icebergTableLayoutHandle.getPartitionColumns());
        List<HivePartition> partitions = icebergTableLayoutHandle.getPartitions().get();

        Optional<DiscretePredicates> discretePredicates = getDiscretePredicates(partitionColumns, partitions);

        TupleDomain<ColumnHandle> predicate;
        RowExpression subfieldPredicate;
        if (isPushdownFilterEnabled(session)) {
            Map<String, ColumnHandle> predicateColumns = icebergTableLayoutHandle.getPredicateColumns().entrySet()
                    .stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            predicate = getPredicate(icebergTableLayoutHandle, partitionColumns, partitions, predicateColumns);

            // capture subfields from domainPredicate to add to remainingPredicate
            // so those filters don't get lost
            Map<String, com.facebook.presto.common.type.Type> columnTypes = getColumns(icebergTable.schema(), icebergTable.spec(), typeManager).stream()
                    .collect(toImmutableMap(IcebergColumnHandle::getName, icebergColumnHandle -> getColumnMetadata(session, tableHandle, icebergColumnHandle).getType()));

            subfieldPredicate = getSubfieldPredicate(session, icebergTableLayoutHandle, columnTypes, functionResolution, rowExpressionService);
        }
        else {
            predicate = createPredicate(partitionColumns, partitions);
            subfieldPredicate = TRUE_CONSTANT;
        }

        // combine subfieldPredicate with remainingPredicate
        RowExpression combinedRemainingPredicate = getCombinedRemainingPredicate(icebergTableLayoutHandle, subfieldPredicate);

        return new ConnectorTableLayout(
                icebergTableLayoutHandle,
                Optional.empty(),
                predicate,
                Optional.empty(),
                Optional.empty(),
                discretePredicates,
                ImmutableList.of(),
                Optional.of(combinedRemainingPredicate));
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
        return finishInsert(session, (IcebergWritableTableHandle) tableHandle, fragments, computedStatistics);
    }

    protected ConnectorInsertTableHandle beginIcebergTableInsert(IcebergTableHandle table, Table icebergTable)
    {
        transaction = icebergTable.newTransaction();

        return new IcebergWritableTableHandle(
                table.getSchemaName(),
                table.getIcebergTableName(),
                SchemaParser.toJson(icebergTable.schema()),
                PartitionSpecParser.toJson(icebergTable.spec()),
                getColumns(icebergTable.schema(), icebergTable.spec(), typeManager),
                icebergTable.location(),
                getFileFormat(icebergTable),
                icebergTable.properties());
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        if (fragments.isEmpty()) {
            transaction.commitTransaction();
            return Optional.empty();
        }

        IcebergWritableTableHandle table = (IcebergWritableTableHandle) insertHandle;
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
    public ColumnHandle getDeleteRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return primitiveIcebergColumnHandle(0, "$row_id", BIGINT, Optional.empty());
    }

    @Override
    public boolean isLegacyGetLayoutSupported(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return !isPushdownFilterEnabled(session);
    }

    protected List<ColumnMetadata> getColumnMetadatas(Table table)
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
    public ConnectorTableHandle getTableHandleForStatisticsCollection(ConnectorSession session, SchemaTableName tableName, Map<String, Object> analyzeProperties)
    {
        return getTableHandle(session, tableName);
    }

    @Override
    public TableStatisticsMetadata getStatisticsCollectionMetadata(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        Set<ColumnStatisticMetadata> columnStatistics = tableMetadata.getColumns().stream()
                .filter(column -> !column.isHidden())
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
        TableStatisticsMaker.writeTableStatistics(nodeVersion, icebergTableHandle, icebergTable, session, computedStatistics);
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

        return beginIcebergTableInsert(table, icebergTable);
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
        IcebergTableHandle handle = (IcebergTableHandle) tableHandle;
        Table icebergTable = getIcebergTable(session, handle.getSchemaTableName());
        return TableStatisticsMaker.getTableStatistics(session, constraint, handle, icebergTable, columnHandles.stream().map(IcebergColumnHandle.class::cast).collect(Collectors.toList()));
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

        // Get Iceberg tables schema with missing filesystem metadata will fail.
        // See https://github.com/prestodb/presto/pull/21181
        Optional<Schema> tableSchema = tryGetSchema(table);
        Optional<String> tableSchemaJson = tableSchema.map(SchemaParser::toJson);

        return new IcebergTableHandle(
                tableName.getSchemaName(),
                new IcebergTableName(name.getTableName(), name.getTableType(), tableSnapshotId, name.getChangelogEndSnapshot()),
                name.getSnapshotId().isPresent(),
                TupleDomain.all(),
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
        try (CloseableIterable<FileScanTask> files = icebergTable.newScan().planFiles()) {
            removeScanFiles(icebergTable, files);
        }
        catch (IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "failed to scan files for delete", e);
        }
    }

    @Override
    public ConnectorTableHandle beginDelete(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle handle = (IcebergTableHandle) tableHandle;
        if (handle.isSnapshotSpecified()) {
            throw new PrestoException(NOT_SUPPORTED, "This connector do not allow delete data at specified snapshot");
        }
        throw new PrestoException(NOT_SUPPORTED, "This connector only supports delete where one or more partitions are deleted entirely");
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

        TupleDomain<ColumnHandle> domainPredicate = layoutHandle.getDomainPredicate()
                .transform(subfield -> isEntireColumn(subfield) ? subfield.getRootName() : null)
                .transform(layoutHandle.getPredicateColumns()::get)
                .transform(ColumnHandle.class::cast);

        if (domainPredicate.isAll()) {
            return true;
        }

        Set<Integer> predicateColumnIds = domainPredicate.getDomains().get().keySet().stream()
                .map(IcebergColumnHandle.class::cast)
                .map(IcebergColumnHandle::getId)
                .collect(toImmutableSet());
        Table icebergTable = getIcebergTable(session, handle.getSchemaTableName());

        boolean supportsMetadataDelete = true;
        for (PartitionSpec spec : icebergTable.specs().values()) {
            // Currently we do not support delete when any partition columns in predicate is not transform by identity()
            Set<Integer> partitionColumnSourceIds = spec.fields().stream()
                    .filter(field -> field.transform().isIdentity())
                    .map(PartitionField::sourceId)
                    .collect(Collectors.toSet());

            if (!partitionColumnSourceIds.containsAll(predicateColumnIds)) {
                supportsMetadataDelete = false;
                break;
            }
        }

        return supportsMetadataDelete;
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

        TableScan scan = icebergTable.newScan();
        TupleDomain<ColumnHandle> domainPredicate = layoutHandle.getDomainPredicate()
                .transform(subfield -> isEntireColumn(subfield) ? subfield.getRootName() : null)
                .transform(layoutHandle.getPredicateColumns()::get)
                .transform(ColumnHandle.class::cast);

        if (!domainPredicate.isAll()) {
            Expression filterExpression = toIcebergExpression(handle.getPredicate());
            scan = scan.filter(filterExpression);
        }

        try (CloseableIterable<FileScanTask> files = scan.planFiles()) {
            return OptionalLong.of(removeScanFiles(icebergTable, files));
        }
        catch (IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "failed to scan files for delete", e);
        }
    }

    /**
     * Deletes all the files within a particular scan
     *
     * @return the number of rows deleted from all files
     */
    private long removeScanFiles(Table icebergTable, Iterable<FileScanTask> scan)
    {
        transaction = icebergTable.newTransaction();
        DeleteFiles deletes = transaction.newDelete();
        AtomicLong rowsDeleted = new AtomicLong(0L);
        scan.forEach(t -> {
            deletes.deleteFile(t.file());
            rowsDeleted.addAndGet(t.estimatedRowsCount());
        });
        deletes.commit();
        transaction.commitTransaction();
        return rowsDeleted.get();
    }

    private static long getSnapshotIdForTableVersion(Table table, ConnectorTableVersion tableVersion)
    {
        if (tableVersion.getVersionType() == ConnectorTableVersion.VersionType.TIMESTAMP) {
            if (tableVersion.getVersionExpressionType() instanceof TimestampWithTimeZoneType) {
                long millisUtc = new SqlTimestampWithTimeZone((long) tableVersion.getTableVersion()).getMillisUtc();
                return getSnapshotIdAsOfTime(table, millisUtc);
            }
            throw new PrestoException(NOT_SUPPORTED, "Unsupported table version expression type: " + tableVersion.getVersionExpressionType());
        }
        if (tableVersion.getVersionType() == ConnectorTableVersion.VersionType.VERSION) {
            if (tableVersion.getVersionExpressionType() instanceof BigintType) {
                long snapshotId = (long) tableVersion.getTableVersion();
                if (table.snapshot(snapshotId) == null) {
                    throw new PrestoException(ICEBERG_INVALID_SNAPSHOT_ID, "Iceberg snapshot ID does not exists: " + snapshotId);
                }
                return snapshotId;
            }
            throw new PrestoException(NOT_SUPPORTED, "Unsupported table version expression type: " + tableVersion.getVersionExpressionType());
        }
        throw new PrestoException(NOT_SUPPORTED, "Unsupported table version type: " + tableVersion.getVersionType());
    }
}
