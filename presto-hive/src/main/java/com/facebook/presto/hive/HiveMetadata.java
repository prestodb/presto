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
package com.facebook.presto.hive;

import com.facebook.presto.hadoop.HadoopFileStatus;
import com.facebook.presto.hive.metastore.HiveMetastore;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorNewTableLayout;
import com.facebook.presto.spi.ConnectorNodePartitioning;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.ConnectorViewDefinition;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.DiscretePredicates;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.ViewNotFoundException;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.predicate.NullableValue;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.security.Privilege;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.airlift.concurrent.MoreFutures;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.joda.time.DateTimeZone;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.facebook.presto.hive.HiveColumnHandle.SAMPLE_WEIGHT_COLUMN_NAME;
import static com.facebook.presto.hive.HiveColumnHandle.updateRowIdHandle;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_COLUMN_ORDER_MISMATCH;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_CONCURRENT_MODIFICATION_DETECTED;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_FILESYSTEM_ERROR;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_PATH_ALREADY_EXISTS;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_TIMEZONE_MISMATCH;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_UNSUPPORTED_FORMAT;
import static com.facebook.presto.hive.HiveTableProperties.BUCKET_COUNT_PROPERTY;
import static com.facebook.presto.hive.HiveTableProperties.CLUSTERED_BY_PROPERTY;
import static com.facebook.presto.hive.HiveTableProperties.PARTITIONED_BY_PROPERTY;
import static com.facebook.presto.hive.HiveTableProperties.STORAGE_FORMAT_PROPERTY;
import static com.facebook.presto.hive.HiveTableProperties.getBucketProperty;
import static com.facebook.presto.hive.HiveTableProperties.getHiveStorageFormat;
import static com.facebook.presto.hive.HiveTableProperties.getPartitionedBy;
import static com.facebook.presto.hive.HiveType.toHiveType;
import static com.facebook.presto.hive.HiveUtil.PRESTO_VIEW_FLAG;
import static com.facebook.presto.hive.HiveUtil.annotateColumnComment;
import static com.facebook.presto.hive.HiveUtil.decodeViewData;
import static com.facebook.presto.hive.HiveUtil.encodeViewData;
import static com.facebook.presto.hive.HiveUtil.hiveColumnHandles;
import static com.facebook.presto.hive.HiveUtil.schemaTableName;
import static com.facebook.presto.hive.HiveWriteUtils.checkTableIsWritable;
import static com.facebook.presto.hive.HiveWriteUtils.createDirectory;
import static com.facebook.presto.hive.HiveWriteUtils.isWritableType;
import static com.facebook.presto.hive.HiveWriteUtils.pathExists;
import static com.facebook.presto.hive.HiveWriteUtils.renameDirectory;
import static com.facebook.presto.hive.util.Types.checkType;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.StandardErrorCode.PERMISSION_DENIED;
import static com.facebook.presto.spi.StandardErrorCode.USER_ERROR;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.concat;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static org.apache.hadoop.hive.serde.serdeConstants.STRING_TYPE_NAME;

public class HiveMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(HiveMetadata.class);
    private static final int PARTITION_COMMIT_BATCH_SIZE = 8;

    private final String connectorId;
    private final boolean allowDropTable;
    private final boolean allowRenameTable;
    private final boolean allowAddColumn;
    private final boolean allowRenameColumn;
    private final boolean allowCorruptWritesForTesting;
    private final HiveMetastore metastore;
    private final HdfsEnvironment hdfsEnvironment;
    private final HivePartitionManager partitionManager;
    private final DateTimeZone timeZone;
    private final TypeManager typeManager;
    private final LocationService locationService;
    private final TableParameterCodec tableParameterCodec;
    private final JsonCodec<PartitionUpdate> partitionUpdateCodec;
    private final Executor renameExecutor;
    private final boolean respectTableFormat;
    private final HiveStorageFormat defaultStorageFormat;

    private final AtomicReference<Runnable> rollbackAction = new AtomicReference<>();

    public HiveMetadata(
            String connectorId,
            HiveMetastore metastore,
            HdfsEnvironment hdfsEnvironment,
            HivePartitionManager partitionManager,
            DateTimeZone timeZone,
            boolean allowDropTable,
            boolean allowRenameTable,
            boolean allowAddColumn,
            boolean allowRenameColumn,
            boolean allowCorruptWritesForTesting,
            boolean respectTableFormat,
            HiveStorageFormat defaultStorageFormat,
            TypeManager typeManager,
            LocationService locationService,
            TableParameterCodec tableParameterCodec,
            JsonCodec<PartitionUpdate> partitionUpdateCodec,
            Executor renameExecutor)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");

        this.allowDropTable = allowDropTable;
        this.allowRenameTable = allowRenameTable;
        this.allowAddColumn = allowAddColumn;
        this.allowRenameColumn = allowRenameColumn;
        this.allowCorruptWritesForTesting = allowCorruptWritesForTesting;

        this.metastore = requireNonNull(metastore, "metastore is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.partitionManager = requireNonNull(partitionManager, "partitionManager is null");
        this.timeZone = requireNonNull(timeZone, "timeZone is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.locationService = requireNonNull(locationService, "locationService is null");
        this.tableParameterCodec = requireNonNull(tableParameterCodec, "tableParameterCodec is null");
        this.partitionUpdateCodec = requireNonNull(partitionUpdateCodec, "partitionUpdateCodec is null");
        this.respectTableFormat = respectTableFormat;
        this.defaultStorageFormat = requireNonNull(defaultStorageFormat, "defaultStorageFormat is null");

        this.renameExecutor = requireNonNull(renameExecutor, "renameExecution is null");
    }

    public HiveMetastore getMetastore()
    {
        return metastore;
    }

    public HivePartitionManager getPartitionManager()
    {
        return partitionManager;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return metastore.getAllDatabases();
    }

    @Override
    public HiveTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        requireNonNull(tableName, "tableName is null");
        if (!metastore.getTable(tableName.getSchemaName(), tableName.getTableName()).isPresent()) {
            return null;
        }
        return new HiveTableHandle(connectorId, tableName.getSchemaName(), tableName.getTableName());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        SchemaTableName tableName = schemaTableName(tableHandle);
        return getTableMetadata(tableName);
    }

    private ConnectorTableMetadata getTableMetadata(SchemaTableName tableName)
    {
        Optional<Table> table = metastore.getTable(tableName.getSchemaName(), tableName.getTableName());
        if (!table.isPresent() || table.get().getTableType().equals(TableType.VIRTUAL_VIEW.name())) {
            throw new TableNotFoundException(tableName);
        }

        Function<HiveColumnHandle, ColumnMetadata> metadataGetter = columnMetadataGetter(table.get(), typeManager);
        boolean sampled = false;
        ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
        for (HiveColumnHandle columnHandle : hiveColumnHandles(connectorId, table.get())) {
            if (columnHandle.getName().equals(SAMPLE_WEIGHT_COLUMN_NAME)) {
                sampled = true;
            }
            else {
                columns.add(metadataGetter.apply(columnHandle));
            }
        }

        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
        try {
            HiveStorageFormat format = extractHiveStorageFormat(table.get());
            properties.put(STORAGE_FORMAT_PROPERTY, format);
        }
        catch (PrestoException ignored) {
            // todo fail if format is not known
        }
        List<String> partitionedBy = table.get().getPartitionKeys().stream()
                .map(FieldSchema::getName)
                .collect(toList());
        if (!partitionedBy.isEmpty()) {
            properties.put(PARTITIONED_BY_PROPERTY, partitionedBy);
        }
        Optional<HiveBucketProperty> bucketProperty = HiveBucketProperty.fromStorageDescriptor(table.get().getSd(), table.get().getTableName());
        if (bucketProperty.isPresent()) {
            properties.put(BUCKET_COUNT_PROPERTY, bucketProperty.get().getBucketCount());
            properties.put(CLUSTERED_BY_PROPERTY, bucketProperty.get().getClusteredBy());
        }
        if (table.get().isSetParameters()) {
            properties.putAll(tableParameterCodec.decode(table.get().getParameters()));
        }

        return new ConnectorTableMetadata(tableName, columns.build(), properties.build(), table.get().getOwner(), sampled);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        ImmutableList.Builder<SchemaTableName> tableNames = ImmutableList.builder();
        for (String schemaName : listSchemas(session, schemaNameOrNull)) {
            for (String tableName : metastore.getAllTables(schemaName).orElse(emptyList())) {
                tableNames.add(new SchemaTableName(schemaName, tableName));
            }
        }
        return tableNames.build();
    }

    private List<String> listSchemas(ConnectorSession session, String schemaNameOrNull)
    {
        if (schemaNameOrNull == null) {
            return listSchemaNames(session);
        }
        return ImmutableList.of(schemaNameOrNull);
    }

    @Override
    public ColumnHandle getSampleWeightColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        SchemaTableName tableName = schemaTableName(tableHandle);
        Optional<Table> table = metastore.getTable(tableName.getSchemaName(), tableName.getTableName());
        if (!table.isPresent()) {
            throw new TableNotFoundException(tableName);
        }
        for (HiveColumnHandle columnHandle : hiveColumnHandles(connectorId, table.get())) {
            if (columnHandle.getName().equals(SAMPLE_WEIGHT_COLUMN_NAME)) {
                return columnHandle;
            }
        }
        return null;
    }

    @Override
    public boolean canCreateSampledTables(ConnectorSession session)
    {
        return true;
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        SchemaTableName tableName = schemaTableName(tableHandle);
        Optional<Table> table = metastore.getTable(tableName.getSchemaName(), tableName.getTableName());
        if (!table.isPresent()) {
            throw new TableNotFoundException(tableName);
        }
        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        for (HiveColumnHandle columnHandle : hiveColumnHandles(connectorId, table.get())) {
            if (!columnHandle.getName().equals(SAMPLE_WEIGHT_COLUMN_NAME)) {
                columnHandles.put(columnHandle.getName(), columnHandle);
            }
        }
        return columnHandles.build();
    }

    @SuppressWarnings("TryWithIdenticalCatches")
    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            try {
                columns.put(tableName, getTableMetadata(tableName).getColumns());
            }
            catch (HiveViewNotSupportedException e) {
                // view is not supported
            }
            catch (TableNotFoundException e) {
                // table disappeared during listing operation
            }
        }
        return columns.build();
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (prefix.getSchemaName() == null || prefix.getTableName() == null) {
            return listTables(session, prefix.getSchemaName());
        }
        return ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
    }

    /**
     * NOTE: This method does not return column comment
     */
    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        checkType(tableHandle, HiveTableHandle.class, "tableHandle");
        return checkType(columnHandle, HiveColumnHandle.class, "columnHandle").getColumnMetadata(typeManager);
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        checkArgument(!isNullOrEmpty(tableMetadata.getOwner()), "Table owner is null or empty");

        SchemaTableName schemaTableName = tableMetadata.getTable();
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();
        List<String> partitionedBy = getPartitionedBy(tableMetadata.getProperties());
        Optional<HiveBucketProperty> bucketProperty = getBucketProperty(tableMetadata.getProperties());
        List<HiveColumnHandle> columnHandles = getColumnHandles(connectorId, tableMetadata, ImmutableSet.copyOf(partitionedBy));
        HiveStorageFormat hiveStorageFormat = getHiveStorageFormat(tableMetadata.getProperties());
        Map<String, String> additionalTableParameters = tableParameterCodec.encode(tableMetadata.getProperties());

        LocationHandle locationHandle = locationService.forNewTable(session.getQueryId(), schemaName, tableName);
        Path targetPath = locationService.targetPathRoot(locationHandle);
        createDirectory(hdfsEnvironment, targetPath);

        createTable(schemaName, tableName, tableMetadata.getOwner(), columnHandles, hiveStorageFormat, partitionedBy, bucketProperty, additionalTableParameters, targetPath);
    }

    private Table createTable(
            String schemaName,
            String tableName,
            String tableOwner,
            List<HiveColumnHandle> columnHandles,
            HiveStorageFormat hiveStorageFormat,
            List<String> partitionedBy,
            Optional<HiveBucketProperty> bucketProperty,
            Map<String, String> additionalTableParameters,
            Path targetPath)
    {
        Map<String, HiveColumnHandle> columnHandlesByName = Maps.uniqueIndex(columnHandles, HiveColumnHandle::getName);
        List<FieldSchema> partitionColumns = partitionedBy.stream()
                .map(columnHandlesByName::get)
                .map(column -> new FieldSchema(column.getName(), column.getHiveType().getHiveTypeName(), null))
                .collect(toList());

        Set<String> partitionColumnNames = ImmutableSet.copyOf(partitionedBy);

        boolean sampled = false;
        ImmutableList.Builder<FieldSchema> columns = ImmutableList.builder();
        for (HiveColumnHandle columnHandle : columnHandles) {
            String name = columnHandle.getName();
            String type = columnHandle.getHiveType().getHiveTypeName();
            if (name.equals(SAMPLE_WEIGHT_COLUMN_NAME)) {
                columns.add(new FieldSchema(name, type, "Presto sample weight column"));
                sampled = true;
            }
            else if (!partitionColumnNames.contains(name)) {
                verify(!columnHandle.isPartitionKey(), "Column handles are not consistent with partitioned by property");
                columns.add(new FieldSchema(name, type, null));
            }
            else {
                verify(columnHandle.isPartitionKey(), "Column handles are not consistent with partitioned by property");
            }
        }

        Table table = new Table();
        table.setDbName(schemaName);
        table.setTableName(tableName);
        table.setOwner(tableOwner);
        table.setTableType(TableType.MANAGED_TABLE.toString());
        String tableComment = "Created by Presto";
        if (sampled) {
            tableComment = "Sampled table created by Presto. Only query this table from Hive if you understand how Presto implements sampling.";
        }
        table.setParameters(ImmutableMap.<String, String>builder()
                .put("comment", tableComment)
                .putAll(additionalTableParameters)
                .build());
        table.setPartitionKeys(partitionColumns);
        table.setSd(makeStorageDescriptor(tableName, hiveStorageFormat, targetPath, columns.build(), bucketProperty));

        PrivilegeGrantInfo allPrivileges = new PrivilegeGrantInfo("all", 0, tableOwner, PrincipalType.USER, true);
        table.setPrivileges(new PrincipalPrivilegeSet(
                ImmutableMap.of(tableOwner, ImmutableList.of(allPrivileges)),
                ImmutableMap.of(),
                ImmutableMap.of()));

        metastore.createTable(table);
        return table;
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column)
    {
        if (!allowAddColumn) {
            throw new PrestoException(PERMISSION_DENIED, "Adding Columns is disabled in this Hive catalog");
        }

        HiveTableHandle handle = checkType(tableHandle, HiveTableHandle.class, "tableHandle");
        Optional<Table> tableMetadata = metastore.getTable(handle.getSchemaName(), handle.getTableName());
        if (!tableMetadata.isPresent()) {
            throw new TableNotFoundException(handle.getSchemaTableName());
        }
        Table table = tableMetadata.get();
        StorageDescriptor sd = table.getSd();

        ImmutableList.Builder<FieldSchema> columns = ImmutableList.builder();
        columns.addAll(sd.getCols());
        columns.add(new FieldSchema(column.getName(), toHiveType(column.getType()).getHiveTypeName(), column.getComment()));
        sd.setCols(columns.build());

        table.setSd(sd);
        metastore.alterTable(handle.getSchemaName(), handle.getTableName(), table);
    }

    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle source, String target)
    {
        if (!allowRenameColumn) {
            throw new PrestoException(PERMISSION_DENIED, "Renaming columns is disabled in this Hive catalog");
        }

        HiveTableHandle hiveTableHandle = checkType(tableHandle, HiveTableHandle.class, "tableHandle");
        HiveColumnHandle sourceHandle = checkType(source, HiveColumnHandle.class, "columnHandle");
        Optional<Table> tableMetadata = metastore.getTable(hiveTableHandle.getSchemaName(), hiveTableHandle.getTableName());
        if (!tableMetadata.isPresent()) {
            throw new TableNotFoundException(hiveTableHandle.getSchemaTableName());
        }
        Table table = tableMetadata.get();
        StorageDescriptor sd = table.getSd();
        ImmutableList.Builder<FieldSchema> columns = ImmutableList.builder();
        for (FieldSchema fieldSchema : sd.getCols()) {
            if (fieldSchema.getName().equals(sourceHandle.getName())) {
                columns.add(new FieldSchema(target, fieldSchema.getType(), fieldSchema.getComment()));
            }
            else {
                columns.add(fieldSchema);
            }
        }
        sd.setCols(columns.build());
        table.setSd(sd);
        metastore.alterTable(hiveTableHandle.getSchemaName(), hiveTableHandle.getTableName(), table);
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        if (!allowRenameTable) {
            throw new PrestoException(PERMISSION_DENIED, "Renaming tables is disabled in this Hive catalog");
        }

        HiveTableHandle handle = checkType(tableHandle, HiveTableHandle.class, "tableHandle");
        SchemaTableName tableName = schemaTableName(tableHandle);
        Optional<Table> source = metastore.getTable(handle.getSchemaName(), handle.getTableName());
        if (!source.isPresent()) {
            throw new TableNotFoundException(tableName);
        }
        Table table = source.get();
        table.setDbName(newTableName.getSchemaName());
        table.setTableName(newTableName.getTableName());
        metastore.alterTable(handle.getSchemaName(), handle.getTableName(), table);
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        HiveTableHandle handle = checkType(tableHandle, HiveTableHandle.class, "tableHandle");
        SchemaTableName tableName = schemaTableName(tableHandle);

        if (!allowDropTable) {
            throw new PrestoException(PERMISSION_DENIED, "DROP TABLE is disabled in this Hive catalog");
        }

        Optional<Table> target = metastore.getTable(handle.getSchemaName(), handle.getTableName());
        if (!target.isPresent()) {
            throw new TableNotFoundException(tableName);
        }
        Table table = target.get();

        if (!session.getUser().equals(table.getOwner())) {
            throw new PrestoException(PERMISSION_DENIED, format("Unable to drop table '%s': owner of the table is different from session user", table));
        }
        metastore.dropTable(handle.getSchemaName(), handle.getTableName());
    }

    @Override
    public HiveOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
    {
        checkNoRollback();

        verifyJvmTimeZone();

        checkArgument(!isNullOrEmpty(tableMetadata.getOwner()), "Table owner is null or empty");

        HiveStorageFormat tableStorageFormat = getHiveStorageFormat(tableMetadata.getProperties());
        List<String> partitionedBy = getPartitionedBy(tableMetadata.getProperties());
        Optional<HiveBucketProperty> bucketProperty = getBucketProperty(tableMetadata.getProperties());
        Map<String, String> additionalTableParameters = tableParameterCodec.encode(tableMetadata.getProperties());

        // get the root directory for the database
        SchemaTableName schemaTableName = tableMetadata.getTable();
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();

        List<HiveColumnHandle> columnHandles = getColumnHandles(connectorId, tableMetadata, ImmutableSet.copyOf(partitionedBy));

        HiveOutputTableHandle result = new HiveOutputTableHandle(
                connectorId,
                schemaName,
                tableName,
                columnHandles,
                session.getQueryId(),
                locationService.forNewTable(session.getQueryId(), schemaName, tableName),
                tableStorageFormat,
                respectTableFormat ? tableStorageFormat : defaultStorageFormat,
                partitionedBy,
                bucketProperty,
                tableMetadata.getOwner(),
                additionalTableParameters);

        setRollback(() -> rollbackCreateTable(result));
        return result;
    }

    @Override
    public void finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments)
    {
        HiveOutputTableHandle handle = checkType(tableHandle, HiveOutputTableHandle.class, "tableHandle");

        List<PartitionUpdate> partitionUpdates = fragments.stream()
                .map(Slice::getBytes)
                .map(partitionUpdateCodec::fromJson)
                .collect(toList());

        Path targetPath = locationService.targetPathRoot(handle.getLocationHandle());
        Path writePath = locationService.writePathRoot(handle.getLocationHandle()).get();

        // rename if using a temporary directory
        if (!targetPath.equals(writePath)) {
            // verify no one raced us to create the target directory
            if (pathExists(hdfsEnvironment, targetPath)) {
                throw new PrestoException(HIVE_PATH_ALREADY_EXISTS, format("Target directory for table '%s.%s' already exists: %s",
                        handle.getSchemaName(),
                        handle.getTableName(),
                        targetPath));
            }
            // rename the temporary directory to the target
            renameDirectory(hdfsEnvironment, handle.getSchemaName(), handle.getTableName(), writePath, targetPath);
        }

        PartitionCommitter partitionCommitter = new PartitionCommitter(handle.getSchemaName(), handle.getTableName(), metastore, PARTITION_COMMIT_BATCH_SIZE);
        try {
            partitionUpdates = PartitionUpdate.mergePartitionUpdates(partitionUpdates);

            Table table = createTable(
                    handle.getSchemaName(),
                    handle.getTableName(),
                    handle.getTableOwner(),
                    handle.getInputColumns(),
                    handle.getTableStorageFormat(),
                    handle.getPartitionedBy(),
                    handle.getBucketProperty(),
                    handle.getAdditionalTableParameters(),
                    targetPath);

            if (!handle.getPartitionedBy().isEmpty()) {
                if (respectTableFormat) {
                    Verify.verify(handle.getPartitionStorageFormat() == handle.getTableStorageFormat());
                }
                partitionUpdates.stream()
                        .map(partitionUpdate -> createPartition(table, partitionUpdate))
                        .forEach(partitionCommitter::addPartition);
            }
            partitionCommitter.flush();
        }
        catch (Throwable throwable) {
            partitionCommitter.abort();
            rollbackPartitionUpdates(partitionUpdates, "table creation");
            throw throwable;
        }

        clearRollback();
    }

    private void rollbackCreateTable(ConnectorOutputTableHandle tableHandle)
    {
        HiveOutputTableHandle handle = checkType(tableHandle, HiveOutputTableHandle.class, "tableHandle");
        cleanupTempDirectory(locationService.writePathRoot(handle.getLocationHandle()).get().toString(), handle.getFilePrefix(), "create table");
        // Note: there is no need to cleanup the target directory as it will only be written
        // to during the commit call and the commit call cleans up after failures.
    }

    @Override
    public HiveInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        checkNoRollback();

        verifyJvmTimeZone();

        SchemaTableName tableName = schemaTableName(tableHandle);
        Optional<Table> table = metastore.getTable(tableName.getSchemaName(), tableName.getTableName());
        if (!table.isPresent()) {
            throw new TableNotFoundException(tableName);
        }

        checkTableIsWritable(table.get());

        List<HiveColumnHandle> handles = hiveColumnHandles(connectorId, table.get());

        for (HiveColumnHandle hiveColumnHandle : handles) {
            if (!isWritableType(hiveColumnHandle.getHiveType())) {
                throw new PrestoException(NOT_SUPPORTED, format("Inserting into Hive table with column type %s not supported", hiveColumnHandle.getHiveType()));
            }
        }

        HiveStorageFormat tableStorageFormat = extractHiveStorageFormat(table.get());
        HiveInsertTableHandle result = new HiveInsertTableHandle(
                connectorId,
                tableName.getSchemaName(),
                tableName.getTableName(),
                handles,
                session.getQueryId(),
                locationService.forExistingTable(session.getQueryId(), table.get()),
                HiveBucketProperty.fromStorageDescriptor(table.get().getSd(), table.get().getTableName()),
                tableStorageFormat,
                respectTableFormat ? tableStorageFormat : defaultStorageFormat);

        setRollback(() -> rollbackInsert(result));
        return result;
    }

    private static class PartitionCommitter
    {
        private final String schemaName;
        private final String tableName;
        private final HiveMetastore metastore;
        private final int batchSize;
        private final List<Partition> batch;
        private final List<Partition> createdPartitions = new ArrayList<>();

        public PartitionCommitter(String schemaName, String tableName, HiveMetastore metastore, int batchSize)
        {
            this.schemaName = schemaName;
            this.tableName = tableName;
            this.metastore = metastore;
            this.batchSize = batchSize;
            this.batch = new ArrayList<>(batchSize);
        }

        public List<Partition> getCreatedPartitions()
        {
            return ImmutableList.copyOf(createdPartitions);
        }

        public void addPartition(Partition partition)
        {
            batch.add(partition);
            if (batch.size() >= batchSize) {
                addBatch();
            }
        }

        public void flush()
        {
            if (!batch.isEmpty()) {
                addBatch();
            }
        }

        public void abort()
        {
            // drop created partitions
            for (Partition createdPartition : getCreatedPartitions()) {
                try {
                    metastore.dropPartition(schemaName, tableName, createdPartition.getValues());
                }
                catch (Exception e) {
                    log.error(e, "Error rolling back new partition '%s' in table '%s.%s", createdPartition.getValues(), schemaName, tableName);
                }
            }
        }

        private void addBatch()
        {
            metastore.addPartitions(schemaName, tableName, batch);
            createdPartitions.addAll(batch);
            batch.clear();
        }
    }

    @Override
    public void finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments)
    {
        HiveInsertTableHandle handle = checkType(insertHandle, HiveInsertTableHandle.class, "invalid insertTableHandle");

        List<PartitionUpdate> partitionUpdates = fragments.stream()
                .map(Slice::getBytes)
                .map(partitionUpdateCodec::fromJson)
                .collect(toList());

        HiveStorageFormat tableStorageFormat = handle.getTableStorageFormat();
        PartitionCommitter partitionCommitter = new PartitionCommitter(handle.getSchemaName(), handle.getTableName(), metastore, PARTITION_COMMIT_BATCH_SIZE);
        try {
            partitionUpdates = PartitionUpdate.mergePartitionUpdates(partitionUpdates);

            Optional<Table> table = metastore.getTable(handle.getSchemaName(), handle.getTableName());
            if (!table.isPresent()) {
                throw new TableNotFoundException(new SchemaTableName(handle.getSchemaName(), handle.getTableName()));
            }
            if (!table.get().getSd().getInputFormat().equals(tableStorageFormat.getInputFormat()) && respectTableFormat) {
                throw new PrestoException(HIVE_CONCURRENT_MODIFICATION_DETECTED, "Table format changed during insert");
            }

            List<CompletableFuture<?>> fileRenameFutures = new ArrayList<>();
            for (PartitionUpdate partitionUpdate : partitionUpdates) {
                if (!partitionUpdate.getName().isEmpty() && partitionUpdate.isNew()) {
                    // move data to final location
                    if (!partitionUpdate.getWritePath().equals(partitionUpdate.getTargetPath())) {
                        renameDirectory(hdfsEnvironment,
                                table.get().getDbName(),
                                table.get().getTableName(),
                                new Path(partitionUpdate.getWritePath()),
                                new Path(partitionUpdate.getTargetPath()));
                    }
                    // add new partition
                    Partition partition = createPartition(table.get(), partitionUpdate);
                    if (!partition.getSd().getInputFormat().equals(handle.getPartitionStorageFormat().getInputFormat()) && respectTableFormat) {
                        throw new PrestoException(HIVE_CONCURRENT_MODIFICATION_DETECTED, "Partition format changed during insert");
                    }
                    partitionCommitter.addPartition(partition);
                }
                else {
                    // move data to final location
                    if (!partitionUpdate.getWritePath().equals(partitionUpdate.getTargetPath())) {
                        Path writeDir = new Path(partitionUpdate.getWritePath());
                        Path targetDir = new Path(partitionUpdate.getTargetPath());

                        FileSystem fileSystem;
                        try {
                            fileSystem = hdfsEnvironment.getFileSystem(targetDir);
                        }
                        catch (IOException e) {
                            throw new PrestoException(HIVE_FILESYSTEM_ERROR, e);
                        }

                        for (String fileName : partitionUpdate.getFileNames()) {
                            fileRenameFutures.add(CompletableFuture.runAsync(() -> {
                                Path source = new Path(writeDir, fileName);
                                Path target = new Path(targetDir, fileName);
                                try {
                                    fileSystem.rename(source, target);
                                }
                                catch (IOException e) {
                                    throw new PrestoException(HIVE_FILESYSTEM_ERROR, format("Error moving INSERT data from %s to final location %s", source, target), e);
                                }
                            }, renameExecutor));
                        }
                    }
                }
            }
            partitionCommitter.flush();
            for (CompletableFuture<?> fileRenameFuture : fileRenameFutures) {
                MoreFutures.getFutureValue(fileRenameFuture, PrestoException.class);
            }
        }
        catch (Throwable t) {
            partitionCommitter.abort();
            rollbackPartitionUpdates(partitionUpdates, "insert");
            throw t;
        }

        clearRollback();
    }

    private Partition createPartition(Table table, PartitionUpdate partitionUpdate)
    {
        List<String> values = HivePartitionManager.extractPartitionKeyValues(partitionUpdate.getName());
        Partition partition = new Partition();
        partition.setDbName(table.getDbName());
        partition.setTableName(table.getTableName());
        partition.setValues(values);

        if (respectTableFormat) {
            partition.setSd(table.getSd().deepCopy());
            partition.getSd().setLocation(partitionUpdate.getTargetPath());
        }
        else {
            partition.setSd(makeStorageDescriptor(
                    table.getTableName(),
                    defaultStorageFormat,
                    new Path(partitionUpdate.getTargetPath()),
                    table.getSd().getCols(),
                    HiveBucketProperty.fromStorageDescriptor(table.getSd(), table.getTableName())));
        }

        return partition;
    }

    private void rollbackInsert(ConnectorInsertTableHandle insertHandle)
    {
        HiveInsertTableHandle handle = checkType(insertHandle, HiveInsertTableHandle.class, "invalid insertHandle");

        // if there is a temp directory, we only need to cleanup temp files in this directory
        Optional<Path> writePath = locationService.writePathRoot(handle.getLocationHandle());
        if (writePath.isPresent()) {
            cleanupTempDirectory(writePath.get().toString(), handle.getFilePrefix(), "insert");
            // Note: in this case there is no need to cleanup the target directory as it will only
            // be written to during the commit call and the commit call cleans up after failures.
            return;
        }

        // Otherwise, insert was directly into the target table and partitions, and all must be checked for temp files
        Optional<Table> table = metastore.getTable(handle.getSchemaName(), handle.getTableName());
        if (!table.isPresent()) {
            log.error("Error rolling back insert into table %s.%s. Table was dropped during insert, and data directory may contain temporary data", handle.getSchemaName(), handle.getTableName());
            return;
        }

        Set<String> locationsToClean = new HashSet<>();

        // check the base directory of the table (this is where new partitions are created)
        String tableDirectory = locationService.targetPathRoot(handle.getLocationHandle()).toString();
        locationsToClean.add(tableDirectory);

        // check every existing partition that is outside for the base directory
        if (!table.get().getPartitionKeys().isEmpty()) {
            List<String> partitionNames = metastore.getPartitionNames(handle.getSchemaName(), handle.getTableName())
                    .orElse(ImmutableList.of());
            for (List<String> partitionNameBatch : Iterables.partition(partitionNames, 10)) {
                metastore.getPartitionsByNames(handle.getSchemaName(), handle.getTableName(), partitionNameBatch).orElse(ImmutableMap.of()).values().stream()
                        .map(partition -> partition.getSd().getLocation())
                        .filter(location -> !location.startsWith(tableDirectory))
                        .forEach(locationsToClean::add);
            }
        }

        // delete any file that starts with the unique prefix of this query
        List<String> notDeletedFiles = new ArrayList<>();
        for (String location : locationsToClean) {
            notDeletedFiles.addAll(recursiveDeleteFilesStartingWith(location, handle.getFilePrefix()));
        }
        if (!notDeletedFiles.isEmpty()) {
            log.error("Cannot delete insert data files %s", notDeletedFiles);
        }

        // Note: we can not delete any of these locations since we do not know who created them
    }

    private void cleanupTempDirectory(String location, String filePrefix, String actionName)
    {
        // to be safe only delete files that start with the unique prefix for this query
        List<String> notDeletedFiles = recursiveDeleteFilesStartingWith(location, filePrefix);
        if (!notDeletedFiles.isEmpty()) {
            log.warn("Error rolling back " + actionName + " temporary data files %s", notDeletedFiles.stream()
                    .collect(joining(", ")));
        }

        // try to delete the temp directory
        if (!deleteIfExists(location)) {
            // this is temp data so an error isn't a big problem
            log.debug("Error deleting " + actionName + " temp data in %s", location);
        }
    }

    private void rollbackPartitionUpdates(List<PartitionUpdate> partitionUpdates, String actionName)
    {
        for (PartitionUpdate partitionUpdate : partitionUpdates) {
            String targetPath = partitionUpdate.getTargetPath();
            String writePath = partitionUpdate.getWritePath();

            // delete temp data if we used a temp dir
            if (!writePath.equals(targetPath)) {
                // to be safe only delete the files we know we created in the temp directory
                List<String> notDeletedFiles = deleteFilesFrom(writePath, partitionUpdate.getFileNames());
                if (!notDeletedFiles.isEmpty()) {
                    log.warn("Error rolling back " + actionName + " temporary data files %s", notDeletedFiles.stream()
                            .collect(joining(", ")));
                }

                // try to delete the temp directory
                if (!deleteIfExists(writePath)) {
                    // this is temp data so an error isn't a big problem
                    log.debug("Error deleting " + actionName + " temp data in %s", writePath);
                }
            }

            // delete data from target directory
            List<String> notDeletedFiles = deleteFilesFrom(targetPath, partitionUpdate.getFileNames());
            if (!notDeletedFiles.isEmpty()) {
                log.error("Error rolling back " + actionName + " data files %s", notDeletedFiles.stream()
                        .collect(joining(", ")));
            }

            // only try to delete directory if the partition is new
            if (partitionUpdate.isNew()) {
                if (!deleteIfExists(targetPath)) {
                    log.debug("Cannot delete " + actionName + " directory %s", targetPath);
                }
            }
        }
    }

    /**
     * Attempts to remove the file or empty directory.
     * @return true if the location no longer exists
     */
    public boolean deleteIfExists(String location)
    {
        Path path = new Path(location);

        FileSystem fileSystem;
        try {
            fileSystem = hdfsEnvironment.getFileSystem(path);
        }
        catch (IOException ignored) {
            return false;
        }

        return deleteIfExists(fileSystem, path);
    }

    /**
     * Attempts to remove the file or empty directory.
     * @return true if the location no longer exists
     */
    private static boolean deleteIfExists(FileSystem fileSystem, Path path)
    {
        try {
            // attempt to delete the path
            if (fileSystem.delete(path, false)) {
                return true;
            }

            // delete failed
            // check if path still exists
            return !fileSystem.exists(path);
        }
        catch (FileNotFoundException ignored) {
            // path was already removed or never existed
            return true;
        }
        catch (IOException ignored) {
        }
        return false;
    }

    /**
     * Attempt to remove the {@code fileNames} files within {@code location}.
     * @return the files that could not be removed
     */
    private List<String> deleteFilesFrom(String location, List<String> fileNames)
    {
        Path directory = new Path(location);
        FileSystem fileSystem;
        try {
            fileSystem = hdfsEnvironment.getFileSystem(directory);
        }
        catch (IOException e) {
            return fileNames;
        }

        ImmutableList.Builder<String> notDeletedFiles = ImmutableList.builder();
        for (String fileName : fileNames) {
            Path file = new Path(directory, fileName);
            if (!deleteIfExists(fileSystem, file)) {
                notDeletedFiles.add(file.toString());
            }
        }
        return notDeletedFiles.build();
    }

    /**
     * Attempt to remove all files in all directories within {@code location} that start with the {@code filePrefix}.
     * @return the files starting with the {@code filePrefix} that could not be removed
     */
    private List<String> recursiveDeleteFilesStartingWith(String location, String filePrefix)
    {
        FileSystem fileSystem;
        try {
            Path directory = new Path(location);
            fileSystem = hdfsEnvironment.getFileSystem(directory);
        }
        catch (IOException e) {
            return ImmutableList.of(location + "/" + filePrefix + "*");
        }

        return recursiveDeleteFilesStartingWith(fileSystem, new Path(location), filePrefix);
    }

    /**
     * Attempt to remove all files in all directories within {@code location} that start with the {@code filePrefix}.
     * @return the files starting with the {@code filePrefix} that could not be removed
     */
    private static List<String> recursiveDeleteFilesStartingWith(FileSystem fileSystem, Path directory, String filePrefix)
    {
        FileStatus[] allFiles;
        try {
            allFiles = fileSystem.listStatus(directory);
        }
        catch (IOException e) {
            return ImmutableList.of(directory + "/" + filePrefix + "*");
        }

        ImmutableList.Builder<String> notDeletedFiles = ImmutableList.builder();
        for (FileStatus fileStatus : allFiles) {
            Path path = fileStatus.getPath();
            if (HadoopFileStatus.isFile(fileStatus) && path.getName().startsWith(filePrefix)) {
                if (!deleteIfExists(fileSystem, path)) {
                    notDeletedFiles.add(path.toString());
                }
            }
            else if (HadoopFileStatus.isDirectory(fileStatus)) {
                notDeletedFiles.addAll(recursiveDeleteFilesStartingWith(fileSystem, path, filePrefix));
            }
        }
        return notDeletedFiles.build();
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName viewName, String viewData, boolean replace)
    {
        if (replace) {
            try {
                dropView(session, viewName);
            }
            catch (ViewNotFoundException ignored) {
            }
        }

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("comment", "Presto View")
                .put(PRESTO_VIEW_FLAG, "true")
                .build();

        FieldSchema dummyColumn = new FieldSchema("dummy", STRING_TYPE_NAME, null);

        StorageDescriptor sd = new StorageDescriptor();
        sd.setCols(ImmutableList.of(dummyColumn));
        sd.setSerdeInfo(new SerDeInfo());

        Table table = new Table();
        table.setDbName(viewName.getSchemaName());
        table.setTableName(viewName.getTableName());
        table.setOwner(session.getUser());
        table.setTableType(TableType.VIRTUAL_VIEW.name());
        table.setParameters(properties);
        table.setViewOriginalText(encodeViewData(viewData));
        table.setViewExpandedText("/* Presto View */");
        table.setSd(sd);

        PrivilegeGrantInfo allPrivileges = new PrivilegeGrantInfo("all", 0, session.getUser(), PrincipalType.USER, true);
        table.setPrivileges(new PrincipalPrivilegeSet(
                ImmutableMap.of(session.getUser(), ImmutableList.of(allPrivileges)),
                ImmutableMap.of(),
                ImmutableMap.of()));

        try {
            metastore.createTable(table);
        }
        catch (TableAlreadyExistsException e) {
            throw new ViewAlreadyExistsException(e.getTableName());
        }
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        ConnectorViewDefinition view = getViews(session, viewName.toSchemaTablePrefix()).get(viewName);
        if (view == null) {
            throw new ViewNotFoundException(viewName);
        }

        try {
            metastore.dropTable(viewName.getSchemaName(), viewName.getTableName());
        }
        catch (TableNotFoundException e) {
            throw new ViewNotFoundException(e.getTableName());
        }
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, String schemaNameOrNull)
    {
        ImmutableList.Builder<SchemaTableName> tableNames = ImmutableList.builder();
        for (String schemaName : listSchemas(session, schemaNameOrNull)) {
            for (String tableName : metastore.getAllViews(schemaName).orElse(emptyList())) {
                tableNames.add(new SchemaTableName(schemaName, tableName));
            }
        }
        return tableNames.build();
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, SchemaTablePrefix prefix)
    {
        ImmutableMap.Builder<SchemaTableName, ConnectorViewDefinition> views = ImmutableMap.builder();
        List<SchemaTableName> tableNames;
        if (prefix.getTableName() != null) {
            tableNames = ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
        }
        else {
            tableNames = listViews(session, prefix.getSchemaName());
        }

        for (SchemaTableName schemaTableName : tableNames) {
            Optional<Table> table = metastore.getTable(schemaTableName.getSchemaName(), schemaTableName.getTableName());
            if (table.isPresent() && HiveUtil.isPrestoView(table.get())) {
                views.put(schemaTableName, new ConnectorViewDefinition(
                        schemaTableName,
                        Optional.ofNullable(table.get().getOwner()),
                        decodeViewData(table.get().getViewOriginalText())));
            }
        }

        return views.build();
    }

    @Override
    public ConnectorTableHandle beginDelete(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector only supports delete where one or more partitions are deleted entirely");
    }

    @Override
    public ColumnHandle getUpdateRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return updateRowIdHandle(connectorId);
    }

    @Override
    public OptionalLong metadataDelete(ConnectorSession session, ConnectorTableHandle tableHandle, ConnectorTableLayoutHandle tableLayoutHandle)
    {
        HiveTableHandle handle = checkType(tableHandle, HiveTableHandle.class, "tableHandle");
        HiveTableLayoutHandle layoutHandle = checkType(tableLayoutHandle, HiveTableLayoutHandle.class, "tableLayoutHandle");

        for (HivePartition hivePartition : getOrComputePartitions(layoutHandle, session, tableHandle)) {
            metastore.dropPartitionByName(handle.getSchemaName(), handle.getTableName(), hivePartition.getPartitionId());
        }
        // it is too expensive to determine the exact number of deleted rows
        return OptionalLong.empty();
    }

    private List<HivePartition> getOrComputePartitions(HiveTableLayoutHandle layoutHandle, ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        if (layoutHandle.getPartitions().isPresent()) {
            return layoutHandle.getPartitions().get();
        }
        else {
            TupleDomain<ColumnHandle> promisedPredicate = layoutHandle.getPromisedPredicate();
            Predicate<Map<ColumnHandle, NullableValue>> predicate = convertToPredicate(promisedPredicate);
            List<ConnectorTableLayoutResult> tableLayoutResults = getTableLayouts(session, tableHandle, new Constraint<>(promisedPredicate, predicate), Optional.empty());
            return checkType(Iterables.getOnlyElement(tableLayoutResults).getTableLayout().getHandle(), HiveTableLayoutHandle.class, "tableLayoutHandle").getPartitions().get();
        }
    }

    @VisibleForTesting
    static Predicate<Map<ColumnHandle, NullableValue>> convertToPredicate(TupleDomain<ColumnHandle> tupleDomain)
    {
        return bindings -> tupleDomain.contains(TupleDomain.fromFixedValues(bindings));
    }

    @Override
    public boolean supportsMetadataDelete(ConnectorSession session, ConnectorTableHandle tableHandle, ConnectorTableLayoutHandle tableLayoutHandle)
    {
        HiveTableLayoutHandle layoutHandle = checkType(tableLayoutHandle, HiveTableLayoutHandle.class, "tableLayoutHandle");

        // return true if none of the partitions is <UNPARTITIONED>
        return layoutHandle.getPartitions().get().stream()
                .noneMatch(partition -> HivePartition.UNPARTITIONED_ID.equals(partition.getPartitionId()));
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle tableHandle, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        HiveTableHandle handle = checkType(tableHandle, HiveTableHandle.class, "tableHandle");

        HivePartitionResult hivePartitionResult = partitionManager.getPartitions(session, metastore, tableHandle, constraint.getSummary());

        List<HivePartition> partitions = hivePartitionResult.getPartitions().stream()
                .filter(partition -> constraint.predicate().test(partition.getKeys()))
                .collect(toList());

        return ImmutableList.of(new ConnectorTableLayoutResult(
                getTableLayout(
                        session,
                        new HiveTableLayoutHandle(
                                handle.getClientId(),
                                ImmutableList.copyOf(hivePartitionResult.getPartitionColumns()),
                                partitions,
                                hivePartitionResult.getEnforcedConstraint(),
                                hivePartitionResult.getBucketHandle())),
                hivePartitionResult.getUnenforcedConstraint()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle layoutHandle)
    {
        HiveTableLayoutHandle hiveLayoutHandle = checkType(layoutHandle, HiveTableLayoutHandle.class, "layoutHandle");
        List<ColumnHandle> partitionColumns = hiveLayoutHandle.getPartitionColumns();
        List<TupleDomain<ColumnHandle>> partitionDomains = hiveLayoutHandle.getPartitions().get().stream()
                .map(HivePartition::getTupleDomain)
                .collect(toList());

        TupleDomain<ColumnHandle> predicate = TupleDomain.none();
        if (!partitionDomains.isEmpty()) {
            predicate = TupleDomain.columnWiseUnion(partitionDomains);
        }

        Optional<DiscretePredicates> discretePredicates = Optional.empty();
        if (!partitionColumns.isEmpty()) {
            discretePredicates = Optional.of(new DiscretePredicates(partitionColumns, partitionDomains));
        }

        return new ConnectorTableLayout(
                hiveLayoutHandle,
                Optional.empty(),
                predicate,
                hiveLayoutHandle.getBucketHandle().map(hiveBucketHandle -> new ConnectorNodePartitioning(
                        new HivePartitioningHandle(
                                connectorId,
                                hiveBucketHandle.getBucketCount(),
                                hiveBucketHandle.getColumns().stream()
                                        .map(HiveColumnHandle::getHiveType)
                                        .collect(Collectors.toList())),
                        hiveBucketHandle.getColumns().stream()
                                .map(hiveColumnHandle -> (ColumnHandle) hiveColumnHandle)
                                .collect(toList()))),
                Optional.empty(),
                discretePredicates,
                ImmutableList.of());
    }

    @Override
    public Optional<ConnectorNewTableLayout> getNewTableLayout(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        Optional<HiveBucketProperty> bucketProperty = getBucketProperty(tableMetadata.getProperties());
        if (!bucketProperty.isPresent()) {
            return Optional.empty();
        }
        List<String> clusteredBy = bucketProperty.get().getClusteredBy();
        Map<String, HiveType> hiveTypeMap = tableMetadata.getColumns().stream()
                .collect(toMap(ColumnMetadata::getName, column -> toHiveType(column.getType())));
        return Optional.of(new ConnectorNewTableLayout(
                new HivePartitioningHandle(
                        connectorId,
                        bucketProperty.get().getBucketCount(),
                        clusteredBy.stream()
                                .map(hiveTypeMap::get)
                                .collect(toList())),
                clusteredBy));
    }

    @Override
    public void grantTablePrivileges(ConnectorSession session, SchemaTableName schemaTableName, Set<Privilege> privileges, String grantee, boolean grantOption)
    {
        String schemaName = schemaTableName.getSchemaName();

        String tableName = schemaTableName.getTableName();

        Set<PrivilegeGrantInfo> privilegeGrantInfoSet = privileges.stream()
                .map(privilege -> new PrivilegeGrantInfo(privilege.name().toLowerCase(), 0, session.getUser(), PrincipalType.USER, grantOption))
                .collect(toSet());

        metastore.grantTablePrivileges(schemaName, tableName, grantee, privilegeGrantInfoSet);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("clientId", connectorId)
                .toString();
    }

    private void verifyJvmTimeZone()
    {
        if (!allowCorruptWritesForTesting && !timeZone.equals(DateTimeZone.getDefault())) {
            throw new PrestoException(HIVE_TIMEZONE_MISMATCH, format(
                    "To write Hive data, your JVM timezone must match the Hive storage timezone. Add -Duser.timezone=%s to your JVM arguments.",
                    timeZone.getID()));
        }
    }

    private static HiveStorageFormat extractHiveStorageFormat(Table table)
    {
        StorageDescriptor descriptor = table.getSd();
        if (descriptor == null) {
            throw new PrestoException(HIVE_INVALID_METADATA, "Table is missing storage descriptor");
        }
        SerDeInfo serdeInfo = descriptor.getSerdeInfo();
        if (serdeInfo == null) {
            throw new PrestoException(HIVE_INVALID_METADATA, "Table storage descriptor is missing SerDe info");
        }
        String outputFormat = descriptor.getOutputFormat();
        String serializationLib = serdeInfo.getSerializationLib();

        for (HiveStorageFormat format : HiveStorageFormat.values()) {
            if (format.getOutputFormat().equals(outputFormat) && format.getSerDe().equals(serializationLib)) {
                return format;
            }
        }
        throw new PrestoException(HIVE_UNSUPPORTED_FORMAT, format("Output format %s with SerDe %s is not supported", outputFormat, serializationLib));
    }

    private static void validatePartitionColumns(ConnectorTableMetadata tableMetadata)
    {
        List<String> partitionedBy = getPartitionedBy(tableMetadata.getProperties());

        List<String> allColumns = tableMetadata.getColumns().stream()
                .map(ColumnMetadata::getName)
                .collect(toList());

        if (!allColumns.containsAll(partitionedBy)) {
            throw new PrestoException(USER_ERROR, format("Partition columns %s not present in schema", Sets.difference(ImmutableSet.copyOf(partitionedBy), ImmutableSet.copyOf(allColumns))));
        }

        if (allColumns.size() == partitionedBy.size()) {
            throw new PrestoException(USER_ERROR, "Table contains only partition columns");
        }

        if (!allColumns.subList(allColumns.size() - partitionedBy.size(), allColumns.size()).equals(partitionedBy)) {
            throw new PrestoException(HIVE_COLUMN_ORDER_MISMATCH, "Partition keys must be the last columns in the table and in the same order as the table properties: " + partitionedBy);
        }
    }

    private static List<HiveColumnHandle> getColumnHandles(String connectorId, ConnectorTableMetadata tableMetadata, Set<String> partitionColumnNames)
    {
        validatePartitionColumns(tableMetadata);

        ImmutableList.Builder<HiveColumnHandle> columnHandles = ImmutableList.builder();
        int ordinal = 0;
        for (ColumnMetadata column : tableMetadata.getColumns()) {
            columnHandles.add(new HiveColumnHandle(
                    connectorId,
                    column.getName(),
                    toHiveType(column.getType()),
                    column.getType().getTypeSignature(),
                    ordinal,
                    partitionColumnNames.contains(column.getName())));
            ordinal++;
        }
        if (tableMetadata.isSampled()) {
            columnHandles.add(new HiveColumnHandle(
                    connectorId,
                    SAMPLE_WEIGHT_COLUMN_NAME,
                    toHiveType(BIGINT),
                    BIGINT.getTypeSignature(),
                    ordinal,
                    false));
        }

        return columnHandles.build();
    }

    private static Function<HiveColumnHandle, ColumnMetadata> columnMetadataGetter(Table table, TypeManager typeManager)
    {
        ImmutableList.Builder<String> columnNames = ImmutableList.builder();
        table.getPartitionKeys().stream().map(FieldSchema::getName).forEach(columnNames::add);
        table.getSd().getCols().stream().map(FieldSchema::getName).forEach(columnNames::add);
        List<String> allColumnNames = columnNames.build();
        if (allColumnNames.size() > Sets.newHashSet(allColumnNames).size()) {
            throw new PrestoException(HIVE_INVALID_METADATA,
                    format("Hive metadata for table %s is invalid: Table descriptor contains duplicate columns", table.getTableName()));
        }

        List<FieldSchema> tableColumns = table.getSd().getCols();
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        for (FieldSchema field : concat(tableColumns, table.getPartitionKeys())) {
            if ((field.getComment() != null) && !field.getComment().equals("from deserializer")) {
                builder.put(field.getName(), field.getComment());
            }
        }
        Map<String, String> columnComment = builder.build();

        return handle -> new ColumnMetadata(
                handle.getName(),
                typeManager.getType(handle.getTypeSignature()),
                annotateColumnComment(columnComment.get(handle.getName()), handle.isPartitionKey()),
                false);
    }

    private void checkNoRollback()
    {
        checkState(rollbackAction.get() == null, "Cannot begin a new write while in an existing one");
    }

    private void setRollback(Runnable action)
    {
        checkState(rollbackAction.compareAndSet(null, action), "Should not have to override existing rollback action");
    }

    private void clearRollback()
    {
        rollbackAction.set(null);
    }

    public void rollback()
    {
        Runnable rollbackAction = this.rollbackAction.getAndSet(null);
        if (rollbackAction != null) {
            rollbackAction.run();
        }
    }

    private static StorageDescriptor makeStorageDescriptor(
            String tableName,
            HiveStorageFormat format,
            Path targetPath,
            List<FieldSchema> columns,
            Optional<HiveBucketProperty> bucketProperty)
    {
        SerDeInfo serdeInfo = new SerDeInfo();
        serdeInfo.setName(tableName);
        serdeInfo.setSerializationLib(format.getSerDe());
        serdeInfo.setParameters(ImmutableMap.of());

        StorageDescriptor sd = new StorageDescriptor();
        sd.setLocation(targetPath.toString());
        sd.setCols(columns);
        sd.setSerdeInfo(serdeInfo);
        sd.setInputFormat(format.getInputFormat());
        sd.setOutputFormat(format.getOutputFormat());
        sd.setParameters(ImmutableMap.of());

        bucketProperty.ifPresent(property -> {
                sd.setBucketCols(property.getClusteredBy());
                sd.setNumBuckets(property.getBucketCount());
        });

        return sd;
    }
}
