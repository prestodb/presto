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

import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.SemiTransactionalHiveMetastore;
import com.facebook.presto.hive.metastore.SemiTransactionalHiveMetastore.WriteMode;
import com.facebook.presto.hive.metastore.StorageFormat;
import com.facebook.presto.hive.metastore.Table;
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
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.mapred.JobConf;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.HIDDEN;
import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.PARTITION_KEY;
import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.hive.HiveColumnHandle.PATH_COLUMN_NAME;
import static com.facebook.presto.hive.HiveColumnHandle.updateRowIdHandle;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_COLUMN_ORDER_MISMATCH;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_CONCURRENT_MODIFICATION_DETECTED;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_TIMEZONE_MISMATCH;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_UNSUPPORTED_FORMAT;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_WRITER_CLOSE_ERROR;
import static com.facebook.presto.hive.HiveTableProperties.BUCKETED_BY_PROPERTY;
import static com.facebook.presto.hive.HiveTableProperties.BUCKET_COUNT_PROPERTY;
import static com.facebook.presto.hive.HiveTableProperties.PARTITIONED_BY_PROPERTY;
import static com.facebook.presto.hive.HiveTableProperties.STORAGE_FORMAT_PROPERTY;
import static com.facebook.presto.hive.HiveTableProperties.getBucketProperty;
import static com.facebook.presto.hive.HiveTableProperties.getHiveStorageFormat;
import static com.facebook.presto.hive.HiveTableProperties.getPartitionedBy;
import static com.facebook.presto.hive.HiveType.HIVE_STRING;
import static com.facebook.presto.hive.HiveType.toHiveType;
import static com.facebook.presto.hive.HiveUtil.PRESTO_VIEW_FLAG;
import static com.facebook.presto.hive.HiveUtil.annotateColumnComment;
import static com.facebook.presto.hive.HiveUtil.decodeViewData;
import static com.facebook.presto.hive.HiveUtil.encodeViewData;
import static com.facebook.presto.hive.HiveUtil.hiveColumnHandles;
import static com.facebook.presto.hive.HiveUtil.schemaTableName;
import static com.facebook.presto.hive.HiveUtil.toPartitionValues;
import static com.facebook.presto.hive.HiveWriteUtils.checkTableIsWritable;
import static com.facebook.presto.hive.HiveWriteUtils.initializeSerializer;
import static com.facebook.presto.hive.HiveWriteUtils.isWritableType;
import static com.facebook.presto.hive.metastore.MetastoreUtil.getHiveSchema;
import static com.facebook.presto.hive.metastore.SemiTransactionalHiveMetastore.WriteMode.DIRECT_TO_TARGET_EXISTING_DIRECTORY;
import static com.facebook.presto.hive.metastore.SemiTransactionalHiveMetastore.WriteMode.DIRECT_TO_TARGET_NEW_DIRECTORY;
import static com.facebook.presto.hive.metastore.SemiTransactionalHiveMetastore.WriteMode.STAGE_AND_MOVE_TO_TARGET_DIRECTORY;
import static com.facebook.presto.hive.metastore.StorageFormat.VIEW_STORAGE_FORMAT;
import static com.facebook.presto.hive.metastore.StorageFormat.fromHiveStorageFormat;
import static com.facebook.presto.hive.util.Types.checkType;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_SCHEMA_PROPERTY;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.StandardErrorCode.SCHEMA_NOT_EMPTY;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.concat;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

public class HiveMetadata
        implements ConnectorMetadata
{
    public static final String PRESTO_VERSION_NAME = "presto_version";
    public static final String PRESTO_QUERY_ID_NAME = "presto_query_id";

    private final String connectorId;
    private final boolean allowCorruptWritesForTesting;
    private final SemiTransactionalHiveMetastore metastore;
    private final HdfsEnvironment hdfsEnvironment;
    private final HivePartitionManager partitionManager;
    private final DateTimeZone timeZone;
    private final TypeManager typeManager;
    private final LocationService locationService;
    private final TableParameterCodec tableParameterCodec;
    private final JsonCodec<PartitionUpdate> partitionUpdateCodec;
    private final boolean respectTableFormat;
    private final boolean bucketExecutionEnabled;
    private final boolean bucketWritingEnabled;
    private final HiveStorageFormat defaultStorageFormat;
    private final TypeTranslator typeTranslator;
    private final String prestoVersion;

    public HiveMetadata(
            String connectorId,
            SemiTransactionalHiveMetastore metastore,
            HdfsEnvironment hdfsEnvironment,
            HivePartitionManager partitionManager,
            DateTimeZone timeZone,
            boolean allowCorruptWritesForTesting,
            boolean respectTableFormat,
            boolean bucketExecutionEnabled,
            boolean bucketWritingEnabled,
            HiveStorageFormat defaultStorageFormat,
            TypeManager typeManager,
            LocationService locationService,
            TableParameterCodec tableParameterCodec,
            JsonCodec<PartitionUpdate> partitionUpdateCodec,
            TypeTranslator typeTranslator,
            String prestoVersion)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");

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
        this.bucketExecutionEnabled = bucketExecutionEnabled;
        this.bucketWritingEnabled = bucketWritingEnabled;
        this.defaultStorageFormat = requireNonNull(defaultStorageFormat, "defaultStorageFormat is null");
        this.typeTranslator = requireNonNull(typeTranslator, "typeTranslator is null");
        this.prestoVersion = requireNonNull(prestoVersion, "prestoVersion is null");
    }

    public SemiTransactionalHiveMetastore getMetastore()
    {
        return metastore;
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
        ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
        for (HiveColumnHandle columnHandle : hiveColumnHandles(connectorId, table.get())) {
            columns.add(metadataGetter.apply(columnHandle));
        }

        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
        try {
            HiveStorageFormat format = extractHiveStorageFormat(table.get());
            properties.put(STORAGE_FORMAT_PROPERTY, format);
        }
        catch (PrestoException ignored) {
            // todo fail if format is not known
        }
        List<String> partitionedBy = table.get().getPartitionColumns().stream()
                .map(Column::getName)
                .collect(toList());
        if (!partitionedBy.isEmpty()) {
            properties.put(PARTITIONED_BY_PROPERTY, partitionedBy);
        }
        Optional<HiveBucketProperty> bucketProperty = table.get().getStorage().getBucketProperty();
        if (bucketProperty.isPresent()) {
            properties.put(BUCKET_COUNT_PROPERTY, bucketProperty.get().getBucketCount());
            properties.put(BUCKETED_BY_PROPERTY, bucketProperty.get().getBucketedBy());
        }
        properties.putAll(tableParameterCodec.decode(table.get().getParameters()));

        return new ConnectorTableMetadata(tableName, columns.build(), properties.build());
    }

    @Override
    public Optional<Object> getInfo(ConnectorTableLayoutHandle layoutHandle)
    {
        HiveTableLayoutHandle tableLayoutHandle = checkType(layoutHandle, HiveTableLayoutHandle.class, "layoutHandle");
        if (!tableLayoutHandle.getPartitions().isPresent()) {
            return Optional.of(new HiveInputInfo(tableLayoutHandle.getPartitions().get().stream()
                    .map(HivePartition::getPartitionId)
                    .collect(Collectors.toList())));
        }

        return Optional.empty();
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
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        SchemaTableName tableName = schemaTableName(tableHandle);
        Optional<Table> table = metastore.getTable(tableName.getSchemaName(), tableName.getTableName());
        if (!table.isPresent()) {
            throw new TableNotFoundException(tableName);
        }
        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        for (HiveColumnHandle columnHandle : hiveColumnHandles(connectorId, table.get())) {
            columnHandles.put(columnHandle.getName(), columnHandle);
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
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties)
    {
        Database database = new Database();
        database.setName(schemaName);

        HiveSchemaProperties.getLocation(properties).ifPresent(locationUri -> {
            try {
                hdfsEnvironment.getFileSystem(session.getUser(), new Path(locationUri));
            }
            catch (IOException e) {
                throw new PrestoException(INVALID_SCHEMA_PROPERTY, "Invalid location URI: " + locationUri, e);
            }
            database.setLocationUri(locationUri);
        });

        database.setOwnerType(PrincipalType.USER);
        database.setOwnerName(session.getUser());

        metastore.createDatabase(database);
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName)
    {
        // basic sanity check to provide a better error message
        if (!listTables(session, schemaName).isEmpty() ||
                !listViews(session, schemaName).isEmpty()) {
            throw new PrestoException(SCHEMA_NOT_EMPTY, "Schema not empty: " + schemaName);
        }
        metastore.dropDatabase(schemaName);
    }

    @Override
    public void renameSchema(ConnectorSession session, String source, String target)
    {
        metastore.renameDatabase(source, target);
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        SchemaTableName schemaTableName = tableMetadata.getTable();
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();
        List<String> partitionedBy = getPartitionedBy(tableMetadata.getProperties());
        Optional<HiveBucketProperty> bucketProperty = getBucketProperty(tableMetadata.getProperties());
        if (bucketProperty.isPresent() && !bucketWritingEnabled) {
            throw new PrestoException(NOT_SUPPORTED, "Writing to bucketed Hive table has been temporarily disabled");
        }
        List<HiveColumnHandle> columnHandles = getColumnHandles(connectorId, tableMetadata, ImmutableSet.copyOf(partitionedBy), typeTranslator);
        HiveStorageFormat hiveStorageFormat = getHiveStorageFormat(tableMetadata.getProperties());
        Map<String, String> additionalTableParameters = tableParameterCodec.encode(tableMetadata.getProperties());

        hiveStorageFormat.validateColumns(columnHandles);

        LocationHandle locationHandle = locationService.forNewTable(metastore, session.getUser(), session.getQueryId(), schemaName, tableName);
        Path targetPath = locationService.targetPathRoot(locationHandle);

        Table table = buildTableObject(session.getQueryId(), schemaName, tableName, session.getUser(), columnHandles, hiveStorageFormat, partitionedBy, bucketProperty, additionalTableParameters, targetPath, prestoVersion);
        PrincipalPrivilegeSet principalPrivilegeSet = buildInitialPrivilegeSet(table.getOwner());
        metastore.createTable(session, table, principalPrivilegeSet, Optional.empty());
    }

    private static Table buildTableObject(
            String queryId,
            String schemaName,
            String tableName,
            String tableOwner,
            List<HiveColumnHandle> columnHandles,
            HiveStorageFormat hiveStorageFormat,
            List<String> partitionedBy,
            Optional<HiveBucketProperty> bucketProperty,
            Map<String, String> additionalTableParameters,
            Path targetPath,
            String prestoVersion)
    {
        Map<String, HiveColumnHandle> columnHandlesByName = Maps.uniqueIndex(columnHandles, HiveColumnHandle::getName);
        List<Column> partitionColumns = partitionedBy.stream()
                .map(columnHandlesByName::get)
                .map(column -> new Column(column.getName(), column.getHiveType(), Optional.empty()))
                .collect(toList());

        Set<String> partitionColumnNames = ImmutableSet.copyOf(partitionedBy);

        ImmutableList.Builder<Column> columns = ImmutableList.builder();
        for (HiveColumnHandle columnHandle : columnHandles) {
            String name = columnHandle.getName();
            HiveType type = columnHandle.getHiveType();
            if (!partitionColumnNames.contains(name)) {
                verify(!columnHandle.isPartitionKey(), "Column handles are not consistent with partitioned by property");
                columns.add(new Column(name, type, Optional.empty()));
            }
            else {
                verify(columnHandle.isPartitionKey(), "Column handles are not consistent with partitioned by property");
            }
        }

        Table.Builder tableBuilder = Table.builder()
                .setDatabaseName(schemaName)
                .setTableName(tableName)
                .setOwner(tableOwner)
                .setTableType(TableType.MANAGED_TABLE.name())
                .setDataColumns(columns.build())
                .setPartitionColumns(partitionColumns)
                .setParameters(ImmutableMap.<String, String>builder()
                        .put("comment", "Created by Presto")
                        .put(PRESTO_VERSION_NAME, prestoVersion)
                        .put(PRESTO_QUERY_ID_NAME, queryId)
                        .putAll(additionalTableParameters)
                        .build());
        tableBuilder.getStorageBuilder()
                .setStorageFormat(fromHiveStorageFormat(hiveStorageFormat))
                .setBucketProperty(bucketProperty)
                .setLocation(targetPath.toString());

        return tableBuilder.build();
    }

    private static PrincipalPrivilegeSet buildInitialPrivilegeSet(String tableOwner)
    {
        return new PrincipalPrivilegeSet(
                ImmutableMap.of(tableOwner, ImmutableList.of(
                        new PrivilegeGrantInfo("SELECT", 0, tableOwner, PrincipalType.USER, true),
                        new PrivilegeGrantInfo("INSERT", 0, tableOwner, PrincipalType.USER, true),
                        new PrivilegeGrantInfo("UPDATE", 0, tableOwner, PrincipalType.USER, true),
                        new PrivilegeGrantInfo("DELETE", 0, tableOwner, PrincipalType.USER, true)
                )),
                ImmutableMap.of(),
                ImmutableMap.of());
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column)
    {
        HiveTableHandle handle = checkType(tableHandle, HiveTableHandle.class, "tableHandle");

        metastore.addColumn(handle.getSchemaName(), handle.getTableName(), column.getName(), toHiveType(typeTranslator, column.getType()), column.getComment());
    }

    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle source, String target)
    {
        HiveTableHandle hiveTableHandle = checkType(tableHandle, HiveTableHandle.class, "tableHandle");
        HiveColumnHandle sourceHandle = checkType(source, HiveColumnHandle.class, "columnHandle");

        metastore.renameColumn(hiveTableHandle.getSchemaName(), hiveTableHandle.getTableName(), sourceHandle.getName(), target);
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        HiveTableHandle handle = checkType(tableHandle, HiveTableHandle.class, "tableHandle");
        metastore.renameTable(handle.getSchemaName(), handle.getTableName(), newTableName.getSchemaName(), newTableName.getTableName());
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        HiveTableHandle handle = checkType(tableHandle, HiveTableHandle.class, "tableHandle");
        SchemaTableName tableName = schemaTableName(tableHandle);

        Optional<Table> target = metastore.getTable(handle.getSchemaName(), handle.getTableName());
        if (!target.isPresent()) {
            throw new TableNotFoundException(tableName);
        }
        metastore.dropTable(session, handle.getSchemaName(), handle.getTableName());
    }

    @Override
    public HiveOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
    {
        verifyJvmTimeZone();

        HiveStorageFormat tableStorageFormat = getHiveStorageFormat(tableMetadata.getProperties());
        List<String> partitionedBy = getPartitionedBy(tableMetadata.getProperties());
        Optional<HiveBucketProperty> bucketProperty = getBucketProperty(tableMetadata.getProperties());
        Map<String, String> additionalTableParameters = tableParameterCodec.encode(tableMetadata.getProperties());

        // get the root directory for the database
        SchemaTableName schemaTableName = tableMetadata.getTable();
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();

        List<HiveColumnHandle> columnHandles = getColumnHandles(connectorId, tableMetadata, ImmutableSet.copyOf(partitionedBy), typeTranslator);
        HiveStorageFormat partitionStorageFormat = respectTableFormat ? tableStorageFormat : defaultStorageFormat;

        // unpartitioned tables ignore the partition storage format
        HiveStorageFormat actualStorageFormat = partitionedBy.isEmpty() ? tableStorageFormat : partitionStorageFormat;
        actualStorageFormat.validateColumns(columnHandles);

        LocationHandle locationHandle = locationService.forNewTable(metastore, session.getUser(), session.getQueryId(), schemaName, tableName);
        HiveOutputTableHandle result = new HiveOutputTableHandle(
                connectorId,
                schemaName,
                tableName,
                columnHandles,
                session.getQueryId(),
                metastore.generatePageSinkMetadata(schemaTableName),
                locationHandle,
                tableStorageFormat,
                partitionStorageFormat,
                partitionedBy,
                bucketProperty,
                session.getUser(),
                additionalTableParameters);

        Path writePathRoot = locationService.writePathRoot(locationHandle).get();
        Path targetPathRoot = locationService.targetPathRoot(locationHandle);
        WriteMode mode = writePathRoot.equals(targetPathRoot) ? DIRECT_TO_TARGET_NEW_DIRECTORY : STAGE_AND_MOVE_TO_TARGET_DIRECTORY;
        metastore.declareIntentionToWrite(session, mode, writePathRoot, result.getFilePrefix(), schemaTableName);

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

        Table table = buildTableObject(
                session.getQueryId(),
                handle.getSchemaName(),
                handle.getTableName(),
                handle.getTableOwner(),
                handle.getInputColumns(),
                handle.getTableStorageFormat(),
                handle.getPartitionedBy(),
                handle.getBucketProperty(),
                handle.getAdditionalTableParameters(),
                targetPath,
                prestoVersion);
        PrincipalPrivilegeSet principalPrivilegeSet = buildInitialPrivilegeSet(handle.getTableOwner());

        partitionUpdates = PartitionUpdate.mergePartitionUpdates(partitionUpdates);

        if (handle.getBucketProperty().isPresent()) {
            ImmutableList<PartitionUpdate> partitionUpdatesForMissingBuckets = computePartitionUpdatesForMissingBuckets(handle, table, partitionUpdates);
            // replace partitionUpdates before creating the empty files so that those files will be cleaned up if we end up rollback
            partitionUpdates = PartitionUpdate.mergePartitionUpdates(Iterables.concat(partitionUpdates, partitionUpdatesForMissingBuckets));
            for (PartitionUpdate partitionUpdate : partitionUpdatesForMissingBuckets) {
                Optional<Partition> partition = table.getPartitionColumns().isEmpty() ? Optional.empty() : Optional.of(buildPartitionObject(session.getQueryId(), table, partitionUpdate));
                createEmptyFile(partitionUpdate.getWritePath(), table, partition, partitionUpdate.getFileNames());
            }
        }

        metastore.createTable(session, table, principalPrivilegeSet, Optional.of(writePath));

        if (!handle.getPartitionedBy().isEmpty()) {
            if (respectTableFormat) {
                Verify.verify(handle.getPartitionStorageFormat() == handle.getTableStorageFormat());
            }
            partitionUpdates.forEach(partitionUpdate ->
                    metastore.addPartition(session, handle.getSchemaName(), handle.getTableName(), buildPartitionObject(session.getQueryId(), table, partitionUpdate), partitionUpdate.getWritePath()));
        }
    }

    private ImmutableList<PartitionUpdate> computePartitionUpdatesForMissingBuckets(HiveWritableTableHandle handle, Table table, List<PartitionUpdate> partitionUpdates)
    {
        ImmutableList.Builder<PartitionUpdate> partitionUpdatesForMissingBucketsBuilder = ImmutableList.builder();
        HiveStorageFormat storageFormat = table.getPartitionColumns().isEmpty() ? handle.getTableStorageFormat() : handle.getPartitionStorageFormat();
        for (PartitionUpdate partitionUpdate : partitionUpdates) {
            int bucketCount = handle.getBucketProperty().get().getBucketCount();

            List<String> fileNamesForMissingBuckets = computeFileNamesForMissingBuckets(
                    storageFormat,
                    partitionUpdate.getTargetPath(),
                    handle.getFilePrefix(),
                    bucketCount,
                    partitionUpdate);
            partitionUpdatesForMissingBucketsBuilder.add(new PartitionUpdate(
                    partitionUpdate.getName(),
                    partitionUpdate.isNew(),
                    partitionUpdate.getWritePath(),
                    partitionUpdate.getTargetPath(),
                    fileNamesForMissingBuckets));
        }
        return partitionUpdatesForMissingBucketsBuilder.build();
    }

    private List<String> computeFileNamesForMissingBuckets(HiveStorageFormat storageFormat, Path targetPath, String filePrefix, int bucketCount, PartitionUpdate partitionUpdate)
    {
        if (partitionUpdate.getFileNames().size() == bucketCount) {
            // fast path for common case
            return ImmutableList.of();
        }
        JobConf conf = new JobConf(hdfsEnvironment.getConfiguration(targetPath));
        String fileExtension = HiveWriterFactory.getFileExtension(conf, fromHiveStorageFormat(storageFormat));
        Set<String> fileNames = partitionUpdate.getFileNames().stream()
                .collect(Collectors.toSet());
        ImmutableList.Builder<String> missingFileNamesBuilder = ImmutableList.builder();
        for (int i = 0; i < bucketCount; i++) {
            String fileName = HiveWriterFactory.computeBucketedFileName(filePrefix, i) + fileExtension;
            if (!fileNames.contains(fileName)) {
                missingFileNamesBuilder.add(fileName);
            }
        }
        List<String> missingFileNames = missingFileNamesBuilder.build();
        verify(fileNames.size() + missingFileNames.size() == bucketCount);
        return missingFileNames;
    }

    private void createEmptyFile(Path path, Table table, Optional<Partition> partition, List<String> fileNames)
    {
        JobConf conf = new JobConf(hdfsEnvironment.getConfiguration(path));

        Properties schema;
        StorageFormat format;
        if (partition.isPresent()) {
            schema = getHiveSchema(partition.get(), table);
            format = partition.get().getStorage().getStorageFormat();
        }
        else {
            schema = getHiveSchema(table);
            format = table.getStorage().getStorageFormat();
        }

        for (String fileName : fileNames) {
            writeEmptyFile(new Path(path, fileName), conf, schema, format.getSerDe(), format.getOutputFormat());
        }
    }

    private static void writeEmptyFile(Path target, JobConf conf, Properties properties, String serDe, String outputFormatName)
    {
        // Some serializers such as Avro set a property in the schema.
        initializeSerializer(conf, properties, serDe);

        // The code below is not a try with resources because RecordWriter is not Closeable.
        FileSinkOperator.RecordWriter recordWriter = HiveWriteUtils.createRecordWriter(target, conf, properties, outputFormatName);
        try {
            recordWriter.close(false);
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_WRITER_CLOSE_ERROR, "Error write empty file to Hive", e);
        }
    }

    @Override
    public HiveInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        verifyJvmTimeZone();

        SchemaTableName tableName = schemaTableName(tableHandle);
        Optional<Table> table = metastore.getTable(tableName.getSchemaName(), tableName.getTableName());
        if (!table.isPresent()) {
            throw new TableNotFoundException(tableName);
        }

        checkTableIsWritable(table.get());

        for (Column column : table.get().getDataColumns()) {
            if (!isWritableType(column.getType())) {
                throw new PrestoException(
                        NOT_SUPPORTED,
                        format("Inserting into Hive table %s.%s with column type %s not supported", table.get().getDatabaseName(), table.get().getTableName(), column.getType()));
            }
        }

        List<HiveColumnHandle> handles = hiveColumnHandles(connectorId, table.get()).stream()
                .filter(columnHandle -> !columnHandle.isHidden())
                .collect(toList());

        HiveStorageFormat tableStorageFormat = extractHiveStorageFormat(table.get());
        LocationHandle locationHandle = locationService.forExistingTable(metastore, session.getUser(), session.getQueryId(), table.get());
        HiveInsertTableHandle result = new HiveInsertTableHandle(
                connectorId,
                tableName.getSchemaName(),
                tableName.getTableName(),
                handles,
                session.getQueryId(),
                metastore.generatePageSinkMetadata(tableName),
                locationHandle,
                table.get().getStorage().getBucketProperty(),
                tableStorageFormat,
                respectTableFormat ? tableStorageFormat : defaultStorageFormat);

        Optional<Path> writePathRoot = locationService.writePathRoot(locationHandle);
        Path targetPathRoot = locationService.targetPathRoot(locationHandle);
        if (writePathRoot.isPresent()) {
            WriteMode mode = writePathRoot.get().equals(targetPathRoot) ? DIRECT_TO_TARGET_NEW_DIRECTORY : STAGE_AND_MOVE_TO_TARGET_DIRECTORY;
            metastore.declareIntentionToWrite(session, mode, writePathRoot.get(), result.getFilePrefix(), tableName);
        }
        else {
            metastore.declareIntentionToWrite(session, DIRECT_TO_TARGET_EXISTING_DIRECTORY, targetPathRoot, result.getFilePrefix(), tableName);
        }

        return result;
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
        partitionUpdates = PartitionUpdate.mergePartitionUpdates(partitionUpdates);

        Optional<Table> table = metastore.getTable(handle.getSchemaName(), handle.getTableName());
        if (!table.isPresent()) {
            throw new TableNotFoundException(new SchemaTableName(handle.getSchemaName(), handle.getTableName()));
        }
        if (!table.get().getStorage().getStorageFormat().getInputFormat().equals(tableStorageFormat.getInputFormat()) && respectTableFormat) {
            throw new PrestoException(HIVE_CONCURRENT_MODIFICATION_DETECTED, "Table format changed during insert");
        }

        if (handle.getBucketProperty().isPresent()) {
            ImmutableList<PartitionUpdate> partitionUpdatesForMissingBuckets = computePartitionUpdatesForMissingBuckets(handle, table.get(), partitionUpdates);
            // replace partitionUpdates before creating the empty files so that those files will be cleaned up if we end up rollback
            partitionUpdates = PartitionUpdate.mergePartitionUpdates(Iterables.concat(partitionUpdates, partitionUpdatesForMissingBuckets));
            for (PartitionUpdate partitionUpdate : partitionUpdatesForMissingBuckets) {
                Optional<Partition> partition = table.get().getPartitionColumns().isEmpty() ? Optional.empty() : Optional.of(buildPartitionObject(session.getQueryId(), table.get(), partitionUpdate));
                createEmptyFile(partitionUpdate.getWritePath(), table.get(), partition, partitionUpdate.getFileNames());
            }
        }

        for (PartitionUpdate partitionUpdate : partitionUpdates) {
            if (partitionUpdate.getName().isEmpty()) {
                // insert into unpartitioned table
                metastore.finishInsertIntoExistingTable(
                        session,
                        handle.getSchemaName(),
                        handle.getTableName(),
                        partitionUpdate.getWritePath(),
                        partitionUpdate.getFileNames());
            }
            else if (!partitionUpdate.isNew()) {
                // insert into existing partition
                metastore.finishInsertIntoExistingPartition(
                        session,
                        handle.getSchemaName(),
                        handle.getTableName(),
                        toPartitionValues(partitionUpdate.getName()),
                        partitionUpdate.getWritePath(),
                        partitionUpdate.getFileNames());
            }
            else {
                // insert into new partition
                Partition partition = buildPartitionObject(session.getQueryId(), table.get(), partitionUpdate);
                if (!partition.getStorage().getStorageFormat().getInputFormat().equals(handle.getPartitionStorageFormat().getInputFormat()) && respectTableFormat) {
                    throw new PrestoException(HIVE_CONCURRENT_MODIFICATION_DETECTED, "Partition format changed during insert");
                }
                metastore.addPartition(session, handle.getSchemaName(), handle.getTableName(), partition, partitionUpdate.getWritePath());
            }
        }
    }

    private Partition buildPartitionObject(String queryId, Table table, PartitionUpdate partitionUpdate)
    {
        List<String> values = HivePartitionManager.extractPartitionKeyValues(partitionUpdate.getName());

        Partition.Builder partition = Partition.builder();
        partition.setDatabaseName(table.getDatabaseName());
        partition.setTableName(table.getTableName());
        partition.setValues(values);

        if (respectTableFormat) {
            partition.getStorageBuilder().setStorageFormat(table.getStorage().getStorageFormat());
        }
        else {
            partition.getStorageBuilder().setStorageFormat(fromHiveStorageFormat(defaultStorageFormat));
        }
        partition.setParameters(ImmutableMap.<String, String>builder()
                .put(PRESTO_VERSION_NAME, prestoVersion)
                .put(PRESTO_QUERY_ID_NAME, queryId)
                .build());

        partition.getStorageBuilder().setLocation(partitionUpdate.getTargetPath().toString());
        partition.getStorageBuilder().setBucketProperty(table.getStorage().getBucketProperty());
        partition.setColumns(table.getDataColumns());

        return partition.build();
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName viewName, String viewData, boolean replace)
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("comment", "Presto View")
                .put(PRESTO_VIEW_FLAG, "true")
                .build();

        Column dummyColumn = new Column("dummy", HIVE_STRING, Optional.empty());

        Table.Builder tableBuilder = Table.builder()
                .setDatabaseName(viewName.getSchemaName())
                .setTableName(viewName.getTableName())
                .setOwner(session.getUser())
                .setTableType(TableType.VIRTUAL_VIEW.name())
                .setDataColumns(ImmutableList.of(dummyColumn))
                .setPartitionColumns(ImmutableList.of())
                .setParameters(properties)
                .setViewOriginalText(Optional.of(encodeViewData(viewData)))
                .setViewExpandedText(Optional.of("/* Presto View */"));

        tableBuilder.getStorageBuilder()
                .setStorageFormat(VIEW_STORAGE_FORMAT)
                .setLocation("");
        Table table = tableBuilder.build();
        PrincipalPrivilegeSet principalPrivilegeSet = buildInitialPrivilegeSet(session.getUser());

        Optional<Table> existing = metastore.getTable(viewName.getSchemaName(), viewName.getTableName());
        if (existing.isPresent()) {
            if (!replace || !HiveUtil.isPrestoView(existing.get())) {
                throw new ViewAlreadyExistsException(viewName);
            }

            metastore.replaceView(viewName.getSchemaName(), viewName.getTableName(), table, principalPrivilegeSet);
            return;
        }

        try {
            metastore.createTable(session, table, principalPrivilegeSet, Optional.empty());
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
            metastore.dropTable(session, viewName.getSchemaName(), viewName.getTableName());
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
                        decodeViewData(table.get().getViewOriginalText().get())));
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

        Optional<Table> table = metastore.getTable(handle.getSchemaName(), handle.getTableName());
        if (!table.isPresent()) {
            throw new TableNotFoundException(handle.getSchemaTableName());
        }

        if (table.get().getPartitionColumns().isEmpty()) {
            metastore.truncateUnpartitionedTable(session, handle.getSchemaName(), handle.getTableName());
        }
        else {
            for (HivePartition hivePartition : getOrComputePartitions(layoutHandle, session, tableHandle)) {
                metastore.dropPartition(session, handle.getSchemaName(), handle.getTableName(), toPartitionValues(hivePartition.getPartitionId()));
            }
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
        return true;
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

        Optional<ConnectorNodePartitioning> nodePartitioning = Optional.empty();
        if (bucketExecutionEnabled && hiveLayoutHandle.getBucketHandle().isPresent()) {
            nodePartitioning = hiveLayoutHandle.getBucketHandle().map(hiveBucketHandle -> new ConnectorNodePartitioning(
                    new HivePartitioningHandle(
                            connectorId,
                            hiveBucketHandle.getBucketCount(),
                            hiveBucketHandle.getColumns().stream()
                                    .map(HiveColumnHandle::getHiveType)
                                    .collect(Collectors.toList())),
                    hiveBucketHandle.getColumns().stream()
                            .map(ColumnHandle.class::cast)
                            .collect(toList())));
        }

        return new ConnectorTableLayout(
                hiveLayoutHandle,
                Optional.empty(),
                predicate,
                nodePartitioning,
                Optional.empty(),
                discretePredicates,
                ImmutableList.of());
    }

    @Override
    public Optional<ConnectorNewTableLayout> getInsertLayout(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        HivePartitionResult hivePartitionResult = partitionManager.getPartitions(session, metastore, tableHandle, TupleDomain.all());
        if (!hivePartitionResult.getBucketHandle().isPresent()) {
            return Optional.empty();
        }
        if (!bucketWritingEnabled) {
            throw new PrestoException(NOT_SUPPORTED, "Writing to bucketed Hive table has been temporarily disabled");
        }
        HiveBucketHandle hiveBucketHandle = hivePartitionResult.getBucketHandle().get();
        HivePartitioningHandle partitioningHandle = new HivePartitioningHandle(
                connectorId,
                hiveBucketHandle.getBucketCount(),
                hiveBucketHandle.getColumns().stream()
                        .map(HiveColumnHandle::getHiveType)
                        .collect(Collectors.toList()));
        List<String> partitionColumns = hiveBucketHandle.getColumns().stream()
                .map(HiveColumnHandle::getName)
                .collect(Collectors.toList());
        return Optional.of(new ConnectorNewTableLayout(partitioningHandle, partitionColumns));
    }

    @Override
    public Optional<ConnectorNewTableLayout> getNewTableLayout(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        Optional<HiveBucketProperty> bucketProperty = getBucketProperty(tableMetadata.getProperties());
        if (!bucketProperty.isPresent()) {
            return Optional.empty();
        }
        if (!bucketWritingEnabled) {
            throw new PrestoException(NOT_SUPPORTED, "Writing to bucketed Hive table has been temporarily disabled");
        }
        List<String> bucketedBy = bucketProperty.get().getBucketedBy();
        Map<String, HiveType> hiveTypeMap = tableMetadata.getColumns().stream()
                .collect(toMap(ColumnMetadata::getName, column -> toHiveType(typeTranslator, column.getType())));
        return Optional.of(new ConnectorNewTableLayout(
                new HivePartitioningHandle(
                        connectorId,
                        bucketProperty.get().getBucketCount(),
                        bucketedBy.stream()
                                .map(hiveTypeMap::get)
                                .collect(toList())),
                bucketedBy));
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
    public void revokeTablePrivileges(ConnectorSession session, SchemaTableName schemaTableName, Set<Privilege> privileges, String grantee, boolean grantOption)
    {
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();

        Set<PrivilegeGrantInfo> privilegeGrantInfoSet = privileges.stream()
                .map(privilege -> new PrivilegeGrantInfo(privilege.name().toLowerCase(), 0, session.getUser(), PrincipalType.USER, grantOption))
                .collect(toSet());

        metastore.revokeTablePrivileges(schemaName, tableName, grantee, privilegeGrantInfoSet);
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
        StorageFormat storageFormat = table.getStorage().getStorageFormat();
        String outputFormat = storageFormat.getOutputFormat();
        String serde = storageFormat.getSerDe();

        for (HiveStorageFormat format : HiveStorageFormat.values()) {
            if (format.getOutputFormat().equals(outputFormat) && format.getSerDe().equals(serde)) {
                return format;
            }
        }
        throw new PrestoException(HIVE_UNSUPPORTED_FORMAT, format("Output format %s with SerDe %s is not supported", outputFormat, serde));
    }

    private static void validatePartitionColumns(ConnectorTableMetadata tableMetadata)
    {
        List<String> partitionedBy = getPartitionedBy(tableMetadata.getProperties());

        List<String> allColumns = tableMetadata.getColumns().stream()
                .map(ColumnMetadata::getName)
                .collect(toList());

        if (!allColumns.containsAll(partitionedBy)) {
            throw new PrestoException(INVALID_TABLE_PROPERTY, format("Partition columns %s not present in schema", Sets.difference(ImmutableSet.copyOf(partitionedBy), ImmutableSet.copyOf(allColumns))));
        }

        if (allColumns.size() == partitionedBy.size()) {
            throw new PrestoException(INVALID_TABLE_PROPERTY, "Table contains only partition columns");
        }

        if (!allColumns.subList(allColumns.size() - partitionedBy.size(), allColumns.size()).equals(partitionedBy)) {
            throw new PrestoException(HIVE_COLUMN_ORDER_MISMATCH, "Partition keys must be the last columns in the table and in the same order as the table properties: " + partitionedBy);
        }
    }

    private static List<HiveColumnHandle> getColumnHandles(String connectorId, ConnectorTableMetadata tableMetadata, Set<String> partitionColumnNames, TypeTranslator typeTranslator)
    {
        validatePartitionColumns(tableMetadata);

        ImmutableList.Builder<HiveColumnHandle> columnHandles = ImmutableList.builder();
        int ordinal = 0;
        for (ColumnMetadata column : tableMetadata.getColumns()) {
            HiveColumnHandle.ColumnType columnType;
            if (partitionColumnNames.contains(column.getName())) {
                columnType = PARTITION_KEY;
            }
            else if (column.isHidden()) {
                columnType = HIDDEN;
            }
            else {
                columnType = REGULAR;
            }
            columnHandles.add(new HiveColumnHandle(
                    connectorId,
                    column.getName(),
                    toHiveType(typeTranslator, column.getType()),
                    column.getType().getTypeSignature(),
                    ordinal,
                    columnType));
            ordinal++;
        }

        return columnHandles.build();
    }

    private static Function<HiveColumnHandle, ColumnMetadata> columnMetadataGetter(Table table, TypeManager typeManager)
    {
        ImmutableList.Builder<String> columnNames = ImmutableList.builder();
        table.getPartitionColumns().stream().map(Column::getName).forEach(columnNames::add);
        table.getDataColumns().stream().map(Column::getName).forEach(columnNames::add);
        List<String> allColumnNames = columnNames.build();
        if (allColumnNames.size() > Sets.newHashSet(allColumnNames).size()) {
            throw new PrestoException(HIVE_INVALID_METADATA,
                    format("Hive metadata for table %s is invalid: Table descriptor contains duplicate columns", table.getTableName()));
        }

        List<Column> tableColumns = table.getDataColumns();
        ImmutableMap.Builder<String, Optional<String>> builder = ImmutableMap.builder();
        for (Column field : concat(tableColumns, table.getPartitionColumns())) {
            if ((field.getComment() != null) && !Optional.of("from deserializer").equals(field.getComment())) {
                builder.put(field.getName(), field.getComment());
            }
            else {
                builder.put(field.getName(), Optional.empty());
            }
        }
        // add hidden column
        builder.put(PATH_COLUMN_NAME, Optional.empty());

        Map<String, Optional<String>> columnComment = builder.build();

        return handle -> new ColumnMetadata(
                handle.getName(),
                typeManager.getType(handle.getTypeSignature()),
                annotateColumnComment(columnComment.get(handle.getName()), handle.isPartitionKey()),
                handle.isHidden());
    }

    public void rollback()
    {
        metastore.rollback();
    }

    public void commit()
    {
        metastore.commit();
    }
}
