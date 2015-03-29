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

import com.facebook.presto.hive.metastore.HiveMetastore;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.ConnectorPartitionResult;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.spi.ViewNotFoundException;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.StandardSystemProperty;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.joda.time.DateTimeZone;

import javax.inject.Inject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.facebook.presto.hive.HiveColumnHandle.SAMPLE_WEIGHT_COLUMN_NAME;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_TIMEZONE_MISMATCH;
import static com.facebook.presto.hive.HiveSessionProperties.getHiveStorageFormat;
import static com.facebook.presto.hive.HiveUtil.PRESTO_VIEW_FLAG;
import static com.facebook.presto.hive.HiveUtil.decodeViewData;
import static com.facebook.presto.hive.HiveUtil.encodeViewData;
import static com.facebook.presto.hive.HiveUtil.hiveColumnHandles;
import static com.facebook.presto.hive.HiveUtil.schemaTableName;
import static com.facebook.presto.hive.util.Types.checkType;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.StandardErrorCode.PERMISSION_DENIED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.transform;
import static java.lang.String.format;
import static java.util.UUID.randomUUID;
import static org.apache.hadoop.hive.serde.serdeConstants.STRING_TYPE_NAME;

public class HiveMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(HiveMetadata.class);

    private final String connectorId;
    private final boolean allowDropTable;
    private final boolean allowRenameTable;
    private final boolean allowCorruptWritesForTesting;
    private final HiveMetastore metastore;
    private final HdfsEnvironment hdfsEnvironment;
    private final DateTimeZone timeZone;
    private final HiveStorageFormat hiveStorageFormat;
    private final TypeManager typeManager;
    private final boolean insertS3TempEnabled;
    private final HiveSplitManager splitManager;

    private static final int RENAME_THREADPOOL_SIZE = 20;
    private static final int partionCommitBatchSize = 8;

    @Inject
    @SuppressWarnings("deprecation")
    public HiveMetadata(
            HiveConnectorId connectorId,
            HiveClientConfig hiveClientConfig,
            HiveMetastore metastore,
            HdfsEnvironment hdfsEnvironment,
            @ForHiveClient ExecutorService executorService,
            TypeManager typeManager,
            ConnectorSplitManager splitManager)
    {
        this(connectorId,
                metastore,
                hdfsEnvironment,
                DateTimeZone.forTimeZone(hiveClientConfig.getTimeZone()),
                hiveClientConfig.getAllowDropTable(),
                hiveClientConfig.getAllowRenameTable(),
                hiveClientConfig.getAllowCorruptWritesForTesting(),
                hiveClientConfig.getHiveStorageFormat(),
                hiveClientConfig.getInsertS3TempEnabled(),
                typeManager,
                splitManager);
    }

    public HiveMetadata(
            HiveConnectorId connectorId,
            HiveMetastore metastore,
            HdfsEnvironment hdfsEnvironment,
            DateTimeZone timeZone,
            boolean allowDropTable,
            boolean allowRenameTable,
            boolean allowCorruptWritesForTesting,
            HiveStorageFormat hiveStorageFormat,
            boolean insertS3TempEnabled,
            TypeManager typeManager,
            ConnectorSplitManager splitManager)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null").toString();

        this.allowDropTable = allowDropTable;
        this.allowRenameTable = allowRenameTable;
        this.allowCorruptWritesForTesting = allowCorruptWritesForTesting;

        this.metastore = checkNotNull(metastore, "metastore is null");
        this.hdfsEnvironment = checkNotNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.timeZone = checkNotNull(timeZone, "timeZone is null");
        this.hiveStorageFormat = hiveStorageFormat;
        this.typeManager = checkNotNull(typeManager, "typeManager is null");

        if (!allowCorruptWritesForTesting && !timeZone.equals(DateTimeZone.getDefault())) {
            log.warn("Hive writes are disabled. " +
                            "To write data to Hive, your JVM timezone must match the Hive storage timezone. " +
                            "Add -Duser.timezone=%s to your JVM arguments",
                    timeZone.getID());
        }
        this.insertS3TempEnabled = insertS3TempEnabled;
        this.splitManager = checkType(splitManager, HiveSplitManager.class, "splitManager");
    }

    public HiveMetastore getMetastore()
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
        checkNotNull(tableName, "tableName is null");
        try {
            metastore.getTable(tableName.getSchemaName(), tableName.getTableName());
            return new HiveTableHandle(connectorId, tableName.getSchemaName(), tableName.getTableName(), session);
        }
        catch (NoSuchObjectException e) {
            // table was not found
            return null;
        }
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorTableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        SchemaTableName tableName = schemaTableName(tableHandle);
        return getTableMetadata(tableName);
    }

    private ConnectorTableMetadata getTableMetadata(SchemaTableName tableName)
    {
        try {
            Table table = metastore.getTable(tableName.getSchemaName(), tableName.getTableName());
            if (table.getTableType().equals(TableType.VIRTUAL_VIEW.name())) {
                throw new TableNotFoundException(tableName);
            }
            List<HiveColumnHandle> handles = hiveColumnHandles(typeManager, connectorId, table, false);
            List<ColumnMetadata> columns = ImmutableList.copyOf(transform(handles, columnMetadataGetter(table, typeManager)));
            return new ConnectorTableMetadata(tableName, columns, table.getOwner());
        }
        catch (NoSuchObjectException e) {
            throw new TableNotFoundException(tableName);
        }
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        ImmutableList.Builder<SchemaTableName> tableNames = ImmutableList.builder();
        for (String schemaName : listSchemas(session, schemaNameOrNull)) {
            try {
                for (String tableName : metastore.getAllTables(schemaName)) {
                    tableNames.add(new SchemaTableName(schemaName, tableName));
                }
            }
            catch (NoSuchObjectException e) {
                // schema disappeared during listing operation
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
    public ConnectorColumnHandle getSampleWeightColumnHandle(ConnectorTableHandle tableHandle)
    {
        SchemaTableName tableName = schemaTableName(tableHandle);
        try {
            Table table = metastore.getTable(tableName.getSchemaName(), tableName.getTableName());
            for (HiveColumnHandle columnHandle : hiveColumnHandles(typeManager, connectorId, table, true)) {
                if (columnHandle.getName().equals(SAMPLE_WEIGHT_COLUMN_NAME)) {
                    return columnHandle;
                }
            }
            return null;
        }
        catch (NoSuchObjectException e) {
            throw new TableNotFoundException(tableName);
        }
    }

    @Override
    public boolean canCreateSampledTables(ConnectorSession session)
    {
        return true;
    }

    @Override
    public Map<String, ConnectorColumnHandle> getColumnHandles(ConnectorTableHandle tableHandle)
    {
        SchemaTableName tableName = schemaTableName(tableHandle);
        try {
            Table table = metastore.getTable(tableName.getSchemaName(), tableName.getTableName());
            ImmutableMap.Builder<String, ConnectorColumnHandle> columnHandles = ImmutableMap.builder();
            for (HiveColumnHandle columnHandle : hiveColumnHandles(typeManager, connectorId, table, false)) {
                columnHandles.put(columnHandle.getName(), columnHandle);
            }
            return columnHandles.build();
        }
        catch (NoSuchObjectException e) {
            throw new TableNotFoundException(tableName);
        }
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        checkNotNull(prefix, "prefix is null");
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
        if (prefix.getSchemaName() == null) {
            return listTables(session, prefix.getSchemaName());
        }
        return ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
    }

    /**
     * NOTE: This method does not return column comment
     */
    @Override
    public ColumnMetadata getColumnMetadata(ConnectorTableHandle tableHandle, ConnectorColumnHandle columnHandle)
    {
        checkType(tableHandle, HiveTableHandle.class, "tableHandle");
        return checkType(columnHandle, HiveColumnHandle.class, "columnHandle").getColumnMetadata(typeManager);
    }

    @Override
    public ConnectorTableHandle createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameTable(ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        if (!allowRenameTable) {
            throw new PrestoException(PERMISSION_DENIED, "Renaming tables is disabled in this Hive catalog");
        }

        HiveTableHandle handle = checkType(tableHandle, HiveTableHandle.class, "tableHandle");
        metastore.renameTable(handle.getSchemaName(), handle.getTableName(), newTableName.getSchemaName(), newTableName.getTableName());
    }

    @Override
    public void dropTable(ConnectorTableHandle tableHandle)
    {
        HiveTableHandle handle = checkType(tableHandle, HiveTableHandle.class, "tableHandle");
        SchemaTableName tableName = schemaTableName(tableHandle);

        if (!allowDropTable) {
            throw new PrestoException(PERMISSION_DENIED, "DROP TABLE is disabled in this Hive catalog");
        }

        try {
            Table table = metastore.getTable(handle.getSchemaName(), handle.getTableName());
            if (!handle.getSession().getUser().equals(table.getOwner())) {
                throw new PrestoException(PERMISSION_DENIED, format("Unable to drop table '%s': owner of the table is different from session user", table));
            }
            metastore.dropTable(handle.getSchemaName(), handle.getTableName());
        }
        catch (NoSuchObjectException e) {
            throw new TableNotFoundException(tableName);
        }
    }

    @Override
    public HiveOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        verifyJvmTimeZone();

        checkArgument(!isNullOrEmpty(tableMetadata.getOwner()), "Table owner is null or empty");

        HiveStorageFormat hiveStorageFormat = getHiveStorageFormat(session, this.hiveStorageFormat);

        ImmutableList.Builder<String> columnNames = ImmutableList.builder();
        ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();
        for (ColumnMetadata column : tableMetadata.getColumns()) {
            // TODO: also verify that the OutputFormat supports the type
            if (!HiveRecordSink.isTypeSupported(column.getType())) {
                throw new PrestoException(NOT_SUPPORTED, format("Cannot create table with unsupported type: %s", column.getType().getDisplayName()));
            }
            columnNames.add(column.getName());
            columnTypes.add(column.getType());
        }
        if (tableMetadata.isSampled()) {
            columnNames.add(SAMPLE_WEIGHT_COLUMN_NAME);
            columnTypes.add(BIGINT);
        }

        // get the root directory for the database
        SchemaTableName table = tableMetadata.getTable();
        String schemaName = table.getSchemaName();
        String tableName = table.getTableName();

        String location = getDatabase(schemaName).getLocationUri();
        if (isNullOrEmpty(location)) {
            throw new RuntimeException(format("Database '%s' location is not set", schemaName));
        }

        Path databasePath = new Path(location);
        if (!pathExists(databasePath)) {
            throw new RuntimeException(format("Database '%s' location does not exist: %s", schemaName, databasePath));
        }
        if (!isDirectory(databasePath)) {
            throw new RuntimeException(format("Database '%s' location is not a directory: %s", schemaName, databasePath));
        }

        // verify the target directory for the table
        Path targetPath = new Path(databasePath, tableName);
        if (pathExists(targetPath)) {
            throw new RuntimeException(format("Target directory for table '%s' already exists: %s", table, targetPath));
        }

        if (!useTemporaryDirectory(targetPath)) {
            return new HiveOutputTableHandle(
                    connectorId,
                    schemaName,
                    tableName,
                    columnNames.build(),
                    columnTypes.build(),
                    tableMetadata.getOwner(),
                    targetPath.toString(),
                    targetPath.toString(),
                    session,
                    hiveStorageFormat);
        }

        return new HiveOutputTableHandle(
                connectorId,
                schemaName,
                tableName,
                columnNames.build(),
                columnTypes.build(),
                tableMetadata.getOwner(),
                targetPath.toString(),
                createTemporaryPath(targetPath),
                session,
                hiveStorageFormat);
    }

    @Override
    public void commitCreateTable(ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments)
    {
        HiveOutputTableHandle handle = checkType(tableHandle, HiveOutputTableHandle.class, "tableHandle");

        // verify no one raced us to create the target directory
        Path targetPath = new Path(handle.getTargetPath());

        // rename if using a temporary directory
        if (handle.hasTemporaryPath()) {
            if (pathExists(targetPath)) {
                SchemaTableName table = new SchemaTableName(handle.getSchemaName(), handle.getTableName());
                throw new RuntimeException(format("Unable to commit creation of table '%s': target directory already exists: %s", table, targetPath));
            }
            // rename the temporary directory to the target
            rename(new Path(handle.getTemporaryPath()), targetPath);
        }

        // create the table in the metastore
        List<String> types = FluentIterable.from(handle.getColumnTypes())
                .transform(HiveType::toHiveType)
                .transform(HiveType::getHiveTypeName)
                .toList();

        boolean sampled = false;
        ImmutableList.Builder<FieldSchema> columns = ImmutableList.builder();
        for (int i = 0; i < handle.getColumnNames().size(); i++) {
            String name = handle.getColumnNames().get(i);
            String type = types.get(i);
            if (name.equals(SAMPLE_WEIGHT_COLUMN_NAME)) {
                columns.add(new FieldSchema(name, type, "Presto sample weight column"));
                sampled = true;
            }
            else {
                columns.add(new FieldSchema(name, type, null));
            }
        }

        HiveStorageFormat hiveStorageFormat = handle.getHiveStorageFormat();

        SerDeInfo serdeInfo = new SerDeInfo();
        serdeInfo.setName(handle.getTableName());
        serdeInfo.setSerializationLib(hiveStorageFormat.getSerDe());
        serdeInfo.setParameters(ImmutableMap.<String, String>of());

        StorageDescriptor sd = new StorageDescriptor();
        sd.setLocation(targetPath.toString());
        sd.setCols(columns.build());
        sd.setSerdeInfo(serdeInfo);
        sd.setInputFormat(hiveStorageFormat.getInputFormat());
        sd.setOutputFormat(hiveStorageFormat.getOutputFormat());
        sd.setParameters(ImmutableMap.<String, String>of());

        Table table = new Table();
        table.setDbName(handle.getSchemaName());
        table.setTableName(handle.getTableName());
        table.setOwner(handle.getTableOwner());
        table.setTableType(TableType.MANAGED_TABLE.toString());
        String tableComment = "Created by Presto";
        if (sampled) {
            tableComment = "Sampled table created by Presto. Only query this table from Hive if you understand how Presto implements sampling.";
        }
        table.setParameters(ImmutableMap.of("comment", tableComment));
        table.setPartitionKeys(ImmutableList.<FieldSchema>of());
        table.setSd(sd);

        metastore.createTable(table);
    }

    private Database getDatabase(String database)
    {
        try {
            return metastore.getDatabase(database);
        }
        catch (NoSuchObjectException e) {
            throw new SchemaNotFoundException(database);
        }
    }

    private boolean useTemporaryDirectory(Path path)
    {
        try {
            // skip using temporary directory for S3
            return !(hdfsEnvironment.getFileSystem(path) instanceof PrestoS3FileSystem);
        }
        catch (IOException e) {
            throw new RuntimeException("Failed checking path: " + path, e);
        }
    }

    private boolean pathExists(Path path)
    {
        try {
            return hdfsEnvironment.getFileSystem(path).exists(path);
        }
        catch (IOException e) {
            throw new RuntimeException("Failed checking path: " + path, e);
        }
    }

    private boolean isDirectory(Path path)
    {
        try {
            return hdfsEnvironment.getFileSystem(path).isDirectory(path);
        }
        catch (IOException e) {
            throw new RuntimeException("Failed checking path: " + path, e);
        }
    }

    private void createDirectories(Path path)
    {
        try {
            if (!hdfsEnvironment.getFileSystem(path).mkdirs(path)) {
                throw new IOException("mkdirs returned false");
            }
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to create directory: " + path, e);
        }
    }

    private void rename(Path source, Path target)
    {
        try {
            if (!hdfsEnvironment.getFileSystem(source).rename(source, target)) {
                throw new IOException("rename returned false");
            }
        }
        catch (IOException e) {
            throw new RuntimeException(format("Failed to rename %s to %s", source, target), e);
        }
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
        String view = getViews(session, viewName.toSchemaTablePrefix()).get(viewName);
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
            try {
                for (String tableName : metastore.getAllViews(schemaName)) {
                    tableNames.add(new SchemaTableName(schemaName, tableName));
                }
            }
            catch (NoSuchObjectException e) {
                // schema disappeared during listing operation
            }
        }
        return tableNames.build();
    }

    @Override
    public Map<SchemaTableName, String> getViews(ConnectorSession session, SchemaTablePrefix prefix)
    {
        ImmutableMap.Builder<SchemaTableName, String> views = ImmutableMap.builder();
        List<SchemaTableName> tableNames;
        if (prefix.getTableName() != null) {
            tableNames = ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
        }
        else {
            tableNames = listViews(session, prefix.getSchemaName());
        }

        for (SchemaTableName schemaTableName : tableNames) {
            try {
                Table table = metastore.getTable(schemaTableName.getSchemaName(), schemaTableName.getTableName());
                if (HiveUtil.isPrestoView(table)) {
                    views.put(schemaTableName, decodeViewData(table.getViewOriginalText()));
                }
            }
            catch (NoSuchObjectException ignored) {
            }
        }

        return views.build();
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        verifyJvmTimeZone();

        List<Boolean> partitionBitmap = null;

        ImmutableList.Builder<String> columnNames = ImmutableList.builder();
        ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();

        // call metastore to get table location, outputFormat, serde, partitions indices
        Table table = null;
        SchemaTableName tableSchemaName = getTableName(tableHandle);
        try {
            table = getMetastore().getTable(tableSchemaName.getSchemaName(), tableSchemaName.getTableName());
        }
        catch (NoSuchObjectException e) {
            table = null;
        }

        checkNotNull(table, "Table %s does not exist", tableSchemaName.getTableName());
        if (table.getSd().getNumBuckets() > 0) {
            throw new UnsupportedOperationException("Insert not supported with Bucketed Tables");
        }

        String outputFormat = table.getSd().getOutputFormat();
        SerDeInfo serdeInfo = table.getSd().getSerdeInfo();
        String serdeLib = serdeInfo.getSerializationLib();
        Map<String, String> serdeParameters = serdeInfo.getParameters();

        String location = table.getSd().getLocation();
        ConnectorTableMetadata tableMetadata = getTableMetadata(tableHandle);

        if (table.getPartitionKeysSize() != 0) {
            partitionBitmap = new ArrayList<Boolean>(tableMetadata.getColumns().size());

            for (ColumnMetadata column : tableMetadata.getColumns()) {
                partitionBitmap.add(column.isPartitionKey());
            }
        }

        if (tableMetadata.isSampled()) {
            columnNames.add(SAMPLE_WEIGHT_COLUMN_NAME);
            columnTypes.add(BIGINT);
        }
        for (ColumnMetadata column : tableMetadata.getColumns()) {
            columnNames.add(column.getName());
            columnTypes.add(column.getType());
        }

        return buildInsert(
                tableSchemaName.getSchemaName(),
                tableSchemaName.getTableName(),
                location,
                columnNames.build(),
                columnTypes.build(),
                outputFormat,
                serdeLib,
                serdeParameters,
                partitionBitmap,
                session);
    }

    @Override
    public void commitInsert(ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments)
    {
        HiveInsertTableHandle handle = checkType(insertHandle, HiveInsertTableHandle.class, "invalid insertHandle");
        Map<String, List<String>> filesWritten = new HashMap<String, List<String>>();
        ObjectMapper mapper = new ObjectMapper();

        try {
            for (Slice fragmentSlice : fragments) {
                String fragment = new String(fragmentSlice.getBytes(), "UTF-8");
                if (fragment.length() == 0) {
                    log.warn("Empty fragment for Insert on table " + handle.getTableName());
                    continue;
                }
                Map<String, List<String>> filesWrittenFragment = new HashMap<String, List<String>>();
                filesWrittenFragment = mapper.readValue(fragment, filesWrittenFragment.getClass());
                for (String partition : filesWrittenFragment.keySet()) {
                    if (!filesWritten.containsKey(partition)) {
                        filesWritten.put(partition, new ArrayList<String>());
                    }

                    filesWritten.get(partition).addAll(filesWrittenFragment.get(partition));
                }
            }

            commitInsertWork(handle, filesWritten);
        }
        catch (Exception e) {
            rollbackInsertChanges(handle, filesWritten);
            throw Throwables.propagate(e);
        }
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("clientId", connectorId)
                .toString();
    }

    private void rollbackInsertChanges(HiveInsertTableHandle handle, Map<String, List<String>> filesWritten)
    {
        Path tableLocation = new Path(handle.getTargetPath());
        try {
            for (String partition : filesWritten.keySet()) {
                Path partitionPath;
                if (handle.isOutputTablePartitioned()) {
                    partitionPath = new Path(tableLocation, partition);
                }
                else {
                    partitionPath = tableLocation;
                }
                for (String file : filesWritten.get(partition)) {
                    delete(new Path(partitionPath, file), false);
                }
            }
        }
        catch (IOException e) {
            log.info(String.format("Files with prefix %s need to be deleted manually from location %s, Rollback of changes made during insert failed with %s",
                    handle.getFilePrefix(),
                    handle.getTargetPath(),
                    e.getStackTrace()));
        }
    }

    private void commitInsertWork(HiveInsertTableHandle handle, Map<String, List<String>> filesWritten) throws PrestoException, IOException, TException
    {
        Path targetLocation = new Path(handle.getTargetPath());

        //Move data from temp locations
        if (handle.hasTemporaryPath()) {
            moveInsertIntoData(handle, filesWritten);
        }

        if (handle.isOutputTablePartitioned()) {
            // Get info about all the existing partitions
            Set<String> partitionsKnown = new HashSet<String>();
            HiveTableHandle hiveTableHandle = new HiveTableHandle(handle.getClientId(),
                    handle.getSchemaName(),
                    handle.getTableName(),
                    handle.getConnectorSession());
            ConnectorPartitionResult result = splitManager.getPartitions(hiveTableHandle,
                    TupleDomain.<ConnectorColumnHandle>all());
            List<ConnectorPartition> partitions = result.getPartitions();
            Iterator<ConnectorPartition> pIter = partitions.iterator();

            while (pIter.hasNext()) {
                HivePartition p = (HivePartition) pIter.next();
                // partitionId will be of form pKey1=value1/pKey2=value2
                String partitionId = p.getPartitionId();

                if (!partitionsKnown.add(partitionId.toString())) {
                    throw new RuntimeException(String.format("Table '%s' has duplicate partitions for '%s'", handle.getTableName(), partitionId));
                }
            }

            Table table = getMetastore().getTable(handle.getSchemaName(), handle.getTableName());
            // partition recovery
            findAndRecoverPartitions(handle.getConnectorSession(),
                    targetLocation,
                    filesWritten.keySet(),
                    partitionsKnown,
                    table);
        }
    }

    private void findAndRecoverPartitions(ConnectorSession session,
                                          Path tableLocation,
                                          Set<String> partitionsWritten,
                                          Set<String> partitionsKnown,
                                          final Table table) throws TException
    {
        List<Partition> commitBatch = new ArrayList<Partition>();
        int recovered = 0;

        for (String partition : partitionsWritten) {
            if (!partitionsKnown.contains(partition)) {
                // create the partition location using tableLocation and the values
                Path partLocation = new Path(tableLocation, partition);
                Partition tpart = getMetastore().createPartition(table.getDbName(), table.getTableName(), getPartitionValues(partition), null, table, partLocation.toString());
                commitBatch.add(tpart);
                recovered++;

                if (commitBatch.size() >= partionCommitBatchSize) {
                    getMetastore().addPartitions(commitBatch, table.getDbName(), table.getTableName());
                    commitBatch.clear();
                }
            }
        }

        if (commitBatch.size() > 0) {
            getMetastore().addPartitions(commitBatch, table.getDbName(), table.getTableName());
            commitBatch.clear();
        }

        log.info("Recovered " + recovered + " partitions");
    }

    private void moveInsertIntoData(HiveInsertTableHandle handle, Map<String, List<String>> filesWritten) throws IOException
    {
        ExecutorService executor = Executors.newFixedThreadPool(
                RENAME_THREADPOOL_SIZE,
                new ThreadFactoryBuilder().setNameFormat("hive-client-rename-" + "-%d").build());

        for (String partition : filesWritten.keySet()) {
            Path srcPartition;
            Path destPartition;
            if (handle.isOutputTablePartitioned()) {
                srcPartition = new Path(handle.getTemporaryPath(), partition);
                destPartition = new Path(handle.getTargetPath(), partition);
            }
            else {
                srcPartition = new Path(handle.getTemporaryPath());
                destPartition = new Path(handle.getTargetPath());
            }
            if (!pathExists(destPartition)) {
                createDirectories(destPartition);
            }

            for (String file : filesWritten.get(partition)) {
                final Path srcPath = new Path(srcPartition, file);
                final Path destPath = destPartition;
                executor.execute(new Runnable()
                {
                    @Override
                    public void run()
                    {
                        rename(srcPath, destPath);
                    }
                });

            }
        }

        executor.shutdown();
        while (!executor.isTerminated()) {
            try {
                Thread.sleep(1000);
            }
            catch (InterruptedException e) {
                throw Throwables.propagate(e);
            }
        }
    }

    private ConnectorInsertTableHandle buildInsert(String schemaName,
                                                   String tableName,
                                                   String location,
                                                   List<String> columnNames,
                                                   List<Type> columnTypes,
                                                   String outputFormat,
                                                   String serdeLib,
                                                   Map<String, String> serdeParameters,
                                                   List<Boolean> partitionBitmap,
                                                   ConnectorSession session)
    {
        Path targetPath = new Path(location);
        if (!pathExists(targetPath)) {
            createDirectories(targetPath);
        }

        String tempPath;
        String filePrefix = randomUUID().toString();

        log.info(String.format("Using '%s' as file prefix for insert", filePrefix));

        if ((useTemporaryDirectory(targetPath) || insertS3TempEnabled)) {
            tempPath = createTemporaryPath(targetPath);
        }
        else {
            tempPath = targetPath.toString();
        }

        return new HiveInsertTableHandle(
                connectorId,
                schemaName,
                tableName,
                columnNames,
                columnTypes,
                targetPath.toString(),
                tempPath,
                outputFormat,
                serdeLib,
                serdeParameters,
                partitionBitmap,
                filePrefix,
                session);
    }

    private String createTemporaryPath(Path targetPath)
    {
        // use a per-user temporary directory to avoid permission problems
        // TODO: this should use Hadoop UserGroupInformation
        String temporaryPrefix = "/tmp/presto-" + StandardSystemProperty.USER_NAME.value();

        // create a temporary directory on the same filesystem
        Path temporaryRoot = new Path(targetPath, temporaryPrefix);
        Path temporaryPath = new Path(temporaryRoot, randomUUID().toString());
        createDirectories(temporaryPath);

        return temporaryPath.toString();
    }

    private void verifyJvmTimeZone()
    {
        if (!allowCorruptWritesForTesting && !timeZone.equals(DateTimeZone.getDefault())) {
            throw new PrestoException(HIVE_TIMEZONE_MISMATCH, format(
                    "To write Hive data, your JVM timezone must match the Hive storage timezone. Add -Duser.timezone=%s to your JVM arguments.",
                    timeZone.getID()));
        }
    }

    private static Function<HiveColumnHandle, ColumnMetadata> columnMetadataGetter(Table table, final TypeManager typeManager)
    {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        for (FieldSchema field : concat(table.getSd().getCols(), table.getPartitionKeys())) {
            if (field.getComment() != null) {
                builder.put(field.getName(), field.getComment());
            }
        }
        final Map<String, String> columnComment = builder.build();

        return input -> new ColumnMetadata(
                input.getName(),
                typeManager.getType(input.getTypeSignature()),
                input.getOrdinalPosition(),
                input.isPartitionKey(),
                columnComment.get(input.getName()),
                false);
    }

    private static SchemaTableName getTableName(ConnectorTableHandle tableHandle)
    {
        return checkType(tableHandle, HiveTableHandle.class, "tableHandle").getSchemaTableName();
    }

    public static List<String> getPartitionValues(String partitionName)
    {
        String[] cols = partitionName.split(Path.SEPARATOR);
        List<String> values = new ArrayList<String>();
        for (String col : cols) {
            values.add(col.split("=")[1]);
        }
        return values;
    }

    public void delete(Path source, boolean recursive) throws IOException
    {
        if (!hdfsEnvironment.getFileSystem(source).delete(source, recursive)) {
            throw new IOException(String.format("delete on '%s' returned false", source));
        }
    }
}
