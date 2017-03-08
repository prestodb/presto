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
package com.facebook.presto.hive.metastore.file;

import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.hive.SchemaAlreadyExistsException;
import com.facebook.presto.hive.TableAlreadyExistsException;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.Database;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.HivePrivilegeInfo;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.PrincipalPrivileges;
import com.facebook.presto.hive.metastore.PrincipalType;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.ColumnNotFoundException;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.ByteStreams;
import io.airlift.json.JsonCodec;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static com.facebook.presto.hive.HiveUtil.toPartitionValues;
import static com.facebook.presto.hive.metastore.Database.DEFAULT_DATABASE_NAME;
import static com.facebook.presto.hive.metastore.HivePrivilegeInfo.HivePrivilege.OWNERSHIP;
import static com.facebook.presto.hive.metastore.MetastoreUtil.makePartName;
import static com.facebook.presto.hive.metastore.PrincipalType.ROLE;
import static com.facebook.presto.hive.metastore.PrincipalType.USER;
import static com.facebook.presto.spi.StandardErrorCode.ALREADY_EXISTS;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.metastore.TableType.EXTERNAL_TABLE;
import static org.apache.hadoop.hive.metastore.TableType.MANAGED_TABLE;
import static org.apache.hadoop.hive.metastore.TableType.VIRTUAL_VIEW;

@ThreadSafe
public class FileHiveMetastore
        implements ExtendedHiveMetastore
{
    private static final String PUBLIC_ROLE_NAME = "public";
    private static final String PRESTO_SCHEMA_FILE_NAME = ".prestoSchema";
    private static final String PRESTO_PERMISSIONS_DIRECTORY_NAME = ".prestoPermissions";

    private final HdfsEnvironment hdfsEnvironment;
    private final Path catalogDirectory;
    private final String metastoreUser;
    private final FileSystem metadataFileSystem;

    private final JsonCodec<DatabaseMetadata> databaseCodec = JsonCodec.jsonCodec(DatabaseMetadata.class);
    private final JsonCodec<TableMetadata> tableCodec = JsonCodec.jsonCodec(TableMetadata.class);
    private final JsonCodec<PartitionMetadata> partitionCodec = JsonCodec.jsonCodec(PartitionMetadata.class);
    private final JsonCodec<List<PermissionMetadata>> permissionsCodec = JsonCodec.listJsonCodec(PermissionMetadata.class);

    @Inject
    public FileHiveMetastore(HdfsEnvironment hdfsEnvironment, FileHiveMetastoreConfig config)
    {
        this(hdfsEnvironment, config.getCatalogDirectory(), config.getMetastoreUser());
    }

    public FileHiveMetastore(HdfsEnvironment hdfsEnvironment, String catalogDirectory, String metastoreUser)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.catalogDirectory = new Path(requireNonNull(catalogDirectory, "baseDirectory is null"));
        this.metastoreUser = requireNonNull(metastoreUser, "metastoreUser is null");
        try {
            metadataFileSystem = hdfsEnvironment.getFileSystem(metastoreUser, this.catalogDirectory);
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public synchronized void createDatabase(Database database)
    {
        requireNonNull(database, "database is null");

        if (database.getLocation().isPresent()) {
            throw new PrestoException(HIVE_METASTORE_ERROR, "Database can not be created with a location set");
        }

        verifyDatabaseNotExists(database.getDatabaseName());

        Path databaseMetadataDirectory = getDatabaseMetadataDirectory(database.getDatabaseName());
        writeSchemaFile("database", databaseMetadataDirectory, databaseCodec, new DatabaseMetadata(database), false);
    }

    @Override
    public synchronized void dropDatabase(String databaseName)
    {
        requireNonNull(databaseName, "databaseName is null");

        getRequiredDatabase(databaseName);
        if (!getAllTables(databaseName).orElse(ImmutableList.of()).isEmpty()) {
            throw new PrestoException(HIVE_METASTORE_ERROR, "Database " + databaseName + " is not empty");
        }

        deleteMetadataDirectory(getDatabaseMetadataDirectory(databaseName));
    }

    @Override
    public synchronized void renameDatabase(String databaseName, String newDatabaseName)
    {
        requireNonNull(databaseName, "databaseName is null");
        requireNonNull(newDatabaseName, "newDatabaseName is null");

        getRequiredDatabase(databaseName);
        verifyDatabaseNotExists(newDatabaseName);

        try {
            if (!metadataFileSystem.rename(getDatabaseMetadataDirectory(databaseName), getDatabaseMetadataDirectory(newDatabaseName))) {
                throw new PrestoException(HIVE_METASTORE_ERROR, "Could not rename database metadata directory");
            }
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public synchronized Optional<Database> getDatabase(String databaseName)
    {
        requireNonNull(databaseName, "databaseName is null");

        Path databaseMetadataDirectory = getDatabaseMetadataDirectory(databaseName);
        return readSchemaFile("database", databaseMetadataDirectory, databaseCodec)
                .map(databaseMetadata -> databaseMetadata.toDatabase(databaseName, databaseMetadataDirectory.toString()));
    }

    private Database getRequiredDatabase(String databaseName)
    {
        return getDatabase(databaseName)
                .orElseThrow(() -> new SchemaNotFoundException(databaseName));
    }

    private void verifyDatabaseNotExists(String databaseName)
    {
        if (getDatabase(databaseName).isPresent()) {
            throw new SchemaAlreadyExistsException(databaseName);
        }
    }

    @Override
    public synchronized List<String> getAllDatabases()
    {
        List<String> databases = getChildSchemaDirectories(catalogDirectory).stream()
                .map(Path::getName)
                .collect(toList());
        return ImmutableList.copyOf(databases);
    }

    @Override
    public synchronized void createTable(Table table, PrincipalPrivileges principalPrivileges)
    {
        verifyTableNotExists(table.getDatabaseName(), table.getTableName());

        Path tableMetadataDirectory = getTableMetadataDirectory(table);

        // validate table location
        if (table.getTableType().equals(VIRTUAL_VIEW.name())) {
            checkArgument(table.getStorage().getLocation().isEmpty(), "Storage location for view must be empty");
        }
        else if (table.getTableType().equals(MANAGED_TABLE.name())) {
            if (!tableMetadataDirectory.equals(new Path(table.getStorage().getLocation()))) {
                throw new PrestoException(HIVE_METASTORE_ERROR, "Table directory must be " + tableMetadataDirectory);
            }
        }
        else if (table.getTableType().equals(EXTERNAL_TABLE.name())) {
            try {
                Path externalLocation = new Path(table.getStorage().getLocation());
                FileSystem externalFileSystem = hdfsEnvironment.getFileSystem(metastoreUser, externalLocation);
                if (!externalFileSystem.isDirectory(externalLocation)) {
                    throw new PrestoException(HIVE_METASTORE_ERROR, "External table location does not exist");
                }
                if (isChildDirectory(catalogDirectory, externalLocation)) {
                    throw new PrestoException(HIVE_METASTORE_ERROR, "External table location can not be inside the system metadata directory");
                }
            }
            catch (IOException e) {
                throw new PrestoException(HIVE_METASTORE_ERROR, "Could not validate external location", e);
            }
        }
        else {
            throw new PrestoException(NOT_SUPPORTED, "Table type not supported: " + table.getTableType());
        }

        writeSchemaFile("table", tableMetadataDirectory, tableCodec, new TableMetadata(table), false);

        for (Entry<String, Collection<HivePrivilegeInfo>> entry : principalPrivileges.getUserPrivileges().asMap().entrySet()) {
            setTablePrivileges(entry.getKey(), USER, table.getDatabaseName(), table.getTableName(), entry.getValue());
        }
        for (Entry<String, Collection<HivePrivilegeInfo>> entry : principalPrivileges.getRolePrivileges().asMap().entrySet()) {
            setTablePrivileges(entry.getKey(), ROLE, table.getDatabaseName(), table.getTableName(), entry.getValue());
        }
    }

    @Override
    public synchronized Optional<Table> getTable(String databaseName, String tableName)
    {
        requireNonNull(databaseName, "databaseName is null");
        requireNonNull(tableName, "tableName is null");

        Path tableMetadataDirectory = getTableMetadataDirectory(databaseName, tableName);
        return readSchemaFile("table", tableMetadataDirectory, tableCodec)
                .map(tableMetadata -> tableMetadata.toTable(databaseName, tableName, tableMetadataDirectory.toString()));
    }

    private Table getRequiredTable(String databaseName, String tableName)
    {
        return getTable(databaseName, tableName)
                    .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(databaseName, tableName)));
    }

    private void verifyTableNotExists(String newDatabaseName, String newTableName)
    {
        if (getTable(newDatabaseName, newTableName).isPresent()) {
            throw new TableAlreadyExistsException(new SchemaTableName(newDatabaseName, newTableName));
        }
    }

    @Override
    public synchronized Optional<List<String>> getAllTables(String databaseName)
    {
        requireNonNull(databaseName, "databaseName is null");

        Optional<Database> database = getDatabase(databaseName);
        if (!database.isPresent()) {
            return Optional.empty();
        }

        Path databaseMetadataDirectory = getDatabaseMetadataDirectory(databaseName);
        List<String> tables = getChildSchemaDirectories(databaseMetadataDirectory).stream()
                .map(Path::getName)
                .collect(toList());
        return Optional.of(ImmutableList.copyOf(tables));
    }

    @Override
    public synchronized Optional<List<String>> getAllViews(String databaseName)
    {
        Optional<List<String>> tables = getAllTables(databaseName);
        if (!tables.isPresent()) {
            return Optional.empty();
        }

        List<String> views = tables.get().stream()
                .map(tableName -> getTable(databaseName, tableName))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .filter(table -> table.getTableType().equals(VIRTUAL_VIEW.name()))
                .map(Table::getTableName)
                .collect(toList());

        return Optional.of(ImmutableList.copyOf(views));
    }

    @Override
    public synchronized void dropTable(String databaseName, String tableName, boolean deleteData)
    {
        requireNonNull(databaseName, "databaseName is null");
        requireNonNull(tableName, "tableName is null");

        Table table = getRequiredTable(databaseName, tableName);

        Path tableMetadataDirectory = getTableMetadataDirectory(databaseName, tableName);

        // It is safe to delete the whole meta directory for external tables and views
        if (!table.getTableType().equals(MANAGED_TABLE.name()) || deleteData) {
            deleteMetadataDirectory(tableMetadataDirectory);
        }
        else {
            // in this case we only wan to delete the metadata of a managed table
            deleteSchemaFile("table", tableMetadataDirectory);
            deleteTablePrivileges(table);
        }
    }

    @Override
    public synchronized void replaceTable(String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges)
    {
        Table table = getRequiredTable(databaseName, tableName);
        if (!table.getTableType().equals(VIRTUAL_VIEW.name()) || !newTable.getTableType().equals(VIRTUAL_VIEW.name())) {
            throw new PrestoException(HIVE_METASTORE_ERROR, "Only views can be updated with replaceTable");
        }
        if (!table.getDatabaseName().equals(databaseName) || !table.getTableName().equals(tableName)) {
            throw new PrestoException(HIVE_METASTORE_ERROR, "Replacement table must have same name");
        }

        Path tableMetadataDirectory = getTableMetadataDirectory(table);
        writeSchemaFile("table", tableMetadataDirectory, tableCodec, new TableMetadata(newTable), true);

        // replace existing permissions
        deleteTablePrivileges(table);

        for (Entry<String, Collection<HivePrivilegeInfo>> entry : principalPrivileges.getUserPrivileges().asMap().entrySet()) {
            setTablePrivileges(entry.getKey(), USER, table.getDatabaseName(), table.getTableName(), entry.getValue());
        }
        for (Entry<String, Collection<HivePrivilegeInfo>> entry : principalPrivileges.getRolePrivileges().asMap().entrySet()) {
            setTablePrivileges(entry.getKey(), ROLE, table.getDatabaseName(), table.getTableName(), entry.getValue());
        }
    }

    @Override
    public synchronized void renameTable(String databaseName, String tableName, String newDatabaseName, String newTableName)
    {
        requireNonNull(databaseName, "databaseName is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(newDatabaseName, "newDatabaseName is null");
        requireNonNull(newTableName, "newTableName is null");

        getRequiredTable(databaseName, tableName);
        getRequiredDatabase(newDatabaseName);

        // verify new table does not exist
        verifyTableNotExists(newDatabaseName, newTableName);

        try {
            if (!metadataFileSystem.rename(getTableMetadataDirectory(databaseName, tableName), getTableMetadataDirectory(newDatabaseName, newTableName))) {
                throw new PrestoException(HIVE_METASTORE_ERROR, "Could not rename table directory");
            }
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public synchronized void addColumn(String databaseName, String tableName, String columnName, HiveType columnType, String columnComment)
    {
        alterTable(databaseName, tableName, oldTable -> {
            if (oldTable.getColumn(columnName).isPresent()) {
                throw new PrestoException(ALREADY_EXISTS, "Column already exists: " + columnName);
            }

            return oldTable.withDataColumns(ImmutableList.<Column>builder()
                    .addAll(oldTable.getDataColumns())
                    .add(new Column(columnName, columnType, Optional.ofNullable(columnComment)))
                    .build());
        });
    }

    @Override
    public synchronized void renameColumn(String databaseName, String tableName, String oldColumnName, String newColumnName)
    {
        alterTable(databaseName, tableName, oldTable -> {
            if (oldTable.getColumn(newColumnName).isPresent()) {
                throw new PrestoException(ALREADY_EXISTS, "Column already exists: " + newColumnName);
            }
            if (!oldTable.getColumn(oldColumnName).isPresent()) {
                SchemaTableName name = new SchemaTableName(databaseName, tableName);
                throw new ColumnNotFoundException(name, oldColumnName);
            }
            for (Column column : oldTable.getPartitionColumns()) {
                if (column.getName().equals(oldColumnName)) {
                    throw new PrestoException(NOT_SUPPORTED, "Renaming partition columns is not supported");
                }
            }

            ImmutableList.Builder<Column> newDataColumns = ImmutableList.builder();
            for (Column fieldSchema : oldTable.getDataColumns()) {
                if (fieldSchema.getName().equals(oldColumnName)) {
                    newDataColumns.add(new Column(newColumnName, fieldSchema.getType(), fieldSchema.getComment()));
                }
                else {
                    newDataColumns.add(fieldSchema);
                }
            }

            return oldTable.withDataColumns(newDataColumns.build());
        });
    }

    private void alterTable(String databaseName, String tableName, Function<TableMetadata, TableMetadata> alterFunction)
    {
        requireNonNull(databaseName, "databaseName is null");
        requireNonNull(tableName, "tableName is null");

        Path tableMetadataDirectory = getTableMetadataDirectory(databaseName, tableName);

        TableMetadata oldTableSchema = readSchemaFile("table", tableMetadataDirectory, tableCodec)
                .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(databaseName, tableName)));
        TableMetadata newTableSchema = alterFunction.apply(oldTableSchema);
        if (oldTableSchema == newTableSchema) {
            return;
        }

        writeSchemaFile("table", tableMetadataDirectory, tableCodec, newTableSchema, true);
    }

    @Override
    public synchronized void addPartitions(String databaseName, String tableName, List<Partition> partitions)
    {
        requireNonNull(databaseName, "databaseName is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(partitions, "partitions is null");

        Table table = getRequiredTable(databaseName, tableName);

        TableType tableType = TableType.valueOf(table.getTableType());
        checkArgument(EnumSet.of(MANAGED_TABLE, EXTERNAL_TABLE).contains(tableType), "Invalid table type: %s", tableType);

        try {
            Map<Path, byte[]> schemaFiles = new LinkedHashMap<>();
            for (Partition partition : partitions) {
                verifiedPartition(table, partition);
                Path partitionMetadataDirectory = getPartitionMetadataDirectory(table, partition.getValues());
                Path schemaPath = new Path(partitionMetadataDirectory, PRESTO_SCHEMA_FILE_NAME);
                if (metadataFileSystem.exists(schemaPath)) {
                    throw new PrestoException(HIVE_METASTORE_ERROR, "Partition already exists");
                }
                byte[] schemaJson = partitionCodec.toJsonBytes(new PartitionMetadata(table, partition));
                schemaFiles.put(schemaPath, schemaJson);
            }

            Set<Path> createdFiles = new LinkedHashSet<>();
            try {
                for (Entry<Path, byte[]> entry : schemaFiles.entrySet()) {
                    try (OutputStream outputStream = metadataFileSystem.create(entry.getKey())) {
                        createdFiles.add(entry.getKey());
                        outputStream.write(entry.getValue());
                    }
                    catch (IOException e) {
                        throw new PrestoException(HIVE_METASTORE_ERROR, "Could not write partition schema", e);
                    }
                }
            }
            catch (Throwable e) {
                for (Path createdFile : createdFiles) {
                    try {
                        metadataFileSystem.delete(createdFile, false);
                    }
                    catch (IOException ignored) {
                    }
                }
                throw e;
            }
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    private void verifiedPartition(Table table, Partition partition)
    {
        Path partitionMetadataDirectory = getPartitionMetadataDirectory(table, partition.getValues());

        if (table.getTableType().equals(MANAGED_TABLE.name())) {
            if (!partitionMetadataDirectory.equals(new Path(partition.getStorage().getLocation()))) {
                throw new PrestoException(HIVE_METASTORE_ERROR, "Partition directory must be " + partitionMetadataDirectory);
            }
        }
        else if (table.getTableType().equals(EXTERNAL_TABLE.name())) {
            try {
                Path externalLocation = new Path(partition.getStorage().getLocation());
                FileSystem externalFileSystem = hdfsEnvironment.getFileSystem(metastoreUser, externalLocation);
                if (!externalFileSystem.isDirectory(externalLocation)) {
                    throw new PrestoException(HIVE_METASTORE_ERROR, "External partition location does not exist");
                }
                if (isChildDirectory(catalogDirectory, externalLocation)) {
                    throw new PrestoException(HIVE_METASTORE_ERROR, "External partition location can not be inside the system metadata directory");
                }
            }
            catch (IOException e) {
                throw new PrestoException(HIVE_METASTORE_ERROR, "Could not validate external partition location", e);
            }
        }
        else {
            throw new PrestoException(NOT_SUPPORTED, "Partitions can not be added to " + table.getTableType());
        }
    }

    @Override
    public synchronized void dropPartition(String databaseName, String tableName, List<String> partitionValues, boolean deleteData)
    {
        requireNonNull(databaseName, "databaseName is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(partitionValues, "partitionValues is null");

        Optional<Table> tableReference = getTable(databaseName, tableName);
        if (!tableReference.isPresent()) {
            return;
        }
        Table table = tableReference.get();

        Path partitionMetadataDirectory = getPartitionMetadataDirectory(table, partitionValues);
        if (deleteData) {
            deleteMetadataDirectory(partitionMetadataDirectory);
        }
        else {
            deleteSchemaFile("partition", partitionMetadataDirectory);
        }
    }

    @Override
    public synchronized void alterPartition(String databaseName, String tableName, Partition partition)
    {
        Table table = getRequiredTable(databaseName, tableName);

        verifiedPartition(table, partition);

        Path partitionMetadataDirectory = getPartitionMetadataDirectory(table, partition.getValues());
        writeSchemaFile("partition", partitionMetadataDirectory, partitionCodec, new PartitionMetadata(table, partition), true);
    }

    @Override
    public synchronized Optional<List<String>> getPartitionNames(String databaseName, String tableName)
    {
        requireNonNull(databaseName, "databaseName is null");
        requireNonNull(tableName, "tableName is null");

        Optional<Table> tableReference = getTable(databaseName, tableName);
        if (!tableReference.isPresent()) {
            return Optional.empty();
        }
        Table table = tableReference.get();

        Path tableMetadataDirectory = getTableMetadataDirectory(table);

        List<ArrayDeque<String>> partitions = listPartitions(tableMetadataDirectory, table.getPartitionColumns());

        List<String> partitionNames = partitions.stream()
                .map(partitionValues -> makePartName(table.getPartitionColumns(), ImmutableList.copyOf(partitionValues)))
                .collect(toList());

        return Optional.of(ImmutableList.copyOf(partitionNames));
    }

    private List<ArrayDeque<String>> listPartitions(Path director, List<Column> partitionColumns)
    {
        if (partitionColumns.isEmpty()) {
            return ImmutableList.of();
        }

        try {
            String directoryPrefix = partitionColumns.get(0).getName() + '=';

            List<ArrayDeque<String>> partitionValues = new ArrayList<>();
            for (FileStatus fileStatus : metadataFileSystem.listStatus(director)) {
                if (!fileStatus.isDirectory()) {
                    continue;
                }
                if (!fileStatus.getPath().getName().startsWith(directoryPrefix)) {
                    continue;
                }

                List<ArrayDeque<String>> childPartitionValues;
                if (partitionColumns.size() == 1) {
                    childPartitionValues = ImmutableList.of(new ArrayDeque<>());
                }
                else {
                    childPartitionValues = listPartitions(fileStatus.getPath(), partitionColumns.subList(1, partitionColumns.size()));
                }

                String value = fileStatus.getPath().getName().substring(directoryPrefix.length());
                for (ArrayDeque<String> childPartition : childPartitionValues) {
                    childPartition.addFirst(value);
                    partitionValues.add(childPartition);
                }
            }
            return partitionValues;
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, "Error listing partition directories", e);
        }
    }

    @Override
    public synchronized Optional<Partition> getPartition(String databaseName, String tableName, List<String> partitionValues)
    {
        requireNonNull(databaseName, "databaseName is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(partitionValues, "partitionValues is null");

        Optional<Table> tableReference = getTable(databaseName, tableName);
        if (!tableReference.isPresent()) {
            return Optional.empty();
        }
        Table table = tableReference.get();

        Path partitionDirectory = getPartitionMetadataDirectory(table, partitionValues);
        return readSchemaFile("partition", partitionDirectory, partitionCodec)
                .map(partitionMetadata -> partitionMetadata.toPartition(databaseName, tableName, partitionValues, partitionDirectory.toString()));
    }

    @Override
    public synchronized Optional<List<String>> getPartitionNamesByParts(String databaseName, String tableName, List<String> parts)
    {
        // todo this should be more efficient by selectively walking the directory tree
        return getPartitionNames(databaseName, tableName).map(partitionNames -> partitionNames.stream()
                .filter(partitionName -> partitionMatches(partitionName, parts))
                .collect(toList()));
    }

    private static boolean partitionMatches(String partitionName, List<String> parts)
    {
        List<String> values = toPartitionValues(partitionName);
        if (values.size() != parts.size()) {
            return false;
        }
        for (int i = 0; i < values.size(); i++) {
            String part = parts.get(i);
            if (!part.isEmpty() && !values.get(i).equals(part)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public synchronized Map<String, Optional<Partition>> getPartitionsByNames(String databaseName, String tableName, List<String> partitionNames)
    {
        ImmutableMap.Builder<String, Optional<Partition>> builder = ImmutableMap.builder();
        for (String partitionName : partitionNames) {
            List<String> partitionValues = toPartitionValues(partitionName);
            builder.put(partitionName, getPartition(databaseName, tableName, partitionValues));
        }
        return builder.build();
    }

    @Override
    public synchronized Set<String> getRoles(String user)
    {
        return ImmutableSet.<String>builder()
                .add(PUBLIC_ROLE_NAME)
                .add("admin")  // todo there should be a way to manage the admins list
                .build();
    }

    @Override
    public synchronized Set<HivePrivilegeInfo> getDatabasePrivileges(String user, String databaseName)
    {
        Set<HivePrivilegeInfo> privileges = new HashSet<>();
        if (isDatabaseOwner(user, databaseName)) {
            privileges.add(new HivePrivilegeInfo(OWNERSHIP, true));
        }
        return privileges;
    }

    @Override
    public synchronized Set<HivePrivilegeInfo> getTablePrivileges(String user, String databaseName, String tableName)
    {
        Table table = getRequiredTable(databaseName, tableName);

        Set<HivePrivilegeInfo> privileges = new HashSet<>();
        if (user.equals(table.getOwner())) {
            privileges.add(new HivePrivilegeInfo(OWNERSHIP, true));
        }

        Path permissionsDirectory = getPermissionsDirectory(table);
        privileges.addAll(getTablePrivileges(permissionsDirectory, user, USER));
        for (String role : getRoles(user)) {
            privileges.addAll(getTablePrivileges(permissionsDirectory, role, ROLE));
        }
        return privileges;
    }

    private synchronized Collection<HivePrivilegeInfo> getTablePrivileges(
            Path permissionsDirectory,
            String principalName,
            PrincipalType principalType)
    {
        Path permissionFilePath = getPermissionsPath(permissionsDirectory, principalName, principalType);
        return readFile("permissions", permissionFilePath, permissionsCodec).orElse(ImmutableList.of()).stream()
                .map(PermissionMetadata::toHivePrivilegeInfo)
                .collect(toList());
    }

    @Override
    public synchronized void grantTablePrivileges(String databaseName, String tableName, String grantee, Set<HivePrivilegeInfo> privileges)
    {
        setTablePrivileges(grantee, USER, databaseName, tableName, privileges);
    }

    @Override
    public synchronized void revokeTablePrivileges(String databaseName, String tableName, String grantee, Set<HivePrivilegeInfo> privileges)
    {
        Set<HivePrivilegeInfo> currentPrivileges = getTablePrivileges(grantee, databaseName, tableName);
        currentPrivileges.removeAll(privileges);

        setTablePrivileges(grantee, USER, databaseName, tableName, currentPrivileges);
    }

    private synchronized void setTablePrivileges(
            String principalName,
            PrincipalType principalType,
            String databaseName,
            String tableName,
            Collection<HivePrivilegeInfo> privileges)
    {
        requireNonNull(principalName, "principalName is null");
        requireNonNull(principalType, "principalType is null");
        requireNonNull(databaseName, "databaseName is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(privileges, "privileges is null");

        try {
            Table table = getRequiredTable(databaseName, tableName);

            Path permissionsDirectory = getPermissionsDirectory(table);

            metadataFileSystem.mkdirs(permissionsDirectory);
            if (!metadataFileSystem.isDirectory(permissionsDirectory)) {
                throw new PrestoException(HIVE_METASTORE_ERROR, "Could not create permissions directory");
            }

            Path permissionFilePath = getPermissionsPath(permissionsDirectory, principalName, principalType);
            List<PermissionMetadata> permissions = privileges.stream()
                    .map(PermissionMetadata::new)
                    .collect(toList());
            writeFile("permissions", permissionFilePath, permissionsCodec, permissions, true);
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    private synchronized void deleteTablePrivileges(Table table)
    {
        try {
            Path permissionsDirectory = getPermissionsDirectory(table);
            metadataFileSystem.delete(permissionsDirectory, true);
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, "Could not delete table permissions", e);
        }
    }

    private boolean isDatabaseOwner(String user, String databaseName)
    {
        // all users are "owners" of the default database
        if (DEFAULT_DATABASE_NAME.equalsIgnoreCase(databaseName)) {
            return true;
        }

        Optional<Database> databaseMetadata = getDatabase(databaseName);
        if (!databaseMetadata.isPresent()) {
            return false;
        }

        Database database = databaseMetadata.get();

        // a database can be owned by a user or role
        if (database.getOwnerType() == USER && user.equals(database.getOwnerName())) {
            return true;
        }
        if (database.getOwnerType() == ROLE && getRoles(user).contains(database.getOwnerName())) {
            return true;
        }
        return false;
    }

    private Path getDatabaseMetadataDirectory(String databaseName)
    {
        return new Path(catalogDirectory, databaseName);
    }

    private Path getTableMetadataDirectory(Table table)
    {
        return getTableMetadataDirectory(table.getDatabaseName(), table.getTableName());
    }

    private Path getTableMetadataDirectory(String databaseName, String tableName)
    {
        return new Path(getDatabaseMetadataDirectory(databaseName), tableName);
    }

    private Path getPartitionMetadataDirectory(Table table, List<String> values)
    {
        String partitionName = makePartName(table.getPartitionColumns(), values);
        return getPartitionMetadataDirectory(table, partitionName);
    }

    private Path getPartitionMetadataDirectory(Table table, String partitionName)
    {
        Path tableMetadataDirectory = getTableMetadataDirectory(table);
        return new Path(tableMetadataDirectory, partitionName);
    }

    private Path getPermissionsDirectory(Table table)
    {
        return new Path(getTableMetadataDirectory(table), PRESTO_PERMISSIONS_DIRECTORY_NAME);
    }

    private static Path getPermissionsPath(Path permissionsDirectory, String principalName, PrincipalType principalType)
    {
        return new Path(permissionsDirectory, principalType.name().toLowerCase(Locale.US) + "_" + principalName);
    }

    private List<Path> getChildSchemaDirectories(Path metadataDirectory)
    {
        try {
            if (!metadataFileSystem.isDirectory(metadataDirectory)) {
                return ImmutableList.of();
            }

            ImmutableList.Builder<Path> childSchemaDirectories = ImmutableList.builder();
            for (FileStatus child : metadataFileSystem.listStatus(metadataDirectory)) {
                if (!child.isDirectory()) {
                    continue;
                }
                Path childPath = child.getPath();
                if (childPath.getName().startsWith(".")) {
                    continue;
                }
                if (metadataFileSystem.isFile(new Path(childPath, PRESTO_SCHEMA_FILE_NAME))) {
                    childSchemaDirectories.add(childPath);
                }
            }
            return childSchemaDirectories.build();
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    private void deleteMetadataDirectory(Path metadataDirectory)
    {
        try {
            Path schemaPath = new Path(metadataDirectory, PRESTO_SCHEMA_FILE_NAME);
            if (!metadataFileSystem.isFile(schemaPath)) {
                // if there is no schema file, assume this is not a database, partition or table
                return;
            }

            if (!metadataFileSystem.delete(metadataDirectory, true)) {
                throw new PrestoException(HIVE_METASTORE_ERROR, "Could not delete metadata directory");
            }
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    private <T> Optional<T> readSchemaFile(String type, Path metadataDirectory, JsonCodec<T> codec)
    {
        Path schemaPath = new Path(metadataDirectory, PRESTO_SCHEMA_FILE_NAME);
        return readFile(type + " schema", schemaPath, codec);
    }

    private <T> Optional<T> readFile(String type, Path path, JsonCodec<T> codec)
    {
        try {
            if (!metadataFileSystem.isFile(path)) {
                return Optional.empty();
            }

            try (FSDataInputStream inputStream = metadataFileSystem.open(path)) {
                byte[] json = ByteStreams.toByteArray(inputStream);
                return Optional.of(codec.fromJson(json));
            }
        }
        catch (Exception e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, "Could not read " + type, e);
        }
    }

    private <T> void writeSchemaFile(String type, Path directory, JsonCodec<T> codec, T value, boolean overwrite)
    {
        Path schemaPath = new Path(directory, PRESTO_SCHEMA_FILE_NAME);
        writeFile(type + " schema", schemaPath, codec, value, overwrite);
    }

    private <T> void writeFile(String type, Path path, JsonCodec<T> codec, T value, boolean overwrite)
    {
        try {
            byte[] json = codec.toJsonBytes(value);

            if (!overwrite) {
                if (metadataFileSystem.exists(path)) {
                    throw new PrestoException(HIVE_METASTORE_ERROR, type + " file already exists");
                }
            }

            metadataFileSystem.mkdirs(path.getParent());

            // todo implement safer overwrite code
            try (OutputStream outputStream = metadataFileSystem.create(path, overwrite)) {
                outputStream.write(json);
            }
        }
        catch (Exception e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, "Could not write " + type, e);
        }
    }

    private void deleteSchemaFile(String type, Path metadataDirectory)
    {
        try {
            if (!metadataFileSystem.delete(new Path(metadataDirectory, PRESTO_SCHEMA_FILE_NAME), false)) {
                throw new PrestoException(HIVE_METASTORE_ERROR, "Could not delete " + type + " schema");
            }
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, "Could not delete " + type + " schema", e);
        }
    }

    private static boolean isChildDirectory(Path parentDirectory, Path childDirectory)
    {
        if (parentDirectory.equals(childDirectory)) {
            return true;
        }
        if (childDirectory.isRoot()) {
            return false;
        }
        return isChildDirectory(parentDirectory, childDirectory.getParent());
    }
}
