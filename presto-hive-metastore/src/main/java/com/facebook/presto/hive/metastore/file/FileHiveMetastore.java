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

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveBasicStatistics;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.hive.PartitionNotFoundException;
import com.facebook.presto.hive.SchemaAlreadyExistsException;
import com.facebook.presto.hive.TableAlreadyExistsException;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.Database;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.HiveColumnStatistics;
import com.facebook.presto.hive.metastore.HivePrivilegeInfo;
import com.facebook.presto.hive.metastore.MetastoreContext;
import com.facebook.presto.hive.metastore.MetastoreUtil;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.PartitionNameWithVersion;
import com.facebook.presto.hive.metastore.PartitionStatistics;
import com.facebook.presto.hive.metastore.PartitionWithStatistics;
import com.facebook.presto.hive.metastore.PrincipalPrivileges;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.ColumnNotFoundException;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.security.ConnectorIdentity;
import com.facebook.presto.spi.security.PrestoPrincipal;
import com.facebook.presto.spi.security.RoleGrant;
import com.facebook.presto.spi.statistics.ColumnStatisticType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.ByteStreams;
import io.airlift.units.Duration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_PARTITION_DROPPED_DURING_QUERY;
import static com.facebook.presto.hive.metastore.HivePrivilegeInfo.HivePrivilege.OWNERSHIP;
import static com.facebook.presto.hive.metastore.MetastoreUtil.convertPredicateToParts;
import static com.facebook.presto.hive.metastore.MetastoreUtil.extractPartitionValues;
import static com.facebook.presto.hive.metastore.MetastoreUtil.getHiveBasicStatistics;
import static com.facebook.presto.hive.metastore.MetastoreUtil.makePartName;
import static com.facebook.presto.hive.metastore.MetastoreUtil.toPartitionValues;
import static com.facebook.presto.hive.metastore.MetastoreUtil.updateStatisticsParameters;
import static com.facebook.presto.hive.metastore.MetastoreUtil.verifyCanDropColumn;
import static com.facebook.presto.hive.metastore.PrestoTableType.EXTERNAL_TABLE;
import static com.facebook.presto.hive.metastore.PrestoTableType.MANAGED_TABLE;
import static com.facebook.presto.hive.metastore.PrestoTableType.MATERIALIZED_VIEW;
import static com.facebook.presto.hive.metastore.PrestoTableType.TEMPORARY_TABLE;
import static com.facebook.presto.hive.metastore.PrestoTableType.VIRTUAL_VIEW;
import static com.facebook.presto.spi.StandardErrorCode.ALREADY_EXISTS;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.security.PrincipalType.ROLE;
import static com.facebook.presto.spi.security.PrincipalType.USER;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.hadoop.hive.common.FileUtils.unescapePathName;

@ThreadSafe
public class FileHiveMetastore
        implements ExtendedHiveMetastore
{
    private static final String PUBLIC_ROLE_NAME = "public";
    private static final String ADMIN_ROLE_NAME = "admin";
    private static final String PRESTO_SCHEMA_FILE_NAME = ".prestoSchema";
    private static final String PRESTO_PERMISSIONS_DIRECTORY_NAME = ".prestoPermissions";
    private static final String ICEBERG_TABLE_TYPE_NAME = "table_type";
    private static final String ICEBERG_TABLE_TYPE_VALUE = "iceberg";
    // todo there should be a way to manage the admins list
    private static final Set<String> ADMIN_USERS = ImmutableSet.of("admin", "hive", "hdfs");

    private final HdfsEnvironment hdfsEnvironment;
    private final Path catalogDirectory;
    private final HdfsContext hdfsContext;
    private final FileSystem metadataFileSystem;

    private final JsonCodec<DatabaseMetadata> databaseCodec = JsonCodec.jsonCodec(DatabaseMetadata.class);
    private final JsonCodec<TableMetadata> tableCodec = JsonCodec.jsonCodec(TableMetadata.class);
    private final JsonCodec<PartitionMetadata> partitionCodec = JsonCodec.jsonCodec(PartitionMetadata.class);
    private final JsonCodec<List<PermissionMetadata>> permissionsCodec = JsonCodec.listJsonCodec(PermissionMetadata.class);
    private final JsonCodec<List<String>> rolesCodec = JsonCodec.listJsonCodec(String.class);
    private final JsonCodec<List<RoleGrant>> roleGrantsCodec = JsonCodec.listJsonCodec(RoleGrant.class);

    @Inject
    public FileHiveMetastore(HdfsEnvironment hdfsEnvironment, FileHiveMetastoreConfig config)
    {
        this(hdfsEnvironment, config.getCatalogDirectory(), config.getMetastoreUser());
    }

    public FileHiveMetastore(HdfsEnvironment hdfsEnvironment, String catalogDirectory, String metastoreUser)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.catalogDirectory = new Path(requireNonNull(catalogDirectory, "baseDirectory is null"));
        this.hdfsContext = new HdfsContext(new ConnectorIdentity(metastoreUser, Optional.empty(), Optional.empty()));
        try {
            metadataFileSystem = hdfsEnvironment.getFileSystem(hdfsContext, this.catalogDirectory);
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public synchronized void createDatabase(MetastoreContext metastoreContext, Database database)
    {
        requireNonNull(database, "database is null");

        if (database.getLocation().isPresent()) {
            throw new PrestoException(HIVE_METASTORE_ERROR, "Database can not be created with a location set");
        }

        verifyDatabaseNotExists(metastoreContext, database.getDatabaseName());

        Path databaseMetadataDirectory = getDatabaseMetadataDirectory(database.getDatabaseName());
        writeSchemaFile("database", databaseMetadataDirectory, databaseCodec, new DatabaseMetadata(database), false);
    }

    @Override
    public synchronized void dropDatabase(MetastoreContext metastoreContext, String databaseName)
    {
        requireNonNull(databaseName, "databaseName is null");

        getRequiredDatabase(metastoreContext, databaseName);
        if (!getAllTables(metastoreContext, databaseName).orElse(ImmutableList.of()).isEmpty()) {
            throw new PrestoException(HIVE_METASTORE_ERROR, "Database " + databaseName + " is not empty");
        }

        deleteMetadataDirectory(getDatabaseMetadataDirectory(databaseName));
    }

    @Override
    public synchronized void renameDatabase(MetastoreContext metastoreContext, String databaseName, String newDatabaseName)
    {
        requireNonNull(databaseName, "databaseName is null");
        requireNonNull(newDatabaseName, "newDatabaseName is null");

        getRequiredDatabase(metastoreContext, databaseName);
        verifyDatabaseNotExists(metastoreContext, newDatabaseName);

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
    public synchronized Optional<Database> getDatabase(MetastoreContext metastoreContext, String databaseName)
    {
        requireNonNull(databaseName, "databaseName is null");

        Path databaseMetadataDirectory = getDatabaseMetadataDirectory(databaseName);
        return readSchemaFile("database", databaseMetadataDirectory, databaseCodec)
                .map(databaseMetadata -> databaseMetadata.toDatabase(databaseName, databaseMetadataDirectory.toString()));
    }

    private Database getRequiredDatabase(MetastoreContext metastoreContext, String databaseName)
    {
        return getDatabase(metastoreContext, databaseName)
                .orElseThrow(() -> new SchemaNotFoundException(databaseName));
    }

    private void verifyDatabaseNotExists(MetastoreContext metastoreContext, String databaseName)
    {
        if (getDatabase(metastoreContext, databaseName).isPresent()) {
            throw new SchemaAlreadyExistsException(databaseName);
        }
    }

    @Override
    public synchronized List<String> getAllDatabases(MetastoreContext metastoreContext)
    {
        List<String> databases = getChildSchemaDirectories(catalogDirectory).stream()
                .map(Path::getName)
                .collect(toList());
        return ImmutableList.copyOf(databases);
    }

    @Override
    public synchronized void createTable(MetastoreContext metastoreContext, Table table, PrincipalPrivileges principalPrivileges)
    {
        checkArgument(!table.getTableType().equals(TEMPORARY_TABLE), "temporary tables must never be committed to the metastore");

        verifyTableNotExists(metastoreContext, table.getDatabaseName(), table.getTableName());

        Path tableMetadataDirectory = getTableMetadataDirectory(table);

        // validate table location
        if (table.getTableType().equals(VIRTUAL_VIEW)) {
            checkArgument(table.getStorage().getLocation().isEmpty(), "Storage location for view must be empty");
        }
        else if (table.getTableType().equals(MANAGED_TABLE) || table.getTableType().equals(MATERIALIZED_VIEW)) {
            if (!tableMetadataDirectory.equals(new Path(table.getStorage().getLocation()))) {
                throw new PrestoException(HIVE_METASTORE_ERROR, "Table directory must be " + tableMetadataDirectory);
            }
        }
        else if (table.getTableType().equals(EXTERNAL_TABLE)) {
            try {
                Path externalLocation = new Path(table.getStorage().getLocation());
                FileSystem externalFileSystem = hdfsEnvironment.getFileSystem(hdfsContext, externalLocation);
                if (!externalFileSystem.isDirectory(externalLocation)) {
                    throw new PrestoException(HIVE_METASTORE_ERROR, "External table location does not exist");
                }
                if (isChildDirectory(catalogDirectory, externalLocation) && !isIcebergTable(table.getParameters())) {
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

        if (!table.getTableType().equals(VIRTUAL_VIEW)) {
            File location = new File(new Path(table.getStorage().getLocation()).toUri());
            checkArgument(location.isDirectory(), "Table location is not a directory: %s", location);
            checkArgument(location.exists(), "Table directory does not exist: %s", location);
        }

        writeSchemaFile("table", tableMetadataDirectory, tableCodec, new TableMetadata(table), false);

        for (Entry<String, Collection<HivePrivilegeInfo>> entry : principalPrivileges.getUserPrivileges().asMap().entrySet()) {
            setTablePrivileges(metastoreContext, new PrestoPrincipal(USER, entry.getKey()), table.getDatabaseName(), table.getTableName(), entry.getValue());
        }
        for (Entry<String, Collection<HivePrivilegeInfo>> entry : principalPrivileges.getRolePrivileges().asMap().entrySet()) {
            setTablePrivileges(metastoreContext, new PrestoPrincipal(ROLE, entry.getKey()), table.getDatabaseName(), table.getTableName(), entry.getValue());
        }
    }

    @Override
    public synchronized Optional<Table> getTable(MetastoreContext metastoreContext, String databaseName, String tableName)
    {
        requireNonNull(databaseName, "databaseName is null");
        requireNonNull(tableName, "tableName is null");

        Path tableMetadataDirectory = getTableMetadataDirectory(databaseName, tableName);
        return readSchemaFile("table", tableMetadataDirectory, tableCodec)
                .map(tableMetadata -> tableMetadata.toTable(databaseName, tableName, tableMetadataDirectory.toString()));
    }

    @Override
    public Set<ColumnStatisticType> getSupportedColumnStatistics(MetastoreContext metastoreContext, Type type)
    {
        return MetastoreUtil.getSupportedColumnStatistics(type);
    }

    @Override
    public synchronized PartitionStatistics getTableStatistics(MetastoreContext metastoreContext, String databaseName, String tableName)
    {
        Path tableMetadataDirectory = getTableMetadataDirectory(databaseName, tableName);
        TableMetadata tableMetadata = readSchemaFile("table", tableMetadataDirectory, tableCodec)
                .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(databaseName, tableName)));
        HiveBasicStatistics basicStatistics = getHiveBasicStatistics(tableMetadata.getParameters());
        Map<String, HiveColumnStatistics> columnStatistics = tableMetadata.getColumnStatistics();
        return new PartitionStatistics(basicStatistics, columnStatistics);
    }

    @Override
    public synchronized Map<String, PartitionStatistics> getPartitionStatistics(MetastoreContext metastoreContext, String databaseName, String tableName, Set<String> partitionNames)
    {
        Table table = getRequiredTable(metastoreContext, databaseName, tableName);
        ImmutableMap.Builder<String, PartitionStatistics> statistics = ImmutableMap.builder();
        for (String partitionName : partitionNames) {
            List<String> partitionValues = extractPartitionValues(partitionName);
            Path partitionDirectory = getPartitionMetadataDirectory(table, ImmutableList.copyOf(partitionValues));
            PartitionMetadata partitionMetadata = readSchemaFile("partition", partitionDirectory, partitionCodec)
                    .orElseThrow(() -> new PartitionNotFoundException(new SchemaTableName(databaseName, tableName), partitionValues));
            HiveBasicStatistics basicStatistics = getHiveBasicStatistics(partitionMetadata.getParameters());
            statistics.put(partitionName, new PartitionStatistics(basicStatistics, partitionMetadata.getColumnStatistics()));
        }
        return statistics.build();
    }

    private Table getRequiredTable(MetastoreContext metastoreContext, String databaseName, String tableName)
    {
        return getTable(metastoreContext, databaseName, tableName)
                .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(databaseName, tableName)));
    }

    private void verifyTableNotExists(MetastoreContext metastoreContext, String newDatabaseName, String newTableName)
    {
        if (getTable(metastoreContext, newDatabaseName, newTableName).isPresent()) {
            throw new TableAlreadyExistsException(new SchemaTableName(newDatabaseName, newTableName));
        }
    }

    @Override
    public synchronized void updateTableStatistics(MetastoreContext metastoreContext, String databaseName, String tableName, Function<PartitionStatistics, PartitionStatistics> update)
    {
        PartitionStatistics originalStatistics = getTableStatistics(metastoreContext, databaseName, tableName);
        PartitionStatistics updatedStatistics = update.apply(originalStatistics);

        Path tableMetadataDirectory = getTableMetadataDirectory(databaseName, tableName);
        TableMetadata tableMetadata = readSchemaFile("table", tableMetadataDirectory, tableCodec)
                .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(databaseName, tableName)));

        TableMetadata updatedMetadata = tableMetadata
                .withParameters(updateStatisticsParameters(tableMetadata.getParameters(), updatedStatistics.getBasicStatistics()))
                .withColumnStatistics(updatedStatistics.getColumnStatistics());

        writeSchemaFile("table", tableMetadataDirectory, tableCodec, updatedMetadata, true);
    }

    @Override
    public synchronized void updatePartitionStatistics(MetastoreContext metastoreContext, String databaseName, String tableName, String partitionName, Function<PartitionStatistics, PartitionStatistics> update)
    {
        PartitionStatistics originalStatistics = getPartitionStatistics(metastoreContext, databaseName, tableName, ImmutableSet.of(partitionName)).get(partitionName);
        if (originalStatistics == null) {
            throw new PrestoException(HIVE_PARTITION_DROPPED_DURING_QUERY, "Statistics result does not contain entry for partition: " + partitionName);
        }
        PartitionStatistics updatedStatistics = update.apply(originalStatistics);

        Table table = getRequiredTable(metastoreContext, databaseName, tableName);
        List<String> partitionValues = extractPartitionValues(partitionName);
        Path partitionDirectory = getPartitionMetadataDirectory(table, partitionValues);
        PartitionMetadata partitionMetadata = readSchemaFile("partition", partitionDirectory, partitionCodec)
                .orElseThrow(() -> new PartitionNotFoundException(new SchemaTableName(databaseName, tableName), partitionValues));

        PartitionMetadata updatedMetadata = partitionMetadata
                .withParameters(updateStatisticsParameters(partitionMetadata.getParameters(), updatedStatistics.getBasicStatistics()))
                .withColumnStatistics(updatedStatistics.getColumnStatistics());

        writeSchemaFile("partition", partitionDirectory, partitionCodec, updatedMetadata, true);
    }

    @Override
    public synchronized Optional<List<String>> getAllTables(MetastoreContext metastoreContext, String databaseName)
    {
        requireNonNull(databaseName, "databaseName is null");

        Optional<Database> database = getDatabase(metastoreContext, databaseName);
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
    public synchronized Optional<List<String>> getAllViews(MetastoreContext metastoreContext, String databaseName)
    {
        Optional<List<String>> tables = getAllTables(metastoreContext, databaseName);
        if (!tables.isPresent()) {
            return Optional.empty();
        }

        List<String> views = tables.get().stream()
                .map(tableName -> getTable(metastoreContext, databaseName, tableName))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .filter(table -> table.getTableType().equals(VIRTUAL_VIEW))
                .map(Table::getTableName)
                .collect(toList());

        return Optional.of(ImmutableList.copyOf(views));
    }

    @Override
    public synchronized void dropTable(MetastoreContext metastoreContext, String databaseName, String tableName, boolean deleteData)
    {
        requireNonNull(databaseName, "databaseName is null");
        requireNonNull(tableName, "tableName is null");

        Table table = getRequiredTable(metastoreContext, databaseName, tableName);

        Path tableMetadataDirectory = getTableMetadataDirectory(databaseName, tableName);

        // It is safe to delete the whole meta directory for external tables and views
        if ((!table.getTableType().equals(MANAGED_TABLE) && !table.getTableType().equals(MATERIALIZED_VIEW)) || deleteData) {
            deleteMetadataDirectory(tableMetadataDirectory);
        }
        else {
            // in this case we only wan to delete the metadata of a managed table
            deleteSchemaFile("table", tableMetadataDirectory);
            deleteTablePrivileges(table);
        }
    }

    @Override
    public synchronized void replaceTable(MetastoreContext metastoreContext, String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges)
    {
        checkArgument(!newTable.getTableType().equals(TEMPORARY_TABLE), "temporary tables must never be stored in the metastore");

        Table table = getRequiredTable(metastoreContext, databaseName, tableName);
        if ((!table.getTableType().equals(VIRTUAL_VIEW) || !newTable.getTableType().equals(VIRTUAL_VIEW)) && !isIcebergTable(table.getParameters())) {
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
            setTablePrivileges(metastoreContext, new PrestoPrincipal(USER, entry.getKey()), table.getDatabaseName(), table.getTableName(), entry.getValue());
        }
        for (Entry<String, Collection<HivePrivilegeInfo>> entry : principalPrivileges.getRolePrivileges().asMap().entrySet()) {
            setTablePrivileges(metastoreContext, new PrestoPrincipal(ROLE, entry.getKey()), table.getDatabaseName(), table.getTableName(), entry.getValue());
        }
    }

    @Override
    public synchronized void renameTable(MetastoreContext metastoreContext, String databaseName, String tableName, String newDatabaseName, String newTableName)
    {
        requireNonNull(databaseName, "databaseName is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(newDatabaseName, "newDatabaseName is null");
        requireNonNull(newTableName, "newTableName is null");

        Table table = getRequiredTable(metastoreContext, databaseName, tableName);
        getRequiredDatabase(metastoreContext, newDatabaseName);
        if (isIcebergTable(table.getParameters())) {
            throw new PrestoException(NOT_SUPPORTED, "Rename not supported for Iceberg tables");
        }

        // verify new table does not exist
        verifyTableNotExists(metastoreContext, newDatabaseName, newTableName);

        try {
            if (!metadataFileSystem.rename(getTableMetadataDirectory(databaseName, tableName), getTableMetadataDirectory(newDatabaseName, newTableName))) {
                throw new PrestoException(HIVE_METASTORE_ERROR, "Could not rename table directory");
            }
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    private static boolean isIcebergTable(Map<String, String> parameters)
    {
        return ICEBERG_TABLE_TYPE_VALUE.equalsIgnoreCase(parameters.get(ICEBERG_TABLE_TYPE_NAME));
    }

    @Override
    public synchronized void addColumn(MetastoreContext metastoreContext, String databaseName, String tableName, String columnName, HiveType columnType, String columnComment)
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
    public synchronized void renameColumn(MetastoreContext metastoreContext, String databaseName, String tableName, String oldColumnName, String newColumnName)
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

    @Override
    public synchronized void dropColumn(MetastoreContext metastoreContext, String databaseName, String tableName, String columnName)
    {
        alterTable(databaseName, tableName, oldTable -> {
            verifyCanDropColumn(this, metastoreContext, databaseName, tableName, columnName);
            if (!oldTable.getColumn(columnName).isPresent()) {
                SchemaTableName name = new SchemaTableName(databaseName, tableName);
                throw new ColumnNotFoundException(name, columnName);
            }

            ImmutableList.Builder<Column> newDataColumns = ImmutableList.builder();
            for (Column fieldSchema : oldTable.getDataColumns()) {
                if (!fieldSchema.getName().equals(columnName)) {
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
    public synchronized void addPartitions(MetastoreContext metastoreContext, String databaseName, String tableName, List<PartitionWithStatistics> partitions)
    {
        requireNonNull(databaseName, "databaseName is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(partitions, "partitions is null");

        Table table = getRequiredTable(metastoreContext, databaseName, tableName);

        checkArgument(EnumSet.of(MANAGED_TABLE, EXTERNAL_TABLE, MATERIALIZED_VIEW).contains(table.getTableType()), "Invalid table type: %s", table.getTableType());

        try {
            Map<Path, byte[]> schemaFiles = new LinkedHashMap<>();
            for (PartitionWithStatistics partitionWithStatistics : partitions) {
                Partition partition = partitionWithStatistics.getPartition();
                verifiedPartition(table, partition);
                Path partitionMetadataDirectory = getPartitionMetadataDirectory(table, partition.getValues());
                Path schemaPath = new Path(partitionMetadataDirectory, PRESTO_SCHEMA_FILE_NAME);
                if (metadataFileSystem.exists(schemaPath)) {
                    throw new PrestoException(HIVE_METASTORE_ERROR, "Partition already exists");
                }
                byte[] schemaJson = partitionCodec.toJsonBytes(new PartitionMetadata(table, partitionWithStatistics));
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

        if (table.getTableType().equals(MANAGED_TABLE) || table.getTableType().equals(MATERIALIZED_VIEW)) {
            if (!partitionMetadataDirectory.equals(new Path(partition.getStorage().getLocation()))) {
                throw new PrestoException(HIVE_METASTORE_ERROR, "Partition directory must be " + partitionMetadataDirectory);
            }
        }
        else if (table.getTableType().equals(EXTERNAL_TABLE)) {
            try {
                Path externalLocation = new Path(partition.getStorage().getLocation());
                FileSystem externalFileSystem = hdfsEnvironment.getFileSystem(hdfsContext, externalLocation);
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
    public synchronized void dropPartition(MetastoreContext metastoreContext, String databaseName, String tableName, List<String> partitionValues, boolean deleteData)
    {
        requireNonNull(databaseName, "databaseName is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(partitionValues, "partitionValues is null");

        Optional<Table> tableReference = getTable(metastoreContext, databaseName, tableName);
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
    public synchronized void alterPartition(MetastoreContext metastoreContext, String databaseName, String tableName, PartitionWithStatistics partitionWithStatistics)
    {
        Table table = getRequiredTable(metastoreContext, databaseName, tableName);

        Partition partition = partitionWithStatistics.getPartition();
        verifiedPartition(table, partition);

        Path partitionMetadataDirectory = getPartitionMetadataDirectory(table, partition.getValues());
        writeSchemaFile("partition", partitionMetadataDirectory, partitionCodec, new PartitionMetadata(table, partitionWithStatistics), true);
    }

    @Override
    public synchronized void createRole(MetastoreContext metastoreContext, String role, String grantor)
    {
        Set<String> roles = new HashSet<>(listRoles(metastoreContext));
        roles.add(role);
        writeFile("roles", getRolesFile(), rolesCodec, ImmutableList.copyOf(roles), true);
    }

    @Override
    public synchronized void dropRole(MetastoreContext metastoreContext, String role)
    {
        Set<String> roles = new HashSet<>(listRoles(metastoreContext));
        roles.remove(role);
        writeFile("roles", getRolesFile(), rolesCodec, ImmutableList.copyOf(roles), true);
        Set<RoleGrant> grants = listRoleGrantsSanitized(metastoreContext);
        writeRoleGrantsFile(grants);
    }

    @Override
    public synchronized Set<String> listRoles(MetastoreContext metastoreContext)
    {
        return ImmutableSet.copyOf(readFile("roles", getRolesFile(), rolesCodec).orElse(ImmutableList.of()));
    }

    @Override
    public synchronized void grantRoles(MetastoreContext metastoreContext, Set<String> roles, Set<PrestoPrincipal> grantees, boolean withAdminOption, PrestoPrincipal grantor)
    {
        Set<String> existingRoles = listRoles(metastoreContext);
        Set<RoleGrant> existingGrants = listRoleGrantsSanitized(metastoreContext);
        Set<RoleGrant> modifiedGrants = new HashSet<>(existingGrants);
        for (PrestoPrincipal grantee : grantees) {
            for (String role : roles) {
                checkArgument(existingRoles.contains(role), "Role does not exist: %s", role);
                if (grantee.getType() == ROLE) {
                    checkArgument(existingRoles.contains(grantee.getName()), "Role does not exist: %s", grantee.getName());
                }

                RoleGrant grantWithAdminOption = new RoleGrant(grantee, role, true);
                RoleGrant grantWithoutAdminOption = new RoleGrant(grantee, role, false);

                if (withAdminOption) {
                    modifiedGrants.remove(grantWithoutAdminOption);
                    modifiedGrants.add(grantWithAdminOption);
                }
                else {
                    modifiedGrants.remove(grantWithAdminOption);
                    modifiedGrants.add(grantWithoutAdminOption);
                }
            }
        }
        modifiedGrants = removeDuplicatedEntries(modifiedGrants);
        if (!existingGrants.equals(modifiedGrants)) {
            writeRoleGrantsFile(modifiedGrants);
        }
    }

    @Override
    public synchronized void revokeRoles(MetastoreContext metastoreContext, Set<String> roles, Set<PrestoPrincipal> grantees, boolean adminOptionFor, PrestoPrincipal grantor)
    {
        Set<RoleGrant> existingGrants = listRoleGrantsSanitized(metastoreContext);
        Set<RoleGrant> modifiedGrants = new HashSet<>(existingGrants);
        for (PrestoPrincipal grantee : grantees) {
            for (String role : roles) {
                RoleGrant grantWithAdminOption = new RoleGrant(grantee, role, true);
                RoleGrant grantWithoutAdminOption = new RoleGrant(grantee, role, false);

                if (modifiedGrants.contains(grantWithAdminOption) || modifiedGrants.contains(grantWithoutAdminOption)) {
                    if (adminOptionFor) {
                        modifiedGrants.remove(grantWithAdminOption);
                        modifiedGrants.add(grantWithoutAdminOption);
                    }
                    else {
                        modifiedGrants.remove(grantWithAdminOption);
                        modifiedGrants.remove(grantWithoutAdminOption);
                    }
                }
            }
        }
        modifiedGrants = removeDuplicatedEntries(modifiedGrants);
        if (!existingGrants.equals(modifiedGrants)) {
            writeRoleGrantsFile(modifiedGrants);
        }
    }

    @Override
    public synchronized Set<RoleGrant> listRoleGrants(MetastoreContext metastoreContext, PrestoPrincipal principal)
    {
        ImmutableSet.Builder<RoleGrant> result = ImmutableSet.builder();
        if (principal.getType() == USER) {
            result.add(new RoleGrant(principal, PUBLIC_ROLE_NAME, false));
            if (ADMIN_USERS.contains(principal.getName())) {
                result.add(new RoleGrant(principal, ADMIN_ROLE_NAME, true));
            }
        }
        result.addAll(listRoleGrantsSanitized(metastoreContext).stream()
                .filter(grant -> grant.getGrantee().equals(principal))
                .collect(toSet()));
        return result.build();
    }

    private synchronized Set<RoleGrant> listRoleGrantsSanitized(MetastoreContext metastoreContext)
    {
        Set<RoleGrant> grants = readRoleGrantsFile();
        Set<String> existingRoles = listRoles(metastoreContext);
        return removeDuplicatedEntries(removeNonExistingRoles(grants, existingRoles));
    }

    private Set<RoleGrant> removeDuplicatedEntries(Set<RoleGrant> grants)
    {
        Map<RoleGranteeTuple, RoleGrant> map = new HashMap<>();
        for (RoleGrant grant : grants) {
            RoleGranteeTuple tuple = new RoleGranteeTuple(grant.getRoleName(), grant.getGrantee());
            map.merge(tuple, grant, (first, second) -> first.isGrantable() ? first : second);
        }
        return ImmutableSet.copyOf(map.values());
    }

    private static Set<RoleGrant> removeNonExistingRoles(Set<RoleGrant> grants, Set<String> existingRoles)
    {
        ImmutableSet.Builder<RoleGrant> result = ImmutableSet.builder();
        for (RoleGrant grant : grants) {
            if (!existingRoles.contains(grant.getRoleName())) {
                continue;
            }
            PrestoPrincipal grantee = grant.getGrantee();
            if (grantee.getType() == ROLE && !existingRoles.contains(grantee.getName())) {
                continue;
            }
            result.add(grant);
        }
        return result.build();
    }

    private Set<RoleGrant> readRoleGrantsFile()
    {
        return ImmutableSet.copyOf(readFile("roleGrants", getRoleGrantsFile(), roleGrantsCodec).orElse(ImmutableList.of()));
    }

    private void writeRoleGrantsFile(Set<RoleGrant> roleGrants)
    {
        writeFile("roleGrants", getRoleGrantsFile(), roleGrantsCodec, ImmutableList.copyOf(roleGrants), true);
    }

    @Override
    public synchronized Optional<List<String>> getPartitionNames(MetastoreContext metastoreContext, String databaseName, String tableName)
    {
        requireNonNull(databaseName, "databaseName is null");
        requireNonNull(tableName, "tableName is null");

        Optional<Table> tableReference = getTable(metastoreContext, databaseName, tableName);
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

                String value = unescapePathName(fileStatus.getPath().getName().substring(directoryPrefix.length()));
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
    public synchronized Optional<Partition> getPartition(MetastoreContext metastoreContext, String databaseName, String tableName, List<String> partitionValues)
    {
        requireNonNull(databaseName, "databaseName is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(partitionValues, "partitionValues is null");

        Optional<Table> tableReference = getTable(metastoreContext, databaseName, tableName);
        if (!tableReference.isPresent()) {
            return Optional.empty();
        }
        Table table = tableReference.get();

        Path partitionDirectory = getPartitionMetadataDirectory(table, partitionValues);
        return readSchemaFile("partition", partitionDirectory, partitionCodec)
                .map(partitionMetadata -> partitionMetadata.toPartition(databaseName, tableName, partitionValues, partitionDirectory.toString()));
    }

    @Override
    public synchronized List<String> getPartitionNamesByFilter(
            MetastoreContext metastoreContext,
            String databaseName,
            String tableName,
            Map<Column, Domain> partitionPredicates)
    {
        List<String> parts = convertPredicateToParts(partitionPredicates);
        // todo this should be more efficient by selectively walking the directory tree
        return getPartitionNames(metastoreContext, databaseName, tableName).map(partitionNames -> partitionNames.stream()
                        .filter(partitionName -> partitionMatches(partitionName, parts))
                        .collect(toImmutableList()))
                .orElse(ImmutableList.of());
    }

    @Override
    public List<PartitionNameWithVersion> getPartitionNamesWithVersionByFilter(
            MetastoreContext metastoreContext,
            String databaseName,
            String tableName,
            Map<Column, Domain> partitionPredicates)
    {
        throw new UnsupportedOperationException();
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
    public synchronized Map<String, Optional<Partition>> getPartitionsByNames(MetastoreContext metastoreContext, String databaseName, String tableName, List<String> partitionNames)
    {
        ImmutableMap.Builder<String, Optional<Partition>> builder = ImmutableMap.builder();
        for (String partitionName : partitionNames) {
            List<String> partitionValues = toPartitionValues(partitionName);
            builder.put(partitionName, getPartition(metastoreContext, databaseName, tableName, partitionValues));
        }
        return builder.build();
    }

    @Override
    public synchronized Set<HivePrivilegeInfo> listTablePrivileges(MetastoreContext metastoreContext, String databaseName, String tableName, PrestoPrincipal principal)
    {
        ImmutableSet.Builder<HivePrivilegeInfo> result = ImmutableSet.builder();
        Table table = getRequiredTable(metastoreContext, databaseName, tableName);
        if (principal.getType() == USER && table.getOwner().equals(principal.getName())) {
            result.add(new HivePrivilegeInfo(OWNERSHIP, true, principal, principal));
        }
        Path permissionFilePath = getPermissionsPath(getPermissionsDirectory(table), principal);
        result.addAll(readFile("permissions", permissionFilePath, permissionsCodec).orElse(ImmutableList.of()).stream()
                .map(PermissionMetadata::toHivePrivilegeInfo)
                .collect(toSet()));
        return result.build();
    }

    @Override
    public synchronized void grantTablePrivileges(MetastoreContext metastoreContext, String databaseName, String tableName, PrestoPrincipal grantee, Set<HivePrivilegeInfo> privileges)
    {
        setTablePrivileges(metastoreContext, grantee, databaseName, tableName, privileges);
    }

    @Override
    public synchronized void revokeTablePrivileges(MetastoreContext metastoreContext, String databaseName, String tableName, PrestoPrincipal grantee, Set<HivePrivilegeInfo> privileges)
    {
        Set<HivePrivilegeInfo> currentPrivileges = listTablePrivileges(metastoreContext, databaseName, tableName, grantee);
        currentPrivileges.removeAll(privileges);

        setTablePrivileges(metastoreContext, grantee, databaseName, tableName, currentPrivileges);
    }

    @Override
    public synchronized void setPartitionLeases(MetastoreContext metastoreContext, String databaseName, String tableName, Map<String, String> partitionNameToLocation, Duration leaseDuration)
    {
        throw new UnsupportedOperationException("setPartitionLeases is not supported in FileHiveMetastore");
    }

    private synchronized void setTablePrivileges(
            MetastoreContext metastoreContext,
            PrestoPrincipal grantee,
            String databaseName,
            String tableName,
            Collection<HivePrivilegeInfo> privileges)
    {
        requireNonNull(grantee, "grantee is null");
        requireNonNull(databaseName, "databaseName is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(privileges, "privileges is null");

        try {
            Table table = getRequiredTable(metastoreContext, databaseName, tableName);

            Path permissionsDirectory = getPermissionsDirectory(table);

            metadataFileSystem.mkdirs(permissionsDirectory);
            if (!metadataFileSystem.isDirectory(permissionsDirectory)) {
                throw new PrestoException(HIVE_METASTORE_ERROR, "Could not create permissions directory");
            }

            Path permissionFilePath = getPermissionsPath(permissionsDirectory, grantee);
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

    private static Path getPermissionsPath(Path permissionsDirectory, PrestoPrincipal grantee)
    {
        return new Path(permissionsDirectory, grantee.getType().toString().toLowerCase(Locale.US) + "_" + grantee.getName());
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

    private Path getRolesFile()
    {
        return new Path(catalogDirectory, ".roles");
    }

    private Path getRoleGrantsFile()
    {
        return new Path(catalogDirectory, ".roleGrants");
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

    private static class RoleGranteeTuple
    {
        private final String role;
        private final PrestoPrincipal grantee;

        private RoleGranteeTuple(String role, PrestoPrincipal grantee)
        {
            this.role = requireNonNull(role, "role is null");
            this.grantee = requireNonNull(grantee, "grantee is null");
        }

        public String getRole()
        {
            return role;
        }

        public PrestoPrincipal getGrantee()
        {
            return grantee;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            RoleGranteeTuple that = (RoleGranteeTuple) o;
            return Objects.equals(role, that.role) &&
                    Objects.equals(grantee, that.grantee);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(role, grantee);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("role", role)
                    .add("grantee", grantee)
                    .toString();
        }
    }
}
