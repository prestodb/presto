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
package com.facebook.presto.hive.metastore;

import com.facebook.presto.hive.HiveType;
import com.facebook.presto.hive.PartitionNotFoundException;
import com.facebook.presto.hive.TableAlreadyExistsException;
import com.facebook.presto.spi.ColumnNotFoundException;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.hive.metastore.TableType;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static com.facebook.presto.hive.HiveUtil.toPartitionValues;
import static com.facebook.presto.hive.metastore.Database.DEFAULT_DATABASE_NAME;
import static com.facebook.presto.hive.metastore.HivePrivilegeInfo.HivePrivilege.OWNERSHIP;
import static com.facebook.presto.hive.metastore.MetastoreUtil.makePartName;
import static com.facebook.presto.hive.metastore.PrincipalType.ROLE;
import static com.facebook.presto.hive.metastore.PrincipalType.USER;
import static com.facebook.presto.spi.StandardErrorCode.ALREADY_EXISTS;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.testing.FileUtils.deleteRecursively;
import static java.util.Collections.unmodifiableList;
import static java.util.Locale.US;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.metastore.TableType.EXTERNAL_TABLE;
import static org.apache.hadoop.hive.metastore.TableType.MANAGED_TABLE;
import static org.apache.hadoop.hive.metastore.TableType.VIRTUAL_VIEW;

@ThreadSafe
public class TestingHiveMetastore
        implements ExtendedHiveMetastore
{
    private static final String PUBLIC_ROLE_NAME = "public";

    @GuardedBy("this")
    private final Map<String, Database> databases = new HashMap<>();
    @GuardedBy("this")
    private final Map<SchemaTableName, Table> relations = new HashMap<>();
    @GuardedBy("this")
    private final Map<PartitionName, Partition> partitions = new HashMap<>();

    @GuardedBy("this")
    private final Map<String, Set<String>> roleGrants = new HashMap<>();
    @GuardedBy("this")
    private final Map<PrincipalTableKey, Set<HivePrivilegeInfo>> tablePrivileges = new HashMap<>();

    private final File baseDirectory;

    public TestingHiveMetastore(File baseDirectory)
    {
        this.baseDirectory = requireNonNull(baseDirectory, "baseDirectory is null");
        checkArgument(!baseDirectory.exists(), "Base directory already exists");
        checkArgument(baseDirectory.mkdirs(), "Could not create base directory");
    }

    @Override
    public synchronized void createDatabase(Database database)
    {
        requireNonNull(database, "database is null");

        if (!database.getLocation().isPresent()) {
            File location = new File(baseDirectory, database.getDatabaseName() + ".db");
            database = Database.builder(database)
                    .setLocation(Optional.of(location.getAbsoluteFile().toURI().toString()))
                    .build();
        }
        File directory = new File(URI.create(database.getLocation().get()));

        if (directory.exists()) {
            throw new PrestoException(HIVE_METASTORE_ERROR, "Database directory already exists");
        }
        if (!isParentDir(directory, baseDirectory)) {
            throw new PrestoException(HIVE_METASTORE_ERROR, "Database directory must be inside of the metastore base directory");
        }
        if (!directory.mkdirs()) {
            throw new PrestoException(HIVE_METASTORE_ERROR, "Could not create database directory");
        }

        if (databases.putIfAbsent(database.getDatabaseName(), database) != null) {
            throw new PrestoException(ALREADY_EXISTS, "Database " + database.getDatabaseName() + " already exists");
        }
    }

    @Override
    public synchronized void dropDatabase(String databaseName)
    {
        if (relations.keySet().stream()
                .map(SchemaTableName::getSchemaName)
                .anyMatch(databaseName::equals)) {
            throw new PrestoException(HIVE_METASTORE_ERROR, "Database " + databaseName + " is not empty");
        }

        databases.remove(databaseName);
    }

    @Override
    public synchronized void renameDatabase(String databaseName, String newDatabaseName)
    {
        databases.remove(databaseName);

        relations.values().forEach(table -> renameTable(table.getDatabaseName(), table.getTableName(), newDatabaseName, table.getTableName()));

        // todo move data to new location
    }

    @Override
    public synchronized List<String> getAllDatabases()
    {
        return ImmutableList.copyOf(databases.keySet());
    }

    @Override
    public synchronized void createTable(Table table, PrincipalPrivileges principalPrivileges)
    {
        TableType tableType = TableType.valueOf(table.getTableType());
        checkArgument(EnumSet.of(MANAGED_TABLE, EXTERNAL_TABLE, VIRTUAL_VIEW).contains(tableType), "Invalid table type: %s", tableType);
        SchemaTableName schemaTableName = new SchemaTableName(table.getDatabaseName(), table.getTableName());

        if (tableType == VIRTUAL_VIEW) {
            checkArgument(table.getStorage().getLocation().isEmpty(), "Storage location for view must be empty");
        }
        else {
            File directory = new File(URI.create(table.getStorage().getLocation()));
            if (!directory.exists()) {
                throw new PrestoException(HIVE_METASTORE_ERROR, "Table directory does not exist");
            }
            if (tableType == MANAGED_TABLE && !isParentDir(directory, baseDirectory)) {
                throw new PrestoException(HIVE_METASTORE_ERROR, "Table directory must be inside of the metastore base directory");
            }
        }

        if (relations.putIfAbsent(schemaTableName, table) != null) {
            throw new TableAlreadyExistsException(schemaTableName);
        }

        for (Entry<String, Collection<HivePrivilegeInfo>> entry : principalPrivileges.getUserPrivileges().asMap().entrySet()) {
            setTablePrivileges(entry.getKey(), USER, table.getDatabaseName(), table.getTableName(), entry.getValue());
        }
        for (Entry<String, Collection<HivePrivilegeInfo>> entry : principalPrivileges.getRolePrivileges().asMap().entrySet()) {
            setTablePrivileges(entry.getKey(), ROLE, table.getDatabaseName(), table.getTableName(), entry.getValue());
        }
    }

    @Override
    public synchronized void dropTable(String databaseName, String tableName, boolean deleteData)
    {
        SchemaTableName schemaTableName = new SchemaTableName(databaseName, tableName);
        Table table = relations.remove(schemaTableName);
        if (table == null) {
            throw new TableNotFoundException(schemaTableName);
        }

        List<String> locations = listAllDataPaths(table);

        // remove partitions
        ImmutableList.copyOf(partitions.keySet()).stream()
                .filter(partitionName -> partitionName.matches(databaseName, tableName))
                .forEach(partitions::remove);

        // remove permissions
        ImmutableList.copyOf(tablePrivileges.keySet()).stream()
                .filter(key -> key.matches(databaseName, tableName))
                .forEach(tablePrivileges::remove);

        // remove data
        if (deleteData && table.getTableType().equals(MANAGED_TABLE.name())) {
            for (String location : locations) {
                File directory = new File(URI.create(location));
                checkArgument(isParentDir(directory, baseDirectory), "Table directory must be inside of the metastore base directory");
                deleteRecursively(directory);
            }
        }
    }

    private synchronized List<String> listAllDataPaths(Table table)
    {
        if (table.getTableType().equals(VIRTUAL_VIEW.name())) {
            return ImmutableList.of();
        }

        ImmutableList.Builder<String> locations = ImmutableList.builder();

        // For unpartitioned table, there should be nothing directly under this directory.
        // But including this location in the set makes the directory content assert more
        // extensive, which is desirable.
        locations.add(table.getStorage().getLocation());

        String tableStorageLocation = table.getStorage().getLocation().endsWith("/") ? table.getStorage().getLocation() : table.getStorage().getLocation() + "/";
        partitions.entrySet().stream()
                .filter(entry -> entry.getKey().matches(table.getDatabaseName(), table.getTableName()))
                .map(entry -> entry.getValue().getStorage().getLocation())
                .filter(location -> !location.startsWith(tableStorageLocation))
                .forEach(locations::add);

        return locations.build();
    }

    @Override
    public synchronized void replaceTable(String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges)
    {
        SchemaTableName schemaTableName = new SchemaTableName(databaseName, tableName);
        Table table = getRequiredTable(schemaTableName);
        checkArgument(newTable.getDatabaseName().equals(databaseName) && newTable.getTableName().equals(tableName), "Replacement table must have same name");
        checkArgument(newTable.getTableType().equals(table.getTableType()), "Replacement table must have same type");
        checkArgument(newTable.getStorage().getLocation().equals(table.getStorage().getLocation()), "Replacement table must have same location");

        // replace table
        relations.put(schemaTableName, newTable);

        // remove old permissions
        ImmutableList.copyOf(tablePrivileges.keySet()).stream()
                .filter(key -> key.matches(databaseName, tableName))
                .forEach(tablePrivileges::remove);

        // add new permissions
        for (Entry<String, Collection<HivePrivilegeInfo>> entry : principalPrivileges.getUserPrivileges().asMap().entrySet()) {
            setTablePrivileges(entry.getKey(), USER, newTable.getDatabaseName(), newTable.getTableName(), entry.getValue());
        }
        for (Entry<String, Collection<HivePrivilegeInfo>> entry : principalPrivileges.getRolePrivileges().asMap().entrySet()) {
            setTablePrivileges(entry.getKey(), ROLE, newTable.getDatabaseName(), newTable.getTableName(), entry.getValue());
        }
    }

    @Override
    public synchronized void renameTable(String databaseName, String tableName, String newDatabaseName, String newTableName)
    {
        SchemaTableName oldName = new SchemaTableName(databaseName, tableName);
        Table oldTable = getRequiredTable(oldName);

        // todo move data to new location
        Table newTable = Table.builder(oldTable)
                .setDatabaseName(newDatabaseName)
                .setTableName(newTableName)
                .build();
        SchemaTableName newName = new SchemaTableName(newDatabaseName, newTableName);
        if (relations.putIfAbsent(newName, newTable) != null) {
            throw new TableAlreadyExistsException(newName);
        }
        relations.remove(oldName);

        // rename partitions
        for (Entry<PartitionName, Partition> entry : ImmutableList.copyOf(partitions.entrySet())) {
            PartitionName partitionName = entry.getKey();
            Partition partition = entry.getValue();
            if (partitionName.matches(databaseName, tableName)) {
                partitions.remove(partitionName);
                partitions.put(
                        new PartitionName(newDatabaseName, newTableName, partitionName.getValues()),
                        Partition.builder(partition)
                                .setDatabaseName(newDatabaseName)
                                .setTableName(newTableName)
                                .build());
            }
        }

        // rename privileges
        for (Entry<PrincipalTableKey, Set<HivePrivilegeInfo>> entry : ImmutableList.copyOf(tablePrivileges.entrySet())) {
            PrincipalTableKey principalTableKey = entry.getKey();
            Set<HivePrivilegeInfo> privileges = entry.getValue();
            if (principalTableKey.matches(databaseName, tableName)) {
                tablePrivileges.remove(principalTableKey);
                tablePrivileges.put(
                        new PrincipalTableKey(principalTableKey.getPrincipalName(), principalTableKey.getPrincipalType(), newTableName, newDatabaseName),
                        privileges);
            }
        }
    }

    @Override
    public synchronized void addColumn(String databaseName, String tableName, String columnName, HiveType columnType, String columnComment)
    {
        SchemaTableName name = new SchemaTableName(databaseName, tableName);
        Table oldTable = getRequiredTable(name);

        if (oldTable.getColumn(columnName).isPresent()) {
            throw new PrestoException(ALREADY_EXISTS, "Column already exists: " + columnName);
        }

        Table newTable = Table.builder(oldTable)
                .addDataColumn(new Column(columnName, columnType, Optional.ofNullable(columnComment)))
                .build();
        relations.put(name, newTable);
    }

    @Override
    public synchronized void renameColumn(String databaseName, String tableName, String oldColumnName, String newColumnName)
    {
        SchemaTableName name = new SchemaTableName(databaseName, tableName);
        Table oldTable = getRequiredTable(name);

        if (oldTable.getColumn(newColumnName).isPresent()) {
            throw new PrestoException(ALREADY_EXISTS, "Column already exists: " + newColumnName);
        }
        if (!oldTable.getColumn(oldColumnName).isPresent()) {
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

        Table newTable = Table.builder(oldTable)
                .setDataColumns(newDataColumns.build())
                .build();
        relations.put(name, newTable);
    }

    @Override
    public synchronized Optional<List<String>> getAllTables(String databaseName)
    {
        if (!databases.containsKey(databaseName)) {
            return Optional.empty();
        }
        return Optional.of(relations.keySet().stream()
                .filter(name -> name.getSchemaName().equals(databaseName))
                .map(SchemaTableName::getTableName)
                .collect(toImmutableList()));
    }

    @Override
    public synchronized Optional<List<String>> getAllViews(String databaseName)
    {
        if (!databases.containsKey(databaseName)) {
            return Optional.empty();
        }

        List<String> views = relations.values().stream()
                .filter(table -> table.getTableType().equals(VIRTUAL_VIEW.name()))
                .map(Table::getTableName)
                .collect(toImmutableList());
        return Optional.of(views);
    }

    @Override
    public synchronized Optional<Database> getDatabase(String databaseName)
    {
        return Optional.ofNullable(databases.get(databaseName));
    }

    @Override
    public synchronized void addPartitions(String databaseName, String tableName, List<Partition> partitions)
    {
        for (Partition partition : partitions) {
            PartitionName name = new PartitionName(databaseName, tableName, partition.getValues());
            if (this.partitions.putIfAbsent(name, partition) != null) {
                throw new PrestoException(HIVE_METASTORE_ERROR, "Partition already exists");
            }
        }
    }

    @Override
    public synchronized void dropPartition(String databaseName, String tableName, List<String> parts, boolean deleteData)
    {
        Table table = getRequiredTable(new SchemaTableName(databaseName, tableName));

        Partition partition = partitions.remove(new PartitionName(databaseName, tableName, parts));
        if (partition == null) {
            throw new PartitionNotFoundException(new SchemaTableName(databaseName, tableName), parts);
        }

        if (deleteData && table.getTableType().equals(MANAGED_TABLE.name())) {
            File directory = new File(URI.create(partition.getStorage().getLocation()));
            checkArgument(isParentDir(directory, baseDirectory), "Partition directory must be inside of the metastore base directory");
            deleteRecursively(directory);
        }
    }

    @Override
    public synchronized void alterPartition(String databaseName, String tableName, Partition partition)
    {
        PartitionName partitionName = new PartitionName(databaseName, tableName, partition.getValues());
        Partition oldPartition = partitions.get(partitionName);
        if (oldPartition == null) {
            throw new PartitionNotFoundException(new SchemaTableName(databaseName, tableName), partition.getValues());
        }
        if (!oldPartition.getStorage().getLocation().equals(partition.getStorage().getLocation())) {
            throw new PrestoException(HIVE_METASTORE_ERROR, "alterPartition can not change storage location");
        }

        dropPartition(databaseName, tableName, partition.getValues(), false);
        addPartitions(databaseName, tableName, ImmutableList.of(partition));
    }

    @Override
    public synchronized Optional<List<String>> getPartitionNames(String databaseName, String tableName)
    {
        return getTable(databaseName, tableName).map(table ->
                partitions.entrySet().stream()
                        .filter(entry -> entry.getKey().matches(databaseName, tableName))
                        .map(entry -> entry.getKey().getPartitionName(table.getPartitionColumns()))
                        .collect(toImmutableList()));
    }

    @Override
    public synchronized Optional<Partition> getPartition(String databaseName, String tableName, List<String> partitionValues)
    {
        PartitionName name = new PartitionName(databaseName, tableName, partitionValues);
        return Optional.ofNullable(partitions.get(name));
    }

    @Override
    public synchronized Optional<List<String>> getPartitionNamesByParts(String databaseName, String tableName, List<String> parts)
    {
        return getTable(databaseName, tableName).map(table ->
                partitions.entrySet().stream()
                        .filter(entry -> partitionMatches(entry.getValue(), databaseName, tableName, parts))
                        .map(entry -> entry.getKey().getPartitionName(table.getPartitionColumns()))
                        .collect(toList()));
    }

    private static boolean partitionMatches(Partition partition, String databaseName, String tableName, List<String> parts)
    {
        if (!partition.getDatabaseName().equals(databaseName) ||
                !partition.getTableName().equals(tableName)) {
            return false;
        }
        List<String> values = partition.getValues();
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
        for (String name : partitionNames) {
            PartitionName partitionName = new PartitionName(databaseName, tableName, toPartitionValues(name));
            Partition partition = partitions.get(partitionName);
            builder.put(name, Optional.ofNullable(partition));
        }
        return builder.build();
    }

    @Override
    public synchronized Optional<Table> getTable(String databaseName, String tableName)
    {
        SchemaTableName schemaTableName = new SchemaTableName(databaseName, tableName);
        return Optional.ofNullable(relations.get(schemaTableName));
    }

    private synchronized Table getRequiredTable(SchemaTableName tableName)
    {
        Table oldTable = relations.get(tableName);
        if (oldTable == null) {
            throw new TableNotFoundException(tableName);
        }
        return oldTable;
    }

    @Override
    public synchronized Set<String> getRoles(String user)
    {
        return ImmutableSet.<String>builder()
                .add(PUBLIC_ROLE_NAME)
                .addAll(roleGrants.getOrDefault(user, ImmutableSet.of()))
                .build();
    }

    public synchronized void setUserRoles(String user, ImmutableSet<String> roles)
    {
        roleGrants.put(user, roles);
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
        Set<HivePrivilegeInfo> privileges = new HashSet<>();
        if (isTableOwner(user, databaseName, tableName)) {
            privileges.add(new HivePrivilegeInfo(OWNERSHIP, true));
        }
        privileges.addAll(tablePrivileges.getOrDefault(new PrincipalTableKey(user, USER, tableName, databaseName), ImmutableSet.of()));
        for (String role : getRoles(user)) {
            privileges.addAll(tablePrivileges.getOrDefault(new PrincipalTableKey(role, ROLE, tableName, databaseName), ImmutableSet.of()));
        }
        return privileges;
    }

    private synchronized void setTablePrivileges(String principalName,
            PrincipalType principalType,
            String databaseName,
            String tableName,
            Iterable<HivePrivilegeInfo> privileges)
    {
        tablePrivileges.put(new PrincipalTableKey(principalName, principalType, tableName, databaseName), ImmutableSet.copyOf(privileges));
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

    private static boolean isParentDir(File directory, File baseDirectory)
    {
        return directory.toPath().startsWith(baseDirectory.toPath());
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

    private boolean isTableOwner(String user, String databaseName, String tableName)
    {
        // a table can only be owned by a user
        Optional<Table> table = getTable(databaseName, tableName);
        return table.isPresent() && user.equals(table.get().getOwner());
    }

    private static class PartitionName
    {
        private final String schemaName;
        private final String tableName;
        private final List<String> values;

        public PartitionName(String schemaName, String tableName, List<String> values)
        {
            this.schemaName = schemaName.toLowerCase(US);
            this.tableName = tableName.toLowerCase(US);
            this.values = unmodifiableList(new ArrayList<>(values));
        }

        public List<String> getValues()
        {
            return values;
        }

        public String getPartitionName(List<Column> partitionColumns)
        {
            return makePartName(partitionColumns, values);
        }

        public boolean matches(String schemaName, String tableName)
        {
            return this.schemaName.equals(schemaName) &&
                    this.tableName.equals(tableName);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(schemaName, tableName, values);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            PartitionName other = (PartitionName) obj;
            return Objects.equals(this.schemaName, other.schemaName)
                    && Objects.equals(this.tableName, other.tableName)
                    && Objects.equals(this.values, other.values);
        }

        @Override
        public String toString()
        {
            return schemaName + "/" + tableName + "/" + values;
        }
    }

    private static class PrincipalTableKey
    {
        private final String principalName;
        private final PrincipalType principalType;
        private final String database;
        private final String table;

        public PrincipalTableKey(String principalName, PrincipalType principalType, String table, String database)
        {
            this.principalName = requireNonNull(principalName, "principalName is null");
            this.principalType = requireNonNull(principalType, "principalType is null");
            this.table = requireNonNull(table, "table is null");
            this.database = requireNonNull(database, "database is null");
        }

        public String getPrincipalName()
        {
            return principalName;
        }

        public PrincipalType getPrincipalType()
        {
            return principalType;
        }

        public boolean matches(String databaseName, String tableName)
        {
            return this.database.equals(databaseName) && this.table.equals(tableName);
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
            PrincipalTableKey that = (PrincipalTableKey) o;
            return Objects.equals(principalName, that.principalName) &&
                    Objects.equals(principalType, that.principalType) &&
                    Objects.equals(table, that.table) &&
                    Objects.equals(database, that.database);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(principalName, principalType, table, database);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("principalName", principalName)
                    .add("principalType", principalType)
                    .add("table", table)
                    .add("database", database)
                    .toString();
        }
    }
}
