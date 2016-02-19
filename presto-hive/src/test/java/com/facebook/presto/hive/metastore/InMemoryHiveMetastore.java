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

import com.facebook.presto.hive.TableAlreadyExistsException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

import java.io.File;
import java.net.URI;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.facebook.presto.hive.AbstractTestHiveClient.listAllDataPaths;
import static com.facebook.presto.hive.HiveUtil.createPartitionName;
import static com.facebook.presto.hive.metastore.HivePrivilegeInfo.HivePrivilege.OWNERSHIP;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableSet;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.testing.FileUtils.deleteRecursively;
import static java.util.Locale.US;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.metastore.api.PrincipalType.ROLE;
import static org.apache.hadoop.hive.metastore.api.PrincipalType.USER;

public class InMemoryHiveMetastore
        implements HiveMetastore
{
    private static final String PUBLIC_ROLE_NAME = "public";
    private final ConcurrentHashMap<String, Database> databases = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<SchemaTableName, Table> relations = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<SchemaTableName, Table> views = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<PartitionName, Partition> partitions = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<String, Set<String>> roleGrants = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<PrincipalTableKey, Set<HivePrivilegeInfo>> tablePrivileges = new ConcurrentHashMap<>();

    private final File baseDirectory;

    public InMemoryHiveMetastore(File baseDirectory)
    {
        this.baseDirectory = requireNonNull(baseDirectory, "baseDirectory is null");
        checkArgument(!baseDirectory.exists(), "Base directory already exists");
        checkArgument(baseDirectory.mkdirs(), "Could not create base directory");
    }

    public void createDatabase(Database database)
    {
        requireNonNull(database, "database is null");

        File directory = new File(URI.create(database.getLocationUri()));
        checkArgument(!directory.exists(), "Database directory already exists");
        checkArgument(isParentDir(directory, baseDirectory), "Database directory must be inside of the metastore base directory");
        checkArgument(directory.mkdirs(), "Could not create database directory");

        if (databases.putIfAbsent(database.getName(), database) != null) {
            throw new IllegalArgumentException("Database " + database.getName() + " already exists");
        }
    }

    @Override
    public List<String> getAllDatabases()
    {
        return ImmutableList.copyOf(databases.keySet());
    }

    @Override
    public void createTable(Table table)
    {
        SchemaTableName schemaTableName = new SchemaTableName(table.getDbName(), table.getTableName());
        Table tableCopy = table.deepCopy();
        if (tableCopy.getSd() == null) {
            tableCopy.setSd(new StorageDescriptor());
        }
        else if (tableCopy.getSd().getLocation() != null) {
            File directory = new File(URI.create(tableCopy.getSd().getLocation()));
            checkArgument(directory.exists(), "Table directory does not exist");
            checkArgument(isParentDir(directory, baseDirectory), "Table directory must be inside of the metastore base directory");
        }

        if (relations.putIfAbsent(schemaTableName, tableCopy) != null) {
            throw new TableAlreadyExistsException(schemaTableName);
        }

        if (tableCopy.getTableType().equals(TableType.VIRTUAL_VIEW.name())) {
            views.put(schemaTableName, tableCopy);
        }

        PrincipalPrivilegeSet privileges = table.getPrivileges();
        if (privileges != null) {
            for (Entry<String, List<PrivilegeGrantInfo>> entry : privileges.getUserPrivileges().entrySet()) {
                String user = entry.getKey();
                Set<HivePrivilegeInfo> userPrivileges = entry.getValue().stream()
                        .map(HivePrivilegeInfo::parsePrivilege)
                        .flatMap(Collection::stream)
                        .collect(toImmutableSet());
                setTablePrivileges(user, USER, table.getDbName(), table.getTableName(), userPrivileges);
            }
            for (Entry<String, List<PrivilegeGrantInfo>> entry : privileges.getRolePrivileges().entrySet()) {
                String role = entry.getKey();
                Set<HivePrivilegeInfo> rolePrivileges = entry.getValue().stream()
                        .map(HivePrivilegeInfo::parsePrivilege)
                        .flatMap(Collection::stream)
                        .collect(toImmutableSet());
                setTablePrivileges(role, ROLE, table.getDbName(), table.getTableName(), rolePrivileges);
            }
        }
    }

    @Override
    public void dropTable(String databaseName, String tableName)
    {
        List<String> locations = listAllDataPaths(this, databaseName, tableName);

        SchemaTableName schemaTableName = new SchemaTableName(databaseName, tableName);
        Table table = relations.remove(schemaTableName);
        if (table == null) {
            throw new TableNotFoundException(schemaTableName);
        }
        views.remove(schemaTableName);
        partitions.keySet().stream()
                .filter(partitionName -> partitionName.matches(databaseName, tableName))
                .forEach(partitions::remove);

        // remove data
        for (String location : locations) {
            if (location != null) {
                File directory = new File(URI.create(location));
                checkArgument(isParentDir(directory, baseDirectory), "Table directory must be inside of the metastore base directory");
                deleteRecursively(directory);
            }
        }
    }

    @Override
    public void alterTable(String databaseName, String tableName, Table newTable)
    {
        SchemaTableName oldName = new SchemaTableName(databaseName, tableName);
        SchemaTableName newName = new SchemaTableName(newTable.getDbName(), newTable.getTableName());

        // if the name did not change, this is a simple schema change
        if (oldName.equals(newName)) {
            if (relations.replace(oldName, newTable) != null) {
                throw new TableNotFoundException(oldName);
            }
            return;
        }

        // remove old table definition and add the new one
        // TODO: use locking to do this properly
        Table table = relations.get(oldName);
        if (table == null) {
            throw new TableNotFoundException(oldName);
        }

        if (relations.putIfAbsent(newName, newTable) != null) {
            throw new TableAlreadyExistsException(newName);
        }
        relations.remove(oldName);
    }

    @Override
    public Optional<List<String>> getAllTables(String databaseName)
    {
        ImmutableList.Builder<String> tables = ImmutableList.builder();
        for (SchemaTableName schemaTableName : this.relations.keySet()) {
            if (schemaTableName.getSchemaName().equals(databaseName)) {
                tables.add(schemaTableName.getTableName());
            }
        }
        return Optional.of(tables.build());
    }

    @Override
    public Optional<List<String>> getAllViews(String databaseName)
    {
        ImmutableList.Builder<String> tables = ImmutableList.builder();
        for (SchemaTableName schemaTableName : this.views.keySet()) {
            if (schemaTableName.getSchemaName().equals(databaseName)) {
                tables.add(schemaTableName.getTableName());
            }
        }
        return Optional.of(tables.build());
    }

    @Override
    public Optional<Database> getDatabase(String databaseName)
    {
        return Optional.ofNullable(databases.get(databaseName));
    }

    @Override
    public void addPartitions(String databaseName, String tableName, List<Partition> partitions)
    {
        Optional<Table> table = getTable(databaseName, tableName);
        if (!table.isPresent()) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        for (Partition partition : partitions) {
            String partitionName = createPartitionName(partition, table.get());
            partition = partition.deepCopy();
            if (partition.getParameters() == null) {
                partition.setParameters(ImmutableMap.of());
            }
            this.partitions.put(new PartitionName(databaseName, tableName, partitionName), partition);
        }
    }

    @Override
    public void dropPartition(String databaseName, String tableName, List<String> parts)
    {
        for (Entry<PartitionName, Partition> entry : partitions.entrySet()) {
            PartitionName partitionName = entry.getKey();
            Partition partition = entry.getValue();
            if (partitionName.matches(databaseName, tableName) && partition.getValues().equals(parts)) {
                partitions.remove(partitionName);
            }
        }
    }

    @Override
    public void dropPartitionByName(String databaseName, String tableName, String partitionName)
    {
        for (PartitionName partition : partitions.keySet()) {
            if (partition.matches(databaseName, tableName, partitionName)) {
                partitions.remove(partition);
            }
        }
    }

    @Override
    public Optional<List<String>> getPartitionNames(String databaseName, String tableName)
    {
        return Optional.of(ImmutableList.copyOf(partitions.entrySet().stream()
                .filter(entry -> entry.getKey().matches(databaseName, tableName))
                .map(entry -> entry.getKey().getPartitionName())
                .collect(toList())));
    }

    @Override
    public Optional<Partition> getPartition(String databaseName, String tableName, String partitionName)
    {
        PartitionName name = new PartitionName(databaseName, tableName, partitionName);
        Partition partition = partitions.get(name);
        if (partition == null) {
            return Optional.empty();
        }
        return Optional.of(partition.deepCopy());
    }

    @Override
    public Optional<List<String>> getPartitionNamesByParts(String databaseName, String tableName, List<String> parts)
    {
        return Optional.of(partitions.entrySet().stream()
                .filter(entry -> partitionMatches(entry.getValue(), databaseName, tableName, parts))
                .map(entry -> entry.getKey().getPartitionName())
                .collect(toList()));
    }

    private static boolean partitionMatches(Partition partition, String databaseName, String tableName, List<String> parts)
    {
        if (!partition.getDbName().equals(databaseName) ||
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
    public Optional<Map<String, Partition>> getPartitionsByNames(String databaseName, String tableName, List<String> partitionNames)
    {
        ImmutableMap.Builder<String, Partition> builder = ImmutableMap.builder();
        for (String name : partitionNames) {
            PartitionName partitionName = new PartitionName(databaseName, tableName, name);
            Partition partition = partitions.get(partitionName);
            if (partition == null) {
                return Optional.empty();
            }
            builder.put(name, partition.deepCopy());
        }
        return Optional.of(builder.build());
    }

    @Override
    public Optional<Table> getTable(String databaseName, String tableName)
    {
        SchemaTableName schemaTableName = new SchemaTableName(databaseName, tableName);
        return Optional.ofNullable(relations.get(schemaTableName));
    }

    @Override
    public Set<String> getRoles(String user)
    {
        return roleGrants.getOrDefault(user, ImmutableSet.of(PUBLIC_ROLE_NAME));
    }

    public void setUserRoles(String user, Set<String> roles)
    {
        if (!roles.contains(PUBLIC_ROLE_NAME)) {
            roles = ImmutableSet.<String>builder()
                    .addAll(roles)
                    .add(PUBLIC_ROLE_NAME)
                    .build();
        }
        roleGrants.put(user, ImmutableSet.copyOf(roles));
    }

    @Override
    public Set<HivePrivilegeInfo> getDatabasePrivileges(String user, String databaseName)
    {
        Set<HivePrivilegeInfo> privileges = new HashSet<>();
        if (isDatabaseOwner(user, databaseName)) {
            privileges.add(new HivePrivilegeInfo(OWNERSHIP, true));
        }
        return privileges;
    }

    @Override
    public Set<HivePrivilegeInfo> getTablePrivileges(String user, String databaseName, String tableName)
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

    public void setTablePrivileges(String principalName,
            PrincipalType principalType,
            String databaseName,
            String tableName,
            Set<HivePrivilegeInfo> privileges)
    {
        tablePrivileges.put(new PrincipalTableKey(principalName, principalType, tableName, databaseName), ImmutableSet.copyOf(privileges));
    }

    @Override
    public void flushCache()
    {
    }

    @Override
    public void grantTablePrivileges(String databaseName, String tableName, String grantee, Set<PrivilegeGrantInfo> privilegeGrantInfoSet)
    {
        Set<HivePrivilegeInfo> hivePrivileges = privilegeGrantInfoSet.stream()
                .map(HivePrivilegeInfo::parsePrivilege)
                .flatMap(Collection::stream)
                .collect(toImmutableSet());

        setTablePrivileges(grantee, USER, databaseName, tableName, hivePrivileges);
    }

    private static boolean isParentDir(File directory, File baseDirectory)
    {
        for (File parent = directory.getParentFile(); parent != null; parent = parent.getParentFile()) {
            if (parent.equals(baseDirectory)) {
                return true;
            }
        }
        return false;
    }

    private static class PartitionName
    {
        private final String schemaName;
        private final String tableName;
        private final String partitionName;

        public PartitionName(String schemaName, String tableName, String partitionName)
        {
            this.schemaName = schemaName.toLowerCase(US);
            this.tableName = tableName.toLowerCase(US);
            this.partitionName = partitionName;
        }

        public String getPartitionName()
        {
            return partitionName;
        }

        public boolean matches(String schemaName, String tableName)
        {
            return this.schemaName.equals(schemaName) &&
                    this.tableName.equals(tableName);
        }

        public boolean matches(String schemaName, String tableName, String partitionName)
        {
            return this.schemaName.equals(schemaName) &&
                    this.tableName.equals(tableName) &&
                    this.partitionName.equals(partitionName);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(schemaName, tableName, partitionName);
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
                    && Objects.equals(this.partitionName, other.partitionName);
        }

        @Override
        public String toString()
        {
            return schemaName + "/" + tableName + "/" + partitionName;
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
            return MoreObjects.toStringHelper(this)
                    .add("principalName", principalName)
                    .add("principalType", principalType)
                    .add("table", table)
                    .add("database", database)
                    .toString();
        }
    }
}
