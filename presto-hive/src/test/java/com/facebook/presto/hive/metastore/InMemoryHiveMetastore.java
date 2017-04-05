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

import com.facebook.presto.hive.SchemaAlreadyExistsException;
import com.facebook.presto.hive.TableAlreadyExistsException;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.apache.hadoop.hive.metastore.api.Table;

import javax.annotation.concurrent.GuardedBy;

import java.io.File;
import java.net.URI;
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
import java.util.function.Function;

import static com.facebook.presto.hive.HiveUtil.toPartitionValues;
import static com.facebook.presto.hive.metastore.HivePrivilegeInfo.HivePrivilege.OWNERSHIP;
import static com.facebook.presto.spi.StandardErrorCode.SCHEMA_NOT_EMPTY;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.testing.FileUtils.deleteRecursively;
import static java.util.Locale.US;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.metastore.TableType.EXTERNAL_TABLE;
import static org.apache.hadoop.hive.metastore.TableType.MANAGED_TABLE;
import static org.apache.hadoop.hive.metastore.TableType.VIRTUAL_VIEW;
import static org.apache.hadoop.hive.metastore.api.PrincipalType.ROLE;
import static org.apache.hadoop.hive.metastore.api.PrincipalType.USER;

public class InMemoryHiveMetastore
        implements HiveMetastore
{
    private static final String PUBLIC_ROLE_NAME = "public";

    @GuardedBy("this")
    private final Map<String, Database> databases = new HashMap<>();
    @GuardedBy("this")
    private final Map<SchemaTableName, Table> relations = new HashMap<>();
    @GuardedBy("this")
    private final Map<SchemaTableName, Table> views = new HashMap<>();
    @GuardedBy("this")
    private final Map<PartitionName, Partition> partitions = new HashMap<>();
    @GuardedBy("this")
    private final Map<String, Set<String>> roleGrants = new HashMap<>();
    @GuardedBy("this")
    private final Map<PrincipalTableKey, Set<HivePrivilegeInfo>> tablePrivileges = new HashMap<>();

    private final File baseDirectory;

    public InMemoryHiveMetastore(File baseDirectory)
    {
        this.baseDirectory = requireNonNull(baseDirectory, "baseDirectory is null");
        checkArgument(!baseDirectory.exists(), "Base directory already exists");
        checkArgument(baseDirectory.mkdirs(), "Could not create base directory");
    }

    @Override
    public synchronized void createDatabase(Database database)
    {
        requireNonNull(database, "database is null");

        File directory;
        if (database.getLocationUri() != null) {
            directory = new File(URI.create(database.getLocationUri()));
        }
        else {
            // use Hive default naming convention
            directory = new File(baseDirectory, database.getName() + ".db");
            database = database.deepCopy();
            database.setLocationUri(directory.toURI().toString());
        }

        checkArgument(!directory.exists(), "Database directory already exists");
        checkArgument(isParentDir(directory, baseDirectory), "Database directory must be inside of the metastore base directory");
        checkArgument(directory.mkdirs(), "Could not create database directory");

        if (databases.putIfAbsent(database.getName(), database) != null) {
            throw new SchemaAlreadyExistsException(database.getName());
        }
    }

    @Override
    public synchronized void dropDatabase(String databaseName)
    {
        if (!databases.containsKey(databaseName)) {
            throw new SchemaNotFoundException(databaseName);
        }
        if (!getAllTables(databaseName).orElse(ImmutableList.of()).isEmpty()) {
            throw new PrestoException(SCHEMA_NOT_EMPTY, "Schema not empty: " + databaseName);
        }
        databases.remove(databaseName);
    }

    @Override
    public synchronized void alterDatabase(String databaseName, Database newDatabase)
    {
        String newDatabaseName = newDatabase.getName();

        if (databaseName.equals(newDatabaseName)) {
            if (databases.replace(databaseName, newDatabase) == null) {
                throw new SchemaNotFoundException(databaseName);
            }
            return;
        }

        Database database = databases.get(databaseName);
        if (database == null) {
            throw new SchemaNotFoundException(databaseName);
        }
        if (databases.putIfAbsent(newDatabaseName, database) != null) {
            throw new SchemaAlreadyExistsException(newDatabaseName);
        }
        databases.remove(databaseName);

        rewriteKeys(relations, name -> new SchemaTableName(newDatabaseName, name.getTableName()));
        rewriteKeys(views, name -> new SchemaTableName(newDatabaseName, name.getTableName()));
        rewriteKeys(partitions, name -> name.withSchemaName(newDatabaseName));
        rewriteKeys(tablePrivileges, name -> name.withDatabase(newDatabaseName));
    }

    @Override
    public synchronized List<String> getAllDatabases()
    {
        return ImmutableList.copyOf(databases.keySet());
    }

    @Override
    public synchronized void createTable(Table table)
    {
        TableType tableType = TableType.valueOf(table.getTableType());
        checkArgument(EnumSet.of(MANAGED_TABLE, EXTERNAL_TABLE, VIRTUAL_VIEW).contains(tableType), "Invalid table type: %s", tableType);

        if (tableType == VIRTUAL_VIEW) {
            checkArgument(table.getSd().getLocation() == null, "Storage location for view must be null");
        }
        else {
            File directory = new File(new Path(table.getSd().getLocation()).toUri());
            checkArgument(directory.exists(), "Table directory does not exist");
            if (tableType == MANAGED_TABLE) {
                checkArgument(isParentDir(directory, baseDirectory), "Table directory must be inside of the metastore base directory");
            }
        }

        SchemaTableName schemaTableName = new SchemaTableName(table.getDbName(), table.getTableName());
        Table tableCopy = table.deepCopy();

        if (relations.putIfAbsent(schemaTableName, tableCopy) != null) {
            throw new TableAlreadyExistsException(schemaTableName);
        }

        if (tableType == VIRTUAL_VIEW) {
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
    public synchronized void dropTable(String databaseName, String tableName, boolean deleteData)
    {
        List<String> locations = listAllDataPaths(this, databaseName, tableName);

        SchemaTableName schemaTableName = new SchemaTableName(databaseName, tableName);
        Table table = relations.remove(schemaTableName);
        if (table == null) {
            throw new TableNotFoundException(schemaTableName);
        }
        views.remove(schemaTableName);
        partitions.keySet().removeIf(partitionName -> partitionName.matches(databaseName, tableName));

        // remove data
        if (deleteData && table.getTableType().equals(MANAGED_TABLE.name())) {
            for (String location : locations) {
                if (location != null) {
                    File directory = new File(new Path(location).toUri());
                    checkArgument(isParentDir(directory, baseDirectory), "Table directory must be inside of the metastore base directory");
                    deleteRecursively(directory);
                }
            }
        }
    }

    private static List<String> listAllDataPaths(HiveMetastore metastore, String schemaName, String tableName)
    {
        ImmutableList.Builder<String> locations = ImmutableList.builder();
        Table table = metastore.getTable(schemaName, tableName).get();
        if (table.getSd().getLocation() != null) {
            // For unpartitioned table, there should be nothing directly under this directory.
            // But including this location in the set makes the directory content assert more
            // extensive, which is desirable.
            locations.add(table.getSd().getLocation());
        }

        Optional<List<String>> partitionNames = metastore.getPartitionNames(schemaName, tableName);
        if (partitionNames.isPresent()) {
            metastore.getPartitionsByNames(schemaName, tableName, partitionNames.get()).stream()
                    .map(partition -> partition.getSd().getLocation())
                    .filter(location -> !location.startsWith(table.getSd().getLocation()))
                    .forEach(locations::add);
        }

        return locations.build();
    }

    @Override
    public synchronized void alterTable(String databaseName, String tableName, Table newTable)
    {
        SchemaTableName oldName = new SchemaTableName(databaseName, tableName);
        SchemaTableName newName = new SchemaTableName(newTable.getDbName(), newTable.getTableName());

        // if the name did not change, this is a simple schema change
        if (oldName.equals(newName)) {
            if (relations.replace(oldName, newTable) == null) {
                throw new TableNotFoundException(oldName);
            }
            return;
        }

        // remove old table definition and add the new one
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
    public synchronized Optional<List<String>> getAllTables(String databaseName)
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
    public synchronized Optional<List<String>> getAllViews(String databaseName)
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
    public synchronized Optional<Database> getDatabase(String databaseName)
    {
        return Optional.ofNullable(databases.get(databaseName));
    }

    @Override
    public synchronized void addPartitions(String databaseName, String tableName, List<Partition> partitions)
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
            this.partitions.put(PartitionName.partition(databaseName, tableName, partitionName), partition);
        }
    }

    private static String createPartitionName(Partition partition, Table table)
    {
        return makePartName(table.getPartitionKeys(), partition.getValues());
    }

    private static String makePartName(List<FieldSchema> partitionColumns, List<String> values)
    {
        checkArgument(partitionColumns.size() == values.size());
        List<String> partitionColumnNames = partitionColumns.stream().map(FieldSchema::getName).collect(toList());
        return FileUtils.makePartName(partitionColumnNames, values);
    }

    @Override
    public synchronized void dropPartition(String databaseName, String tableName, List<String> parts, boolean deleteData)
    {
        partitions.entrySet().removeIf(entry ->
                entry.getKey().matches(databaseName, tableName) && entry.getValue().getValues().equals(parts));
    }

    @Override
    public synchronized void alterPartition(String databaseName, String tableName, Partition partition)
    {
        Optional<Table> table = getTable(databaseName, tableName);
        if (!table.isPresent()) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        String partitionName = createPartitionName(partition, table.get());
        this.partitions.put(PartitionName.partition(databaseName, tableName, partitionName), partition);
    }

    @Override
    public synchronized Optional<List<String>> getPartitionNames(String databaseName, String tableName)
    {
        return Optional.of(ImmutableList.copyOf(partitions.entrySet().stream()
                .filter(entry -> entry.getKey().matches(databaseName, tableName))
                .map(entry -> entry.getKey().getPartitionName())
                .collect(toList())));
    }

    @Override
    public synchronized Optional<Partition> getPartition(String databaseName, String tableName, List<String> partitionValues)
    {
        PartitionName name = PartitionName.partition(databaseName, tableName, partitionValues);
        Partition partition = partitions.get(name);
        if (partition == null) {
            return Optional.empty();
        }
        return Optional.of(partition.deepCopy());
    }

    @Override
    public synchronized Optional<List<String>> getPartitionNamesByParts(String databaseName, String tableName, List<String> parts)
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
    public synchronized List<Partition> getPartitionsByNames(String databaseName, String tableName, List<String> partitionNames)
    {
        ImmutableList.Builder<Partition> builder = ImmutableList.builder();
        for (String name : partitionNames) {
            PartitionName partitionName = PartitionName.partition(databaseName, tableName, name);
            Partition partition = partitions.get(partitionName);
            if (partition == null) {
                return ImmutableList.of();
            }
            builder.add(partition.deepCopy());
        }
        return builder.build();
    }

    @Override
    public synchronized Optional<Table> getTable(String databaseName, String tableName)
    {
        SchemaTableName schemaTableName = new SchemaTableName(databaseName, tableName);
        return Optional.ofNullable(relations.get(schemaTableName));
    }

    @Override
    public synchronized Set<String> getRoles(String user)
    {
        return roleGrants.getOrDefault(user, ImmutableSet.of(PUBLIC_ROLE_NAME));
    }

    public synchronized void setUserRoles(String user, Set<String> roles)
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

    public synchronized void setTablePrivileges(String principalName,
            PrincipalType principalType,
            String databaseName,
            String tableName,
            Set<HivePrivilegeInfo> privileges)
    {
        tablePrivileges.put(new PrincipalTableKey(principalName, principalType, tableName, databaseName), ImmutableSet.copyOf(privileges));
    }

    @Override
    public synchronized void grantTablePrivileges(String databaseName, String tableName, String grantee, Set<PrivilegeGrantInfo> privilegeGrantInfoSet)
    {
        Set<HivePrivilegeInfo> hivePrivileges = privilegeGrantInfoSet.stream()
                .map(HivePrivilegeInfo::parsePrivilege)
                .flatMap(Collection::stream)
                .collect(toImmutableSet());

        setTablePrivileges(grantee, USER, databaseName, tableName, hivePrivileges);
    }

    @Override
    public synchronized void revokeTablePrivileges(String databaseName, String tableName, String grantee, Set<PrivilegeGrantInfo> privilegeGrantInfoSet)
    {
        Set<HivePrivilegeInfo> currentPrivileges = getTablePrivileges(grantee, databaseName, tableName);
        currentPrivileges.removeAll(privilegeGrantInfoSet.stream()
                .map(HivePrivilegeInfo::parsePrivilege)
                .flatMap(Collection::stream)
                .collect(toImmutableSet()));

        setTablePrivileges(grantee, USER, databaseName, tableName, currentPrivileges);
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
        private final List<String> partitionValues;
        private final String partitionName; // does not participate in equals and hashValue

        private PartitionName(String schemaName, String tableName, List<String> partitionValues, String partitionName)
        {
            this.schemaName = requireNonNull(schemaName, "schemaName is null").toLowerCase(US);
            this.tableName = requireNonNull(tableName, "tableName is null").toLowerCase(US);
            this.partitionValues = requireNonNull(partitionValues, "partitionValues is null");
            this.partitionName = partitionName;
        }

        public static PartitionName partition(String schemaName, String tableName, String partitionName)
        {
            return new PartitionName(schemaName.toLowerCase(US), tableName.toLowerCase(US), toPartitionValues(partitionName), partitionName);
        }

        public static PartitionName partition(String schemaName, String tableName, List<String> partitionValues)
        {
            return new PartitionName(schemaName.toLowerCase(US), tableName.toLowerCase(US), partitionValues, null);
        }

        public String getPartitionName()
        {
            return requireNonNull(partitionName, "partitionName is null");
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

        public PartitionName withSchemaName(String schemaName)
        {
            return new PartitionName(schemaName, tableName, partitionValues, partitionName);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(schemaName, tableName, partitionValues);
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
                    && Objects.equals(this.partitionValues, other.partitionValues);
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

        public PrincipalTableKey withDatabase(String database)
        {
            return new PrincipalTableKey(principalName, principalType, table, database);
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

    private static <K, V> void rewriteKeys(Map<K, V> map, Function<K, K> keyRewriter)
    {
        for (K key : ImmutableSet.copyOf(map.keySet())) {
            K newKey = keyRewriter.apply(key);
            if (!newKey.equals(key)) {
                map.put(newKey, map.remove(key));
            }
        }
    }
}
