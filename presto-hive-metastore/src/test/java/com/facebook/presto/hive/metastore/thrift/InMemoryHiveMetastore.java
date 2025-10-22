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
package com.facebook.presto.hive.metastore.thrift;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.hive.HiveTableHandle;
import com.facebook.presto.hive.SchemaAlreadyExistsException;
import com.facebook.presto.hive.TableAlreadyExistsException;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.HivePrivilegeInfo;
import com.facebook.presto.hive.metastore.MetastoreContext;
import com.facebook.presto.hive.metastore.MetastoreOperationResult;
import com.facebook.presto.hive.metastore.MetastoreUtil;
import com.facebook.presto.hive.metastore.PartitionStatistics;
import com.facebook.presto.hive.metastore.PartitionWithStatistics;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.constraints.TableConstraint;
import com.facebook.presto.spi.security.PrestoPrincipal;
import com.facebook.presto.spi.security.RoleGrant;
import com.facebook.presto.spi.statistics.ColumnStatisticType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.Table;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.facebook.presto.hive.HiveBasicStatistics.createEmptyStatistics;
import static com.facebook.presto.hive.metastore.MetastoreOperationResult.EMPTY_RESULT;
import static com.facebook.presto.hive.metastore.MetastoreUtil.convertPredicateToParts;
import static com.facebook.presto.hive.metastore.MetastoreUtil.toPartitionValues;
import static com.facebook.presto.hive.metastore.thrift.ThriftMetastoreUtil.toMetastoreApiPartition;
import static com.facebook.presto.spi.StandardErrorCode.SCHEMA_NOT_EMPTY;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.util.Locale.US;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.metastore.TableType.EXTERNAL_TABLE;
import static org.apache.hadoop.hive.metastore.TableType.MANAGED_TABLE;
import static org.apache.hadoop.hive.metastore.TableType.VIRTUAL_VIEW;

public class InMemoryHiveMetastore
        implements HiveMetastore
{
    @GuardedBy("this")
    private final Map<String, Database> databases = new HashMap<>();
    @GuardedBy("this")
    private final Map<SchemaTableName, Table> relations = new HashMap<>();
    @GuardedBy("this")
    private final Map<SchemaTableName, Table> views = new HashMap<>();
    @GuardedBy("this")
    private final Map<PartitionName, com.facebook.presto.hive.metastore.Partition> partitions = new HashMap<>();
    @GuardedBy("this")
    private final Map<SchemaTableName, PartitionStatistics> columnStatistics = new HashMap<>();
    @GuardedBy("this")
    private final Map<PartitionName, PartitionStatistics> partitionColumnStatistics = new HashMap<>();
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
    public synchronized void createDatabase(MetastoreContext metastoreContext, Database database)
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
        checkArgument(isAncestor(directory, baseDirectory), "Database directory %s must be inside of the metastore base directory %s", directory, baseDirectory);
        checkArgument(directory.mkdirs(), "Could not create database directory");

        if (databases.putIfAbsent(database.getName(), database) != null) {
            throw new SchemaAlreadyExistsException(database.getName());
        }
    }

    @Override
    public synchronized void dropDatabase(MetastoreContext metastoreContext, String databaseName)
    {
        if (!databases.containsKey(databaseName)) {
            throw new SchemaNotFoundException(databaseName);
        }
        if (!getAllTables(metastoreContext, databaseName).orElse(ImmutableList.of()).isEmpty()) {
            throw new PrestoException(SCHEMA_NOT_EMPTY, "Schema not empty: " + databaseName);
        }
        databases.remove(databaseName);
    }

    @Override
    public synchronized void alterDatabase(MetastoreContext metastoreContext, String databaseName, Database newDatabase)
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
    public synchronized List<String> getAllDatabases(MetastoreContext metastoreContext)
    {
        return ImmutableList.copyOf(databases.keySet());
    }

    @Override
    public synchronized MetastoreOperationResult createTable(MetastoreContext metastoreContext, Table table, List<TableConstraint<String>> constraints)
    {
        TableType tableType = TableType.valueOf(table.getTableType());
        checkArgument(EnumSet.of(MANAGED_TABLE, EXTERNAL_TABLE, VIRTUAL_VIEW).contains(tableType), "Invalid table type: %s", tableType);

        if (tableType == VIRTUAL_VIEW) {
            checkArgument(table.getSd().getLocation() == null, "Storage location for view must be null");
        }
        else {
            File directory = new File(new Path(table.getSd().getLocation()).toUri());
            checkArgument(directory.exists(), "Table directory does not exist: %s", directory);
            if (tableType == MANAGED_TABLE) {
                checkArgument(isAncestor(directory, baseDirectory), "Table directory %s must be inside of the metastore base directory %s", directory, baseDirectory);
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
            throw new UnsupportedOperationException();
        }

        return EMPTY_RESULT;
    }

    @Override
    public synchronized void dropTable(MetastoreContext metastoreContext, String databaseName, String tableName, boolean deleteData)
    {
        List<String> locations = listAllDataPaths(metastoreContext, this, databaseName, tableName);

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
                    checkArgument(isAncestor(directory, baseDirectory), "Table directory %s must be inside of the metastore base directory %s", directory, baseDirectory);
                    deleteDirectory(directory);
                }
            }
        }
    }

    private static List<String> listAllDataPaths(MetastoreContext metastoreContext, HiveMetastore metastore, String schemaName, String tableName)
    {
        ImmutableList.Builder<String> locations = ImmutableList.builder();
        Table table = metastore.getTable(metastoreContext, schemaName, tableName).get();
        if (table.getSd().getLocation() != null) {
            // For unpartitioned table, there should be nothing directly under this directory.
            // But including this location in the set makes the directory content assert more
            // extensive, which is desirable.
            locations.add(table.getSd().getLocation());
        }

        Optional<List<String>> partitionNames = metastore.getPartitionNames(metastoreContext, schemaName, tableName);
        if (partitionNames.isPresent()) {
            metastore.getPartitionsByNames(metastoreContext, schemaName, tableName, partitionNames.get()).stream()
                    .map(partition -> partition.getSd().getLocation())
                    .filter(location -> !location.startsWith(table.getSd().getLocation()))
                    .forEach(locations::add);
        }

        return locations.build();
    }

    @Override
    public synchronized MetastoreOperationResult alterTable(MetastoreContext metastoreContext, String databaseName, String tableName, Table newTable)
    {
        SchemaTableName oldName = new SchemaTableName(databaseName, tableName);
        SchemaTableName newName = new SchemaTableName(newTable.getDbName(), newTable.getTableName());

        // if the name did not change, this is a simple schema change
        if (oldName.equals(newName)) {
            if (relations.replace(oldName, newTable) == null) {
                throw new TableNotFoundException(oldName);
            }
            return EMPTY_RESULT;
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

        return EMPTY_RESULT;
    }

    @Override
    public synchronized MetastoreOperationResult alterTableWithEnvironmentContext(MetastoreContext metastoreContext, String databaseName, String tableName, Table newTable, EnvironmentContext environmentContext)
    {
        return alterTable(metastoreContext, databaseName, tableName, newTable);
    }

    @Override
    public synchronized Optional<List<String>> getAllTables(MetastoreContext metastoreContext, String databaseName)
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
    public synchronized Optional<List<String>> getAllViews(MetastoreContext metastoreContext, String databaseName)
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
    public synchronized Optional<Database> getDatabase(MetastoreContext metastoreContext, String databaseName)
    {
        return Optional.ofNullable(databases.get(databaseName));
    }

    @Override
    public synchronized MetastoreOperationResult addPartitions(MetastoreContext metastoreContext, String databaseName, String tableName, List<PartitionWithStatistics> partitionsWithStatistics)
    {
        for (PartitionWithStatistics partitionWithStatistics : partitionsWithStatistics) {
            PartitionName partitionKey = PartitionName.partition(databaseName, tableName, partitionWithStatistics.getPartitionName());
            partitions.put(partitionKey, partitionWithStatistics.getPartition());
            partitionColumnStatistics.put(partitionKey, partitionWithStatistics.getStatistics());
        }

        return EMPTY_RESULT;
    }

    @Override
    public synchronized void dropPartition(MetastoreContext metastoreContext, String databaseName, String tableName, List<String> parts, boolean deleteData)
    {
        partitions.entrySet().removeIf(entry ->
                entry.getKey().matches(databaseName, tableName) && entry.getValue().getValues().equals(parts));
    }

    @Override
    public synchronized MetastoreOperationResult alterPartition(MetastoreContext metastoreContext, String databaseName, String tableName, PartitionWithStatistics partitionWithStatistics)
    {
        PartitionName partitionKey = PartitionName.partition(databaseName, tableName, partitionWithStatistics.getPartitionName());
        partitions.put(partitionKey, partitionWithStatistics.getPartition());
        partitionColumnStatistics.put(partitionKey, partitionWithStatistics.getStatistics());

        return EMPTY_RESULT;
    }

    @Override
    public synchronized Optional<List<String>> getPartitionNames(MetastoreContext metastoreContext, String databaseName, String tableName)
    {
        return Optional.of(ImmutableList.copyOf(partitions.entrySet().stream()
                .filter(entry -> entry.getKey().matches(databaseName, tableName))
                .map(entry -> entry.getKey().getPartitionName())
                .collect(toList())));
    }

    @Override
    public synchronized Optional<Partition> getPartition(MetastoreContext metastoreContext, String databaseName, String tableName, List<String> partitionValues)
    {
        PartitionName name = PartitionName.partition(databaseName, tableName, partitionValues);
        Partition partition = getPartitionFromInMemoryMap(metastoreContext, name);
        if (partition == null) {
            return Optional.empty();
        }
        return Optional.of(partition.deepCopy());
    }

    @Override
    public synchronized Optional<List<String>> getPartitionNamesByParts(MetastoreContext metastoreContext, String databaseName, String tableName, List<String> parts)
    {
        return Optional.of(partitions.entrySet().stream()
                .filter(entry -> partitionMatches(getPartitionFromInMemoryMap(metastoreContext, entry.getKey()), databaseName, tableName, parts))
                .map(entry -> entry.getKey().getPartitionName())
                .collect(toList()));
    }

    @Override
    public List<String> getPartitionNamesByFilter(MetastoreContext metastoreContext, String databaseName, String tableName, Map<Column, Domain> partitionPredicates)
    {
        List<String> parts = convertPredicateToParts(partitionPredicates);
        return getPartitionNamesByParts(metastoreContext, databaseName, tableName, parts).orElse(ImmutableList.of());
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
    public synchronized List<Partition> getPartitionsByNames(MetastoreContext metastoreContext, String databaseName, String tableName, List<String> partitionNames)
    {
        ImmutableList.Builder<Partition> builder = ImmutableList.builder();
        for (String name : partitionNames) {
            PartitionName partitionName = PartitionName.partition(databaseName, tableName, name);
            Partition partition = getPartitionFromInMemoryMap(metastoreContext, partitionName);
            if (partition == null) {
                return ImmutableList.of();
            }
            builder.add(partition.deepCopy());
        }
        return builder.build();
    }

    @Override
    public synchronized Optional<Table> getTable(MetastoreContext metastoreContext, String databaseName, String tableName)
    {
        HiveTableHandle hiveTableHandle = new HiveTableHandle(databaseName, tableName);
        return getTable(metastoreContext, hiveTableHandle);
    }

    @Override
    public Optional<Table> getTable(MetastoreContext metastoreContext, HiveTableHandle hiveTableHandle)
    {
        return Optional.ofNullable(relations.get(hiveTableHandle.getSchemaTableName()));
    }

    @Override
    public Set<ColumnStatisticType> getSupportedColumnStatistics(MetastoreContext metastoreContext, Type type)
    {
        return MetastoreUtil.getSupportedColumnStatistics(type);
    }

    @Override
    public synchronized PartitionStatistics getTableStatistics(MetastoreContext metastoreContext, String databaseName, String tableName)
    {
        SchemaTableName schemaTableName = new SchemaTableName(databaseName, tableName);
        PartitionStatistics statistics = columnStatistics.get(schemaTableName);
        if (statistics == null) {
            statistics = new PartitionStatistics(createEmptyStatistics(), ImmutableMap.of());
        }
        return statistics;
    }

    @Override
    public synchronized Map<String, PartitionStatistics> getPartitionStatistics(MetastoreContext metastoreContext, String databaseName, String tableName, Set<String> partitionNames)
    {
        ImmutableMap.Builder<String, PartitionStatistics> result = ImmutableMap.builder();
        for (String partitionName : partitionNames) {
            PartitionName partitionKey = PartitionName.partition(databaseName, tableName, partitionName);
            PartitionStatistics statistics = partitionColumnStatistics.get(partitionKey);
            if (statistics == null) {
                statistics = new PartitionStatistics(createEmptyStatistics(), ImmutableMap.of());
            }
            result.put(partitionName, statistics);
        }
        return result.build();
    }

    @Override
    public synchronized void updateTableStatistics(MetastoreContext metastoreContext, String databaseName, String tableName, Function<PartitionStatistics, PartitionStatistics> update)
    {
        columnStatistics.put(new SchemaTableName(databaseName, tableName), update.apply(getTableStatistics(metastoreContext, databaseName, tableName)));
    }

    @Override
    public synchronized void updatePartitionStatistics(MetastoreContext metastoreContext, String databaseName, String tableName, String partitionName, Function<PartitionStatistics, PartitionStatistics> update)
    {
        PartitionName partitionKey = PartitionName.partition(databaseName, tableName, partitionName);
        partitionColumnStatistics.put(partitionKey, update.apply(getPartitionStatistics(metastoreContext, databaseName, tableName, ImmutableSet.of(partitionName)).get(partitionName)));
    }

    @Override
    public void createRole(MetastoreContext metastoreContext, String role, String grantor)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropRole(MetastoreContext metastoreContext, String role)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> listRoles(MetastoreContext metastoreContext)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void grantRoles(MetastoreContext metastoreContext, Set<String> roles, Set<PrestoPrincipal> grantees, boolean withAdminOption, PrestoPrincipal grantor)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void revokeRoles(MetastoreContext metastoreContext, Set<String> roles, Set<PrestoPrincipal> grantees, boolean adminOptionFor, PrestoPrincipal grantor)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<RoleGrant> listRoleGrants(MetastoreContext metastoreContext, PrestoPrincipal principal)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<HivePrivilegeInfo> listTablePrivileges(MetastoreContext metastoreContext, String databaseName, String tableName, PrestoPrincipal principal)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void grantTablePrivileges(MetastoreContext metastoreContext, String databaseName, String tableName, PrestoPrincipal grantee, Set<HivePrivilegeInfo> privileges)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void revokeTablePrivileges(MetastoreContext metastoreContext, String databaseName, String tableName, PrestoPrincipal grantee, Set<HivePrivilegeInfo> privileges)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public MetastoreOperationResult dropConstraint(MetastoreContext metastoreContext, String databaseName, String tableName, String constraintName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public MetastoreOperationResult addConstraint(MetastoreContext metastoreContext, String databaseName, String tableName, TableConstraint<String> tableConstraint)
    {
        throw new UnsupportedOperationException();
    }

    private Partition getPartitionFromInMemoryMap(MetastoreContext metastoreContext, PartitionName name)
    {
        com.facebook.presto.hive.metastore.Partition partition = partitions.get(name);
        if (partition == null) {
            return null;
        }
        Partition convertedPartition = toMetastoreApiPartition(partition, metastoreContext.getColumnConverter());
        if (convertedPartition.getParameters() == null) {
            convertedPartition.setParameters(ImmutableMap.of());
        }
        return convertedPartition;
    }

    /**
     * Returns true if the first directory is contained in the baseDirectory, false otherwise.
     */
    private static boolean isAncestor(File directory, File baseDirectory)
    {
        for (File parent = directory.getParentFile(); parent != null; parent = parent.getParentFile()) {
            if (parent.equals(baseDirectory)) {
                return true;
            }
        }
        return false;
    }

    private static void deleteDirectory(File dir)
    {
        try {
            deleteRecursively(dir.toPath(), ALLOW_INSECURE);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
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
