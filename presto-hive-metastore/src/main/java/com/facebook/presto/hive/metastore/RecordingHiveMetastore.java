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

import com.facebook.airlift.json.ObjectMapperProvider;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.hive.ForRecordingHiveMetastore;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.hive.MetastoreClientConfig;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.security.PrestoPrincipal;
import com.facebook.presto.spi.security.RoleGrant;
import com.facebook.presto.spi.statistics.ColumnStatisticType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.weakref.jmx.Managed;

import javax.annotation.concurrent.Immutable;
import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.facebook.presto.hive.metastore.HivePartitionName.hivePartitionName;
import static com.facebook.presto.hive.metastore.HiveTableName.hiveTableName;
import static com.facebook.presto.hive.metastore.PartitionFilter.partitionFilter;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class RecordingHiveMetastore
        implements ExtendedHiveMetastore
{
    private final ExtendedHiveMetastore delegate;
    private final String recordingPath;
    private final boolean replay;

    private volatile Optional<List<String>> allDatabases = Optional.empty();
    private volatile Optional<Set<String>> allRoles = Optional.empty();

    private final Cache<String, Optional<Database>> databaseCache;
    private final Cache<HiveTableName, Optional<Table>> tableCache;
    private final Cache<String, Set<ColumnStatisticType>> supportedColumnStatisticsCache;
    private final Cache<HiveTableName, PartitionStatistics> tableStatisticsCache;
    private final Cache<Set<HivePartitionName>, Map<String, PartitionStatistics>> partitionStatisticsCache;
    private final Cache<String, Optional<List<String>>> allTablesCache;
    private final Cache<String, Optional<List<String>>> allViewsCache;
    private final Cache<HivePartitionName, Optional<Partition>> partitionCache;
    private final Cache<HiveTableName, Optional<List<String>>> partitionNamesCache;
    private final Cache<String, List<String>> partitionNamesByFilterCache;
    private final Cache<Set<HivePartitionName>, Map<String, Optional<Partition>>> partitionsByNamesCache;
    private final Cache<UserTableKey, Set<HivePrivilegeInfo>> tablePrivilegesCache;
    private final Cache<PrestoPrincipal, Set<RoleGrant>> roleGrantsCache;

    @Inject
    public RecordingHiveMetastore(@ForRecordingHiveMetastore ExtendedHiveMetastore delegate, MetastoreClientConfig metastoreClientConfig)
            throws IOException
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        requireNonNull(metastoreClientConfig, "hiveClientConfig is null");
        this.recordingPath = requireNonNull(metastoreClientConfig.getRecordingPath(), "recordingPath is null");
        this.replay = metastoreClientConfig.isReplay();

        databaseCache = createCache(metastoreClientConfig);
        tableCache = createCache(metastoreClientConfig);
        supportedColumnStatisticsCache = createCache(metastoreClientConfig);
        tableStatisticsCache = createCache(metastoreClientConfig);
        partitionStatisticsCache = createCache(metastoreClientConfig);
        allTablesCache = createCache(metastoreClientConfig);
        allViewsCache = createCache(metastoreClientConfig);
        partitionCache = createCache(metastoreClientConfig);
        partitionNamesCache = createCache(metastoreClientConfig);
        partitionNamesByFilterCache = createCache(metastoreClientConfig);
        partitionsByNamesCache = createCache(metastoreClientConfig);
        tablePrivilegesCache = createCache(metastoreClientConfig);
        roleGrantsCache = createCache(metastoreClientConfig);

        if (replay) {
            loadRecording();
        }
    }

    @VisibleForTesting
    void loadRecording()
            throws IOException
    {
        Recording recording = new ObjectMapperProvider().get().readValue(new File(recordingPath), Recording.class);

        allDatabases = recording.getAllDatabases();
        allRoles = recording.getAllRoles();
        databaseCache.putAll(toMap(recording.getDatabases()));
        tableCache.putAll(toMap(recording.getTables()));
        supportedColumnStatisticsCache.putAll(toMap(recording.getSupportedColumnStatistics()));
        tableStatisticsCache.putAll(toMap(recording.getTableStatistics()));
        partitionStatisticsCache.putAll(toMap(recording.getPartitionStatistics()));
        allTablesCache.putAll(toMap(recording.getAllTables()));
        allViewsCache.putAll(toMap(recording.getAllViews()));
        partitionCache.putAll(toMap(recording.getPartitions()));
        partitionNamesCache.putAll(toMap(recording.getPartitionNames()));
        partitionNamesByFilterCache.putAll(toMap(recording.getPartitionNamesByFilter()));
        partitionsByNamesCache.putAll(toMap(recording.getPartitionsByNames()));
        tablePrivilegesCache.putAll(toMap(recording.getTablePrivileges()));
        roleGrantsCache.putAll(toMap(recording.getRoleGrants()));
    }

    private static <K, V> Cache<K, V> createCache(MetastoreClientConfig metastoreClientConfig)
    {
        if (metastoreClientConfig.isReplay()) {
            return CacheBuilder.<K, V>newBuilder()
                    .build();
        }

        return CacheBuilder.<K, V>newBuilder()
                .expireAfterWrite(metastoreClientConfig.getRecordingDuration().toMillis(), MILLISECONDS)
                .build();
    }

    @Managed
    public void writeRecording()
            throws IOException
    {
        if (replay) {
            throw new IllegalStateException("Cannot write recording in replay mode");
        }

        Recording recording = new Recording(
                allDatabases,
                allRoles,
                toPairs(databaseCache),
                toPairs(tableCache),
                toPairs(supportedColumnStatisticsCache),
                toPairs(tableStatisticsCache),
                toPairs(partitionStatisticsCache),
                toPairs(allTablesCache),
                toPairs(allViewsCache),
                toPairs(partitionCache),
                toPairs(partitionNamesCache),
                toPairs(partitionNamesByFilterCache),
                toPairs(partitionsByNamesCache),
                toPairs(tablePrivilegesCache),
                toPairs(roleGrantsCache));
        new ObjectMapperProvider().get()
                .writerWithDefaultPrettyPrinter()
                .writeValue(new File(recordingPath), recording);
    }

    private static <K, V> Map<K, V> toMap(List<Pair<K, V>> pairs)
    {
        return pairs.stream()
                .collect(ImmutableMap.toImmutableMap(Pair::getKey, Pair::getValue));
    }

    private static <K, V> List<Pair<K, V>> toPairs(Cache<K, V> cache)
    {
        return cache.asMap().entrySet().stream()
                .map(entry -> new Pair<>(entry.getKey(), entry.getValue()))
                .collect(toImmutableList());
    }

    @Override
    public Optional<Database> getDatabase(String databaseName)
    {
        return loadValue(databaseCache, databaseName, () -> delegate.getDatabase(databaseName));
    }

    @Override
    public List<String> getAllDatabases()
    {
        if (replay) {
            return allDatabases.orElseThrow(() -> new PrestoException(NOT_FOUND, "Missing entry for all databases"));
        }

        List<String> result = delegate.getAllDatabases();
        allDatabases = Optional.of(result);
        return result;
    }

    @Override
    public Optional<Table> getTable(String databaseName, String tableName)
    {
        return loadValue(tableCache, hiveTableName(databaseName, tableName), () -> delegate.getTable(databaseName, tableName));
    }

    @Override
    public Set<ColumnStatisticType> getSupportedColumnStatistics(Type type)
    {
        return loadValue(supportedColumnStatisticsCache, type.getTypeSignature().toString(), () -> delegate.getSupportedColumnStatistics(type));
    }

    @Override
    public PartitionStatistics getTableStatistics(String databaseName, String tableName)
    {
        return loadValue(
                tableStatisticsCache,
                hiveTableName(databaseName, tableName),
                () -> delegate.getTableStatistics(databaseName, tableName));
    }

    @Override
    public Map<String, PartitionStatistics> getPartitionStatistics(String databaseName, String tableName, Set<String> partitionNames)
    {
        return loadValue(
                partitionStatisticsCache,
                getHivePartitionNames(databaseName, tableName, partitionNames),
                () -> delegate.getPartitionStatistics(databaseName, tableName, partitionNames));
    }

    @Override
    public void updateTableStatistics(String databaseName, String tableName, Function<PartitionStatistics, PartitionStatistics> update)
    {
        verifyRecordingMode();
        delegate.updateTableStatistics(databaseName, tableName, update);
    }

    @Override
    public void updatePartitionStatistics(String databaseName, String tableName, String partitionName, Function<PartitionStatistics, PartitionStatistics> update)
    {
        verifyRecordingMode();
        delegate.updatePartitionStatistics(databaseName, tableName, partitionName, update);
    }

    @Override
    public Optional<List<String>> getAllTables(String databaseName)
    {
        return loadValue(allTablesCache, databaseName, () -> delegate.getAllTables(databaseName));
    }

    @Override
    public Optional<List<String>> getAllViews(String databaseName)
    {
        return loadValue(allViewsCache, databaseName, () -> delegate.getAllViews(databaseName));
    }

    @Override
    public void createDatabase(Database database)
    {
        verifyRecordingMode();
        delegate.createDatabase(database);
    }

    @Override
    public void dropDatabase(String databaseName)
    {
        verifyRecordingMode();
        delegate.dropDatabase(databaseName);
    }

    @Override
    public void renameDatabase(String databaseName, String newDatabaseName)
    {
        verifyRecordingMode();
        delegate.renameDatabase(databaseName, newDatabaseName);
    }

    @Override
    public void createTable(Table table, PrincipalPrivileges principalPrivileges)
    {
        verifyRecordingMode();
        delegate.createTable(table, principalPrivileges);
    }

    @Override
    public void dropTable(String databaseName, String tableName, boolean deleteData)
    {
        verifyRecordingMode();
        delegate.dropTable(databaseName, tableName, deleteData);
    }

    @Override
    public void replaceTable(String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges)
    {
        verifyRecordingMode();
        delegate.replaceTable(databaseName, tableName, newTable, principalPrivileges);
    }

    @Override
    public void renameTable(String databaseName, String tableName, String newDatabaseName, String newTableName)
    {
        verifyRecordingMode();
        delegate.renameTable(databaseName, tableName, newDatabaseName, newTableName);
    }

    @Override
    public void addColumn(String databaseName, String tableName, String columnName, HiveType columnType, String columnComment)
    {
        verifyRecordingMode();
        delegate.addColumn(databaseName, tableName, columnName, columnType, columnComment);
    }

    @Override
    public void renameColumn(String databaseName, String tableName, String oldColumnName, String newColumnName)
    {
        verifyRecordingMode();
        delegate.renameColumn(databaseName, tableName, oldColumnName, newColumnName);
    }

    @Override
    public void dropColumn(String databaseName, String tableName, String columnName)
    {
        verifyRecordingMode();
        delegate.dropColumn(databaseName, tableName, columnName);
    }

    @Override
    public Optional<Partition> getPartition(String databaseName, String tableName, List<String> partitionValues)
    {
        return loadValue(
                partitionCache,
                hivePartitionName(databaseName, tableName, partitionValues),
                () -> delegate.getPartition(databaseName, tableName, partitionValues));
    }

    @Override
    public Optional<List<String>> getPartitionNames(String databaseName, String tableName)
    {
        return loadValue(
                partitionNamesCache,
                hiveTableName(databaseName, tableName),
                () -> delegate.getPartitionNames(databaseName, tableName));
    }

    @Override
    public List<String> getPartitionNamesByFilter(
            String databaseName,
            String tableName,
            Map<Column, Domain> partitionPredicates)
    {
        return loadValue(
                partitionNamesByFilterCache,
                partitionFilter(databaseName, tableName, partitionPredicates).toString(),
                () -> delegate.getPartitionNamesByFilter(databaseName, tableName, partitionPredicates));
    }

    @Override
    public List<PartitionNameWithVersion> getPartitionNamesWithVersionByFilter(
            String databaseName,
            String tableName,
            Map<Column, Domain> partitionPredicates)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Optional<Partition>> getPartitionsByNames(String databaseName, String tableName, List<String> partitionNames)
    {
        return loadValue(
                partitionsByNamesCache,
                getHivePartitionNames(databaseName, tableName, ImmutableSet.copyOf(partitionNames)),
                () -> delegate.getPartitionsByNames(databaseName, tableName, partitionNames));
    }

    @Override
    public void addPartitions(String databaseName, String tableName, List<PartitionWithStatistics> partitions)
    {
        verifyRecordingMode();
        delegate.addPartitions(databaseName, tableName, partitions);
    }

    @Override
    public void dropPartition(String databaseName, String tableName, List<String> parts, boolean deleteData)
    {
        verifyRecordingMode();
        delegate.dropPartition(databaseName, tableName, parts, deleteData);
    }

    @Override
    public void alterPartition(String databaseName, String tableName, PartitionWithStatistics partition)
    {
        verifyRecordingMode();
        delegate.alterPartition(databaseName, tableName, partition);
    }

    @Override
    public Set<HivePrivilegeInfo> listTablePrivileges(String databaseName, String tableName, PrestoPrincipal principal)
    {
        return loadValue(
                tablePrivilegesCache,
                new UserTableKey(principal, databaseName, tableName),
                () -> delegate.listTablePrivileges(databaseName, tableName, principal));
    }

    @Override
    public void grantTablePrivileges(String databaseName, String tableName, PrestoPrincipal grantee, Set<HivePrivilegeInfo> privileges)
    {
        verifyRecordingMode();
        delegate.grantTablePrivileges(databaseName, tableName, grantee, privileges);
    }

    @Override
    public void revokeTablePrivileges(String databaseName, String tableName, PrestoPrincipal grantee, Set<HivePrivilegeInfo> privileges)
    {
        verifyRecordingMode();
        delegate.revokeTablePrivileges(databaseName, tableName, grantee, privileges);
    }

    private Set<HivePartitionName> getHivePartitionNames(String databaseName, String tableName, Set<String> partitionNames)
    {
        return partitionNames.stream()
                .map(partitionName -> HivePartitionName.hivePartitionName(databaseName, tableName, partitionName))
                .collect(ImmutableSet.toImmutableSet());
    }

    @Override
    public void createRole(String role, String grantor)
    {
        verifyRecordingMode();
        delegate.createRole(role, grantor);
    }

    @Override
    public void dropRole(String role)
    {
        verifyRecordingMode();
        delegate.dropRole(role);
    }

    @Override
    public Set<String> listRoles()
    {
        if (replay) {
            return allRoles.orElseThrow(() -> new PrestoException(NOT_FOUND, "Missing entry for roles"));
        }

        Set<String> result = delegate.listRoles();
        allRoles = Optional.of(result);
        return result;
    }

    @Override
    public void grantRoles(Set<String> roles, Set<PrestoPrincipal> grantees, boolean withAdminOption, PrestoPrincipal grantor)
    {
        verifyRecordingMode();
        delegate.grantRoles(roles, grantees, withAdminOption, grantor);
    }

    @Override
    public void revokeRoles(Set<String> roles, Set<PrestoPrincipal> grantees, boolean adminOptionFor, PrestoPrincipal grantor)
    {
        verifyRecordingMode();
        delegate.revokeRoles(roles, grantees, adminOptionFor, grantor);
    }

    @Override
    public Set<RoleGrant> listRoleGrants(PrestoPrincipal principal)
    {
        return loadValue(
                roleGrantsCache,
                principal,
                () -> delegate.listRoleGrants(principal));
    }

    private <K, V> V loadValue(Cache<K, V> cache, K key, Supplier<V> valueSupplier)
    {
        if (replay) {
            return Optional.ofNullable(cache.getIfPresent(key))
                    .orElseThrow(() -> new PrestoException(NOT_FOUND, "Missing entry found for key: " + key));
        }

        V value = valueSupplier.get();
        cache.put(key, value);
        return value;
    }

    private void verifyRecordingMode()
    {
        if (replay) {
            throw new IllegalStateException("Cannot perform Metastore updates in replay mode");
        }
    }

    @Immutable
    public static class Recording
    {
        private final Optional<List<String>> allDatabases;
        private final Optional<Set<String>> allRoles;
        private final List<Pair<String, Optional<Database>>> databases;
        private final List<Pair<HiveTableName, Optional<Table>>> tables;
        private final List<Pair<String, Set<ColumnStatisticType>>> supportedColumnStatistics;
        private final List<Pair<HiveTableName, PartitionStatistics>> tableStatistics;
        private final List<Pair<Set<HivePartitionName>, Map<String, PartitionStatistics>>> partitionStatistics;
        private final List<Pair<String, Optional<List<String>>>> allTables;
        private final List<Pair<String, Optional<List<String>>>> allViews;
        private final List<Pair<HivePartitionName, Optional<Partition>>> partitions;
        private final List<Pair<HiveTableName, Optional<List<String>>>> partitionNames;
        private final List<Pair<String, List<String>>> partitionNamesByFilter;
        private final List<Pair<Set<HivePartitionName>, Map<String, Optional<Partition>>>> partitionsByNames;
        private final List<Pair<UserTableKey, Set<HivePrivilegeInfo>>> tablePrivileges;
        private final List<Pair<PrestoPrincipal, Set<RoleGrant>>> roleGrants;

        @JsonCreator
        public Recording(
                @JsonProperty("allDatabases") Optional<List<String>> allDatabases,
                @JsonProperty("allRoles") Optional<Set<String>> allRoles,
                @JsonProperty("databases") List<Pair<String, Optional<Database>>> databases,
                @JsonProperty("tables") List<Pair<HiveTableName, Optional<Table>>> tables,
                @JsonProperty("supportedColumnStatistics") List<Pair<String, Set<ColumnStatisticType>>> supportedColumnStatistics,
                @JsonProperty("tableStatistics") List<Pair<HiveTableName, PartitionStatistics>> tableStatistics,
                @JsonProperty("partitionStatistics") List<Pair<Set<HivePartitionName>, Map<String, PartitionStatistics>>> partitionStatistics,
                @JsonProperty("allTables") List<Pair<String, Optional<List<String>>>> allTables,
                @JsonProperty("allViews") List<Pair<String, Optional<List<String>>>> allViews,
                @JsonProperty("partitions") List<Pair<HivePartitionName, Optional<Partition>>> partitions,
                @JsonProperty("partitionNames") List<Pair<HiveTableName, Optional<List<String>>>> partitionNames,
                @JsonProperty("partitionNamesByFilter") List<Pair<String, List<String>>> partitionNamesByFilter,
                @JsonProperty("partitionsByNames") List<Pair<Set<HivePartitionName>, Map<String, Optional<Partition>>>> partitionsByNames,
                @JsonProperty("tablePrivileges") List<Pair<UserTableKey, Set<HivePrivilegeInfo>>> tablePrivileges,
                @JsonProperty("roleGrants") List<Pair<PrestoPrincipal, Set<RoleGrant>>> roleGrants)
        {
            this.allDatabases = allDatabases;
            this.allRoles = allRoles;
            this.databases = databases;
            this.tables = tables;
            this.supportedColumnStatistics = supportedColumnStatistics;
            this.tableStatistics = tableStatistics;
            this.partitionStatistics = partitionStatistics;
            this.allTables = allTables;
            this.allViews = allViews;
            this.partitions = partitions;
            this.partitionNames = partitionNames;
            this.partitionNamesByFilter = partitionNamesByFilter;
            this.partitionsByNames = partitionsByNames;
            this.tablePrivileges = tablePrivileges;
            this.roleGrants = roleGrants;
        }

        @JsonProperty
        public Optional<List<String>> getAllDatabases()
        {
            return allDatabases;
        }

        @JsonProperty
        public Optional<Set<String>> getAllRoles()
        {
            return allRoles;
        }

        @JsonProperty
        public List<Pair<String, Optional<Database>>> getDatabases()
        {
            return databases;
        }

        @JsonProperty
        public List<Pair<HiveTableName, Optional<Table>>> getTables()
        {
            return tables;
        }

        @JsonProperty
        public List<Pair<String, Set<ColumnStatisticType>>> getSupportedColumnStatistics()
        {
            return supportedColumnStatistics;
        }

        @JsonProperty
        public List<Pair<HiveTableName, PartitionStatistics>> getTableStatistics()
        {
            return tableStatistics;
        }

        @JsonProperty
        public List<Pair<Set<HivePartitionName>, Map<String, PartitionStatistics>>> getPartitionStatistics()
        {
            return partitionStatistics;
        }

        @JsonProperty
        public List<Pair<String, Optional<List<String>>>> getAllTables()
        {
            return allTables;
        }

        @JsonProperty
        public List<Pair<String, Optional<List<String>>>> getAllViews()
        {
            return allViews;
        }

        @JsonProperty
        public List<Pair<HivePartitionName, Optional<Partition>>> getPartitions()
        {
            return partitions;
        }

        @JsonProperty
        public List<Pair<HiveTableName, Optional<List<String>>>> getPartitionNames()
        {
            return partitionNames;
        }

        @JsonProperty
        public List<Pair<String, List<String>>> getPartitionNamesByFilter()
        {
            return partitionNamesByFilter;
        }

        @JsonProperty
        public List<Pair<Set<HivePartitionName>, Map<String, Optional<Partition>>>> getPartitionsByNames()
        {
            return partitionsByNames;
        }

        @JsonProperty
        public List<Pair<UserTableKey, Set<HivePrivilegeInfo>>> getTablePrivileges()
        {
            return tablePrivileges;
        }

        @JsonProperty
        public List<Pair<PrestoPrincipal, Set<RoleGrant>>> getRoleGrants()
        {
            return roleGrants;
        }
    }

    @Immutable
    public static class Pair<K, V>
    {
        private final K key;
        private final V value;

        @JsonCreator
        public Pair(@JsonProperty("key") K key, @JsonProperty("value") V value)
        {
            this.key = requireNonNull(key, "key is null");
            this.value = requireNonNull(value, "value is null");
        }

        @JsonProperty
        public K getKey()
        {
            return key;
        }

        @JsonProperty
        public V getValue()
        {
            return value;
        }
    }
}
