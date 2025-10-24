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

import com.facebook.airlift.json.JsonObjectMapperProvider;
import com.facebook.airlift.units.Duration;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.hive.ForRecordingHiveMetastore;
import com.facebook.presto.hive.HiveTableHandle;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.hive.MetastoreClientConfig;
import com.facebook.presto.hive.PartitionNameWithVersion;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.constraints.TableConstraint;
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
import com.google.errorprone.annotations.Immutable;
import jakarta.inject.Inject;
import org.weakref.jmx.Managed;

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
    private final Cache<HiveTableHandle, Optional<Table>> tableCache;
    private final Cache<HiveTableName, List<TableConstraint<String>>> tableConstraintsCache;
    private final Cache<String, Set<ColumnStatisticType>> supportedColumnStatisticsCache;
    private final Cache<HiveTableName, PartitionStatistics> tableStatisticsCache;
    private final Cache<Set<HivePartitionName>, Map<String, PartitionStatistics>> partitionStatisticsCache;
    private final Cache<String, Optional<List<String>>> allTablesCache;
    private final Cache<String, Optional<List<String>>> allViewsCache;
    private final Cache<HivePartitionName, Optional<Partition>> partitionCache;
    private final Cache<HiveTableName, Optional<List<PartitionNameWithVersion>>> partitionNamesCache;
    private final Cache<String, List<PartitionNameWithVersion>> partitionNamesByFilterCache;
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
        tableConstraintsCache = createCache(metastoreClientConfig);
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
        Recording recording = new JsonObjectMapperProvider().get().readValue(new File(recordingPath), Recording.class);

        allDatabases = recording.getAllDatabases();
        allRoles = recording.getAllRoles();
        databaseCache.putAll(toMap(recording.getDatabases()));
        tableCache.putAll(toMap(recording.getTables()));
        tableConstraintsCache.putAll(toMap(recording.getTableConstraints()));
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
                toPairs(tableConstraintsCache),
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
        new JsonObjectMapperProvider().get()
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
    public Optional<Database> getDatabase(MetastoreContext metastoreContext, String databaseName)
    {
        return loadValue(databaseCache, databaseName, () -> delegate.getDatabase(metastoreContext, databaseName));
    }

    @Override
    public List<String> getAllDatabases(MetastoreContext metastoreContext)
    {
        if (replay) {
            return allDatabases.orElseThrow(() -> new PrestoException(NOT_FOUND, "Missing entry for all databases"));
        }

        List<String> result = delegate.getAllDatabases(metastoreContext);
        allDatabases = Optional.of(result);
        return result;
    }

    @Override
    public Optional<Table> getTable(MetastoreContext metastoreContext, String databaseName, String tableName)
    {
        return getTable(metastoreContext, new HiveTableHandle(databaseName, tableName));
    }

    @Override
    public Optional<Table> getTable(MetastoreContext metastoreContext, HiveTableHandle hiveTableHandle)
    {
        return loadValue(tableCache, hiveTableHandle, () -> delegate.getTable(metastoreContext, hiveTableHandle));
    }

    public List<TableConstraint<String>> getTableConstraints(MetastoreContext metastoreContext, String databaseName, String tableName)
    {
        return loadValue(tableConstraintsCache, hiveTableName(databaseName, tableName), () -> delegate.getTableConstraints(metastoreContext, databaseName, tableName));
    }

    @Override
    public Set<ColumnStatisticType> getSupportedColumnStatistics(MetastoreContext metastoreContext, Type type)
    {
        return loadValue(supportedColumnStatisticsCache, type.getTypeSignature().toString(), () -> delegate.getSupportedColumnStatistics(metastoreContext, type));
    }

    @Override
    public PartitionStatistics getTableStatistics(MetastoreContext metastoreContext, String databaseName, String tableName)
    {
        return loadValue(
                tableStatisticsCache,
                hiveTableName(databaseName, tableName),
                () -> delegate.getTableStatistics(metastoreContext, databaseName, tableName));
    }

    @Override
    public Map<String, PartitionStatistics> getPartitionStatistics(MetastoreContext metastoreContext, String databaseName, String tableName, Set<String> partitionNames)
    {
        return loadValue(
                partitionStatisticsCache,
                getHivePartitionNames(databaseName, tableName, partitionNames),
                () -> delegate.getPartitionStatistics(metastoreContext, databaseName, tableName, partitionNames));
    }

    @Override
    public void updateTableStatistics(MetastoreContext metastoreContext, String databaseName, String tableName, Function<PartitionStatistics, PartitionStatistics> update)
    {
        verifyRecordingMode();
        delegate.updateTableStatistics(metastoreContext, databaseName, tableName, update);
    }

    @Override
    public void updatePartitionStatistics(MetastoreContext metastoreContext, String databaseName, String tableName, String partitionName, Function<PartitionStatistics, PartitionStatistics> update)
    {
        verifyRecordingMode();
        delegate.updatePartitionStatistics(metastoreContext, databaseName, tableName, partitionName, update);
    }

    @Override
    public Optional<List<String>> getAllTables(MetastoreContext metastoreContext, String databaseName)
    {
        return loadValue(allTablesCache, databaseName, () -> delegate.getAllTables(metastoreContext, databaseName));
    }

    @Override
    public Optional<List<String>> getAllViews(MetastoreContext metastoreContext, String databaseName)
    {
        return loadValue(allViewsCache, databaseName, () -> delegate.getAllViews(metastoreContext, databaseName));
    }

    @Override
    public void createDatabase(MetastoreContext metastoreContext, Database database)
    {
        verifyRecordingMode();
        delegate.createDatabase(metastoreContext, database);
    }

    @Override
    public void dropDatabase(MetastoreContext metastoreContext, String databaseName)
    {
        verifyRecordingMode();
        delegate.dropDatabase(metastoreContext, databaseName);
    }

    @Override
    public void renameDatabase(MetastoreContext metastoreContext, String databaseName, String newDatabaseName)
    {
        verifyRecordingMode();
        delegate.renameDatabase(metastoreContext, databaseName, newDatabaseName);
    }

    @Override
    public MetastoreOperationResult createTable(MetastoreContext metastoreContext, Table table, PrincipalPrivileges principalPrivileges, List<TableConstraint<String>> constraints)
    {
        verifyRecordingMode();
        return delegate.createTable(metastoreContext, table, principalPrivileges, constraints);
    }

    @Override
    public void dropTable(MetastoreContext metastoreContext, String databaseName, String tableName, boolean deleteData)
    {
        verifyRecordingMode();
        delegate.dropTable(metastoreContext, databaseName, tableName, deleteData);
    }

    @Override
    public MetastoreOperationResult replaceTable(MetastoreContext metastoreContext, String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges)
    {
        verifyRecordingMode();
        return delegate.replaceTable(metastoreContext, databaseName, tableName, newTable, principalPrivileges);
    }

    @Override
    public MetastoreOperationResult persistTable(MetastoreContext metastoreContext, String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges, Supplier<PartitionStatistics> update, Map<String, String> additionalParameters)
    {
        verifyRecordingMode();
        return delegate.persistTable(metastoreContext, databaseName, tableName, newTable, principalPrivileges, update, additionalParameters);
    }

    @Override
    public MetastoreOperationResult renameTable(MetastoreContext metastoreContext, String databaseName, String tableName, String newDatabaseName, String newTableName)
    {
        verifyRecordingMode();
        return delegate.renameTable(metastoreContext, databaseName, tableName, newDatabaseName, newTableName);
    }

    @Override
    public MetastoreOperationResult addColumn(MetastoreContext metastoreContext, String databaseName, String tableName, String columnName, HiveType columnType, String columnComment)
    {
        verifyRecordingMode();
        return delegate.addColumn(metastoreContext, databaseName, tableName, columnName, columnType, columnComment);
    }

    @Override
    public MetastoreOperationResult renameColumn(MetastoreContext metastoreContext, String databaseName, String tableName, String oldColumnName, String newColumnName)
    {
        verifyRecordingMode();
        return delegate.renameColumn(metastoreContext, databaseName, tableName, oldColumnName, newColumnName);
    }

    @Override
    public MetastoreOperationResult dropColumn(MetastoreContext metastoreContext, String databaseName, String tableName, String columnName)
    {
        verifyRecordingMode();
        return delegate.dropColumn(metastoreContext, databaseName, tableName, columnName);
    }

    @Override
    public Optional<Partition> getPartition(MetastoreContext metastoreContext, String databaseName, String tableName, List<String> partitionValues)
    {
        return loadValue(
                partitionCache,
                hivePartitionName(databaseName, tableName, partitionValues),
                () -> delegate.getPartition(metastoreContext, databaseName, tableName, partitionValues));
    }

    @Override
    public Optional<List<PartitionNameWithVersion>> getPartitionNames(MetastoreContext metastoreContext, String databaseName, String tableName)
    {
        return loadValue(
                partitionNamesCache,
                hiveTableName(databaseName, tableName),
                () -> delegate.getPartitionNames(metastoreContext, databaseName, tableName));
    }

    @Override
    public List<PartitionNameWithVersion> getPartitionNamesByFilter(
            MetastoreContext metastoreContext,
            String databaseName,
            String tableName,
            Map<Column, Domain> partitionPredicates)
    {
        return loadValue(
                partitionNamesByFilterCache,
                partitionFilter(databaseName, tableName, partitionPredicates).toString(),
                () -> delegate.getPartitionNamesByFilter(metastoreContext, databaseName, tableName, partitionPredicates));
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

    @Override
    public Map<String, Optional<Partition>> getPartitionsByNames(MetastoreContext metastoreContext, String databaseName, String tableName, List<PartitionNameWithVersion> partitionNameWithVersions)
    {
        return loadValue(
                partitionsByNamesCache,
                getHivePartitionNames(databaseName, tableName, ImmutableSet.copyOf(MetastoreUtil.getPartitionNames(partitionNameWithVersions))),
                () -> delegate.getPartitionsByNames(metastoreContext, databaseName, tableName, partitionNameWithVersions));
    }

    @Override
    public MetastoreOperationResult addPartitions(MetastoreContext metastoreContext, String databaseName, String tableName, List<PartitionWithStatistics> partitions)
    {
        verifyRecordingMode();
        return delegate.addPartitions(metastoreContext, databaseName, tableName, partitions);
    }

    @Override
    public void dropPartition(MetastoreContext metastoreContext, String databaseName, String tableName, List<String> parts, boolean deleteData)
    {
        verifyRecordingMode();
        delegate.dropPartition(metastoreContext, databaseName, tableName, parts, deleteData);
    }

    @Override
    public MetastoreOperationResult alterPartition(MetastoreContext metastoreContext, String databaseName, String tableName, PartitionWithStatistics partition)
    {
        verifyRecordingMode();
        return delegate.alterPartition(metastoreContext, databaseName, tableName, partition);
    }

    @Override
    public Set<HivePrivilegeInfo> listTablePrivileges(MetastoreContext metastoreContext, String databaseName, String tableName, PrestoPrincipal principal)
    {
        return loadValue(
                tablePrivilegesCache,
                new UserTableKey(principal, databaseName, tableName),
                () -> delegate.listTablePrivileges(metastoreContext, databaseName, tableName, principal));
    }

    @Override
    public void grantTablePrivileges(MetastoreContext metastoreContext, String databaseName, String tableName, PrestoPrincipal grantee, Set<HivePrivilegeInfo> privileges)
    {
        verifyRecordingMode();
        delegate.grantTablePrivileges(metastoreContext, databaseName, tableName, grantee, privileges);
    }

    @Override
    public void revokeTablePrivileges(MetastoreContext metastoreContext, String databaseName, String tableName, PrestoPrincipal grantee, Set<HivePrivilegeInfo> privileges)
    {
        verifyRecordingMode();
        delegate.revokeTablePrivileges(metastoreContext, databaseName, tableName, grantee, privileges);
    }

    private Set<HivePartitionName> getHivePartitionNames(String databaseName, String tableName, Set<String> partitionNames)
    {
        return partitionNames.stream()
                .map(partitionName -> HivePartitionName.hivePartitionName(databaseName, tableName, partitionName))
                .collect(ImmutableSet.toImmutableSet());
    }

    @Override
    public void createRole(MetastoreContext metastoreContext, String role, String grantor)
    {
        verifyRecordingMode();
        delegate.createRole(metastoreContext, role, grantor);
    }

    @Override
    public void dropRole(MetastoreContext metastoreContext, String role)
    {
        verifyRecordingMode();
        delegate.dropRole(metastoreContext, role);
    }

    @Override
    public Set<String> listRoles(MetastoreContext metastoreContext)
    {
        if (replay) {
            return allRoles.orElseThrow(() -> new PrestoException(NOT_FOUND, "Missing entry for roles"));
        }

        Set<String> result = delegate.listRoles(metastoreContext);
        allRoles = Optional.of(result);
        return result;
    }

    @Override
    public void grantRoles(MetastoreContext metastoreContext, Set<String> roles, Set<PrestoPrincipal> grantees, boolean withAdminOption, PrestoPrincipal grantor)
    {
        verifyRecordingMode();
        delegate.grantRoles(metastoreContext, roles, grantees, withAdminOption, grantor);
    }

    @Override
    public void revokeRoles(MetastoreContext metastoreContext, Set<String> roles, Set<PrestoPrincipal> grantees, boolean adminOptionFor, PrestoPrincipal grantor)
    {
        verifyRecordingMode();
        delegate.revokeRoles(metastoreContext, roles, grantees, adminOptionFor, grantor);
    }

    @Override
    public Set<RoleGrant> listRoleGrants(MetastoreContext metastoreContext, PrestoPrincipal principal)
    {
        return loadValue(
                roleGrantsCache,
                principal,
                () -> delegate.listRoleGrants(metastoreContext, principal));
    }

    @Override
    public void setPartitionLeases(MetastoreContext metastoreContext, String databaseName, String tableName, Map<String, String> partitionNameToLocation, Duration leaseDuration)
    {
        throw new UnsupportedOperationException("setPartitionLeases is not supported in RecordingHiveMetastore");
    }

    @Override
    public MetastoreOperationResult dropConstraint(MetastoreContext metastoreContext, String databaseName, String tableName, String constraintName)
    {
        verifyRecordingMode();
        return delegate.dropConstraint(metastoreContext, databaseName, tableName, constraintName);
    }

    @Override
    public MetastoreOperationResult addConstraint(MetastoreContext metastoreContext, String databaseName, String tableName, TableConstraint<String> tableConstraint)
    {
        verifyRecordingMode();
        return delegate.addConstraint(metastoreContext, databaseName, tableName, tableConstraint);
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
        private final List<Pair<HiveTableHandle, Optional<Table>>> tables;
        private final List<Pair<HiveTableName, List<TableConstraint<String>>>> tableConstraints;
        private final List<Pair<String, Set<ColumnStatisticType>>> supportedColumnStatistics;
        private final List<Pair<HiveTableName, PartitionStatistics>> tableStatistics;
        private final List<Pair<Set<HivePartitionName>, Map<String, PartitionStatistics>>> partitionStatistics;
        private final List<Pair<String, Optional<List<String>>>> allTables;
        private final List<Pair<String, Optional<List<String>>>> allViews;
        private final List<Pair<HivePartitionName, Optional<Partition>>> partitions;
        private final List<Pair<HiveTableName, Optional<List<PartitionNameWithVersion>>>> partitionNames;
        private final List<Pair<String, List<PartitionNameWithVersion>>> partitionNamesByFilter;
        private final List<Pair<Set<HivePartitionName>, Map<String, Optional<Partition>>>> partitionsByNames;
        private final List<Pair<UserTableKey, Set<HivePrivilegeInfo>>> tablePrivileges;
        private final List<Pair<PrestoPrincipal, Set<RoleGrant>>> roleGrants;

        @JsonCreator
        public Recording(
                @JsonProperty("allDatabases") Optional<List<String>> allDatabases,
                @JsonProperty("allRoles") Optional<Set<String>> allRoles,
                @JsonProperty("databases") List<Pair<String, Optional<Database>>> databases,
                @JsonProperty("tables") List<Pair<HiveTableHandle, Optional<Table>>> tables,
                @JsonProperty("tableConstraints") List<Pair<HiveTableName, List<TableConstraint<String>>>> tableConstraints,
                @JsonProperty("supportedColumnStatistics") List<Pair<String, Set<ColumnStatisticType>>> supportedColumnStatistics,
                @JsonProperty("tableStatistics") List<Pair<HiveTableName, PartitionStatistics>> tableStatistics,
                @JsonProperty("partitionStatistics") List<Pair<Set<HivePartitionName>, Map<String, PartitionStatistics>>> partitionStatistics,
                @JsonProperty("allTables") List<Pair<String, Optional<List<String>>>> allTables,
                @JsonProperty("allViews") List<Pair<String, Optional<List<String>>>> allViews,
                @JsonProperty("partitions") List<Pair<HivePartitionName, Optional<Partition>>> partitions,
                @JsonProperty("partitionNames") List<Pair<HiveTableName, Optional<List<PartitionNameWithVersion>>>> partitionNames,
                @JsonProperty("partitionNamesByFilter") List<Pair<String, List<PartitionNameWithVersion>>> partitionNamesByFilter,
                @JsonProperty("partitionsByNames") List<Pair<Set<HivePartitionName>, Map<String, Optional<Partition>>>> partitionsByNames,
                @JsonProperty("tablePrivileges") List<Pair<UserTableKey, Set<HivePrivilegeInfo>>> tablePrivileges,
                @JsonProperty("roleGrants") List<Pair<PrestoPrincipal, Set<RoleGrant>>> roleGrants)
        {
            this.allDatabases = allDatabases;
            this.allRoles = allRoles;
            this.databases = databases;
            this.tables = tables;
            this.tableConstraints = tableConstraints;
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
        public List<Pair<HiveTableHandle, Optional<Table>>> getTables()
        {
            return tables;
        }

        @JsonProperty
        public List<Pair<HiveTableName, List<TableConstraint<String>>>> getTableConstraints()
        {
            return tableConstraints;
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
        public List<Pair<HiveTableName, Optional<List<PartitionNameWithVersion>>>> getPartitionNames()
        {
            return partitionNames;
        }

        @JsonProperty
        public List<Pair<String, List<PartitionNameWithVersion>>> getPartitionNamesByFilter()
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
