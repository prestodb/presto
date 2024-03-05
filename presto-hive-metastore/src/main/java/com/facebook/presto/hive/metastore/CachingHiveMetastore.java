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

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.hive.ForCachingHiveMetastore;
import com.facebook.presto.hive.HiveTableHandle;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.spi.constraints.TableConstraint;
import com.facebook.presto.spi.security.PrestoPrincipal;
import com.facebook.presto.spi.security.RoleGrant;
import com.facebook.presto.spi.statistics.ColumnStatisticType;
import io.airlift.units.Duration;
import org.weakref.jmx.Managed;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Function;

import static com.facebook.presto.hive.metastore.CachingHiveMetastore.MetastoreCacheScope.ALL;
import static com.facebook.presto.hive.metastore.NoopMetastoreCacheStats.NOOP_METASTORE_CACHE_STATS;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static java.util.Objects.requireNonNull;

/**
 * Hive Metastore Cache
 */
@ThreadSafe
public class CachingHiveMetastore
        implements ExtendedHiveMetastore
{
    public enum MetastoreCacheScope
    {
        ALL, PARTITION
    }

    protected final ExtendedHiveMetastore delegate;
    protected final MetastoreCache metastoreCache;

    @Inject
    public CachingHiveMetastore(
            @ForCachingHiveMetastore ExtendedHiveMetastore delegate,
            MetastoreCache metastoreCache)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.metastoreCache = requireNonNull(metastoreCache, "metastoreCache is null");
    }

    public static CachingHiveMetastore memoizeMetastore(ExtendedHiveMetastore delegate, boolean isMetastoreImpersonationEnabled, long maximumSize, int partitionCacheMaxColumnCount)
    {
        return new CachingHiveMetastore(
                delegate,
                new InMemoryMetastoreCache(
                        delegate,
                        newDirectExecutorService(),
                        isMetastoreImpersonationEnabled,
                        OptionalLong.empty(),
                        OptionalLong.empty(),
                        maximumSize,
                        false,
                        ALL,
                        0.0,
                        partitionCacheMaxColumnCount,
                        NOOP_METASTORE_CACHE_STATS));
    }

    @Managed
    public void flushCache()
    {
        metastoreCache.invalidateAll();
    }

    @Override
    public Optional<Database> getDatabase(MetastoreContext metastoreContext, String databaseName)
    {
        return metastoreCache.getDatabase(metastoreContext, databaseName);
    }

    @Override
    public List<String> getAllDatabases(MetastoreContext metastoreContext)
    {
        return metastoreCache.getAllDatabases(metastoreContext);
    }

    @Override
    public Optional<Table> getTable(MetastoreContext metastoreContext, String databaseName, String tableName)
    {
        return getTable(metastoreContext, new HiveTableHandle(databaseName, tableName));
    }

    @Override
    public Optional<Table> getTable(MetastoreContext metastoreContext, HiveTableHandle hiveTableHandle)
    {
        return metastoreCache.getTable(metastoreContext, hiveTableHandle);
    }

    @Override
    public List<TableConstraint<String>> getTableConstraints(MetastoreContext metastoreContext, String databaseName, String tableName)
    {
        return metastoreCache.getTableConstraints(metastoreContext, databaseName, tableName);
    }

    @Override
    public Set<ColumnStatisticType> getSupportedColumnStatistics(MetastoreContext metastoreContext, Type type)
    {
        return delegate.getSupportedColumnStatistics(metastoreContext, type);
    }

    @Override
    public PartitionStatistics getTableStatistics(MetastoreContext metastoreContext, String databaseName, String tableName)
    {
        return metastoreCache.getTableStatistics(metastoreContext, databaseName, tableName);
    }

    @Override
    public Map<String, PartitionStatistics> getPartitionStatistics(MetastoreContext metastoreContext, String databaseName, String tableName, Set<String> partitionNames)
    {
        return metastoreCache.getPartitionStatistics(metastoreContext, databaseName, tableName, partitionNames);
    }

    @Override
    public void updateTableStatistics(MetastoreContext metastoreContext, String databaseName, String tableName, Function<PartitionStatistics, PartitionStatistics> update)
    {
        try {
            delegate.updateTableStatistics(metastoreContext, databaseName, tableName, update);
        }
        finally {
            metastoreCache.invalidateTableStatisticsCache(databaseName, tableName);
        }
    }

    @Override
    public void updatePartitionStatistics(MetastoreContext metastoreContext, String databaseName, String tableName, String partitionName, Function<PartitionStatistics, PartitionStatistics> update)
    {
        try {
            delegate.updatePartitionStatistics(metastoreContext, databaseName, tableName, partitionName, update);
        }
        finally {
            metastoreCache.invalidatePartitionStatisticsCache(databaseName, tableName, partitionName);
        }
    }

    @Override
    public Optional<List<String>> getAllTables(MetastoreContext metastoreContext, String databaseName)
    {
        return metastoreCache.getAllTables(metastoreContext, databaseName);
    }

    @Override
    public Optional<List<String>> getAllViews(MetastoreContext metastoreContext, String databaseName)
    {
        return metastoreCache.getAllViews(metastoreContext, databaseName);
    }

    @Override
    public void createDatabase(MetastoreContext metastoreContext, Database database)
    {
        try {
            delegate.createDatabase(metastoreContext, database);
        }
        finally {
            metastoreCache.invalidateDatabaseCache(database.getDatabaseName());
        }
    }

    @Override
    public void dropDatabase(MetastoreContext metastoreContext, String databaseName)
    {
        try {
            delegate.dropDatabase(metastoreContext, databaseName);
        }
        finally {
            metastoreCache.invalidateDatabaseCache(databaseName);
        }
    }

    @Override
    public void renameDatabase(MetastoreContext metastoreContext, String databaseName, String newDatabaseName)
    {
        try {
            delegate.renameDatabase(metastoreContext, databaseName, newDatabaseName);
        }
        finally {
            metastoreCache.invalidateDatabaseCache(databaseName);
            metastoreCache.invalidateDatabaseCache(newDatabaseName);
        }
    }

    @Override
    public MetastoreOperationResult createTable(MetastoreContext metastoreContext, Table table, PrincipalPrivileges principalPrivileges, List<TableConstraint<String>> constraints)
    {
        try {
            return delegate.createTable(metastoreContext, table, principalPrivileges, constraints);
        }
        finally {
            metastoreCache.invalidateTableCache(table.getDatabaseName(), table.getTableName());
        }
    }

    @Override
    public void dropTable(MetastoreContext metastoreContext, String databaseName, String tableName, boolean deleteData)
    {
        try {
            delegate.dropTable(metastoreContext, databaseName, tableName, deleteData);
        }
        finally {
            metastoreCache.invalidateTableCache(databaseName, tableName);
        }
    }

    @Override
    public void dropTableFromMetastore(MetastoreContext metastoreContext, String databaseName, String tableName)
    {
        try {
            delegate.dropTableFromMetastore(metastoreContext, databaseName, tableName);
        }
        finally {
            metastoreCache.invalidateTableCache(databaseName, tableName);
        }
    }

    @Override
    public MetastoreOperationResult replaceTable(MetastoreContext metastoreContext, String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges)
    {
        try {
            return delegate.replaceTable(metastoreContext, databaseName, tableName, newTable, principalPrivileges);
        }
        finally {
            metastoreCache.invalidateTableCache(databaseName, tableName);
            metastoreCache.invalidateTableCache(newTable.getDatabaseName(), newTable.getTableName());
        }
    }

    @Override
    public MetastoreOperationResult renameTable(MetastoreContext metastoreContext, String databaseName, String tableName, String newDatabaseName, String newTableName)
    {
        try {
            return delegate.renameTable(metastoreContext, databaseName, tableName, newDatabaseName, newTableName);
        }
        finally {
            metastoreCache.invalidateTableCache(databaseName, tableName);
            metastoreCache.invalidateTableCache(newDatabaseName, newTableName);
        }
    }

    @Override
    public MetastoreOperationResult addColumn(MetastoreContext metastoreContext, String databaseName, String tableName, String columnName, HiveType columnType, String columnComment)
    {
        try {
            return delegate.addColumn(metastoreContext, databaseName, tableName, columnName, columnType, columnComment);
        }
        finally {
            metastoreCache.invalidateTableCache(databaseName, tableName);
        }
    }

    @Override
    public MetastoreOperationResult renameColumn(MetastoreContext metastoreContext, String databaseName, String tableName, String oldColumnName, String newColumnName)
    {
        try {
            return delegate.renameColumn(metastoreContext, databaseName, tableName, oldColumnName, newColumnName);
        }
        finally {
            metastoreCache.invalidateTableCache(databaseName, tableName);
        }
    }

    @Override
    public MetastoreOperationResult dropColumn(MetastoreContext metastoreContext, String databaseName, String tableName, String columnName)
    {
        try {
            return delegate.dropColumn(metastoreContext, databaseName, tableName, columnName);
        }
        finally {
            metastoreCache.invalidateTableCache(databaseName, tableName);
        }
    }

    @Override
    public Optional<Partition> getPartition(MetastoreContext metastoreContext, String databaseName, String tableName, List<String> partitionValues)
    {
        return metastoreCache.getPartition(metastoreContext, databaseName, tableName, partitionValues);
    }

    @Override
    public Optional<List<String>> getPartitionNames(MetastoreContext metastoreContext, String databaseName, String tableName)
    {
        return metastoreCache.getPartitionNames(metastoreContext, databaseName, tableName);
    }

    @Override
    public List<String> getPartitionNamesByFilter(
            MetastoreContext metastoreContext,
            String databaseName,
            String tableName,
            Map<Column, Domain> partitionPredicates)
    {
        return metastoreCache.getPartitionNamesByFilter(metastoreContext, databaseName, tableName, partitionPredicates);
    }

    @Override
    public List<PartitionNameWithVersion> getPartitionNamesWithVersionByFilter(
            MetastoreContext metastoreContext,
            String databaseName,
            String tableName,
            Map<Column, Domain> partitionPredicates)
    {
        return delegate.getPartitionNamesWithVersionByFilter(metastoreContext, databaseName, tableName, partitionPredicates);
    }

    @Override
    public Map<String, Optional<Partition>> getPartitionsByNames(MetastoreContext metastoreContext, String databaseName, String tableName, List<String> partitionNames)
    {
        return metastoreCache.getPartitionsByNames(metastoreContext, databaseName, tableName, partitionNames);
    }

    @Override
    public MetastoreOperationResult addPartitions(MetastoreContext metastoreContext, String databaseName, String tableName, List<PartitionWithStatistics> partitions)
    {
        try {
            return delegate.addPartitions(metastoreContext, databaseName, tableName, partitions);
        }
        finally {
            // todo do we need to invalidate all partitions?
            metastoreCache.invalidatePartitionCache(databaseName, tableName);
        }
    }

    @Override
    public void dropPartition(MetastoreContext metastoreContext, String databaseName, String tableName, List<String> parts, boolean deleteData)
    {
        try {
            delegate.dropPartition(metastoreContext, databaseName, tableName, parts, deleteData);
        }
        finally {
            metastoreCache.invalidatePartitionCache(databaseName, tableName);
        }
    }

    @Override
    public MetastoreOperationResult alterPartition(MetastoreContext metastoreContext, String databaseName, String tableName, PartitionWithStatistics partition)
    {
        try {
            return delegate.alterPartition(metastoreContext, databaseName, tableName, partition);
        }
        finally {
            metastoreCache.invalidatePartitionCache(databaseName, tableName);
        }
    }

    @Override
    public void createRole(MetastoreContext metastoreContext, String role, String grantor)
    {
        try {
            delegate.createRole(metastoreContext, role, grantor);
        }
        finally {
            metastoreCache.invalidateRolesCache();
        }
    }

    @Override
    public void dropRole(MetastoreContext metastoreContext, String role)
    {
        try {
            delegate.dropRole(metastoreContext, role);
        }
        finally {
            metastoreCache.invalidateRolesCache();
            metastoreCache.invalidateRoleGrantsCache();
        }
    }

    @Override
    public Set<String> listRoles(MetastoreContext metastoreContext)
    {
        return metastoreCache.listRoles(metastoreContext);
    }

    @Override
    public void grantRoles(MetastoreContext metastoreContext, Set<String> roles, Set<PrestoPrincipal> grantees, boolean withAdminOption, PrestoPrincipal grantor)
    {
        try {
            delegate.grantRoles(metastoreContext, roles, grantees, withAdminOption, grantor);
        }
        finally {
            metastoreCache.invalidateRoleGrantsCache();
        }
    }

    @Override
    public void revokeRoles(MetastoreContext metastoreContext, Set<String> roles, Set<PrestoPrincipal> grantees, boolean adminOptionFor, PrestoPrincipal grantor)
    {
        try {
            delegate.revokeRoles(metastoreContext, roles, grantees, adminOptionFor, grantor);
        }
        finally {
            metastoreCache.invalidateRoleGrantsCache();
        }
    }

    @Override
    public Set<RoleGrant> listRoleGrants(MetastoreContext metastoreContext, PrestoPrincipal principal)
    {
        return metastoreCache.listRoleGrants(metastoreContext, principal);
    }

    @Override
    public void grantTablePrivileges(MetastoreContext metastoreContext, String databaseName, String tableName, PrestoPrincipal grantee, Set<HivePrivilegeInfo> privileges)
    {
        try {
            delegate.grantTablePrivileges(metastoreContext, databaseName, tableName, grantee, privileges);
        }
        finally {
            metastoreCache.invalidateTablePrivilegesCache(grantee, databaseName, tableName);
        }
    }

    @Override
    public void revokeTablePrivileges(MetastoreContext metastoreContext, String databaseName, String tableName, PrestoPrincipal grantee, Set<HivePrivilegeInfo> privileges)
    {
        try {
            delegate.revokeTablePrivileges(metastoreContext, databaseName, tableName, grantee, privileges);
        }
        finally {
            metastoreCache.invalidateTablePrivilegesCache(grantee, databaseName, tableName);
        }
    }

    @Override
    public Set<HivePrivilegeInfo> listTablePrivileges(MetastoreContext metastoreContext, String databaseName, String tableName, PrestoPrincipal principal)
    {
        return metastoreCache.listTablePrivileges(metastoreContext, databaseName, tableName, principal);
    }

    @Override
    public void setPartitionLeases(MetastoreContext metastoreContext, String databaseName, String tableName, Map<String, String> partitionNameToLocation, Duration leaseDuration)
    {
        delegate.setPartitionLeases(metastoreContext, databaseName, tableName, partitionNameToLocation, leaseDuration);
    }

    @Override
    public MetastoreOperationResult dropConstraint(MetastoreContext metastoreContext, String databaseName, String tableName, String constraintName)
    {
        try {
            return delegate.dropConstraint(metastoreContext, databaseName, tableName, constraintName);
        }
        finally {
            metastoreCache.invalidateTableCache(databaseName, tableName);
        }
    }

    @Override
    public MetastoreOperationResult addConstraint(MetastoreContext metastoreContext, String databaseName, String tableName, TableConstraint<String> tableConstraint)
    {
        try {
            return delegate.addConstraint(metastoreContext, databaseName, tableName, tableConstraint);
        }
        finally {
            metastoreCache.invalidateTableCache(databaseName, tableName);
        }
    }

    @Override
    public Optional<Long> lock(MetastoreContext metastoreContext, String databaseName, String tableName)
    {
        return metastoreCache.lock(metastoreContext, databaseName, tableName);
    }

    @Override
    public void unlock(MetastoreContext metastoreContext, long lockId)
    {
        delegate.unlock(metastoreContext, lockId);
    }
}
