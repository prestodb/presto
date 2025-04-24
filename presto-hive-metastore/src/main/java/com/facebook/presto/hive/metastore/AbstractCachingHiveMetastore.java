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

import com.facebook.airlift.units.Duration;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.hive.PartitionNameWithVersion;
import com.facebook.presto.spi.constraints.TableConstraint;
import com.facebook.presto.spi.security.PrestoPrincipal;
import com.facebook.presto.spi.statistics.ColumnStatisticType;
import com.google.errorprone.annotations.ThreadSafe;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Hive Metastore Cache
 */
@ThreadSafe
public abstract class AbstractCachingHiveMetastore
        implements ExtendedHiveMetastore
{
    public enum MetastoreCacheScope
    {
        ALL, PARTITION
    }

    public abstract ExtendedHiveMetastore getDelegate();

    @Override
    public Set<ColumnStatisticType> getSupportedColumnStatistics(MetastoreContext metastoreContext, Type type)
    {
        return getDelegate().getSupportedColumnStatistics(metastoreContext, type);
    }

    @Override
    public void createDatabase(MetastoreContext metastoreContext, Database database)
    {
        try {
            getDelegate().createDatabase(metastoreContext, database);
        }
        finally {
            invalidateDatabaseCache(database.getDatabaseName());
        }
    }

    @Override
    public void dropDatabase(MetastoreContext metastoreContext, String databaseName)
    {
        try {
            getDelegate().dropDatabase(metastoreContext, databaseName);
        }
        finally {
            invalidateDatabaseCache(databaseName);
        }
    }

    @Override
    public void renameDatabase(MetastoreContext metastoreContext, String databaseName, String newDatabaseName)
    {
        try {
            getDelegate().renameDatabase(metastoreContext, databaseName, newDatabaseName);
        }
        finally {
            invalidateDatabaseCache(databaseName);
            invalidateDatabaseCache(newDatabaseName);
        }
    }

    protected abstract void invalidateAll();

    protected abstract void invalidateDatabaseCache(String databaseName);

    @Override
    public MetastoreOperationResult createTable(MetastoreContext metastoreContext, Table table, PrincipalPrivileges principalPrivileges, List<TableConstraint<String>> constraints)
    {
        try {
            return getDelegate().createTable(metastoreContext, table, principalPrivileges, constraints);
        }
        finally {
            invalidateTableCache(table.getDatabaseName(), table.getTableName());
        }
    }

    @Override
    public void dropTable(MetastoreContext metastoreContext, String databaseName, String tableName, boolean deleteData)
    {
        try {
            getDelegate().dropTable(metastoreContext, databaseName, tableName, deleteData);
        }
        finally {
            invalidateTableCache(databaseName, tableName);
        }
    }

    @Override
    public void dropTableFromMetastore(MetastoreContext metastoreContext, String databaseName, String tableName)
    {
        try {
            getDelegate().dropTableFromMetastore(metastoreContext, databaseName, tableName);
        }
        finally {
            invalidateTableCache(databaseName, tableName);
        }
    }

    @Override
    public MetastoreOperationResult replaceTable(MetastoreContext metastoreContext, String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges)
    {
        try {
            return getDelegate().replaceTable(metastoreContext, databaseName, tableName, newTable, principalPrivileges);
        }
        finally {
            invalidateTableCache(databaseName, tableName);
            invalidateTableCache(newTable.getDatabaseName(), newTable.getTableName());
        }
    }

    @Override
    public MetastoreOperationResult renameTable(MetastoreContext metastoreContext, String databaseName, String tableName, String newDatabaseName, String newTableName)
    {
        try {
            return getDelegate().renameTable(metastoreContext, databaseName, tableName, newDatabaseName, newTableName);
        }
        finally {
            invalidateTableCache(databaseName, tableName);
            invalidateTableCache(newDatabaseName, newTableName);
        }
    }

    @Override
    public MetastoreOperationResult addColumn(MetastoreContext metastoreContext, String databaseName, String tableName, String columnName, HiveType columnType, String columnComment)
    {
        try {
            return getDelegate().addColumn(metastoreContext, databaseName, tableName, columnName, columnType, columnComment);
        }
        finally {
            invalidateTableCache(databaseName, tableName);
        }
    }

    @Override
    public MetastoreOperationResult renameColumn(MetastoreContext metastoreContext, String databaseName, String tableName, String oldColumnName, String newColumnName)
    {
        try {
            return getDelegate().renameColumn(metastoreContext, databaseName, tableName, oldColumnName, newColumnName);
        }
        finally {
            invalidateTableCache(databaseName, tableName);
        }
    }

    @Override
    public MetastoreOperationResult dropColumn(MetastoreContext metastoreContext, String databaseName, String tableName, String columnName)
    {
        try {
            return getDelegate().dropColumn(metastoreContext, databaseName, tableName, columnName);
        }
        finally {
            invalidateTableCache(databaseName, tableName);
        }
    }

    protected abstract void invalidateTableCache(String databaseName, String tableName);

    @Override
    public List<PartitionNameWithVersion> getPartitionNamesWithVersionByFilter(
            MetastoreContext metastoreContext,
            String databaseName,
            String tableName,
            Map<Column, Domain> partitionPredicates)
    {
        return getDelegate().getPartitionNamesWithVersionByFilter(metastoreContext, databaseName, tableName, partitionPredicates);
    }

    @Override
    public MetastoreOperationResult addPartitions(MetastoreContext metastoreContext, String databaseName, String tableName, List<PartitionWithStatistics> partitions)
    {
        try {
            return getDelegate().addPartitions(metastoreContext, databaseName, tableName, partitions);
        }
        finally {
            // todo do we need to invalidate all partitions?
            invalidatePartitionCache(databaseName, tableName);
        }
    }

    @Override
    public void dropPartition(MetastoreContext metastoreContext, String databaseName, String tableName, List<String> parts, boolean deleteData)
    {
        try {
            getDelegate().dropPartition(metastoreContext, databaseName, tableName, parts, deleteData);
        }
        finally {
            invalidatePartitionCache(databaseName, tableName);
        }
    }

    @Override
    public MetastoreOperationResult alterPartition(MetastoreContext metastoreContext, String databaseName, String tableName, PartitionWithStatistics partition)
    {
        try {
            return getDelegate().alterPartition(metastoreContext, databaseName, tableName, partition);
        }
        finally {
            invalidatePartitionCache(databaseName, tableName);
        }
    }

    @Override
    public void createRole(MetastoreContext metastoreContext, String role, String grantor)
    {
        try {
            getDelegate().createRole(metastoreContext, role, grantor);
        }
        finally {
            invalidateRolesCache();
        }
    }

    @Override
    public void dropRole(MetastoreContext metastoreContext, String role)
    {
        try {
            getDelegate().dropRole(metastoreContext, role);
        }
        finally {
            invalidateRolesCache();
            invalidateRoleGrantsCache();
        }
    }

    protected abstract void invalidateRoleGrantsCache();

    @Override
    public void grantRoles(MetastoreContext metastoreContext, Set<String> roles, Set<PrestoPrincipal> grantees, boolean withAdminOption, PrestoPrincipal grantor)
    {
        try {
            getDelegate().grantRoles(metastoreContext, roles, grantees, withAdminOption, grantor);
        }
        finally {
            invalidateRoleGrantsCache();
        }
    }

    @Override
    public void revokeRoles(MetastoreContext metastoreContext, Set<String> roles, Set<PrestoPrincipal> grantees, boolean adminOptionFor, PrestoPrincipal grantor)
    {
        try {
            getDelegate().revokeRoles(metastoreContext, roles, grantees, adminOptionFor, grantor);
        }
        finally {
            invalidateRoleGrantsCache();
        }
    }

    protected abstract void invalidateRolesCache();

    protected abstract void invalidatePartitionCache(String databaseName, String tableName);

    protected abstract void invalidateTablePrivilegesCache(PrestoPrincipal grantee, String databaseName, String tableName);

    @Override
    public void grantTablePrivileges(MetastoreContext metastoreContext, String databaseName, String tableName, PrestoPrincipal grantee, Set<HivePrivilegeInfo> privileges)
    {
        try {
            getDelegate().grantTablePrivileges(metastoreContext, databaseName, tableName, grantee, privileges);
        }
        finally {
            invalidateTablePrivilegesCache(grantee, databaseName, tableName);
        }
    }

    @Override
    public void revokeTablePrivileges(MetastoreContext metastoreContext, String databaseName, String tableName, PrestoPrincipal grantee, Set<HivePrivilegeInfo> privileges)
    {
        try {
            getDelegate().revokeTablePrivileges(metastoreContext, databaseName, tableName, grantee, privileges);
        }
        finally {
            invalidateTablePrivilegesCache(grantee, databaseName, tableName);
        }
    }

    @Override
    public void setPartitionLeases(MetastoreContext metastoreContext, String databaseName, String tableName, Map<String, String> partitionNameToLocation, Duration leaseDuration)
    {
        getDelegate().setPartitionLeases(metastoreContext, databaseName, tableName, partitionNameToLocation, leaseDuration);
    }

    @Override
    public MetastoreOperationResult dropConstraint(MetastoreContext metastoreContext, String databaseName, String tableName, String constraintName)
    {
        try {
            return getDelegate().dropConstraint(metastoreContext, databaseName, tableName, constraintName);
        }
        finally {
            invalidateTableCache(databaseName, tableName);
        }
    }

    @Override
    public MetastoreOperationResult addConstraint(MetastoreContext metastoreContext, String databaseName, String tableName, TableConstraint<String> tableConstraint)
    {
        try {
            return getDelegate().addConstraint(metastoreContext, databaseName, tableName, tableConstraint);
        }
        finally {
            invalidateTableCache(databaseName, tableName);
        }
    }

    @Override
    public void unlock(MetastoreContext metastoreContext, long lockId)
    {
        getDelegate().unlock(metastoreContext, lockId);
    }
}
