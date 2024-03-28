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
import com.facebook.presto.hive.HiveTableHandle;
import com.facebook.presto.spi.constraints.TableConstraint;
import com.facebook.presto.spi.security.PrestoPrincipal;
import com.facebook.presto.spi.security.RoleGrant;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Defines the contract for a metastore cache that supports operations for caching metastore data and invalidating it when necessary.
 * This interface is designed to be implemented by various caching strategies, including but not limited to in-memory caching and distributed caching systems.
 * The primary goal of implementing this interface is to enhance the performance of metastore operations by reducing the need for repetitive
 * and potentially expensive data retrieval operations.
 */
public interface MetastoreCache
{
    Optional<Database> getDatabase(
            MetastoreContext metastoreContext,
            String databaseName);

    List<String> getAllDatabases(MetastoreContext metastoreContext);

    Optional<Table> getTable(
            MetastoreContext metastoreContext,
            HiveTableHandle hiveTableHandle);

    Optional<List<String>> getAllTables(
            MetastoreContext metastoreContext,
            String databaseName);

    PartitionStatistics getTableStatistics(
            MetastoreContext metastoreContext,
            String databaseName,
            String tableName);

    List<TableConstraint<String>> getTableConstraints(
            MetastoreContext metastoreContext,
            String databaseName,
            String tableName);

    Map<String, PartitionStatistics> getPartitionStatistics(
            MetastoreContext metastoreContext,
            String databaseName,
            String tableName,
            Set<String> partitionNames);

    Optional<List<String>> getAllViews(
            MetastoreContext metastoreContext,
            String databaseName);

    Map<String, Optional<Partition>> getPartitionsByNames(
            MetastoreContext metastoreContext,
            String databaseName,
            String tableName,
            List<String> partitionNames);

    Optional<Partition> getPartition(
            MetastoreContext metastoreContext,
            String databaseName,
            String tableName,
            List<String> partitionValues);

    List<String> getPartitionNamesByFilter(
            MetastoreContext metastoreContext,
            String databaseName,
            String tableName,
            Map<Column, Domain> partitionPredicates);

    Optional<List<String>> getPartitionNames(
            MetastoreContext metastoreContext,
            String databaseName,
            String tableName);

    Set<HivePrivilegeInfo> listTablePrivileges(
            MetastoreContext metastoreContext,
            String databaseName,
            String tableName,
            PrestoPrincipal principal);

    Set<String> listRoles(MetastoreContext metastoreContext);

    Set<RoleGrant> listRoleGrants(
            MetastoreContext metastoreContext,
            PrestoPrincipal principal);

    Optional<Long> lock(
            MetastoreContext metastoreContext,
            String databaseName,
            String tableName);

    void invalidateAll();

    void invalidateTableStatisticsCache(
            String databaseName,
            String tableName);

    void invalidatePartitionStatisticsCache(
            String databaseName,
            String tableName,
            String partitionName);

    void invalidateDatabaseCache(String databaseName);

    void invalidatePartitionCache(
            String databaseName,
            String tableName);

    void invalidateTablePrivilegesCache(
            PrestoPrincipal grantee,
            String databaseName,
            String tableName);

    void invalidateTableCache(
            String databaseName,
            String tableName);

    void invalidateRolesCache();

    void invalidateRoleGrantsCache();
}
