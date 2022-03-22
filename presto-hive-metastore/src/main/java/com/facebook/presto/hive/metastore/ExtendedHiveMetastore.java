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
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.spi.security.PrestoPrincipal;
import com.facebook.presto.spi.security.RoleGrant;
import com.facebook.presto.spi.statistics.ColumnStatisticType;
import io.airlift.units.Duration;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

public interface ExtendedHiveMetastore
{
    Optional<Database> getDatabase(MetastoreContext metastoreContext, String databaseName);

    List<String> getAllDatabases(MetastoreContext metastoreContext);

    Optional<Table> getTable(MetastoreContext metastoreContext, String databaseName, String tableName);

    Set<ColumnStatisticType> getSupportedColumnStatistics(MetastoreContext metastoreContext, Type type);

    PartitionStatistics getTableStatistics(MetastoreContext metastoreContext, String databaseName, String tableName);

    Map<String, PartitionStatistics> getPartitionStatistics(MetastoreContext metastoreContext, String databaseName, String tableName, Set<String> partitionNames);

    void updateTableStatistics(MetastoreContext metastoreContext, String databaseName, String tableName, Function<PartitionStatistics, PartitionStatistics> update);

    void updatePartitionStatistics(MetastoreContext metastoreContext, String databaseName, String tableName, String partitionName, Function<PartitionStatistics, PartitionStatistics> update);

    Optional<List<String>> getAllTables(MetastoreContext metastoreContext, String databaseName);

    Optional<List<String>> getAllViews(MetastoreContext metastoreContext, String databaseName);

    void createDatabase(MetastoreContext metastoreContext, Database database);

    void dropDatabase(MetastoreContext metastoreContext, String databaseName);

    void renameDatabase(MetastoreContext metastoreContext, String databaseName, String newDatabaseName);

    void createTable(MetastoreContext metastoreContext, Table table, PrincipalPrivileges principalPrivileges);

    void dropTable(MetastoreContext metastoreContext, String databaseName, String tableName, boolean deleteData);

    /**
     * This should only be used if the semantic here is drop and add. Trying to
     * alter one field of a table object previously acquired from getTable is
     * probably not what you want.
     */
    void replaceTable(MetastoreContext metastoreContext, String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges);

    void renameTable(MetastoreContext metastoreContext, String databaseName, String tableName, String newDatabaseName, String newTableName);

    void addColumn(MetastoreContext metastoreContext, String databaseName, String tableName, String columnName, HiveType columnType, String columnComment);

    void renameColumn(MetastoreContext metastoreContext, String databaseName, String tableName, String oldColumnName, String newColumnName);

    void dropColumn(MetastoreContext metastoreContext, String databaseName, String tableName, String columnName);

    Optional<Partition> getPartition(MetastoreContext metastoreContext, String databaseName, String tableName, List<String> partitionValues);

    Optional<List<String>> getPartitionNames(MetastoreContext metastoreContext, String databaseName, String tableName);

    List<String> getPartitionNamesByFilter(
            MetastoreContext metastoreContext,
            String databaseName,
            String tableName,
            Map<Column, Domain> partitionPredicates);

    List<PartitionNameWithVersion> getPartitionNamesWithVersionByFilter(
            MetastoreContext metastoreContext,
            String databaseName,
            String tableName,
            Map<Column, Domain> partitionPredicates);

    Map<String, Optional<Partition>> getPartitionsByNames(MetastoreContext metastoreContext, String databaseName, String tableName, List<String> partitionNames);

    void addPartitions(MetastoreContext metastoreContext, String databaseName, String tableName, List<PartitionWithStatistics> partitions);

    void dropPartition(MetastoreContext metastoreContext, String databaseName, String tableName, List<String> parts, boolean deleteData);

    void alterPartition(MetastoreContext metastoreContext, String databaseName, String tableName, PartitionWithStatistics partition);

    void createRole(MetastoreContext metastoreContext, String role, String grantor);

    void dropRole(MetastoreContext metastoreContext, String role);

    Set<String> listRoles(MetastoreContext metastoreContext);

    void grantRoles(MetastoreContext metastoreContext, Set<String> roles, Set<PrestoPrincipal> grantees, boolean withAdminOption, PrestoPrincipal grantor);

    void revokeRoles(MetastoreContext metastoreContext, Set<String> roles, Set<PrestoPrincipal> grantees, boolean adminOptionFor, PrestoPrincipal grantor);

    Set<RoleGrant> listRoleGrants(MetastoreContext metastoreContext, PrestoPrincipal principal);

    void grantTablePrivileges(MetastoreContext metastoreContext, String databaseName, String tableName, PrestoPrincipal grantee, Set<HivePrivilegeInfo> privileges);

    void revokeTablePrivileges(MetastoreContext metastoreContext, String databaseName, String tableName, PrestoPrincipal grantee, Set<HivePrivilegeInfo> privileges);

    Set<HivePrivilegeInfo> listTablePrivileges(MetastoreContext metastoreContext, String databaseName, String tableName, PrestoPrincipal principal);

    void setPartitionLeases(MetastoreContext metastoreContext, String databaseName, String tableName, Map<String, String> partitionNameToLocation, Duration leaseDuration);
}
