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
import com.facebook.presto.common.NotSupportedException;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.hive.HiveTableHandle;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.hive.PartitionNameWithVersion;
import com.facebook.presto.spi.constraints.TableConstraint;
import com.facebook.presto.spi.security.PrestoPrincipal;
import com.facebook.presto.spi.security.RoleGrant;
import com.facebook.presto.spi.statistics.ColumnStatisticType;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

public interface ExtendedHiveMetastore
{
    Optional<Database> getDatabase(MetastoreContext metastoreContext, String databaseName);

    List<String> getAllDatabases(MetastoreContext metastoreContext);

    Optional<Table> getTable(MetastoreContext metastoreContext, String databaseName, String tableName);

    /*
    If some extended hiveMetastore wants to get additional information available in the HiveTableHandle
    they can override this method to get the additional information.
     */
    default Optional<Table> getTable(MetastoreContext metastoreContext, HiveTableHandle hiveTableHandle)
    {
        return getTable(metastoreContext, hiveTableHandle.getSchemaName(), hiveTableHandle.getTableName());
    }

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

    MetastoreOperationResult createTable(MetastoreContext metastoreContext, Table table, PrincipalPrivileges principalPrivileges, List<TableConstraint<String>> constraints);

    void dropTable(MetastoreContext metastoreContext, String databaseName, String tableName, boolean deleteData);

    /**
     * Drop table from the metastore, but preserve the data. If no override
     * is available, default to dropTable with deleteData set to false.
     */
    default void dropTableFromMetastore(MetastoreContext metastoreContext, String databaseName, String tableName)
    {
        dropTable(metastoreContext, databaseName, tableName, false);
    }

    /**
     * This should only be used if the semantic here is drop and add. Trying to
     * alter one field of a table object previously acquired from getTable is
     * probably not what you want.
     */
    MetastoreOperationResult replaceTable(MetastoreContext metastoreContext, String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges);

    MetastoreOperationResult persistTable(MetastoreContext metastoreContext, String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges, Supplier<PartitionStatistics> update, Map<String, String> additionalParameters);

    /**
     * Atomically commit an Iceberg metadata location change using compare-and-swap
     * (CAS) semantics.
     *
     * <p>For Iceberg tables, the CAS guard applies to the table's
     * {@code metadata_location} parameter (the source of truth for the current
     * Iceberg metadata file pointer): the commit succeeds only if
     * {@code previousMetadataLocation} matches the current {@code metadata_location}
     * value in the metastore. Implementations may update other table fields as
     * required by the underlying metastore API; callers must not assume that only
     * a specific field changes.
     *
     * <p>This is more reliable than the {@code alter_table} path used by
     * {@link #persistTable} for Iceberg metadata commits, which has weak
     * last-writer-wins semantics under concurrent commits.
     *
     * <p>CAS failures (i.e. when {@code previousMetadataLocation} does not match
     * the metastore's current value) MUST be surfaced as a thrown exception.
     * Implementations must not silently no-op on a CAS mismatch — callers rely
     * on this for correctness.
     *
     * @param metastoreContext the metastore context
     * @param databaseName the database name
     * @param tableName the table name
     * @param newMetadataLocation the new Iceberg metadata file location to commit
     * @param previousMetadataLocation CAS guard: must match the current
     *     {@code metadata_location} value in the metastore
     * @return the metastore operation result
     * @throws UnsupportedOperationException if the metastore implementation does not support this API
     */
    default MetastoreOperationResult commitTableData(MetastoreContext metastoreContext, String databaseName, String tableName, String newMetadataLocation, String previousMetadataLocation)
    {
        throw new UnsupportedOperationException("commitTableData is not supported by this metastore implementation");
    }

    MetastoreOperationResult renameTable(MetastoreContext metastoreContext, String databaseName, String tableName, String newDatabaseName, String newTableName);

    MetastoreOperationResult addColumn(MetastoreContext metastoreContext, String databaseName, String tableName, String columnName, HiveType columnType, String columnComment);

    MetastoreOperationResult renameColumn(MetastoreContext metastoreContext, String databaseName, String tableName, String oldColumnName, String newColumnName);

    MetastoreOperationResult dropColumn(MetastoreContext metastoreContext, String databaseName, String tableName, String columnName);

    Optional<Partition> getPartition(MetastoreContext metastoreContext, String databaseName, String tableName, List<String> partitionValues);

    Optional<List<PartitionNameWithVersion>> getPartitionNames(MetastoreContext metastoreContext, String databaseName, String tableName);

    List<PartitionNameWithVersion> getPartitionNamesByFilter(
            MetastoreContext metastoreContext,
            String databaseName,
            String tableName,
            Map<Column, Domain> partitionPredicates);

    List<PartitionNameWithVersion> getPartitionNamesWithVersionByFilter(
            MetastoreContext metastoreContext,
            String databaseName,
            String tableName,
            Map<Column, Domain> partitionPredicates);

    Map<String, Optional<Partition>> getPartitionsByNames(MetastoreContext metastoreContext, String databaseName, String tableName, List<PartitionNameWithVersion> partitionNames);

    MetastoreOperationResult addPartitions(MetastoreContext metastoreContext, String databaseName, String tableName, List<PartitionWithStatistics> partitions);

    void dropPartition(MetastoreContext metastoreContext, String databaseName, String tableName, List<String> parts, boolean deleteData);

    MetastoreOperationResult alterPartition(MetastoreContext metastoreContext, String databaseName, String tableName, PartitionWithStatistics partition);

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

    default Optional<Long> lock(MetastoreContext metastoreContext, String databaseName, String tableName)
    {
        throw new NotSupportedException("Lock is not supported by default");
    }

    default void unlock(MetastoreContext metastoreContext, long lockId)
    {
        throw new NotSupportedException("Unlock is not supported by default");
    }

    default List<TableConstraint<String>> getTableConstraints(MetastoreContext metastoreContext, String schemaName, String tableName)
    {
        return ImmutableList.of();
    }

    // Different metastore systems could implement this commit batch size differently based on different underlying database capacity.
    // Default batch partition commit size is set to 10.
    default int getPartitionCommitBatchSize()
    {
        return 10;
    }

    MetastoreOperationResult dropConstraint(MetastoreContext metastoreContext, String databaseName, String tableName, String constraintName);

    MetastoreOperationResult addConstraint(MetastoreContext metastoreContext, String databaseName, String tableName, TableConstraint<String> tableConstraint);
}
