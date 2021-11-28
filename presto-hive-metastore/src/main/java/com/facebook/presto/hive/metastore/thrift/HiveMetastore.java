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
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.HivePrivilegeInfo;
import com.facebook.presto.hive.metastore.MetastoreContext;
import com.facebook.presto.hive.metastore.PartitionNameWithVersion;
import com.facebook.presto.hive.metastore.PartitionStatistics;
import com.facebook.presto.hive.metastore.PartitionWithStatistics;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.security.PrestoPrincipal;
import com.facebook.presto.spi.security.RoleGrant;
import com.facebook.presto.spi.statistics.ColumnStatisticType;
import io.airlift.units.Duration;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_INVALID_METADATA;

public interface HiveMetastore
{
    void createDatabase(MetastoreContext metastoreContext, Database database);

    void dropDatabase(MetastoreContext metastoreContext, String databaseName);

    void alterDatabase(MetastoreContext metastoreContext, String databaseName, Database database);

    void createTable(MetastoreContext metastoreContext, Table table);

    void dropTable(MetastoreContext metastoreContext, String databaseName, String tableName, boolean deleteData);

    void alterTable(MetastoreContext metastoreContext, String databaseName, String tableName, Table table);

    List<String> getAllDatabases(MetastoreContext metastoreContext);

    Optional<List<String>> getAllTables(MetastoreContext metastoreContext, String databaseName);

    Optional<List<String>> getAllViews(MetastoreContext metastoreContext, String databaseName);

    Optional<Database> getDatabase(MetastoreContext metastoreContext, String databaseName);

    void addPartitions(MetastoreContext metastoreContext, String databaseName, String tableName, List<PartitionWithStatistics> partitions);

    void dropPartition(MetastoreContext metastoreContext, String databaseName, String tableName, List<String> parts, boolean deleteData);

    void alterPartition(MetastoreContext metastoreContext, String databaseName, String tableName, PartitionWithStatistics partition);

    Optional<List<String>> getPartitionNames(MetastoreContext metastoreContext, String databaseName, String tableName);

    Optional<List<String>> getPartitionNamesByParts(MetastoreContext metastoreContext, String databaseName, String tableName, List<String> parts);

    List<String> getPartitionNamesByFilter(MetastoreContext metastoreContext, String databaseName, String tableName, Map<Column, Domain> partitionPredicates);

    default List<PartitionNameWithVersion> getPartitionNamesWithVersionByFilter(MetastoreContext metastoreContext, String databaseName, String tableName, Map<Column, Domain> partitionPredicates)
    {
        throw new UnsupportedOperationException();
    }

    Optional<Partition> getPartition(MetastoreContext metastoreContext, String databaseName, String tableName, List<String> partitionValues);

    List<Partition> getPartitionsByNames(MetastoreContext metastoreContext, String databaseName, String tableName, List<String> partitionNames);

    Optional<Table> getTable(MetastoreContext metastoreContext, String databaseName, String tableName);

    Set<ColumnStatisticType> getSupportedColumnStatistics(MetastoreContext metastoreContext, Type type);

    PartitionStatistics getTableStatistics(MetastoreContext metastoreContext, String databaseName, String tableName);

    Map<String, PartitionStatistics> getPartitionStatistics(MetastoreContext metastoreContext, String databaseName, String tableName, Set<String> partitionNames);

    void updateTableStatistics(MetastoreContext metastoreContext, String databaseName, String tableName, Function<PartitionStatistics, PartitionStatistics> update);

    void updatePartitionStatistics(MetastoreContext metastoreContext, String databaseName, String tableName, String partitionName, Function<PartitionStatistics, PartitionStatistics> update);

    void createRole(MetastoreContext metastoreContext, String role, String grantor);

    void dropRole(MetastoreContext metastoreContext, String role);

    Set<String> listRoles(MetastoreContext metastoreContext);

    void grantRoles(MetastoreContext metastoreContext, Set<String> roles, Set<PrestoPrincipal> grantees, boolean withAdminOption, PrestoPrincipal grantor);

    void revokeRoles(MetastoreContext metastoreContext, Set<String> roles, Set<PrestoPrincipal> grantees, boolean adminOptionFor, PrestoPrincipal grantor);

    Set<RoleGrant> listRoleGrants(MetastoreContext metastoreContext, PrestoPrincipal principal);

    void grantTablePrivileges(MetastoreContext metastoreContext, String databaseName, String tableName, PrestoPrincipal grantee, Set<HivePrivilegeInfo> privileges);

    void revokeTablePrivileges(MetastoreContext metastoreContext, String databaseName, String tableName, PrestoPrincipal grantee, Set<HivePrivilegeInfo> privileges);

    Set<HivePrivilegeInfo> listTablePrivileges(MetastoreContext metastoreContext, String databaseName, String tableName, PrestoPrincipal principal);

    default void setPartitionLeases(MetastoreContext metastoreContext, String databaseName, String tableName, Map<String, String> partitionNameToLocation, Duration leaseDuration)
    {
        throw new UnsupportedOperationException();
    }

    default boolean isTableOwner(MetastoreContext metastoreContext, String user, String databaseName, String tableName)
    {
        // a table can only be owned by a user
        Optional<Table> table = getTable(metastoreContext, databaseName, tableName);
        return table.isPresent() && user.equals(table.get().getOwner());
    }

    default Optional<List<FieldSchema>> getFields(MetastoreContext metastoreContext, String databaseName, String tableName)
    {
        Optional<Table> table = getTable(metastoreContext, databaseName, tableName);
        if (!table.isPresent()) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }

        if (table.get().getSd() == null) {
            throw new PrestoException(HIVE_INVALID_METADATA, "Table is missing storage descriptor");
        }

        return Optional.of(table.get().getSd().getCols());
    }
}
