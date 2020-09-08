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
import com.facebook.presto.hive.metastore.PartitionNameWithVersion;
import com.facebook.presto.hive.metastore.PartitionStatistics;
import com.facebook.presto.hive.metastore.PartitionWithStatistics;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.security.PrestoPrincipal;
import com.facebook.presto.spi.security.RoleGrant;
import com.facebook.presto.spi.statistics.ColumnStatisticType;
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
    void createDatabase(Database database);

    void dropDatabase(String databaseName);

    void alterDatabase(String databaseName, Database database);

    void createTable(Table table);

    void dropTable(String databaseName, String tableName, boolean deleteData);

    void alterTable(String databaseName, String tableName, Table table);

    List<String> getAllDatabases();

    Optional<List<String>> getAllTables(String databaseName);

    Optional<List<String>> getAllViews(String databaseName);

    Optional<Database> getDatabase(String databaseName);

    void addPartitions(String databaseName, String tableName, List<PartitionWithStatistics> partitions);

    void dropPartition(String databaseName, String tableName, List<String> parts, boolean deleteData);

    void alterPartition(String databaseName, String tableName, PartitionWithStatistics partition);

    Optional<List<String>> getPartitionNames(String databaseName, String tableName);

    Optional<List<String>> getPartitionNamesByParts(String databaseName, String tableName, List<String> parts);

    List<String> getPartitionNamesByFilter(String databaseName, String tableName, Map<Column, Domain> partitionPredicates);

    default List<PartitionNameWithVersion> getPartitionNamesWithVersionByFilter(String databaseName, String tableName, Map<Column, Domain> partitionPredicates)
    {
        throw new UnsupportedOperationException();
    }

    Optional<Partition> getPartition(String databaseName, String tableName, List<String> partitionValues);

    List<Partition> getPartitionsByNames(String databaseName, String tableName, List<String> partitionNames);

    Optional<Table> getTable(String databaseName, String tableName);

    Set<ColumnStatisticType> getSupportedColumnStatistics(Type type);

    PartitionStatistics getTableStatistics(String databaseName, String tableName);

    Map<String, PartitionStatistics> getPartitionStatistics(String databaseName, String tableName, Set<String> partitionNames);

    void updateTableStatistics(String databaseName, String tableName, Function<PartitionStatistics, PartitionStatistics> update);

    void updatePartitionStatistics(String databaseName, String tableName, String partitionName, Function<PartitionStatistics, PartitionStatistics> update);

    void createRole(String role, String grantor);

    void dropRole(String role);

    Set<String> listRoles();

    void grantRoles(Set<String> roles, Set<PrestoPrincipal> grantees, boolean withAdminOption, PrestoPrincipal grantor);

    void revokeRoles(Set<String> roles, Set<PrestoPrincipal> grantees, boolean adminOptionFor, PrestoPrincipal grantor);

    Set<RoleGrant> listRoleGrants(PrestoPrincipal principal);

    void grantTablePrivileges(String databaseName, String tableName, PrestoPrincipal grantee, Set<HivePrivilegeInfo> privileges);

    void revokeTablePrivileges(String databaseName, String tableName, PrestoPrincipal grantee, Set<HivePrivilegeInfo> privileges);

    Set<HivePrivilegeInfo> listTablePrivileges(String databaseName, String tableName, PrestoPrincipal principal);

    default boolean isTableOwner(String user, String databaseName, String tableName)
    {
        // a table can only be owned by a user
        Optional<Table> table = getTable(databaseName, tableName);
        return table.isPresent() && user.equals(table.get().getOwner());
    }

    default Optional<List<FieldSchema>> getFields(String databaseName, String tableName)
    {
        Optional<Table> table = getTable(databaseName, tableName);
        if (!table.isPresent()) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }

        if (table.get().getSd() == null) {
            throw new PrestoException(HIVE_INVALID_METADATA, "Table is missing storage descriptor");
        }

        return Optional.of(table.get().getSd().getCols());
    }
}
