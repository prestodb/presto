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

import com.facebook.presto.hive.HiveType;
import com.facebook.presto.hive.PartitionStatistics;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

public interface ExtendedHiveMetastore
{
    Optional<Database> getDatabase(String databaseName);

    List<String> getAllDatabases();

    Optional<Table> getTable(String databaseName, String tableName);

    boolean supportsColumnStatistics();

    PartitionStatistics getTableStatistics(String databaseName, String tableName);

    Map<String, PartitionStatistics> getPartitionStatistics(String databaseName, String tableName, Set<String> partitionNames);

    void updateTableStatistics(String databaseName, String tableName, Function<PartitionStatistics, PartitionStatistics> update);

    void updatePartitionStatistics(String databaseName, String tableName, String partitionName, Function<PartitionStatistics, PartitionStatistics> update);

    Optional<List<String>> getAllTables(String databaseName);

    Optional<List<String>> getAllViews(String databaseName);

    void createDatabase(Database database);

    void dropDatabase(String databaseName);

    void renameDatabase(String databaseName, String newDatabaseName);

    void createTable(Table table, PrincipalPrivileges principalPrivileges);

    void dropTable(String databaseName, String tableName, boolean deleteData);

    /**
     * This should only be used if the semantic here is drop and add. Trying to
     * alter one field of a table object previously acquired from getTable is
     * probably not what you want.
     */
    void replaceTable(String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges);

    void renameTable(String databaseName, String tableName, String newDatabaseName, String newTableName);

    void updateTableParameters(String databaseName, String tableName, Function<Map<String, String>, Map<String, String>> update);

    void addColumn(String databaseName, String tableName, String columnName, HiveType columnType, String columnComment);

    void renameColumn(String databaseName, String tableName, String oldColumnName, String newColumnName);

    void dropColumn(String databaseName, String tableName, String columnName);

    Optional<Partition> getPartition(String databaseName, String tableName, List<String> partitionValues);

    Optional<List<String>> getPartitionNames(String databaseName, String tableName);

    Optional<List<String>> getPartitionNamesByParts(String databaseName, String tableName, List<String> parts);

    Map<String, Optional<Partition>> getPartitionsByNames(String databaseName, String tableName, List<String> partitionNames);

    /**
     * Adds partitions to the table in a single atomic task.  The implementation
     * must either add all partitions and return normally, or add no partitions and
     * throw an exception.
     */
    void addPartitions(String databaseName, String tableName, List<Partition> partitions);

    void dropPartition(String databaseName, String tableName, List<String> parts, boolean deleteData);

    void alterPartition(String databaseName, String tableName, Partition partition);

    void updatePartitionParameters(String databaseName, String tableName, List<String> partitionValues, Function<Map<String, String>, Map<String, String>> update);

    Set<String> getRoles(String user);

    Set<HivePrivilegeInfo> getDatabasePrivileges(String user, String databaseName);

    Set<HivePrivilegeInfo> getTablePrivileges(String user, String databaseName, String tableName);

    void grantTablePrivileges(String databaseName, String tableName, String grantee, Set<HivePrivilegeInfo> privileges);

    void revokeTablePrivileges(String databaseName, String tableName, String grantee, Set<HivePrivilegeInfo> privileges);
}
