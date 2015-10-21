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

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.Table;

import java.io.Closeable;
import java.util.List;

public interface HiveMetastoreClient extends Closeable
{
    List<String> getAllDatabases();
    Database getDatabase(String databaseName);
    List<String> getAllTables(String databaseName);
    List<String> getTableNamesByFilter(String databaseName, String filter);
    void createTable(Table table);
    void dropTable(String databaseName, String name, boolean deleteData);
    void alterTable(String databaseName, String tableName, Table newTable);
    Table getTable(String databaseName, String tableName);
    List<String> getPartitionNames(String databaseName, String tableName);
    List<String> getPartitionNamesPS(String databaseName, String tableName, List<String> partitionValues);
    int addPartitions(List<Partition> newPartitions);
    boolean dropPartition(String databaseName, String tableName, List<String> partitionValues, boolean deleteData);
    boolean dropPartitionByName(String databaseName, String tableName, String partitionName, boolean deleteData);
    Partition getPartitionByName(String databaseName, String tableName, String partitionName);
    List<Partition> getPartitionsByNames(String databaseName, String tableName, List<String> partitionNames);
    List<Role> listRoles(String principalName, PrincipalType principalType);
    PrincipalPrivilegeSet getPrivilegeSet(HiveObjectRef hiveObject, String userName, List<String> groupNames);
    void close();
}
