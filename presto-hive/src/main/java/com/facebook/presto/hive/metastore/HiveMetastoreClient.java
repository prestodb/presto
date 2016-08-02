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
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import java.io.Closeable;
import java.util.List;

public interface HiveMetastoreClient
        extends Closeable
{
    @Override
    void close();

    List<String> getAllDatabases()
            throws TException;

    Database getDatabase(String databaseName)
            throws TException;

    List<String> getAllTables(String databaseName)
            throws TException;

    List<String> getTableNamesByFilter(String databaseName, String filter)
            throws TException;

    void createDatabase(Database database)
            throws TException;

    void dropDatabase(String databaseName, boolean deleteData, boolean cascade)
            throws TException;

    void alterDatabase(String databaseName, Database database)
            throws TException;

    void createTable(Table table)
            throws TException;

    void dropTable(String databaseName, String name, boolean deleteData)
            throws TException;

    void alterTable(String databaseName, String tableName, Table newTable)
            throws TException;

    Table getTable(String databaseName, String tableName)
            throws TException;

    List<String> getPartitionNames(String databaseName, String tableName)
            throws TException;

    List<String> getPartitionNamesFiltered(String databaseName, String tableName, List<String> partitionValues)
            throws TException;

    int addPartitions(List<Partition> newPartitions)
            throws TException;

    boolean dropPartition(String databaseName, String tableName, List<String> partitionValues, boolean deleteData)
            throws TException;

    void alterPartition(String databaseName, String tableName, Partition partition)
            throws TException;

    Partition getPartition(String databaseName, String tableName, List<String> partitionValues)
            throws TException;

    List<Partition> getPartitionsByNames(String databaseName, String tableName, List<String> partitionNames)
            throws TException;

    List<Role> listRoles(String principalName, PrincipalType principalType)
            throws TException;

    PrincipalPrivilegeSet getPrivilegeSet(HiveObjectRef hiveObject, String userName, List<String> groupNames)
            throws TException;

    List<String> getRoleNames()
            throws TException;

    boolean grantPrivileges(PrivilegeBag privilegeBag)
            throws TException;

    boolean revokePrivileges(PrivilegeBag privilegeBag)
            throws TException;
}
