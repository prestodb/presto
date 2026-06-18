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

import org.apache.hadoop.hive.metastore.api.CheckLockRequest;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.NotNullConstraintsResponse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysResponse;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.RolePrincipalGrant;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UniqueConstraintsResponse;
import org.apache.hadoop.hive.metastore.api.UnlockRequest;
import org.apache.thrift.TException;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface HiveMetastoreClient
        extends Closeable
{
    @Override
    void close();

    String getDelegationToken(String owner, String renewer)
            throws TException;

    List<String> getDatabases(String pattern)
            throws TException;

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

    void createTableWithConstraints(Table table, List<SQLPrimaryKey> primaryKeys, List<SQLUniqueConstraint> uniqueConstraints, List<SQLNotNullConstraint> notNullConstraints)
            throws TException;

    void dropTable(String databaseName, String name, boolean deleteData)
            throws TException;

    void alterTable(String databaseName, String tableName, Table newTable)
            throws TException;

    void alterTableWithEnvironmentContext(String databaseName, String tableName, Table newTable, EnvironmentContext context)
            throws TException;

    Table getTable(String databaseName, String tableName)
            throws TException;

    List<FieldSchema> getFields(String databaseName, String tableName)
            throws TException;

    List<ColumnStatisticsObj> getTableColumnStatistics(String databaseName, String tableName, List<String> columnNames)
            throws TException;

    void setTableColumnStatistics(String databaseName, String tableName, List<ColumnStatisticsObj> statistics)
            throws TException;

    void deleteTableColumnStatistics(String databaseName, String tableName, String columnName)
            throws TException;

    Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(String databaseName, String tableName, List<String> partitionNames, List<String> columnNames)
            throws TException;

    void setPartitionColumnStatistics(String databaseName, String tableName, String partitionName, List<ColumnStatisticsObj> statistics)
            throws TException;

    void deletePartitionColumnStatistics(String databaseName, String tableName, String partitionName, String columnName)
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

    List<HiveObjectPrivilege> listPrivileges(String principalName, PrincipalType principalType, HiveObjectRef hiveObjectRef)
            throws TException;

    List<String> getRoleNames()
            throws TException;

    void createRole(String role, String grantor)
            throws TException;

    void dropRole(String role)
            throws TException;

    boolean grantPrivileges(PrivilegeBag privilegeBag)
            throws TException;

    boolean revokePrivileges(PrivilegeBag privilegeBag)
            throws TException;

    void grantRole(String role, String granteeName, PrincipalType granteeType, String grantorName, PrincipalType grantorType, boolean grantOption)
            throws TException;

    void revokeRole(String role, String granteeName, PrincipalType granteeType, boolean grantOption)
            throws TException;

    List<RolePrincipalGrant> listRoleGrants(String name, PrincipalType principalType)
            throws TException;

    void setUGI(String userName)
            throws TException;

    LockResponse checkLock(CheckLockRequest request)
            throws TException;

    LockResponse lock(LockRequest request)
            throws TException;

    void unlock(UnlockRequest request)
            throws TException;

    Optional<PrimaryKeysResponse> getPrimaryKey(String dbName, String tableName)
            throws TException;

    Optional<UniqueConstraintsResponse> getUniqueConstraints(String catName, String dbName, String tableName)
            throws TException;

    Optional<NotNullConstraintsResponse> getNotNullConstraints(String catName, String dbName, String tableName)
            throws TException;

    void dropConstraint(String dbName, String tableName, String constraintName)
            throws TException;

    void addUniqueConstraint(List<SQLUniqueConstraint> constraint)
            throws TException;

    void addPrimaryKeyConstraint(List<SQLPrimaryKey> constraint)
            throws TException;

    void addNotNullConstraint(List<SQLNotNullConstraint> constraint)
            throws TException;
}
