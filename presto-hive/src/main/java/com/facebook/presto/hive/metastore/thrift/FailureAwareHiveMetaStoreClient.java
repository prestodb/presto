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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class FailureAwareHiveMetaStoreClient
        implements HiveMetastoreClient
{
    private final HiveMetastoreClient delegate;
    private final ResponseHandle responseHandle;

    public FailureAwareHiveMetaStoreClient(HiveMetastoreClient client, ResponseHandle responseHandle)
    {
        this.delegate = requireNonNull(client, "client is null");
        this.responseHandle = requireNonNull(responseHandle, "responseHandle is null");
    }

    @VisibleForTesting
    public HiveMetastoreClient getDelegate()
    {
        return delegate;
    }

    @Override
    public void close()
    {
        delegate.close();
    }

    @Override
    public List<String> getAllDatabases()
            throws TException
    {
        return runWithHandle(delegate::getAllDatabases);
    }

    @Override
    public Database getDatabase(String databaseName)
            throws TException
    {
        return runWithHandle(() -> delegate.getDatabase(databaseName));
    }

    @Override
    public List<String> getAllTables(String databaseName)
            throws TException
    {
        return runWithHandle(() -> delegate.getAllTables(databaseName));
    }

    @Override
    public List<String> getTableNamesByFilter(String databaseName, String filter)
            throws TException
    {
        return runWithHandle(() -> delegate.getTableNamesByFilter(databaseName, filter));
    }

    @Override
    public void createDatabase(Database database)
            throws TException
    {
        runWithHandle(() -> delegate.createDatabase(database));
    }

    @Override
    public void dropDatabase(String databaseName, boolean deleteData, boolean cascade)
            throws TException
    {
        runWithHandle(() -> delegate.dropDatabase(databaseName, deleteData, cascade));
    }

    @Override
    public void alterDatabase(String databaseName, Database database)
            throws TException
    {
        runWithHandle(() -> delegate.alterDatabase(databaseName, database));
    }

    @Override
    public void createTable(Table table)
            throws TException
    {
        runWithHandle(() -> delegate.createTable(table));
    }

    @Override
    public void dropTable(String databaseName, String name, boolean deleteData)
            throws TException
    {
        runWithHandle(() -> delegate.dropTable(databaseName, name, deleteData));
    }

    @Override
    public void alterTable(String databaseName, String tableName, Table newTable)
            throws TException
    {
        runWithHandle(() -> delegate.alterTable(databaseName, tableName, newTable));
    }

    @Override
    public Table getTable(String databaseName, String tableName)
            throws TException
    {
        return runWithHandle(() -> delegate.getTable(databaseName, tableName));
    }

    @Override
    public List<FieldSchema> getFields(String databaseName, String tableName)
            throws TException
    {
        return runWithHandle(() -> delegate.getFields(databaseName, tableName));
    }

    @Override
    public List<ColumnStatisticsObj> getTableColumnStatistics(String databaseName, String tableName, List<String> columnNames)
            throws TException
    {
        return runWithHandle(() -> delegate.getTableColumnStatistics(databaseName, tableName, columnNames));
    }

    @Override
    public void setTableColumnStatistics(String databaseName, String tableName, List<ColumnStatisticsObj> statistics)
            throws TException
    {
        runWithHandle(() -> delegate.setTableColumnStatistics(databaseName, tableName, statistics));
    }

    @Override
    public void deleteTableColumnStatistics(String databaseName, String tableName, String columnName)
            throws TException
    {
        runWithHandle(() -> delegate.deleteTableColumnStatistics(databaseName, tableName, columnName));
    }

    @Override
    public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(String databaseName, String tableName, List<String> partitionNames, List<String> columnNames)
            throws TException
    {
        return runWithHandle(() -> delegate.getPartitionColumnStatistics(databaseName, tableName, partitionNames, columnNames));
    }

    @Override
    public void setPartitionColumnStatistics(String databaseName, String tableName, String partitionName, List<ColumnStatisticsObj> statistics)
            throws TException
    {
        runWithHandle(() -> delegate.setPartitionColumnStatistics(databaseName, tableName, partitionName, statistics));
    }

    @Override
    public void deletePartitionColumnStatistics(String databaseName, String tableName, String partitionName, String columnName)
            throws TException
    {
        runWithHandle(() -> delegate.deletePartitionColumnStatistics(databaseName, tableName, partitionName, columnName));
    }

    @Override
    public List<String> getPartitionNames(String databaseName, String tableName)
            throws TException
    {
        return runWithHandle(() -> delegate.getPartitionNames(databaseName, tableName));
    }

    @Override
    public List<String> getPartitionNamesFiltered(String databaseName, String tableName, List<String> partitionValues)
            throws TException
    {
        return runWithHandle(() -> delegate.getPartitionNamesFiltered(databaseName, tableName, partitionValues));
    }

    @Override
    public int addPartitions(List<Partition> newPartitions)
            throws TException
    {
        return runWithHandle(() -> delegate.addPartitions(newPartitions));
    }

    @Override
    public boolean dropPartition(String databaseName, String tableName, List<String> partitionValues, boolean deleteData)
            throws TException
    {
        return runWithHandle(() -> delegate.dropPartition(databaseName, tableName, partitionValues, deleteData));
    }

    @Override
    public void alterPartition(String databaseName, String tableName, Partition partition)
            throws TException
    {
        runWithHandle(() -> delegate.alterPartition(databaseName, tableName, partition));
    }

    @Override
    public Partition getPartition(String databaseName, String tableName, List<String> partitionValues)
            throws TException
    {
        return runWithHandle(() -> delegate.getPartition(databaseName, tableName, partitionValues));
    }

    @Override
    public List<Partition> getPartitionsByNames(String databaseName, String tableName, List<String> partitionNames)
            throws TException
    {
        return runWithHandle(() -> delegate.getPartitionsByNames(databaseName, tableName, partitionNames));
    }

    @Override
    public List<Role> listRoles(String principalName, PrincipalType principalType)
            throws TException
    {
        return runWithHandle(() -> delegate.listRoles(principalName, principalType));
    }

    @Override
    public PrincipalPrivilegeSet getPrivilegeSet(HiveObjectRef hiveObject, String userName, List<String> groupNames)
            throws TException
    {
        return runWithHandle(() -> delegate.getPrivilegeSet(hiveObject, userName, groupNames));
    }

    @Override
    public List<HiveObjectPrivilege> listPrivileges(String principalName, PrincipalType principalType, HiveObjectRef hiveObjectRef)
            throws TException
    {
        return runWithHandle(() -> delegate.listPrivileges(principalName, principalType, hiveObjectRef));
    }

    @Override
    public List<String> getRoleNames()
            throws TException
    {
        return runWithHandle(delegate::getRoleNames);
    }

    @Override
    public boolean grantPrivileges(PrivilegeBag privilegeBag)
            throws TException
    {
        return runWithHandle(() -> delegate.grantPrivileges(privilegeBag));
    }

    @Override
    public boolean revokePrivileges(PrivilegeBag privilegeBag)
            throws TException
    {
        return runWithHandle(() -> delegate.revokePrivileges(privilegeBag));
    }

    @Override
    public void setUGI(String userName)
            throws TException
    {
        runWithHandle(() -> delegate.setUGI(userName));
    }

    public interface ResponseHandle
    {
        void success();

        void failed(TException e);
    }

    public static final ResponseHandle NO_OPS_RESPONSE_HANDLE = new ResponseHandle()
    {
        @Override
        public void success()
        {
        }

        @Override
        public void failed(TException e)
        {
        }
    };

    private <T> T runWithHandle(ThrowingSupplier<T> supplier)
            throws TException
    {
        T t;
        try {
            t = supplier.get();
        }
        catch (TException e) {
            responseHandle.failed(e);
            throw e;
        }
        responseHandle.success();
        return t;
    }

    private void runWithHandle(ThrowingRunnable runnable)
            throws TException
    {
        try {
            runnable.run();
            responseHandle.success();
        }
        catch (TException e) {
            responseHandle.failed(e);
            throw e;
        }
    }

    private interface ThrowingSupplier<T>
    {
        T get()
                throws TException;
    }

    private interface ThrowingRunnable
    {
        void run()
                throws TException;
    }
}
