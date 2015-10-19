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
package com.facebook.presto.hive;

import com.facebook.presto.hive.metastore.HiveMetastoreClient;
import com.facebook.presto.hive.metastore.HiveMetastoreClientException;
import com.facebook.presto.hive.metastore.HiveMetastoreClientNoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;

import java.util.List;

public class ThriftHiveMetastoreClient implements HiveMetastoreClient
{
    private final TTransport transport;
    private final ThriftHiveMetastore.Client thriftMetastoreClient;

    public ThriftHiveMetastoreClient(TTransport transport)
    {
        this.transport = transport;
        this.thriftMetastoreClient = new ThriftHiveMetastore.Client(new TBinaryProtocol(transport));
    }

    public ThriftHiveMetastoreClient(TProtocol protocol)
    {
        this.transport = protocol.getTransport();
        this.thriftMetastoreClient = new ThriftHiveMetastore.Client(protocol);
    }

    @Override
    public void close()
    {
        transport.close();
    }

    @Override
    public List<String> getAllDatabases()
    {
        try {
            return thriftMetastoreClient.get_all_databases();
        }
        catch (TException e) {
            throw new HiveMetastoreClientException(e.getMessage());
        }
    }

    @Override
    public Database getDatabase(String dbName)
    {
        try {
            return thriftMetastoreClient.get_database(dbName);
        }
        catch (NoSuchObjectException e) {
            throw new HiveMetastoreClientNoSuchObjectException();
        }
        catch (TException e) {
            throw new HiveMetastoreClientException(e.getMessage());
        }
    }

    @Override
    public List<String> getAllTables(String databaseName)
    {
        try {
            return thriftMetastoreClient.get_all_tables(databaseName);
        }
        catch (NoSuchObjectException e) {
            throw new HiveMetastoreClientNoSuchObjectException();
        }
        catch (TException e) {
            throw new HiveMetastoreClientException(e.getMessage());
        }
    }

    @Override
    public List<String> getTableNamesByFilter(String databaseName, String filter)
    {
        try {
            return thriftMetastoreClient.get_table_names_by_filter(databaseName, filter, Short.MAX_VALUE);
        }
        catch (NoSuchObjectException e) {
            throw new HiveMetastoreClientNoSuchObjectException();
        }
        catch (TException e) {
            throw new HiveMetastoreClientException(e.getMessage());
        }
    }

    @Override
    public void createTable(Table table)
    {
        try {
            thriftMetastoreClient.create_table(table);
        }
        catch (TException e) {
            throw new HiveMetastoreClientException(e.getMessage());
        }
    }

    @Override
    public void dropTable(String databaseName, String name, boolean deleteData)
    {
        try {
            thriftMetastoreClient.drop_table(databaseName, name, deleteData);
        }
        catch (NoSuchObjectException e) {
            throw new HiveMetastoreClientNoSuchObjectException();
        }
        catch (TException e) {
            throw new HiveMetastoreClientException(e.getMessage());
        }
    }

    @Override
    public void alterTable(String databaseName, String tableName, Table newTable)
    {
        try {
            thriftMetastoreClient.alter_table(databaseName, tableName, newTable);
        }
        catch (NoSuchObjectException e) {
            throw new HiveMetastoreClientNoSuchObjectException();
        }
        catch (TException e) {
            throw new HiveMetastoreClientException(e.getMessage());
        }
    }

    @Override
    public Table getTable(String databaseName, String tableName)
    {
        try {
            return thriftMetastoreClient.get_table(databaseName, tableName);
        }
        catch (NoSuchObjectException e) {
            throw new HiveMetastoreClientNoSuchObjectException();
        }
        catch (TException e) {
            throw new HiveMetastoreClientException(e.getMessage());
        }
    }

    @Override
    public List<String> getPartitionNames(String databaseName, String tableName)
    {
        try {
            return thriftMetastoreClient.get_partition_names(databaseName, tableName, Short.MAX_VALUE);
        }
        catch (NoSuchObjectException e) {
            throw new HiveMetastoreClientNoSuchObjectException();
        }
        catch (TException e) {
            throw new HiveMetastoreClientException(e.getMessage());
        }
    }

    @Override
    public List<String> getPartitionNamesPS(String databaseName, String tableName, List<String> partitionValues)
    {
        try {
            return thriftMetastoreClient.get_partition_names_ps(databaseName, tableName, partitionValues, Short.MAX_VALUE);
        }
        catch (NoSuchObjectException e) {
            throw new HiveMetastoreClientNoSuchObjectException();
        }
        catch (TException e) {
            throw new HiveMetastoreClientException(e.getMessage());
        }
    }

    @Override
    public int addPartitions(List<Partition> newPartitions)
    {
        try {
            return thriftMetastoreClient.add_partitions(newPartitions);
        }
        catch (TException e) {
            throw new HiveMetastoreClientException(e.getMessage());
        }
    }

    @Override
    public boolean dropPartition(String databaseName, String tableName, List<String> partitionValues, boolean deleteData)
    {
        try {
            return thriftMetastoreClient.drop_partition(databaseName, tableName, partitionValues, deleteData);
        }
        catch (NoSuchObjectException e) {
            throw new HiveMetastoreClientNoSuchObjectException();
        }
        catch (TException e) {
            throw new HiveMetastoreClientException(e.getMessage());
        }
    }

    @Override
    public boolean dropPartitionByName(String databaseName, String tableName, String partitionName, boolean deleteData)
    {
        try {
            return thriftMetastoreClient.drop_partition_by_name(databaseName, tableName, partitionName, deleteData);
        }
        catch (NoSuchObjectException e) {
            throw new HiveMetastoreClientNoSuchObjectException();
        }
        catch (TException e) {
            throw new HiveMetastoreClientException(e.getMessage());
        }
    }

    @Override
    public Partition getPartitionByName(String databaseName, String tableName, String partitionName)
    {
        try {
            return thriftMetastoreClient.get_partition_by_name(databaseName, tableName, partitionName);
        }
        catch (NoSuchObjectException e) {
            throw new HiveMetastoreClientNoSuchObjectException();
        }
        catch (TException e) {
            throw new HiveMetastoreClientException(e.getMessage());
        }
    }

    @Override
    public List<Partition> getPartitionsByNames(String databaseName, String tableName, List<String> partitionNames)
    {
        try {
            return thriftMetastoreClient.get_partitions_by_names(databaseName, tableName, partitionNames);
        }
        catch (NoSuchObjectException e) {
            throw new HiveMetastoreClientNoSuchObjectException();
        }
        catch (TException e) {
            throw new HiveMetastoreClientException(e.getMessage());
        }
    }

    @Override
    public List<Role> listRoles(String principalName, PrincipalType principalType)
    {
        try {
            return thriftMetastoreClient.list_roles(principalName, principalType);
        }
        catch (NoSuchObjectException e) {
            throw new HiveMetastoreClientNoSuchObjectException();
        }
        catch (TException e) {
            throw new HiveMetastoreClientException(e.getMessage());
        }
    }

    @Override
    public PrincipalPrivilegeSet getPrivilegeSet(HiveObjectRef hiveObject, String userName, List<String> groupNames)
    {
        try {
            return thriftMetastoreClient.get_privilege_set(hiveObject, userName, groupNames);
        }
        catch (NoSuchObjectException e) {
            throw new HiveMetastoreClientNoSuchObjectException();
        }
        catch (TException e) {
            throw new HiveMetastoreClientException(e.getMessage());
        }
    }
}
