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
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class ThriftHiveMetastoreClient
        implements HiveMetastoreClient
{
    private final TTransport transport;
    private final ThriftHiveMetastore.Client client;

    public ThriftHiveMetastoreClient(TTransport transport)
    {
        this.transport = requireNonNull(transport, "transport is null");
        this.client = new ThriftHiveMetastore.Client(new TBinaryProtocol(transport));
    }

    public ThriftHiveMetastoreClient(TProtocol protocol)
    {
        this.transport = protocol.getTransport();
        this.client = new ThriftHiveMetastore.Client(protocol);
    }

    @Override
    public void close()
    {
        transport.close();
    }

    @Override
    public List<String> getAllDatabases()
            throws TException
    {
        return client.get_all_databases();
    }

    @Override
    public Database getDatabase(String dbName)
            throws TException
    {
        return client.get_database(dbName);
    }

    @Override
    public List<String> getAllTables(String databaseName)
            throws TException
    {
        return client.get_all_tables(databaseName);
    }

    @Override
    public List<String> getTableNamesByFilter(String databaseName, String filter)
            throws TException
    {
        return client.get_table_names_by_filter(databaseName, filter, (short) -1);
    }

    @Override
    public void createTable(Table table)
            throws TException
    {
        client.create_table(table);
    }

    @Override
    public void dropTable(String databaseName, String name, boolean deleteData)
            throws TException
    {
        client.drop_table(databaseName, name, deleteData);
    }

    @Override
    public void alterTable(String databaseName, String tableName, Table newTable)
            throws TException
    {
        client.alter_table(databaseName, tableName, newTable);
    }

    @Override
    public Table getTable(String databaseName, String tableName)
            throws TException
    {
        return client.get_table(databaseName, tableName);
    }

    @Override
    public List<String> getPartitionNames(String databaseName, String tableName)
            throws TException
    {
        return client.get_partition_names(databaseName, tableName, (short) -1);
    }

    @Override
    public List<String> getPartitionNamesFiltered(String databaseName, String tableName, List<String> partitionValues)
            throws TException
    {
        return client.get_partition_names_ps(databaseName, tableName, partitionValues, (short) -1);
    }

    @Override
    public int addPartitions(List<Partition> newPartitions)
            throws TException
    {
        return client.add_partitions(newPartitions);
    }

    @Override
    public boolean dropPartition(String databaseName, String tableName, List<String> partitionValues, boolean deleteData)
            throws TException
    {
        return client.drop_partition(databaseName, tableName, partitionValues, deleteData);
    }

    @Override
    public boolean dropPartitionByName(String databaseName, String tableName, String partitionName, boolean deleteData)
            throws TException
    {
        return client.drop_partition_by_name(databaseName, tableName, partitionName, deleteData);
    }

    @Override
    public Partition getPartitionByName(String databaseName, String tableName, String partitionName)
            throws TException
    {
        return client.get_partition_by_name(databaseName, tableName, partitionName);
    }

    @Override
    public List<Partition> getPartitionsByNames(String databaseName, String tableName, List<String> partitionNames)
            throws TException
    {
        return client.get_partitions_by_names(databaseName, tableName, partitionNames);
    }

    @Override
    public List<Role> listRoles(String principalName, PrincipalType principalType)
            throws TException
    {
        return client.list_roles(principalName, principalType);
    }

    @Override
    public PrincipalPrivilegeSet getPrivilegeSet(HiveObjectRef hiveObject, String userName, List<String> groupNames)
            throws TException
    {
        return client.get_privilege_set(hiveObject, userName, groupNames);
    }

    @Override
    public List<String> getRoleNames()
            throws TException
    {
        return client.get_role_names();
    }

    @Override
    public boolean grantPrivileges(PrivilegeBag privilegeBag)
            throws TException
    {
        return client.grant_privileges(privilegeBag);
    }

    @Override
    public boolean revokePrivileges(PrivilegeBag privilegeBag)
            throws TException
    {
        return client.revoke_privileges(privilegeBag);
    }
}
