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

import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableStatsRequest;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
    public void createDatabase(Database database)
            throws TException
    {
        client.create_database(database);
    }

    @Override
    public void dropDatabase(String databaseName, boolean deleteData, boolean cascade)
            throws TException
    {
        client.drop_database(databaseName, deleteData, cascade);
    }

    @Override
    public void alterDatabase(String databaseName, Database database)
            throws TException
    {
        client.alter_database(databaseName, database);
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
    public List<ColumnStatisticsObj> getTableColumnStatistics(String databaseName, String tableName, List<String> columnNames)
            throws TException
    {
        TableStatsRequest tableStatsRequest = new TableStatsRequest(databaseName, tableName, columnNames);
        return client.get_table_statistics_req(tableStatsRequest).getTableStats();
    }

    @Override
    public void setTableColumnStatistics(String databaseName, String tableName, List<ColumnStatisticsObj> statistics)
            throws TException
    {
        ColumnStatisticsDesc statisticsDescription = new ColumnStatisticsDesc(true, databaseName, tableName);
        ColumnStatistics request = new ColumnStatistics(statisticsDescription, statistics);
        client.update_table_column_statistics(request);
    }

    @Override
    public void deleteTableColumnStatistics(String databaseName, String tableName, String columnName)
            throws TException
    {
        client.delete_table_column_statistics(databaseName, tableName, columnName);
    }

    @Override
    public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(String databaseName, String tableName, List<String> partitionNames, List<String> columnNames)
            throws TException
    {
        PartitionsStatsRequest partitionsStatsRequest = new PartitionsStatsRequest(databaseName, tableName, columnNames, partitionNames);
        return client.get_partitions_statistics_req(partitionsStatsRequest).getPartStats();
    }

    @Override
    public void setPartitionColumnStatistics(String databaseName, String tableName, String partitionName, List<ColumnStatisticsObj> statistics)
            throws TException
    {
        ColumnStatisticsDesc statisticsDescription = new ColumnStatisticsDesc(false, databaseName, tableName);
        statisticsDescription.setPartName(partitionName);
        ColumnStatistics request = new ColumnStatistics(statisticsDescription, statistics);
        client.update_partition_column_statistics(request);
    }

    @Override
    public void deletePartitionColumnStatistics(String databaseName, String tableName, String partitionName, String columnName)
            throws TException
    {
        client.delete_partition_column_statistics(databaseName, tableName, partitionName, columnName);
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
    public void alterPartition(String databaseName, String tableName, Partition partition)
            throws TException
    {
        client.alter_partition(databaseName, tableName, partition);
    }

    @Override
    public Partition getPartition(String databaseName, String tableName, List<String> partitionValues)
            throws TException
    {
        return client.get_partition(databaseName, tableName, partitionValues);
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
    public List<HiveObjectPrivilege> listPrivileges(String principalName, PrincipalType principalType, HiveObjectRef hiveObjectRef)
            throws TException
    {
        return client.list_privileges(principalName, principalType, hiveObjectRef);
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

    @Override
    public void setUGI(String userName)
            throws TException
    {
        client.set_ugi(userName, new ArrayList<>());
    }
}
