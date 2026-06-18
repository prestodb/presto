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

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hive.metastore.api.AddNotNullConstraintRequest;
import org.apache.hadoop.hive.metastore.api.AddPrimaryKeyRequest;
import org.apache.hadoop.hive.metastore.api.AddUniqueConstraintRequest;
import org.apache.hadoop.hive.metastore.api.CheckLockRequest;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DropConstraintRequest;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalRequest;
import org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalResponse;
import org.apache.hadoop.hive.metastore.api.GrantRevokeRoleRequest;
import org.apache.hadoop.hive.metastore.api.GrantRevokeRoleResponse;
import org.apache.hadoop.hive.metastore.api.GrantRevokeType;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NotNullConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.NotNullConstraintsResponse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysRequest;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysResponse;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.RolePrincipalGrant;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableStatsRequest;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.hive.metastore.api.UniqueConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.UniqueConstraintsResponse;
import org.apache.hadoop.hive.metastore.api.UnlockRequest;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.hive.MetadataUtils.constructSchemaName;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static org.apache.thrift.TApplicationException.UNKNOWN_METHOD;

public class ThriftHiveMetastoreClient
        implements HiveMetastoreClient
{
    private final TTransport transport;
    private final ThriftHiveMetastore.Client client;
    private final Optional<String> catalogName;

    public ThriftHiveMetastoreClient(TTransport transport, String catalogName)
    {
        this.transport = requireNonNull(transport, "transport is null");
        this.client = new ThriftHiveMetastore.Client(new TBinaryProtocol(transport));
        this.catalogName = Optional.ofNullable(catalogName);
    }

    public ThriftHiveMetastoreClient(TProtocol protocol, String catalogName)
    {
        this.transport = protocol.getTransport();
        this.client = new ThriftHiveMetastore.Client(protocol);
        this.catalogName = Optional.ofNullable(catalogName);
    }

    @Override
    public void close()
    {
        transport.close();
    }

    @Override
    public String getDelegationToken(String owner, String renewer)
            throws TException
    {
        return client.get_delegation_token(owner, renewer);
    }
    @Override
    public List<String> getDatabases(String pattern)
            throws TException
    {
        return client.get_databases(constructSchemaName(catalogName, pattern));
    }

    @Override
    public List<String> getAllDatabases()
            throws TException
    {
        if (catalogName.isPresent()) {
            return getDatabases(constructSchemaName(catalogName, null));
        }
        return client.get_all_databases();
    }

    @Override
    public Database getDatabase(String dbName)
            throws TException
    {
        return client.get_database(constructSchemaName(catalogName, dbName));
    }

    @Override
    public List<String> getAllTables(String databaseName)
            throws TException
    {
        return client.get_all_tables(constructSchemaName(catalogName, databaseName));
    }

    @Override
    public List<String> getTableNamesByFilter(String databaseName, String filter)
            throws TException
    {
        return client.get_table_names_by_filter(constructSchemaName(catalogName, databaseName), filter, (short) -1);
    }

    @Override
    public void createDatabase(Database database)
            throws TException
    {
        if (catalogName.isPresent()) {
            database.setCatalogName(catalogName.get());
        }
        client.create_database(database);
    }

    @Override
    public void dropDatabase(String databaseName, boolean deleteData, boolean cascade)
            throws TException
    {
        client.drop_database(constructSchemaName(catalogName, databaseName), deleteData, cascade);
    }

    @Override
    public void alterDatabase(String databaseName, Database database)
            throws TException
    {
        client.alter_database(constructSchemaName(catalogName, databaseName), database);
    }

    @Override
    public void createTable(Table table)
            throws TException
    {
        if (catalogName.isPresent()) {
            table.setCatName(catalogName.get());
        }
        client.create_table(table);
    }

    @Override
    public void createTableWithConstraints(Table table, List<SQLPrimaryKey> primaryKeys, List<SQLUniqueConstraint> uniqueConstraints, List<SQLNotNullConstraint> notNullConstraints)
            throws TException
    {
        if (catalogName.isPresent()) {
            table.setCatName(catalogName.get());
        }
        client.create_table_with_constraints(table, primaryKeys, emptyList(), uniqueConstraints, notNullConstraints, emptyList(), emptyList());
    }

    @Override
    public void dropTable(String databaseName, String name, boolean deleteData)
            throws TException
    {
        client.drop_table(constructSchemaName(catalogName, databaseName), name, deleteData);
    }

    @Override
    public void alterTable(String databaseName, String tableName, Table newTable)
            throws TException
    {
        client.alter_table(constructSchemaName(catalogName, databaseName), tableName, newTable);
    }

    @Override
    public void alterTableWithEnvironmentContext(String databaseName, String tableName, Table newTable, EnvironmentContext context)
            throws TException
    {
        client.alter_table_with_environment_context(constructSchemaName(catalogName, databaseName), tableName, newTable, context);
    }

    @Override
    public Table getTable(String databaseName, String tableName)
            throws TException
    {
        return client.get_table(constructSchemaName(catalogName, databaseName), tableName);
    }

    @Override
    public List<FieldSchema> getFields(String databaseName, String tableName)
            throws TException
    {
        return client.get_fields(constructSchemaName(catalogName, databaseName), tableName);
    }

    @Override
    public List<ColumnStatisticsObj> getTableColumnStatistics(String databaseName, String tableName, List<String> columnNames)
            throws TException
    {
        TableStatsRequest tableStatsRequest = new TableStatsRequest(databaseName, tableName, columnNames);
        if (catalogName.isPresent()) {
            tableStatsRequest.setCatName(catalogName.get());
        }
        return client.get_table_statistics_req(tableStatsRequest).getTableStats();
    }

    @Override
    public void setTableColumnStatistics(String databaseName, String tableName, List<ColumnStatisticsObj> statistics)
            throws TException
    {
        ColumnStatisticsDesc statisticsDescription = new ColumnStatisticsDesc(true, databaseName, tableName);
        if (catalogName.isPresent()) {
            statisticsDescription.setCatName(catalogName.get());
        }
        ColumnStatistics request = new ColumnStatistics(statisticsDescription, statistics);
        client.update_table_column_statistics(request);
    }

    @Override
    public void deleteTableColumnStatistics(String databaseName, String tableName, String columnName)
            throws TException
    {
        client.delete_table_column_statistics(constructSchemaName(catalogName, databaseName), tableName, columnName);
    }

    @Override
    public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(String databaseName, String tableName, List<String> partitionNames, List<String> columnNames)
            throws TException
    {
        PartitionsStatsRequest partitionsStatsRequest = new PartitionsStatsRequest(databaseName, tableName, columnNames, partitionNames);
        if (catalogName.isPresent()) {
            partitionsStatsRequest.setCatName(catalogName.get());
        }
        return client.get_partitions_statistics_req(partitionsStatsRequest).getPartStats();
    }

    @Override
    public void setPartitionColumnStatistics(String databaseName, String tableName, String partitionName, List<ColumnStatisticsObj> statistics)
            throws TException
    {
        ColumnStatisticsDesc statisticsDescription = new ColumnStatisticsDesc(false, databaseName, tableName);
        statisticsDescription.setPartName(partitionName);
        if (catalogName.isPresent()) {
            statisticsDescription.setCatName(catalogName.get());
        }
        ColumnStatistics request = new ColumnStatistics(statisticsDescription, statistics);
        client.update_partition_column_statistics(request);
    }

    @Override
    public void deletePartitionColumnStatistics(String databaseName, String tableName, String partitionName, String columnName)
            throws TException
    {
        client.delete_partition_column_statistics(constructSchemaName(catalogName, databaseName), tableName, partitionName, columnName);
    }

    @Override
    public List<String> getPartitionNames(String databaseName, String tableName)
            throws TException
    {
        return client.get_partition_names(constructSchemaName(catalogName, databaseName), tableName, (short) -1);
    }

    @Override
    public List<String> getPartitionNamesFiltered(String databaseName, String tableName, List<String> partitionValues)
            throws TException
    {
        return client.get_partition_names_ps(constructSchemaName(catalogName, databaseName), tableName, partitionValues, (short) -1);
    }

    @Override
    public int addPartitions(List<Partition> newPartitions)
            throws TException
    {
        // Check if catalog name is present, update each partition in the newPartitions list by setting its catalog name.
        if (catalogName.isPresent()) {
            String catalog = catalogName.get();
            for (Partition partition : newPartitions) {
                partition.setCatName(catalog);
            }
        }
        return client.add_partitions(newPartitions);
    }

    @Override
    public boolean dropPartition(String databaseName, String tableName, List<String> partitionValues, boolean deleteData)
            throws TException
    {
        return client.drop_partition(constructSchemaName(catalogName, databaseName), tableName, partitionValues, deleteData);
    }

    @Override
    public void alterPartition(String databaseName, String tableName, Partition partition)
            throws TException
    {
        client.alter_partition(constructSchemaName(catalogName, databaseName), tableName, partition);
    }

    @Override
    public Partition getPartition(String databaseName, String tableName, List<String> partitionValues)
            throws TException
    {
        return client.get_partition(constructSchemaName(catalogName, databaseName), tableName, partitionValues);
    }

    @Override
    public List<Partition> getPartitionsByNames(String databaseName, String tableName, List<String> partitionNames)
            throws TException
    {
        return client.get_partitions_by_names(constructSchemaName(catalogName, databaseName), tableName, partitionNames);
    }

    @Override
    public List<Role> listRoles(String principalName, PrincipalType principalType)
            throws TException
    {
        return client.list_roles(principalName, principalType);
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
    public void createRole(String roleName, String grantor)
            throws TException
    {
        Role role = new Role(roleName, 0, grantor);
        client.create_role(role);
    }

    @Override
    public void dropRole(String role)
            throws TException
    {
        client.drop_role(role);
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
    public void grantRole(String role, String granteeName, PrincipalType granteeType, String grantorName, PrincipalType grantorType, boolean grantOption)
            throws TException
    {
        List<RolePrincipalGrant> grants = listRoleGrants(granteeName, granteeType);
        for (RolePrincipalGrant grant : grants) {
            if (grant.getRoleName().equals(role)) {
                if (grant.isGrantOption() == grantOption) {
                    return;
                }
                if (!grant.isGrantOption() && grantOption) {
                    revokeRole(role, granteeName, granteeType, false);
                    break;
                }
            }
        }
        createGrant(role, granteeName, granteeType, grantorName, grantorType, grantOption);
    }

    private void createGrant(String role, String granteeName, PrincipalType granteeType, String grantorName, PrincipalType grantorType, boolean grantOption)
            throws TException
    {
        GrantRevokeRoleRequest request = new GrantRevokeRoleRequest();
        request.setRequestType(GrantRevokeType.GRANT);
        request.setRoleName(role);
        request.setPrincipalName(granteeName);
        request.setPrincipalType(granteeType);
        request.setGrantor(grantorName);
        request.setGrantorType(grantorType);
        request.setGrantOption(grantOption);
        GrantRevokeRoleResponse response = client.grant_revoke_role(request);
        if (!response.isSetSuccess()) {
            throw new MetaException("GrantRevokeResponse missing success field");
        }
    }

    @Override
    public void revokeRole(String role, String granteeName, PrincipalType granteeType, boolean grantOption)
            throws TException
    {
        List<RolePrincipalGrant> grants = listRoleGrants(granteeName, granteeType);
        RolePrincipalGrant currentGrant = null;
        for (RolePrincipalGrant grant : grants) {
            if (grant.getRoleName().equals(role)) {
                currentGrant = grant;
                break;
            }
        }

        if (currentGrant == null) {
            return;
        }

        if (!currentGrant.isGrantOption() && grantOption) {
            return;
        }

        removeGrant(role, granteeName, granteeType, grantOption);
    }

    private void removeGrant(String role, String granteeName, PrincipalType granteeType, boolean grantOption)
            throws TException
    {
        GrantRevokeRoleRequest request = new GrantRevokeRoleRequest();
        request.setRequestType(GrantRevokeType.REVOKE);
        request.setRoleName(role);
        request.setPrincipalName(granteeName);
        request.setPrincipalType(granteeType);
        request.setGrantOption(grantOption);
        GrantRevokeRoleResponse response = client.grant_revoke_role(request);
        if (!response.isSetSuccess()) {
            throw new MetaException("GrantRevokeResponse missing success field");
        }
    }

    @Override
    public List<RolePrincipalGrant> listRoleGrants(String principalName, PrincipalType principalType)
            throws TException
    {
        GetRoleGrantsForPrincipalRequest request = new GetRoleGrantsForPrincipalRequest(principalName, principalType);
        GetRoleGrantsForPrincipalResponse resp = client.get_role_grants_for_principal(request);
        return ImmutableList.copyOf(resp.getPrincipalGrants());
    }

    @Override
    public void setUGI(String userName)
            throws TException
    {
        client.set_ugi(userName, new ArrayList<>());
    }

    @Override
    public LockResponse checkLock(CheckLockRequest request)
            throws TException
    {
        return client.check_lock(request);
    }

    @Override
    public LockResponse lock(LockRequest request)
            throws TException
    {
        return client.lock(request);
    }

    @Override
    public void unlock(UnlockRequest request)
            throws TException
    {
        client.unlock(request);
    }

    public Optional<PrimaryKeysResponse> getPrimaryKey(String dbName, String tableName)
            throws TException
    {
        PrimaryKeysRequest pkRequest = new PrimaryKeysRequest(dbName, tableName);
        if (catalogName.isPresent()) {
            pkRequest.setCatName(catalogName.get());
        }

        try {
            return Optional.of(client.get_primary_keys(pkRequest));
        }
        catch (TApplicationException e) {
            // If we are talking to Hive version < 3 which doesn't support table constraints,
            // then we will get an UNKNOWN_METHOD error.
            if (e.getType() == UNKNOWN_METHOD) {
                return Optional.empty();
            }
            throw e;
        }
    }

    @Override
    public Optional<UniqueConstraintsResponse> getUniqueConstraints(String catName, String dbName, String tableName)
            throws TException
    {
        if (catalogName.isPresent()) {
            catName = catalogName.get();
        }
        UniqueConstraintsRequest uniqueConstraintsRequest = new UniqueConstraintsRequest(catName, dbName, tableName);

        try {
            return Optional.of(client.get_unique_constraints(uniqueConstraintsRequest));
        }
        catch (TApplicationException e) {
            if (e.getType() == UNKNOWN_METHOD) {
                return Optional.empty();
            }
            throw e;
        }
    }

    @Override
    public Optional<NotNullConstraintsResponse> getNotNullConstraints(String catName, String dbName, String tableName)
            throws TException
    {
        if (catalogName.isPresent()) {
            catName = catalogName.get();
        }
        NotNullConstraintsRequest notNullConstraintsRequest = new NotNullConstraintsRequest(catName, dbName, tableName);

        try {
            return Optional.of(client.get_not_null_constraints(notNullConstraintsRequest));
        }
        catch (TApplicationException e) {
            if (e.getType() == UNKNOWN_METHOD) {
                return Optional.empty();
            }
            throw e;
        }
    }

    @Override
    public void dropConstraint(String dbName, String tableName, String constraintName)
            throws TException
    {
        DropConstraintRequest dropConstraintRequest = new DropConstraintRequest(dbName, tableName, constraintName);
        if (catalogName.isPresent()) {
            dropConstraintRequest.setCatName(catalogName.get());
        }
        client.drop_constraint(dropConstraintRequest);
    }

    @Override
    public void addUniqueConstraint(List<SQLUniqueConstraint> constraint)
            throws TException
    {
        List<SQLUniqueConstraint> updatedConstraints = constraint;
        if (catalogName.isPresent()) {
            updatedConstraints = constraint.stream().map(uniqueConstraint -> {
                uniqueConstraint = uniqueConstraint.deepCopy();
                uniqueConstraint.setCatName(catalogName.get());
                return uniqueConstraint;
            })
            .collect(toImmutableList());
        }
        AddUniqueConstraintRequest addUniqueConstraintRequest = new AddUniqueConstraintRequest(updatedConstraints);
        client.add_unique_constraint(addUniqueConstraintRequest);
    }

    @Override
    public void addPrimaryKeyConstraint(List<SQLPrimaryKey> constraint)
            throws TException
    {
        List<SQLPrimaryKey> updatedConstraints = constraint;
        if (catalogName.isPresent()) {
            updatedConstraints = constraint.stream().map(primaryKeyConstraint -> {
                primaryKeyConstraint = primaryKeyConstraint.deepCopy();
                primaryKeyConstraint.setCatName(catalogName.get());
                return primaryKeyConstraint;
            })
            .collect(toImmutableList());
        }
        AddPrimaryKeyRequest addPrimaryKeyRequest = new AddPrimaryKeyRequest(updatedConstraints);
        client.add_primary_key(addPrimaryKeyRequest);
    }

    @Override
    public void addNotNullConstraint(List<SQLNotNullConstraint> constraint)
            throws TException
    {
        List<SQLNotNullConstraint> updatedConstraints = constraint;
        if (catalogName.isPresent()) {
            updatedConstraints = constraint.stream().map(notNullConstraint -> {
                notNullConstraint = notNullConstraint.deepCopy();
                notNullConstraint.setCatName(catalogName.get());
                return notNullConstraint;
            })
            .collect(toImmutableList());
        }
        AddNotNullConstraintRequest addNotNullConstraintRequest = new AddNotNullConstraintRequest(updatedConstraints);
        client.add_not_null_constraint(addNotNullConstraintRequest);
    }
}
