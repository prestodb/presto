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
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.PartitionNameWithVersion;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.RolePrincipalGrant;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.hive.metastore.api.PrincipalType.ROLE;
import static org.apache.hadoop.hive.metastore.api.PrincipalType.USER;

public class MockHiveMetastoreClient
        implements HiveMetastoreClient
{
    public static final String TEST_DATABASE = "testdb";
    public static final String BAD_DATABASE = "baddb";
    public static final String TEST_TABLE = "testtbl";
    public static final String TEST_PARTITION1 = "key=testpartition1";
    public static final String TEST_PARTITION2 = "key=testpartition2";
    public static final List<String> TEST_PARTITION_VALUES1 = ImmutableList.of("testpartition1");
    public static final List<String> TEST_PARTITION_VALUES2 = ImmutableList.of("testpartition2");
    public static final PartitionNameWithVersion TEST_PARTITION_NAME_WITH_VERSION1 = new PartitionNameWithVersion(TEST_PARTITION1, 1);
    public static final PartitionNameWithVersion TEST_PARTITION_NAME_WITH_VERSION2 = new PartitionNameWithVersion(TEST_PARTITION2, 2);
    public static final List<String> TEST_ROLES = ImmutableList.of("testrole");
    public static final List<RolePrincipalGrant> TEST_ROLE_GRANTS = ImmutableList.of(
            new RolePrincipalGrant("role1", "user", USER, false, 0, "grantor1", USER),
            new RolePrincipalGrant("role2", "role1", ROLE, true, 0, "grantor2", ROLE));

    private static final StorageDescriptor DEFAULT_STORAGE_DESCRIPTOR =
            new StorageDescriptor(ImmutableList.of(), "", null, null, false, 0, new SerDeInfo(TEST_TABLE, null, ImmutableMap.of()), null, null, ImmutableMap.of());

    private final AtomicInteger accessCount = new AtomicInteger();
    private boolean throwException;

    public void setThrowException(boolean throwException)
    {
        this.throwException = throwException;
    }

    public int getAccessCount()
    {
        return accessCount.get();
    }

    @Override
    public List<String> getAllDatabases()
    {
        accessCount.incrementAndGet();
        if (throwException) {
            throw new IllegalStateException();
        }
        return ImmutableList.of(TEST_DATABASE);
    }

    @Override
    public List<String> getAllTables(String dbName)
    {
        accessCount.incrementAndGet();
        if (throwException) {
            throw new RuntimeException();
        }
        if (!dbName.equals(TEST_DATABASE)) {
            return ImmutableList.of(); // As specified by Hive specification
        }
        return ImmutableList.of(TEST_TABLE);
    }

    @Override
    public Database getDatabase(String name)
            throws TException
    {
        accessCount.incrementAndGet();
        if (throwException) {
            throw new RuntimeException();
        }
        if (!name.equals(TEST_DATABASE)) {
            throw new NoSuchObjectException();
        }
        return new Database(TEST_DATABASE, null, null, null);
    }

    @Override
    public Table getTable(String dbName, String tableName)
            throws TException
    {
        accessCount.incrementAndGet();
        if (throwException) {
            throw new RuntimeException();
        }
        if (!dbName.equals(TEST_DATABASE) || !tableName.equals(TEST_TABLE)) {
            throw new NoSuchObjectException();
        }
        return new Table(
                TEST_TABLE,
                TEST_DATABASE,
                "",
                0,
                0,
                0,
                DEFAULT_STORAGE_DESCRIPTOR,
                ImmutableList.of(new FieldSchema("key", "string", null)),
                null,
                "",
                "",
                TableType.MANAGED_TABLE.name());
    }

    @Override
    public List<FieldSchema> getFields(String databaseName, String tableName)
            throws TException
    {
        return ImmutableList.of(new FieldSchema("key", "string", null));
    }

    @Override
    public List<ColumnStatisticsObj> getTableColumnStatistics(String databaseName, String tableName, List<String> columnNames)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setTableColumnStatistics(String databaseName, String tableName, List<ColumnStatisticsObj> statistics)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteTableColumnStatistics(String databaseName, String tableName, String columnName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(String databaseName, String tableName, List<String> partitionNames, List<String> columnNames)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setPartitionColumnStatistics(String databaseName, String tableName, String partitionName, List<ColumnStatisticsObj> statistics)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deletePartitionColumnStatistics(String databaseName, String tableName, String partitionName, String columnName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> getTableNamesByFilter(String databaseName, String filter)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> getPartitionNames(String dbName, String tableName)
    {
        accessCount.incrementAndGet();
        if (throwException) {
            throw new RuntimeException();
        }
        if (!dbName.equals(TEST_DATABASE) || !tableName.equals(TEST_TABLE)) {
            return ImmutableList.of();
        }
        return ImmutableList.of(TEST_PARTITION1, TEST_PARTITION2);
    }

    @Override
    public List<String> getPartitionNamesFiltered(String dbName, String tableName, List<String> partValues)
            throws TException
    {
        accessCount.incrementAndGet();
        if (throwException) {
            throw new RuntimeException();
        }
        if (!dbName.equals(TEST_DATABASE) || !tableName.equals(TEST_TABLE)) {
            throw new NoSuchObjectException();
        }
        return ImmutableList.of(TEST_PARTITION1, TEST_PARTITION2);
    }

    public List<PartitionNameWithVersion> getPartitionNamesWithVersionByFilter(String dbName, String tableName, Map<Column, Domain> partitionPredicates)
            throws TException
    {
        accessCount.incrementAndGet();
        if (throwException) {
            throw new RuntimeException();
        }
        if (!dbName.equals(TEST_DATABASE) || !tableName.equals(TEST_TABLE)) {
            throw new NoSuchObjectException();
        }
        return ImmutableList.of(TEST_PARTITION_NAME_WITH_VERSION1, TEST_PARTITION_NAME_WITH_VERSION2);
    }

    @Override
    public Partition getPartition(String dbName, String tableName, List<String> partitionValues)
            throws TException
    {
        accessCount.incrementAndGet();
        if (throwException) {
            throw new RuntimeException();
        }
        if (!dbName.equals(TEST_DATABASE) || !tableName.equals(TEST_TABLE) || !ImmutableSet.of(TEST_PARTITION_VALUES1, TEST_PARTITION_VALUES2).contains(partitionValues)) {
            throw new NoSuchObjectException();
        }
        return new Partition(null, TEST_DATABASE, TEST_TABLE, 0, 0, DEFAULT_STORAGE_DESCRIPTOR, ImmutableMap.of());
    }

    @Override
    public List<Partition> getPartitionsByNames(String dbName, String tableName, List<String> names)
            throws TException
    {
        accessCount.incrementAndGet();
        if (throwException) {
            throw new RuntimeException();
        }
        if (!dbName.equals(TEST_DATABASE) || !tableName.equals(TEST_TABLE) || !ImmutableSet.of(TEST_PARTITION1, TEST_PARTITION2).containsAll(names)) {
            throw new NoSuchObjectException();
        }
        return Lists.transform(names, name -> {
            try {
                return new Partition(ImmutableList.copyOf(Warehouse.getPartValuesFromPartName(name)), TEST_DATABASE, TEST_TABLE, 0, 0, DEFAULT_STORAGE_DESCRIPTOR, ImmutableMap.of());
            }
            catch (MetaException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void createDatabase(Database database)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropDatabase(String databaseName, boolean deleteData, boolean cascade)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterDatabase(String databaseName, Database database)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createTable(Table table)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropTable(String databaseName, String name, boolean deleteData)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterTable(String databaseName, String tableName, Table newTable)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int addPartitions(List<Partition> newPartitions)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean dropPartition(String databaseName, String tableName, List<String> partitionValues, boolean deleteData)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterPartition(String databaseName, String tableName, Partition partition)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Role> listRoles(String principalName, PrincipalType principalType)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<HiveObjectPrivilege> listPrivileges(String principalName, PrincipalType principalType, HiveObjectRef hiveObjectRef)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> getRoleNames()
    {
        accessCount.incrementAndGet();
        if (throwException) {
            throw new IllegalStateException();
        }
        return TEST_ROLES;
    }

    @Override
    public void createRole(String role, String grantor)
            throws TException
    {
        // No-op
    }

    @Override
    public void dropRole(String role)
            throws TException
    {
        // No-op
    }

    @Override
    public boolean grantPrivileges(PrivilegeBag privilegeBag)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean revokePrivileges(PrivilegeBag privilegeBag)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void grantRole(String role, String granteeName, PrincipalType granteeType, String grantorName, PrincipalType grantorType, boolean grantOption)
            throws TException
    {
        // No-op
    }

    @Override
    public void revokeRole(String role, String granteeName, PrincipalType granteeType, boolean grantOption)
            throws TException
    {
        // No-op
    }

    @Override
    public List<RolePrincipalGrant> listRoleGrants(String name, PrincipalType principalType)
            throws TException
    {
        accessCount.incrementAndGet();
        if (throwException) {
            throw new IllegalStateException();
        }
        return TEST_ROLE_GRANTS;
    }

    @Override
    public void close()
    {
        // No-op
    }

    @Override
    public void setUGI(String userName)
    {
        // No-op
    }
}
