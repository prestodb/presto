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

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class MockHiveMetastoreClient
        implements HiveMetastoreClient
{
    static final String TEST_DATABASE = "testdb";
    static final String BAD_DATABASE = "baddb";
    static final String TEST_TABLE = "testtbl";
    static final String TEST_PARTITION1 = "key=testpartition1";
    static final String TEST_PARTITION2 = "key=testpartition2";

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
            throws TException
    {
        accessCount.incrementAndGet();
        if (throwException) {
            throw new IllegalStateException();
        }
        return ImmutableList.of(TEST_DATABASE);
    }

    @Override
    public List<String> getAllTables(String dbName)
            throws TException
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
        return new Table(TEST_TABLE, TEST_DATABASE, "", 0, 0, 0, null, ImmutableList.of(new FieldSchema("key", "String", null)), null, "", "", "");
    }

    @Override
    public List<String> getTableNamesByFilter(String databaseName, String filter)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> getPartitionNames(String dbName, String tableName)
            throws TException
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

    @Override
    public Partition getPartitionByName(String dbName, String tableName, String partName)
            throws TException
    {
        accessCount.incrementAndGet();
        if (throwException) {
            throw new RuntimeException();
        }
        if (!dbName.equals(TEST_DATABASE) || !tableName.equals(TEST_TABLE) || !ImmutableSet.of(TEST_PARTITION1, TEST_PARTITION2).contains(partName)) {
            throw new NoSuchObjectException();
        }
        return new Partition(null, TEST_DATABASE, TEST_TABLE, 0, 0, null, null);
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
                return new Partition(ImmutableList.copyOf(Warehouse.getPartValuesFromPartName(name)), TEST_DATABASE, TEST_TABLE, 0, 0, null, null);
            }
            catch (MetaException e) {
                throw Throwables.propagate(e);
            }
        });
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
    public boolean dropPartitionByName(String databaseName, String tableName, String partitionName, boolean deleteData)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Role> listRoles(String principalName, PrincipalType principalType)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public PrincipalPrivilegeSet getPrivilegeSet(HiveObjectRef hiveObject, String userName, List<String> groupNames)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> getRoleNames()
    {
        throw new UnsupportedOperationException();
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
    public void close()
    {
        // No-op
    }
}
