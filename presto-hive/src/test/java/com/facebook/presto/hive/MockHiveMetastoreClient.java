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

import com.facebook.presto.hive.shaded.org.apache.thrift.TException;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.weakref.jmx.com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class MockHiveMetastoreClient
        extends HiveMetastoreClient
{
    static final String TEST_DATABASE = "testdb";
    static final String BAD_DATABASE = "baddb";
    static final String TEST_TABLE = "testtbl";
    static final String TEST_PARTITION1 = "key=testpartition1";
    static final String TEST_PARTITION2 = "key=testpartition2";

    private final AtomicInteger accessCount = new AtomicInteger();
    private boolean throwException;

    MockHiveMetastoreClient()
    {
        super(null);
    }

    public void setThrowException(boolean throwException)
    {
        this.throwException = throwException;
    }

    public int getAccessCount()
    {
        return accessCount.get();
    }

    @Override
    public List<String> get_all_databases()
            throws TException
    {
        accessCount.incrementAndGet();
        if (throwException) {
            throw new IllegalStateException();
        }
        return ImmutableList.of(TEST_DATABASE);
    }

    @Override
    public List<String> get_all_tables(String db_name)
            throws TException
    {
        accessCount.incrementAndGet();
        if (throwException) {
            throw new RuntimeException();
        }
        if (!db_name.equals(TEST_DATABASE)) {
            return ImmutableList.of(); // As specified by Hive specification
        }
        return ImmutableList.of(TEST_TABLE);
    }

    @Override
    public Database get_database(String name)
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
    public Table get_table(String dbname, String tbl_name)
            throws TException
    {
        accessCount.incrementAndGet();
        if (throwException) {
            throw new RuntimeException();
        }
        if (!dbname.equals(TEST_DATABASE) || !tbl_name.equals(TEST_TABLE)) {
            throw new NoSuchObjectException();
        }
        return new Table(TEST_TABLE, TEST_DATABASE, "", 0, 0, 0, null, ImmutableList.of(new FieldSchema("key", "String", null)), null, "", "", "");
    }

    @Override
    public List<String> get_partition_names(String db_name, String tbl_name, short max_parts)
            throws TException
    {
        accessCount.incrementAndGet();
        if (throwException) {
            throw new RuntimeException();
        }
        if (!db_name.equals(TEST_DATABASE) || !tbl_name.equals(TEST_TABLE)) {
            return ImmutableList.of();
        }
        return ImmutableList.of(TEST_PARTITION1, TEST_PARTITION2);
    }

    @Override
    public List<String> get_partition_names_ps(String db_name, String tbl_name, List<String> part_vals, short max_parts)
            throws TException
    {
        accessCount.incrementAndGet();
        if (throwException) {
            throw new RuntimeException();
        }
        if (!db_name.equals(TEST_DATABASE) || !tbl_name.equals(TEST_TABLE)) {
            throw new NoSuchObjectException();
        }
        return ImmutableList.of(TEST_PARTITION1, TEST_PARTITION2);
    }

    @Override
    public Partition get_partition_by_name(String db_name, String tbl_name, String part_name)
            throws TException
    {
        accessCount.incrementAndGet();
        if (throwException) {
            throw new RuntimeException();
        }
        if (!db_name.equals(TEST_DATABASE) || !tbl_name.equals(TEST_TABLE) || !ImmutableSet.of(TEST_PARTITION1, TEST_PARTITION2).contains(part_name)) {
            throw new NoSuchObjectException();
        }
        return new Partition(null, TEST_DATABASE, TEST_TABLE, 0, 0, null, null);
    }

    @Override
    public List<Partition> get_partitions_by_names(String db_name, String tbl_name, List<String> names)
            throws TException
    {
        accessCount.incrementAndGet();
        if (throwException) {
            throw new RuntimeException();
        }
        if (!db_name.equals(TEST_DATABASE) || !tbl_name.equals(TEST_TABLE) || !ImmutableSet.of(TEST_PARTITION1, TEST_PARTITION2).containsAll(names)) {
            throw new NoSuchObjectException();
        }
        return Lists.transform(names, new Function<String, Partition>()
        {
            @Override
            public Partition apply(String name)
            {
                try {
                    return new Partition(ImmutableList.copyOf(Warehouse.getPartValuesFromPartName(name)), TEST_DATABASE, TEST_TABLE, 0, 0, null, null);
                }
                catch (MetaException e) {
                    throw Throwables.propagate(e);
                }
            }
        });
    }

    @Override
    public void close()
    {
        // No-op
    }
}
