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
package com.facebook.presto.iceberg.hive;

import com.facebook.presto.Session;
import com.facebook.presto.hive.HdfsConfiguration;
import com.facebook.presto.hive.HdfsConfigurationInitializer;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveHdfsConfiguration;
import com.facebook.presto.hive.MetastoreClientConfig;
import com.facebook.presto.hive.authentication.NoHdfsAuthentication;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.file.FileHiveMetastore;
import com.facebook.presto.iceberg.IcebergDistributedSmokeTestBase;
import com.facebook.presto.iceberg.IcebergUtil;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableSet;
import org.apache.iceberg.Table;
import org.testng.annotations.Test;

import java.io.File;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.facebook.presto.hive.metastore.CachingHiveMetastore.memoizeMetastore;
import static com.facebook.presto.iceberg.CatalogType.HIVE;
import static java.lang.String.format;
import static org.testng.Assert.fail;

public class TestIcebergSmokeHive
        extends IcebergDistributedSmokeTestBase
{
    public TestIcebergSmokeHive()
    {
        super(HIVE);
    }

    @Test
    public void testConcurrentInsert()
    {
        final Session session = getSession();
        assertUpdate(session, "CREATE TABLE test_concurrent_insert (col0 INTEGER, col1 VARCHAR) WITH (format = 'ORC')");

        int concurrency = 5;
        final String[] strings = {"one", "two", "three", "four", "five"};
        final CountDownLatch countDownLatch = new CountDownLatch(concurrency);
        AtomicInteger value = new AtomicInteger(0);
        Set<Throwable> errors = new CopyOnWriteArraySet<>();
        List<Thread> threads = Stream.generate(() -> new Thread(() -> {
            int i = value.getAndIncrement();
            try {
                getQueryRunner().execute(session, format("INSERT INTO test_concurrent_insert VALUES(%s, '%s')", i + 1, strings[i]));
            }
            catch (Throwable throwable) {
                errors.add(throwable);
            }
            finally {
                countDownLatch.countDown();
            }
        })).limit(concurrency).collect(Collectors.toList());

        threads.forEach(Thread::start);

        try {
            final int seconds = 10;
            if (!countDownLatch.await(seconds, TimeUnit.SECONDS)) {
                fail(format("Failed to insert in %s seconds", seconds));
            }
            if (!errors.isEmpty()) {
                fail(format("Failed to insert concurrently: %s", errors.stream().map(Throwable::getMessage).collect(Collectors.joining(" & "))));
            }
            assertQuery(session, "SELECT count(*) FROM test_concurrent_insert", "SELECT " + concurrency);
            assertQuery(session, "SELECT * FROM test_concurrent_insert", "VALUES(1, 'one'), (2, 'two'), (3, 'three'), (4, 'four'), (5, 'five')");
        }
        catch (InterruptedException e) {
            fail("Interrupted when await insertion", e);
        }
        finally {
            dropTable(session, "test_concurrent_insert");
        }
    }

    @Override
    protected String getLocation(String schema, String table)
    {
        File tempLocation = ((DistributedQueryRunner) getQueryRunner()).getCoordinator().getDataDirectory().toFile();
        return format("%scatalog/%s/%s", tempLocation.toURI(), schema, table);
    }

    protected static HdfsEnvironment getHdfsEnvironment()
    {
        HiveClientConfig hiveClientConfig = new HiveClientConfig();
        MetastoreClientConfig metastoreClientConfig = new MetastoreClientConfig();
        HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(new HdfsConfigurationInitializer(hiveClientConfig, metastoreClientConfig),
                ImmutableSet.of(),
                hiveClientConfig);
        return new HdfsEnvironment(hdfsConfiguration, metastoreClientConfig, new NoHdfsAuthentication());
    }

    protected ExtendedHiveMetastore getFileHiveMetastore()
    {
        FileHiveMetastore fileHiveMetastore = new FileHiveMetastore(getHdfsEnvironment(),
                getCatalogDirectory().toFile().getPath(),
                "test");
        return memoizeMetastore(fileHiveMetastore, false, 1000, 0);
    }

    @Override
    protected Table getIcebergTable(ConnectorSession session, String schema, String tableName)
    {
        return IcebergUtil.getHiveIcebergTable(getFileHiveMetastore(),
                getHdfsEnvironment(),
                session,
                SchemaTableName.valueOf(schema + "." + tableName));
    }
}
