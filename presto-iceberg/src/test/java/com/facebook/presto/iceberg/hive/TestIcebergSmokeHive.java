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
import com.facebook.presto.iceberg.IcebergConfig;
import com.facebook.presto.iceberg.IcebergDistributedSmokeTestBase;
import com.facebook.presto.iceberg.IcebergQueryRunner;
import com.facebook.presto.iceberg.IcebergUtil;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.iceberg.Table;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.util.List;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.facebook.presto.hive.metastore.CachingHiveMetastore.memoizeMetastore;
import static com.facebook.presto.iceberg.CatalogType.HIVE;
import static java.lang.String.format;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestIcebergSmokeHive
        extends IcebergDistributedSmokeTestBase
{
    public TestIcebergSmokeHive()
    {
        super(HIVE);
    }

    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.createIcebergQueryRunner(
                ImmutableMap.of(),
                ImmutableMap.of("iceberg.catalog.type", catalogType.name()),
                new IcebergConfig().getFileFormat(),
                true,
                false,
                OptionalInt.of(1));
    }

    @DataProvider
    public Object[][] concurrencyValues()
    {
        return new Object[][] {
                {4},
                {5},
                {6}
        };
    }

    @Test(dataProvider = "concurrencyValues")
    public void testSuccessfulConcurrentInsert(int concurrency)
    {
        final Session session = getSession();
//        assertUpdate(session, "CREATE TABLE test_concurrent_insert (col0 INTEGER, col1 VARCHAR, col2 TIMESTAMP) WITH (format = 'ORC', commit_retries = " + concurrency + ")");
        assertUpdate(session, "CREATE TABLE test_concurrent_insert WITH (format = 'ORC', commit_retries = " + concurrency + ") AS SELECT * FROM tpch.tiny.nation", 25);

        int commitRetries = concurrency + 1;
//        final String[] strings = {"one", "two", "three", "four", "five", "six", "seven"};
        final CountDownLatch countDownLatch = new CountDownLatch(commitRetries);
        AtomicInteger value = new AtomicInteger(0);
        Set<Throwable> errors = new CopyOnWriteArraySet<>();
        List<Thread> threads = Stream.generate(() -> new Thread(() -> {
            int i = value.getAndIncrement();
            try {
//                getQueryRunner().execute(session, format("INSERT INTO test_concurrent_insert VALUES(%s, '%s', cast('2024-01-18 11:41:15.529' as timestamp))", i + 1, strings[i]));
                getQueryRunner().execute(session, "insert into test_concurrent_insert select * from tpch.tiny.nation");
            }
            catch (Throwable throwable) {
                errors.add(throwable);
            }
            finally {
                countDownLatch.countDown();
            }
        })).limit(commitRetries).collect(Collectors.toList());

        threads.forEach(Thread::start);

        try {
            final int seconds = 10;
            if (!countDownLatch.await(seconds, TimeUnit.SECONDS)) {
                fail(format("Failed to insert in %s seconds", seconds));
            }
            if (!errors.isEmpty()) {
                fail(format("Failed to insert concurrently: %s", errors.stream().map(Throwable::getMessage).collect(Collectors.joining(" & "))));
            }
//            assertQuery(session, "SELECT count(*) FROM test_concurrent_insert", "SELECT " + commitRetries);
            switch (concurrency) {
                case 4:
                    assertQuery(session, "SELECT count(*) FROM test_concurrent_insert", "SELECT 150");
//                    assertQuery(session, "SELECT * FROM test_concurrent_insert", "VALUES(1, 'one', cast('2024-01-18 11:41:15.529' as timestamp)), (2, 'two', cast('2024-01-18 11:41:15.529' as timestamp)), (3, 'three', cast('2024-01-18 11:41:15.529' as timestamp)), (4, 'four', cast('2024-01-18 11:41:15.529' as timestamp)), (5, 'five', cast('2024-01-18 11:41:15.529' as timestamp))");
                    break;
                case 5:
                    assertQuery(session, "SELECT count(*) FROM test_concurrent_insert", "SELECT 175");
//                    assertQuery(session, "SELECT * FROM test_concurrent_insert", "VALUES(1, 'one', cast('2024-01-18 11:41:15.529' as timestamp)), (2, 'two', cast('2024-01-18 11:41:15.529' as timestamp)), (3, 'three', cast('2024-01-18 11:41:15.529' as timestamp)), (4, 'four', cast('2024-01-18 11:41:15.529' as timestamp)), (5, 'five', cast('2024-01-18 11:41:15.529' as timestamp)), (6, 'six', cast('2024-01-18 11:41:15.529' as timestamp))");
                    break;
                case 6:
                    assertQuery(session, "SELECT count(*) FROM test_concurrent_insert", "SELECT 200");
//                    assertQuery(session, "SELECT * FROM test_concurrent_insert", "VALUES(1, 'one', cast('2024-01-18 11:41:15.529' as timestamp)), (2, 'two', cast('2024-01-18 11:41:15.529' as timestamp)), (3, 'three', cast('2024-01-18 11:41:15.529' as timestamp)), (4, 'four', cast('2024-01-18 11:41:15.529' as timestamp)), (5, 'five', cast('2024-01-18 11:41:15.529' as timestamp)), (6, 'six', cast('2024-01-18 11:41:15.529' as timestamp)), (7, 'seven', cast('2024-01-18 11:41:15.529' as timestamp))");
                    break;
            }
        }
        catch (InterruptedException e) {
            fail("Interrupted when await insertion", e);
        }
        finally {
            dropTable(session, "test_concurrent_insert");
        }
    }

    @Test
    public void testSuccessfulConcurrentInsertFailure()
    {
        final Session session = getSession();
        int concurrency = 3;
//        assertUpdate(session, "CREATE TABLE test_concurrent_insert_fail (col0 INTEGER, col1 VARCHAR, col2 TIMESTAMP, col3 TIMESTAMP, col4 TIME, col5 BIGINT) WITH (format = 'ORC', commit_retries = " + concurrency + ")");
        assertUpdate(session, "CREATE TABLE test_concurrent_insert_fail WITH (format = 'ORC', commit_retries = " + concurrency + ") AS SELECT * FROM tpch.sf100.lineitem", 600037902);

        int commitRetries = concurrency + 1;
        int numberOfSubmittedQueries = commitRetries + 5;
        final String[] strings = {"one", "two", "three", "four", "five"};
        final CountDownLatch countDownLatch = new CountDownLatch(numberOfSubmittedQueries);
        AtomicInteger value = new AtomicInteger(0);
        Set<Throwable> errors = new CopyOnWriteArraySet<>();
        List<Thread> threads = Stream.generate(() -> new Thread(() -> {
            int i = value.getAndIncrement();
            try {
//                getQueryRunner().execute(session, format("INSERT INTO test_concurrent_insert_fail VALUES(%s, '%s', cast(current_timestamp as timestamp), cast(current_timestamp as timestamp), cast(current_timestamp as time), 34567)", i + 1, strings[i % 5]));
                getQueryRunner().execute(session, "insert into test_concurrent_insert_fail select * from tpch.sf100.lineitem");
            }
            catch (Throwable throwable) {
                errors.add(throwable);
            }
            finally {
                countDownLatch.countDown();
            }
        })).limit(numberOfSubmittedQueries).collect(Collectors.toList());

        threads.forEach(Thread::start);

        try {
            final int seconds = 10;
            if (!countDownLatch.await(seconds, TimeUnit.SECONDS)) {
                fail(format("Failed to insert in %s seconds", seconds));
            }
            assertFalse(errors.isEmpty());
            String msgRegex = "^java\\.lang\\.RuntimeException: Metadata location \\[.*\\] is not same as table metadata location \\[.*\\] for tpch.test_concurrent_insert_fail$";
            assertTrue(errors.stream().findAny().filter(t -> {
                Pattern pattern = Pattern.compile(msgRegex);
                Matcher matcher = pattern.matcher(t.toString());
                return matcher.matches();
            }).isPresent());
        }
        catch (InterruptedException e) {
            fail("Interrupted when await insertion", e);
        }
        finally {
            dropTable(session, "test_concurrent_insert_fail");
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
