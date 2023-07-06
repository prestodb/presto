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
package com.facebook.presto.iceberg;

import com.facebook.presto.Session;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.OptionalInt;

import static com.facebook.presto.iceberg.IcebergQueryRunner.createIcebergQueryRunner;
import static org.apache.iceberg.FileFormat.PARQUET;

public class TestIcebergFileListCaching
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createIcebergQueryRunner(
                ImmutableMap.of(),
                ImmutableMap.of(
                        "iceberg.file-status-cache-tables", "*",
                        "iceberg.file-status-cache-size", "100",
                        "iceberg.file-status-cache-expire-time", "24h"),
                PARQUET,
                false,
                true,
                OptionalInt.of(1));
    }

    @BeforeClass
    public void setUp()
    {
        assertQuerySucceeds("CREATE SCHEMA iceberg.test_schema");
        assertQuerySucceeds("CREATE TABLE iceberg.test_schema.nation as SELECT * FROM tpch.tiny.nation WITH NO DATA");
        // Insert into table 5 times. The table should have 5 different files created.
        for (int i = 0; i <= 4; i++) {
            assertQuerySucceeds("INSERT INTO iceberg.test_schema.nation SELECT * FROM tpch.tiny.nation where regionkey = " + i);
        }
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        assertQuerySucceeds("DROP TABLE IF EXISTS iceberg.test_schema.nation");
        assertQuerySucceeds("DROP SCHEMA IF EXISTS iceberg.test_schema");
    }

    @Test
    public void testIcebergFileListCaching()
    {
        String jmxMetricsQuery = "SELECT sum(size), sum(hitcount), sum(misscount) from jmx.current.\"com.facebook.presto.iceberg:name=iceberg,type=icebergfilelistcache\"";
        String ordersQuery = "SELECT * FROM iceberg.test_schema.nation where regionkey = ";

        // Initial cache entries, hitcount, misscount will all be zero
        assertQuery(jmxMetricsQuery, "VALUES (0, 0, 0)");

        // All these queries are reading from a different underlying files.
        // So on first run, the misscount and cache entries should increase
        // while hitcount remains zero
        runQueryAndTestCacheEntries(ordersQuery + "0", jmxMetricsQuery, "VALUES (1, 0, 1)");
        runQueryAndTestCacheEntries(ordersQuery + "1", jmxMetricsQuery, "VALUES (2, 0, 2)");
        runQueryAndTestCacheEntries(ordersQuery + "2", jmxMetricsQuery, "VALUES (3, 0, 3)");
        runQueryAndTestCacheEntries(ordersQuery + "3", jmxMetricsQuery, "VALUES (4, 0, 4)");
        runQueryAndTestCacheEntries(ordersQuery + "4", jmxMetricsQuery, "VALUES (5, 0, 5)");

        // Run the same queries again, the hitcount should increase by one for each query.
        runQueryAndTestCacheEntries(ordersQuery + "0", jmxMetricsQuery, "VALUES (5, 1, 5)");
        runQueryAndTestCacheEntries(ordersQuery + "1", jmxMetricsQuery, "VALUES (5, 2, 5)");
        runQueryAndTestCacheEntries(ordersQuery + "2", jmxMetricsQuery, "VALUES (5, 3, 5)");
        runQueryAndTestCacheEntries(ordersQuery + "3", jmxMetricsQuery, "VALUES (5, 4, 5)");
        runQueryAndTestCacheEntries(ordersQuery + "4", jmxMetricsQuery, "VALUES (5, 5, 5)");

        // Test cache session property works
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty("iceberg", "use_file_list_cache", "false")
                .build();

        assertQuerySucceeds(session, ordersQuery + "4");
        // Cache stats should not change from the last run as it was disabled.
        assertQuery(jmxMetricsQuery, "VALUES (5, 5, 5)");
    }

    void runQueryAndTestCacheEntries(String ordersQuery, String jmxMetricsQuery, String expected)
    {
        assertQuerySucceeds(ordersQuery);
        assertQuery(jmxMetricsQuery, expected);
    }
}
