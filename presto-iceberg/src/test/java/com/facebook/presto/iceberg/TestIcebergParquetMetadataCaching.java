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

import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.OptionalInt;

import static com.facebook.presto.iceberg.FileFormat.PARQUET;
import static com.facebook.presto.iceberg.IcebergQueryRunner.createIcebergQueryRunner;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestIcebergParquetMetadataCaching
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createIcebergQueryRunner(
                ImmutableMap.of(),
                ImmutableMap.of(
                        "iceberg.parquet.metadata-cache-enabled", "true",
                        "iceberg.parquet.metadata-cache-size", "100MB",
                        "iceberg.parquet.metadata-cache-ttl-since-last-access", "1h"),
                PARQUET,
                false,
                true,
                OptionalInt.of(2));
    }

    @BeforeClass
    public void setUp()
    {
        assertQuerySucceeds("CREATE SCHEMA iceberg.test_schema");
        assertQuerySucceeds("CREATE TABLE iceberg.test_schema.nation AS SELECT * FROM tpch.tiny.nation");
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        assertQuerySucceeds("DROP TABLE IF EXISTS iceberg.test_schema.nation");
        assertQuerySucceeds("DROP SCHEMA IF EXISTS iceberg.test_schema");
    }

    @Test
    public void testParquetMetadataCaching()
    {
        String jmxMetricsQuery = "SELECT sum(size), sum(hitcount), sum(misscount) from jmx.current.\"com.facebook.presto.hive:name=iceberg_parquetmetadata,type=cachestatsmbean\"";
        String ordersQuery = "SELECT * FROM iceberg.test_schema.nation";

        // Initial cache entries, hitcount, misscount will all be zero
        assertQuery(jmxMetricsQuery, "VALUES (0, 0, 0)");

        for (int i = 0; i < 3; i++) {
            assertQuerySucceeds(ordersQuery);
            assertQuerySucceeds(ordersQuery);
            assertQuerySucceeds(ordersQuery);
        }

        MaterializedResult result = computeActual(jmxMetricsQuery);

        assertEquals(result.getRowCount(), 1);

        long numCacheEntries = (long) result.getMaterializedRows().get(0).getField(0);
        long hitCount = (long) result.getMaterializedRows().get(0).getField(1);
        long missCount = (long) result.getMaterializedRows().get(0).getField(2);

        assertTrue(numCacheEntries > 0);
        assertTrue(hitCount > 0);
        assertTrue(missCount > 0);
    }
}
