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
package com.facebook.presto.spark;

import com.facebook.presto.spark.classloader_interface.PrestoSparkShuffleStats;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.apache.spark.SparkContext;
import org.apache.spark.util.AccumulatorContext$;
import org.apache.spark.util.AccumulatorV2;
import org.apache.spark.util.CollectionAccumulator;
import org.testng.annotations.Test;
import scala.Option;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.spark.PrestoSparkQueryExecutionFactory.PRESTO_SPARK_SHUFFLE_STATS_COLLECTOR;
import static com.facebook.presto.spark.PrestoSparkQueryExecutionFactory.PRESTO_SPARK_SHUFFLE_STATS_GENERIC_COLLECTOR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestPrestoSparkNativeSimpleQueries
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
    {
        return PrestoSparkNativeQueryRunnerUtils.createPrestoSparkNativeQueryRunner();
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return PrestoSparkNativeQueryRunnerUtils.createJavaQueryRunner();
    }

    @Override
    protected void assertQuery(String sql)
    {
        super.assertQuery(sql);
        PrestoSparkNativeQueryRunnerUtils.assertShuffleMetadata();
    }

    @Test
    public void testMapOnlyQueries()
    {
        assertQuery("SELECT * FROM orders");
        assertQuery("SELECT orderkey, custkey FROM orders WHERE orderkey <= 200");
        assertQuery("SELECT nullif(orderkey, custkey) FROM orders");
        assertQuery("SELECT orderkey, custkey FROM orders ORDER BY orderkey LIMIT 4");
    }

    @Test
    public void testAggregations()
    {
        assertQuery("SELECT count(*) c FROM lineitem WHERE partkey % 10 = 1 GROUP BY partkey");
    }

    @Test
    public void testJoinsWithGenericShuffleStats()
    {
        assertQuery("SELECT * FROM orders o, lineitem l WHERE o.orderkey = l.orderkey AND o.orderkey % 2 = 1");
        SparkContext.getOrCreate();
        Option<AccumulatorV2<?, ?>> accumulator = AccumulatorContext$.MODULE$.lookForAccumulatorByName(PRESTO_SPARK_SHUFFLE_STATS_GENERIC_COLLECTOR);
        assertTrue(accumulator.isDefined());
        java.util.List<List<Map<String, Long>>> stats = ((CollectionAccumulator<List<Map<String, Long>>>) accumulator.get()).value();
        assertTrue(!stats.isEmpty());
        List<Map<String, Long>> metrics = stats.get(0);
        assertTrue(!metrics.isEmpty());
        for (Map.Entry<String, Long> entry : metrics.get(0).entrySet()) {
            String[] desc = entry.getKey().split("\\|");
            assertTrue(desc.length == 4);
            assertNotNull(entry.getValue());
        }
    }

    @Test
    public void testJoinsWithShuffleStats()
    {
        assertQuery("SELECT * FROM orders o, lineitem l WHERE o.orderkey = l.orderkey AND o.orderkey % 2 = 1");
        SparkContext.getOrCreate();
        Option<AccumulatorV2<?, ?>> accumulator = AccumulatorContext$.MODULE$.lookForAccumulatorByName(PRESTO_SPARK_SHUFFLE_STATS_COLLECTOR);
        assertTrue(accumulator.isDefined());
        java.util.List<PrestoSparkShuffleStats> stats = ((CollectionAccumulator<PrestoSparkShuffleStats>) accumulator.get()).value();
        assertEquals(stats.size(), 10);
        assertEquals(stats.get(2).getProcessedRows(), 3694);
    }

    @Test
    public void testFailures()
    {
        assertQueryFails("SELECT orderkey / 0 FROM orders", ".*division by zero.*");
    }

    /**
     * Test native execution of cpp functions declared via a json file.
     * `eq()` Scalar function & `sum()` Aggregate function are defined in `src/test/resources/external_functions.json`
     */
    @Test
    public void testJsonFileBasedFunction()
    {
        assertQuery("SELECT json.test_schema.eq(1, linenumber) FROM lineitem", "SELECT 1 = linenumber FROM lineitem");
        assertQuery("SELECT json.test_schema.sum(linenumber) FROM lineitem", "SELECT sum(linenumber) FROM lineitem");
    }
}
