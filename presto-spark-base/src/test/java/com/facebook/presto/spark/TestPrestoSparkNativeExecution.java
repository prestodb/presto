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

import com.facebook.presto.Session;
import com.facebook.presto.spark.classloader_interface.PrestoSparkNativeExecutionShuffleManager;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableMap;
import org.apache.spark.SparkEnv;
import org.apache.spark.shuffle.ShuffleHandle;
import org.apache.spark.shuffle.sort.BypassMergeSortShuffleHandle;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.NATIVE_EXECUTION_ENABLED;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestPrestoSparkNativeExecution
        extends AbstractTestQueryFramework
{
    private static final String SPARK_SHUFFLE_MANAGER = "spark.shuffle.manager";
    private static final String FALLBACK_SPARK_SHUFFLE_MANAGER = "spark.fallback.shuffle.manager";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return PrestoSparkQueryRunner.createHivePrestoSparkQueryRunner();
    }

    @Test
    public void testNativeExecutionWithTableWrite()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(NATIVE_EXECUTION_ENABLED, "true")
                .setSystemProperty("table_writer_merge_operator_enabled", "false")
                .setCatalogSessionProperty("hive", "collect_column_statistics_on_write", "false")
                .build();

        // Expecting 0 row updated since currently the NativeExecutionOperator is dummy.
        assertUpdate(session, "CREATE TABLE test_tablescan as SELECT orderkey, custkey FROM orders", 0);
    }

    @Test(priority = 2, dependsOnMethods = "testNativeExecutionWithTableWrite")
    public void testNativeExecutionShuffleManager()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(NATIVE_EXECUTION_ENABLED, "true")
                .setSystemProperty("table_writer_merge_operator_enabled", "false")
                .setCatalogSessionProperty("hive", "collect_column_statistics_on_write", "false")
                .build();

        PrestoSparkQueryRunner queryRunner = (PrestoSparkQueryRunner) getQueryRunner();

        // Reset the spark context to register the native execution shuffle manager. We want to let the query runner use the default spark shuffle
        // manager to generate the test tables and only test the new native execution shuffle manager on the test below test cases.
        queryRunner.resetSparkContext(getNativeExecutionShuffleConfigs());
        // Expecting 0 row updated since currently the NativeExecutionOperator is dummy.
        queryRunner.execute(session, "CREATE TABLE test_aggregate as SELECT  partkey, count(*) c FROM lineitem WHERE partkey % 10 = 1 GROUP BY partkey");

        assertNotNull(SparkEnv.get());
        assertTrue(SparkEnv.get().shuffleManager() instanceof PrestoSparkNativeExecutionShuffleManager);
        PrestoSparkNativeExecutionShuffleManager shuffleManager = (PrestoSparkNativeExecutionShuffleManager) SparkEnv.get().shuffleManager();
        Optional<ShuffleHandle> shuffleHandle = shuffleManager.getShuffleHandle(0);
        assertTrue(shuffleHandle.isPresent());
        assertTrue(shuffleHandle.get() instanceof BypassMergeSortShuffleHandle);
        BypassMergeSortShuffleHandle<?, ?> bypassMergeSortShuffleHandle = (BypassMergeSortShuffleHandle<?, ?>) shuffleHandle.get();
        int shuffleId = shuffleHandle.get().shuffleId();
        assertEquals(0, shuffleId);
        assertEquals(shuffleManager.getNumOfPartitions(shuffleId), bypassMergeSortShuffleHandle.numMaps());
    }

    private Map<String, String> getNativeExecutionShuffleConfigs()
    {
        ImmutableMap.Builder<String, String> sparkConfigs = ImmutableMap.builder();
        sparkConfigs.put(SPARK_SHUFFLE_MANAGER, "com.facebook.presto.spark.classloader_interface.PrestoSparkNativeExecutionShuffleManager");
        sparkConfigs.put(FALLBACK_SPARK_SHUFFLE_MANAGER, "org.apache.spark.shuffle.sort.SortShuffleManager");
        return sparkConfigs.build();
    }
}
