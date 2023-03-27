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
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.NATIVE_EXECUTION_ENABLED;
import static com.facebook.presto.SystemSessionProperties.NATIVE_EXECUTION_EXECUTABLE_PATH;
import static com.facebook.presto.hive.HiveSessionProperties.PUSHDOWN_FILTER_ENABLED;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * Following program argument is needed to run Spark native tests.
 *
 * - PRESTO_SERVER (-DPRESTO_SERVER=/path/to/native/process/bin)
 *   - This tells Spark where to find the Presto native binary to launch native process. Set it in
 *     program arguments like -DPRESTO_SERVER=/path/to/native/process/bin
 *
 * Tests can be running in Interactive Debugging Mode. Interactive Debugging Mode allows you to have an easier debugging
 * experience by allowing Spark side not launching its own native process, but instead communicating with an already
 * launched native process. This gives developers flexibility to hookup any IDEs or debuggers with the native process.
 * Following JVM argument is needed in order to enable this mode:
 *
 * - NATIVE_PORT (-DNATIVE_PORT=<port>)
 *   - This is the port your externally launched native process listens to. It is used to tell Spark where to send
 *     requests. This port number has to be the same as to which your externally launched process listens. Set it in
 *     program arguments like -DNATIVE_PORT=7777. When this is set, PRESTO_SERVER is not required.
 */
public class TestPrestoSparkNativeExecution
        extends AbstractTestQueryFramework
{
    private static final String SPARK_SHUFFLE_MANAGER = "spark.shuffle.manager";
    private static final String FALLBACK_SPARK_SHUFFLE_MANAGER = "spark.fallback.shuffle.manager";

    protected Session getNativeSession()
    {
        Session.SessionBuilder sessionBuilder = Session.builder(getSession())
                .setSystemProperty(NATIVE_EXECUTION_ENABLED, "true");
        if (System.getProperty("NATIVE_PORT") == null) {
            sessionBuilder.setSystemProperty(NATIVE_EXECUTION_EXECUTABLE_PATH, requireNonNull(System.getProperty("PRESTO_SERVER"), "Native worker binary path is missing. Add -DPRESTO_SERVER=/path/to/native/process/bin to your JVM arguments."));
        }
        return sessionBuilder.build();
    }

    @Override
    protected QueryRunner createQueryRunner()
    {
        Map<String, String> configs = new HashMap<>();
        // prevent to use the default Prestissimo config files since the Presto-Spark will generate the configs on-the-fly.
        configs.put("catalog.config-dir", "/");
        return PrestoSparkQueryRunner.createHivePrestoSparkQueryRunner(configs);
    }

    @Test
    public void testNativeExecutionWithProjection()
    {
        assertUpdate("CREATE TABLE test_order WITH (format = 'DWRF') as SELECT orderkey, custkey FROM orders LIMIT 100", 100);

        Session session = Session.builder(getNativeSession())
                .setSystemProperty("table_writer_merge_operator_enabled", "false")
                .setCatalogSessionProperty("hive", "collect_column_statistics_on_write", "false")
                .setCatalogSessionProperty("hive", PUSHDOWN_FILTER_ENABLED, "true")
                .build();

        // Reset the spark context to register the native execution shuffle manager. We want to let the query runner use the default spark shuffle
        // manager to generate the test tables and only test the new native execution shuffle manager on the test below test cases.
        PrestoSparkQueryRunner queryRunner = (PrestoSparkQueryRunner) getQueryRunner();
        queryRunner.resetSparkContext(getNativeExecutionShuffleConfigs());
        try {
            assertQuerySucceeds(session, "SELECT * FROM test_order");
        }
        finally {
            queryRunner.resetSparkContext();
        }
    }

    // TODO: re-enable the test once the shuffle integration is ready.
    @Ignore
    @Test(priority = 2, dependsOnMethods = "testNativeExecutionWithProjection")
    public void testNativeExecutionShuffleManager()
    {
        Session session = Session.builder(getNativeSession())
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
