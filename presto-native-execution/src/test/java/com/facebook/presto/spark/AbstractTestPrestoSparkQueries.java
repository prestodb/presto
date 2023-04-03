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
import com.facebook.presto.hive.HiveExternalWorkerQueryRunner;
import com.facebook.presto.spark.classloader_interface.PrestoSparkNativeExecutionShuffleManager;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableMap;
import org.apache.spark.SparkEnv;
import org.apache.spark.shuffle.ShuffleHandle;
import org.apache.spark.shuffle.sort.BypassMergeSortShuffleHandle;

import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.NATIVE_EXECUTION_ENABLED;
import static com.facebook.presto.SystemSessionProperties.NATIVE_EXECUTION_EXECUTABLE_PATH;
import static com.facebook.presto.SystemSessionProperties.TABLE_WRITER_MERGE_OPERATOR_ENABLED;
import static com.facebook.presto.hive.HiveSessionProperties.COLLECT_COLUMN_STATISTICS_ON_WRITE;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.SPARK_INITIAL_PARTITION_COUNT;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.SPARK_PARTITION_COUNT_AUTO_TUNE_ENABLED;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class AbstractTestPrestoSparkQueries
        extends AbstractTestQueryFramework
{
    private static final String SPARK_SHUFFLE_MANAGER = "spark.shuffle.manager";
    private static final String FALLBACK_SPARK_SHUFFLE_MANAGER = "spark.fallback.shuffle.manager";

    protected Session getNativeSession()
    {
        Session.SessionBuilder sessionBuilder = Session.builder(getSession())
                .setSystemProperty(NATIVE_EXECUTION_ENABLED, "true")
                .setSystemProperty(TABLE_WRITER_MERGE_OPERATOR_ENABLED, "false")
                .setSystemProperty(SPARK_INITIAL_PARTITION_COUNT, "1")
                .setSystemProperty(SPARK_PARTITION_COUNT_AUTO_TUNE_ENABLED, "false")
                .setCatalogSessionProperty("hive", COLLECT_COLUMN_STATISTICS_ON_WRITE, "false");

        if (System.getProperty("NATIVE_PORT") == null) {
            sessionBuilder.setSystemProperty(NATIVE_EXECUTION_EXECUTABLE_PATH, requireNonNull(System.getProperty("PRESTO_SERVER"), "Native worker binary path is missing. Add -DPRESTO_SERVER=/path/to/native/process/bin to your JVM arguments."));
        }
        return sessionBuilder.build();
    }

    @Override
    protected QueryRunner createQueryRunner()
    {
        // prevent to use the default Prestissimo config files since the Presto-Spark will generate the configs on-the-fly.
        return PrestoSparkNativeQueryRunner.createPrestoSparkNativeQueryRunner(
                ImmutableMap.of("catalog.config-dir", "/"), getNativeExecutionShuffleConfigs());
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        String dataDirectory = System.getProperty("DATA_DIR");
        return HiveExternalWorkerQueryRunner.createJavaQueryRunner(Optional.ofNullable(dataDirectory).map(Paths::get), "legacy");
    }

    protected void assertShuffleMetadata()
    {
        assertNotNull(SparkEnv.get());
        assertTrue(SparkEnv.get().shuffleManager() instanceof PrestoSparkNativeExecutionShuffleManager);
        PrestoSparkNativeExecutionShuffleManager shuffleManager = (PrestoSparkNativeExecutionShuffleManager) SparkEnv.get().shuffleManager();
        Set<Integer> partitions = shuffleManager.getAllPartitions();

        for (Integer partition : partitions) {
            Optional<ShuffleHandle> shuffleHandle = shuffleManager.getShuffleHandle(partition);
            assertTrue(shuffleHandle.isPresent());
            assertTrue(shuffleHandle.get() instanceof BypassMergeSortShuffleHandle);
        }
    }

    protected void assertQuery(String sql)
    {
        assertQuery(getNativeSession(), sql, getQueryRunner().getDefaultSession(), sql);
        assertShuffleMetadata();
    }

    protected void assertQuerySucceeds(String sql)
    {
        assertQuerySucceeds(getNativeSession(), sql);
    }

    protected void assertQueryFails(String sql, String expectedMessageRegExp)
    {
        assertQueryFails(getNativeSession(), sql, expectedMessageRegExp);
    }

    protected Map<String, String> getNativeExecutionShuffleConfigs()
    {
        ImmutableMap.Builder<String, String> sparkConfigs = ImmutableMap.builder();
        sparkConfigs.put(SPARK_SHUFFLE_MANAGER, "com.facebook.presto.spark.classloader_interface.PrestoSparkNativeExecutionShuffleManager");
        sparkConfigs.put(FALLBACK_SPARK_SHUFFLE_MANAGER, "org.apache.spark.shuffle.sort.SortShuffleManager");
        return sparkConfigs.build();
    }
}
