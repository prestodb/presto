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

import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class AbstractTestPrestoSparkQueries
        extends AbstractTestQueryFramework
{
    private static final String SPARK_SHUFFLE_MANAGER = "spark.shuffle.manager";
    private static final String FALLBACK_SPARK_SHUFFLE_MANAGER = "spark.fallback.shuffle.manager";

    @Override
    protected QueryRunner createQueryRunner()
    {
        ImmutableMap.Builder<String, String> builder = new ImmutableMap.Builder<String, String>()
                // Do not use default Prestissimo config files. Presto-Spark will generate the configs on-the-fly.
                .put("catalog.config-dir", "/")
                .put("native-execution-enabled", "true")
                .put("spark.initial-partition-count", "1")
                .put("spark.partition-count-auto-tune-enabled", "false");

        if (System.getProperty("NATIVE_PORT") == null) {
            String path = requireNonNull(System.getProperty("PRESTO_SERVER"),
                    "Native worker binary path is missing. " +
                    "Add -DPRESTO_SERVER=/path/to/native/process/bin to your JVM arguments.");
            builder.put("native-execution-executable-path", path);
        }

        return PrestoSparkNativeQueryRunner.createPrestoSparkNativeQueryRunner(
                builder.build(),
                getNativeExecutionShuffleConfigs());
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
        super.assertQuery(sql);
        assertShuffleMetadata();
    }

    protected Map<String, String> getNativeExecutionShuffleConfigs()
    {
        ImmutableMap.Builder<String, String> sparkConfigs = ImmutableMap.builder();
        sparkConfigs.put(SPARK_SHUFFLE_MANAGER, "com.facebook.presto.spark.classloader_interface.PrestoSparkNativeExecutionShuffleManager");
        sparkConfigs.put(FALLBACK_SPARK_SHUFFLE_MANAGER, "org.apache.spark.shuffle.sort.SortShuffleManager");
        return sparkConfigs.build();
    }
}
