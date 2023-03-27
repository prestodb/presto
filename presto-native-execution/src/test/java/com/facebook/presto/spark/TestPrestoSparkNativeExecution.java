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

import com.facebook.presto.spark.classloader_interface.PrestoSparkNativeExecutionShuffleManager;
import org.apache.spark.SparkEnv;
import org.apache.spark.shuffle.ShuffleHandle;
import org.apache.spark.shuffle.sort.BypassMergeSortShuffleHandle;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

import java.util.Optional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * Following JVM argument is needed to run Spark native tests.
 * <p>
 * - PRESTO_SERVER
 * - This tells Spark where to find the Presto native binary to launch the process.
 * Example: -DPRESTO_SERVER=/path/to/native/process/bin
 * <p>
 * - DATA_DIR
 * - Optional path to store TPC-H tables used in the test. If this directory is empty, it will be
 * populated. If tables already exists, they will be reused.
 * <p>
 * Tests can be running in Interactive Debugging Mode that allows for easier debugging
 * experience. Instead of launching its own native process, the test will connect to an existing
 * native process. This gives developers flexibility to connect IDEA and debuggers to the native process.
 * Enable this mode by setting NATIVE_PORT JVM argument.
 * <p>
 * - NATIVE_PORT
 * - This is the port your externally launched native process listens to. It is used to tell Spark where to send
 * requests. This port number has to be the same as to which your externally launched process listens.
 * Example: -DNATIVE_PORT=7777.
 * When NATIVE_PORT is specified, PRESTO_SERVER argument is not requires and is ignored if specified.
 */
public class TestPrestoSparkNativeExecution
        extends AbstractTestPrestoSparkQueries
{
    @Test
    public void testMapOnlyQueries()
    {
        assertQuery("SELECT orderkey, custkey FROM orders WHERE orderkey <= 200");
        assertQuerySucceeds("SELECT * FROM orders");
        assertQueryFails("SELECT sequence(1, orderkey) FROM orders",
                ".*Scalar function name not registered: presto.default.sequence.*");
        assertQueryFails("SELECT orderkey / 0 FROM orders", ".*division by zero.*");
        assertQueryFails("SELECT orderkey, custkey FROM orders LIMIT 4", ".*Failure in LocalWriteFile: path .* already exists.*");
    }

    // TODO: re-enable the test once the shuffle integration is ready.
    @Ignore
    @Test(priority = 2, dependsOnMethods = "testMapOnlyQueries")
    public void testNativeExecutionShuffleManager()
    {
        PrestoSparkQueryRunner queryRunner = (PrestoSparkQueryRunner) getQueryRunner();

        // Reset the spark context to register the native execution shuffle manager. We want to let the query runner use the default spark shuffle
        // manager to generate the test tables and only test the new native execution shuffle manager on the test below test cases.
        // Expecting 0 row updated since currently the NativeExecutionOperator is dummy.
        queryRunner.execute("CREATE TABLE test_aggregate as SELECT  partkey, count(*) c FROM lineitem WHERE partkey % 10 = 1 GROUP BY partkey");

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
}
