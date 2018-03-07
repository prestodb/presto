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
package com.facebook.presto.plugin.memory;

import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tests.DistributedQueryRunner;
import io.airlift.units.Duration;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.facebook.presto.plugin.memory.MemoryQueryRunner.createQueryRunner;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static io.airlift.testing.Assertions.assertLessThan;
import static io.airlift.units.Duration.nanosSince;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestMemoryWorkerCrash
{
    private DistributedQueryRunner queryRunner;

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        queryRunner = createQueryRunner();
    }

    @Test
    public void tableAccessAfterWorkerCrash()
            throws Exception
    {
        queryRunner.execute("CREATE TABLE test_nation as SELECT * FROM tpch.tiny.nation");
        assertQuery("SELECT * FROM test_nation ORDER BY nationkey", "SELECT * FROM tpch.tiny.nation ORDER BY nationkey");
        closeWorker();
        assertFails("SELECT * FROM test_nation ORDER BY nationkey", "No nodes available to run query");
        queryRunner.execute("INSERT INTO test_nation SELECT * FROM tpch.tiny.nation");
        assertFails("SELECT * FROM test_nation ORDER BY nationkey", "No nodes available to run query");

        queryRunner.execute("CREATE TABLE test_region as SELECT * FROM tpch.tiny.region");
        assertQuery("SELECT * FROM test_region ORDER BY regionkey", "SELECT * FROM tpch.tiny.region ORDER BY regionkey");
    }

    private void closeWorker()
            throws Exception
    {
        int nodeCount = queryRunner.getNodeCount();
        TestingPrestoServer worker = queryRunner.getServers().stream()
                .filter(server -> !server.isCoordinator())
                .findAny()
                .orElseThrow(() -> new IllegalStateException("No worker nodes"));
        worker.close();
        waitForNodes(nodeCount - 1);
    }

    private void assertQuery(String sql, String expected)
    {
        MaterializedResult rows = queryRunner.execute(sql);
        MaterializedResult expectedRows = queryRunner.execute(expected);

        assertEquals(rows, expectedRows);
    }

    private void assertFails(String sql, String expectedMessage)
    {
        try {
            queryRunner.execute(sql);
        }
        catch (RuntimeException ex) {
            // pass
            assertEquals(ex.getMessage(), expectedMessage);
            return;
        }
        fail("Query should fail");
    }

    private void waitForNodes(int numberOfNodes)
            throws InterruptedException
    {
        long start = System.nanoTime();
        while (queryRunner.getCoordinator().refreshNodes().getActiveNodes().size() < numberOfNodes) {
            assertLessThan(nanosSince(start), new Duration(10, SECONDS));
            MILLISECONDS.sleep(10);
        }
    }
}
