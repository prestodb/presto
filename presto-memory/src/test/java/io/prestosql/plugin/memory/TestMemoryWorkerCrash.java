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
package io.prestosql.plugin.memory;

import io.airlift.units.Duration;
import io.prestosql.server.testing.TestingPrestoServer;
import io.prestosql.tests.AbstractTestQueryFramework;
import io.prestosql.tests.DistributedQueryRunner;
import org.testng.annotations.Test;

import static io.airlift.testing.Assertions.assertLessThan;
import static io.airlift.units.Duration.nanosSince;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

@Test(singleThreaded = true)
public class TestMemoryWorkerCrash
        extends AbstractTestQueryFramework
{
    protected TestMemoryWorkerCrash()
    {
        super(MemoryQueryRunner::createQueryRunner);
    }

    @Test
    public void tableAccessAfterWorkerCrash()
            throws Exception
    {
        getQueryRunner().execute("CREATE TABLE test_nation as SELECT * FROM nation");
        assertQuery("SELECT * FROM test_nation ORDER BY nationkey", "SELECT * FROM nation ORDER BY nationkey");
        closeWorker();
        assertQueryFails("SELECT * FROM test_nation ORDER BY nationkey", "No nodes available to run query");
        getQueryRunner().execute("INSERT INTO test_nation SELECT * FROM tpch.tiny.nation");

        assertQueryFails("SELECT * FROM test_nation ORDER BY nationkey", "No nodes available to run query");

        getQueryRunner().execute("CREATE TABLE test_region as SELECT * FROM tpch.tiny.region");
        assertQuery("SELECT * FROM test_region ORDER BY regionkey", "SELECT * FROM region ORDER BY regionkey");
    }

    private void closeWorker()
            throws Exception
    {
        int nodeCount = getNodeCount();
        DistributedQueryRunner queryRunner = (DistributedQueryRunner) getQueryRunner();
        TestingPrestoServer worker = queryRunner.getServers().stream()
                .filter(server -> !server.isCoordinator())
                .findAny()
                .orElseThrow(() -> new IllegalStateException("No worker nodes"));
        worker.close();
        waitForNodes(nodeCount - 1);
    }

    private void waitForNodes(int numberOfNodes)
            throws InterruptedException
    {
        DistributedQueryRunner queryRunner = (DistributedQueryRunner) getQueryRunner();
        long start = System.nanoTime();
        while (queryRunner.getCoordinator().refreshNodes().getActiveNodes().size() < numberOfNodes) {
            assertLessThan(nanosSince(start), new Duration(10, SECONDS));
            MILLISECONDS.sleep(10);
        }
    }
}
