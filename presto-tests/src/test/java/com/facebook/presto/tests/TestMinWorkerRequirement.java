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
package com.facebook.presto.tests;

import com.facebook.presto.tests.tpch.TpchQueryRunnerBuilder;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

// run single threaded to avoid creating multiple query runners at once
// failure detector is disabled in these tests to prevent flakiness since the tests assert a specific number of workers are present
@Test(singleThreaded = true)
public class TestMinWorkerRequirement
{
    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Insufficient active worker nodes. Waited 1.00ns for at least 5 workers, but only 4 workers are active")
    public void testInsufficientWorkerNodes()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = TpchQueryRunnerBuilder.builder()
                .setCoordinatorProperties(ImmutableMap.<String, String>builder()
                        .put("query-manager.required-workers", "5")
                        .put("query-manager.required-workers-max-wait", "1ns")
                        .put("failure-detector.enabled", "false")
                        .build())
                .setNodeCount(4)
                .build()) {
            queryRunner.execute("SELECT 1");
            fail("Expected exception due to insufficient active worker nodes");
        }
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Insufficient active worker nodes. Waited 1.00ns for at least 4 workers, but only 3 workers are active")
    public void testInsufficientWorkerNodesWithCoordinatorExcluded()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = TpchQueryRunnerBuilder.builder()
                .setCoordinatorProperties(ImmutableMap.<String, String>builder()
                        .put("node-scheduler.include-coordinator", "false")
                        .put("query-manager.required-workers", "4")
                        .put("query-manager.required-workers-max-wait", "1ns")
                        .put("failure-detector.enabled", "false")
                        .build())
                .setNodeCount(4)
                .build()) {
            queryRunner.execute("SELECT 1");
            fail("Expected exception due to insufficient active worker nodes");
        }
    }

    @Test
    public void testInsufficientWorkerNodesAfterDrop()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = TpchQueryRunnerBuilder.builder()
                .setCoordinatorProperties(ImmutableMap.<String, String>builder()
                        .put("query-manager.required-workers", "4")
                        .put("query-manager.required-workers-max-wait", "1ns")
                        .put("failure-detector.enabled", "false")
                        .build())
                .setNodeCount(4)
                .build()) {
            queryRunner.execute("SELECT 1");
            assertEquals(queryRunner.getCoordinator().refreshNodes().getActiveNodes().size(), 4);

            try {
                // Query should still be allowed to run if active workers drop down below the minimum required nodes
                queryRunner.getServers().get(0).close();
                assertEquals(queryRunner.getCoordinator().refreshNodes().getActiveNodes().size(), 3);
                queryRunner.execute("SELECT 1");
            }
            catch (RuntimeException e) {
                assertEquals(e.getMessage(), "Insufficient active worker nodes. Waited 1.00ns for at least 4 workers, but only 3 workers are active");
            }
        }
    }
}
