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
@Test(singleThreaded = true)
public class TestMinWorkerRequirement
{
    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Cluster is still initializing, there are insufficient active worker nodes \\(4\\) to run query")
    public void testInsufficientWorkerNodes()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = TpchQueryRunnerBuilder.builder()
                .setSingleCoordinatorProperty("query-manager.initialization-required-workers", "5")
                .setNodeCount(4)
                .build()) {
            queryRunner.execute("SELECT 1");
            fail("Expected exception due to insufficient active worker nodes");
        }
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Cluster is still initializing, there are insufficient active worker nodes \\(3\\) to run query")
    public void testInsufficientWorkerNodesWithCoordinatorExcluded()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = TpchQueryRunnerBuilder.builder()
                .setSingleExtraProperty("node-scheduler.include-coordinator", "false")
                .setSingleCoordinatorProperty("query-manager.initialization-required-workers", "4")
                .setNodeCount(4)
                .build()) {
            queryRunner.execute("SELECT 1");
            fail("Expected exception due to insufficient active worker nodes");
        }
    }

    @Test
    public void testSufficientWorkerNodes()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = TpchQueryRunnerBuilder.builder()
                .setSingleCoordinatorProperty("query-manager.initialization-required-workers", "4")
                .setNodeCount(4)
                .build()) {
            queryRunner.execute("SELECT 1");
            assertEquals(queryRunner.getCoordinator().refreshNodes().getActiveNodes().size(), 4);

            // Query should still be allowed to run if active workers drop down below the minimum required nodes
            queryRunner.getServers().get(0).close();
            assertEquals(queryRunner.getCoordinator().refreshNodes().getActiveNodes().size(), 3);
            queryRunner.execute("SELECT 1");
        }
    }

    @Test
    public void testInitializationTimeout()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = TpchQueryRunnerBuilder.builder()
                .setCoordinatorProperties(ImmutableMap.<String, String>builder()
                        .put("query-manager.initialization-required-workers", "5")
                        .put("query-manager.initialization-timeout", "1ns")
                        .build())
                .setNodeCount(4)
                .build()) {
            queryRunner.execute("SELECT 1");
            assertEquals(queryRunner.getCoordinator().refreshNodes().getActiveNodes().size(), 4);
        }
    }
}
