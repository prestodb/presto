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

import com.facebook.presto.Session;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.session.ResourceEstimates;
import com.facebook.presto.tests.tpch.TpchQueryRunnerBuilder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.execution.QueryState.QUEUED;
import static com.facebook.presto.execution.QueryState.RUNNING;
import static com.facebook.presto.execution.TestQueryRunnerUtil.createQuery;
import static com.facebook.presto.execution.TestQueryRunnerUtil.waitForQueryState;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

// run single threaded to avoid creating multiple query runners at once
@Test(singleThreaded = true)
public class TestMinWorkerRequirement
{
    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Cluster is still initializing, there are insufficient active worker nodes \\(4\\) to run query. Waiting for \\(5\\) active worker nodes.")
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

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Cluster is still initializing, there are insufficient active worker nodes \\(3\\) to run query. Waiting for \\(4\\) active worker nodes.")
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
    @Test(timeOut = 50_000)
    public void testInsufficientWorkerNodesWithQueuePolicy()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = TpchQueryRunnerBuilder.builder()
                .setSingleCoordinatorProperty("query-manager.initialization-required-workers", "5")
                .setSingleExtraProperty("query-manager.queue-queries.insufficient-workers", "true")
                .setNodeCount(4)
                .build()) {
            QueryId scheduled = createQuery(queryRunner, newSession("scheduled", ImmutableSet.of(), null), "SELECT 1");
            waitForQueryState(queryRunner, scheduled, QUEUED);
        }
    }

    private static Session newSession(String source, Set<String> clientTags, ResourceEstimates resourceEstimates)
    {
        return testSessionBuilder()
                .setCatalog(null)
                .setSchema(null)
                .setSource(source)
                .setClientTags(clientTags)
                .setResourceEstimates(resourceEstimates)
                .build();
    }

    @Test(timeOut = 50_000)
    public void testInsufficientWorkerNodesWithCoordinatorExcludedWithQueuePolicy()
            throws Exception
    {
        Map<String, String> extraProperties = new HashMap<>();
        extraProperties.put("query-manager.initialization-required-workers", "4");
        extraProperties.put("query-manager.queue-queries.insufficient-workers", "true");
        extraProperties.put("node-scheduler.include-coordinator", "false");
        try (DistributedQueryRunner queryRunner = TpchQueryRunnerBuilder.builder()
                .setExtraProperties(extraProperties)
                .setNodeCount(4)
                .build()) {
            QueryId scheduled = createQuery(queryRunner, newSession("scheduled", ImmutableSet.of(), null), "SELECT 1");
            waitForQueryState(queryRunner, scheduled, QUEUED);
        }
    }

    @Test
    public void testSufficientWorkerNodesWithQueuePolicy()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = TpchQueryRunnerBuilder.builder()
                .setSingleCoordinatorProperty("query-manager.initialization-required-workers", "4")
                .setSingleExtraProperty("query-manager.queue-queries.insufficient-workers", "true")
                .setNodeCount(4)
                .build()) {
            QueryId scheduled = createQuery(queryRunner, newSession("scheduled", ImmutableSet.of(), null), "SELECT 1");
            waitForQueryState(queryRunner, scheduled, RUNNING);
        }
    }
}
