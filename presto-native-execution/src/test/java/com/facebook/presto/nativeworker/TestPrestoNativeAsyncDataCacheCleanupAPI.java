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
package com.facebook.presto.nativeworker;

import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.nativeworker.NativeApiEndpointUtils.fetchScalarLongMetrics;
import static com.facebook.presto.nativeworker.NativeApiEndpointUtils.sendWorkerRequest;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createCustomer;
import static java.lang.Thread.sleep;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

public class TestPrestoNativeAsyncDataCacheCleanupAPI
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner() throws Exception
    {
        return PrestoNativeQueryRunnerUtils.createQueryRunner(true);
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner() throws Exception
    {
        return PrestoNativeQueryRunnerUtils.createJavaQueryRunner();
    }

    @Override
    protected void createTables()
    {
        QueryRunner queryRunner = (QueryRunner) getExpectedQueryRunner();
        createCustomer(queryRunner);
    }

    @Test
    public void testAsyncDataCacheCleanup() throws Exception
    {
        String metricsEndPoint = "/v1/info/metrics";
        String cacheCleanupEndPoint = "/v1/memory";

        QueryRunner queryRunner = getQueryRunner();
        DistributedQueryRunner distributedQueryRunner = (DistributedQueryRunner) queryRunner;
        Set<InternalNode> workerNodes = getWorkerNodes(distributedQueryRunner);

        // Collect initial cache metrics
        Metrics initialMetrics = collectCacheMetrics(workerNodes, metricsEndPoint);
        assertEquals(0, initialMetrics.hits, "Cache hits should be zero initially.");
        assertEquals(0, initialMetrics.entries, "Cache entries should be zero initially.");

        // Execute queries to populate cache
        for (int i = 0; i < 4; i++) {
            assertQuery("SELECT count(*) FROM customer");
        }
        sleep(60000); // Sleep to allow cache updates

        // Collect cache metrics after queries
        Metrics populatedMetrics = collectCacheMetrics(workerNodes, metricsEndPoint);
        assertNotEquals(0, populatedMetrics.hits, "Cache should have hits after queries.");
        assertNotEquals(0, populatedMetrics.entries, "Cache should have entries after queries.");

        // Trigger async cache cleanup
        workerNodes.parallelStream().forEach(worker -> {
            int responseCode = sendWorkerRequest(worker.getInternalUri().toString(), "\"CLEAN_ASYNC_DATA_CACHE\"", cacheCleanupEndPoint);
            assertEquals(200, responseCode, "Expected a 200 OK response for cache cleanup.");
        });
        sleep(60000); // Sleep to allow cache updates

        // Validate cache is cleared
        Metrics finalMetrics = collectCacheMetrics(workerNodes, metricsEndPoint);
        assertEquals(0, finalMetrics.entries, "Cache should be empty after cleanup.");
    }

    private Metrics collectCacheMetrics(Set<InternalNode> workerNodes, String endpoint)
            throws Exception
    {
        int hits = 0;
        int entries = 0;
        for (InternalNode worker : workerNodes) {
            Map<String, Long> metrics = fetchScalarLongMetrics(worker.getInternalUri().toString(), endpoint, "GET");
            hits += metrics.getOrDefault("velox_memory_cache_num_hits", 0L);
            entries += metrics.getOrDefault("velox_memory_cache_num_entries", 0L);
        }
        return new Metrics(hits, entries);
    }

    private static class Metrics
    {
        final int hits;
        final int entries;

        Metrics(int hits, int entries)
        {
            this.hits = hits;
            this.entries = entries;
        }
    }

    private boolean isCoordinator(DistributedQueryRunner distributedQueryRunner, InternalNode node)
    {
        return distributedQueryRunner.getCoordinator().getNodeManager().getCoordinators().contains(node);
    }

    private Set<InternalNode> getWorkerNodes(DistributedQueryRunner queryRunner)
    {
        return queryRunner.getCoordinator()
                .getNodeManager()
                .getAllNodes()
                .getActiveNodes()
                .stream()
                .filter(node -> !isCoordinator(queryRunner, node))
                .collect(Collectors.toSet());
    }
    @Test
    public void testAsyncDataCacheCleanupApiFormat()
    {
        String endPoint = "/v1/memory";
        QueryRunner queryRunner = getQueryRunner();
        DistributedQueryRunner distributedQueryRunner = (DistributedQueryRunner) queryRunner;
        Set<InternalNode> workerNodes = getWorkerNodes(distributedQueryRunner);
        InternalNode workerNode = workerNodes.iterator().next();

        int responseCode = sendWorkerRequest(workerNode.getInternalUri().toString(), "\"CLEAN_ASYNC_DATA_CACHE\"", endPoint);
        assertEquals(responseCode, 200, "Expected a 200 OK response for valid shutdown request");

        responseCode = sendWorkerRequest(workerNode.getInternalUri().toString(), "INVALID_BODY", endPoint);
        assertEquals(responseCode, 400, "Expected a 400 Bad Request response for invalid body");

        queryRunner.close();
    }
}
