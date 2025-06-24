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

import com.facebook.presto.Session;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.facebook.presto.nativeworker.NativeApiEndpointUtils.fetchScalarLongMetrics;
import static com.facebook.presto.nativeworker.NativeApiEndpointUtils.sendWorkerRequest;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createCustomer;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

public class TestPrestoNativeAsyncDataCacheCleanupAPI
        extends AbstractTestQueryFramework
{
    private static final String memoryCacheCleanupEndPoint = "/v1/operation/server/clearCache?type=memory";
    private static final String ssdCacheCleanupEndPoint = "/v1/operation/server/clearCache?type=ssd";
    private static final String metricsEndPoint = "/v1/info/metrics";
    private static final String writeToSsdEndPoint = "/v1/operation/server/writeSsd";

    @Override
    protected QueryRunner createQueryRunner() throws Exception
    {
        return PrestoNativeQueryRunnerUtils.nativeHiveQueryRunnerBuilder()
                .setCacheMaxSize(4096)
                .setUseThrift(true)
                .setAddStorageFormatToPath(true)
                .setEnableRuntimeMetricsCollection(true)
                .setEnableSsdCache(true)
                .build();
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner() throws Exception
    {
        return PrestoNativeQueryRunnerUtils.javaHiveQueryRunnerBuilder()
                .setAddStorageFormatToPath(true)
                .build();
    }

    @Override
    protected void createTables()
    {
        QueryRunner queryRunner = (QueryRunner) getExpectedQueryRunner();
        createCustomer(queryRunner);
    }

    @Test(groups = {"async_data_cache"}, enabled = false)
    public void testAsyncDataCacheCleanup() throws Exception
    {
        Session session = Session.builder(super.getSession())
                .setCatalogSessionProperty("hive", "node_selection_strategy", "SOFT_AFFINITY")
                .build();

        QueryRunner queryRunner = getQueryRunner();
        DistributedQueryRunner distributedQueryRunner = (DistributedQueryRunner) queryRunner;
        Set<InternalNode> workerNodes = getWorkerNodes(distributedQueryRunner);

        // 1. Collect initial cache metrics
        Metrics initialMetrics = collectCacheMetrics(workerNodes, metricsEndPoint);
        assertEquals(0, initialMetrics.memoryCacheHits, "Cache hits should be zero initially.");
        assertEquals(0, initialMetrics.memoryCacheEntries, "Cache entries should be zero initially.");

        // 2. Execute queries to populate cache
        for (int i = 0; i < 4; i++) {
            queryRunner.execute(session, "SELECT count(*) FROM customer");
        }
        TimeUnit.SECONDS.sleep(60); // Sleep to allow cache updates

        // 3. Collect cache metrics after queries
        Metrics populatedMetrics = collectCacheMetrics(workerNodes, metricsEndPoint);
        assertNotEquals(0, populatedMetrics.memoryCacheHits, "Cache should have hits after queries.");
        assertNotEquals(0, populatedMetrics.memoryCacheEntries, "Cache should have entries after queries.");

        // 4. Write cache data to ssd
        workerNodes.parallelStream().forEach(worker -> {
            int responseCode = sendWorkerRequest(worker.getInternalUri().toString(), writeToSsdEndPoint);
            assertEquals(200, responseCode, "Expected a 200 OK response for writing to ssd cache.");
        });
        TimeUnit.SECONDS.sleep(60); // Sleep to allow cache updates

        // 5. Collect SSD metrics after ssd write
        Metrics ssdMetrics = collectCacheMetrics(workerNodes, metricsEndPoint);
        assertNotEquals(0, ssdMetrics.ssdCacheWriteEntries, "SSD Cache should have write entries after write API call.");
        assertEquals(0, ssdMetrics.ssdCacheReadEntries, "SSD Cache should have 0 read entries currently.");

        // 6. Trigger memory async cache cleanup
        workerNodes.parallelStream().forEach(worker -> {
            int responseCode = sendWorkerRequest(worker.getInternalUri().toString(), memoryCacheCleanupEndPoint);
            assertEquals(200, responseCode, "Expected a 200 OK response for cache cleanup.");
        });
        TimeUnit.SECONDS.sleep(60); // Sleep to allow cache updates

        // 7. Validate memory cache is cleared
        Metrics finalMetrics = collectCacheMetrics(workerNodes, metricsEndPoint);
        assertEquals(0, finalMetrics.memoryCacheEntries, "Cache should be empty after cleanup.");

        // 8. Execute queries to read from SSD cache
        for (int i = 0; i < 4; i++) {
            queryRunner.execute(session, "SELECT count(*) FROM customer");
        }
        TimeUnit.SECONDS.sleep(60); // Sleep to allow cache updates

        // 9. Collect SSD metrics to check read entries metrics
        Metrics populatedSsdMetrics = collectCacheMetrics(workerNodes, metricsEndPoint);
        assertNotEquals(0, populatedSsdMetrics.ssdCacheReadEntries, "SSD Cache should have non-zero read entries.");

        // 10. Trigger SSD cache clean up
        workerNodes.parallelStream().forEach(worker -> {
            int responseCode = sendWorkerRequest(worker.getInternalUri().toString(), ssdCacheCleanupEndPoint);
            assertEquals(200, responseCode, "Expected a 200 OK response for cache cleanup.");
        });
        TimeUnit.SECONDS.sleep(60); // Sleep to allow cache updates

        // 11. Validate SSD cache is cleared
        Metrics finalSSDCacheMetrics = collectCacheMetrics(workerNodes, metricsEndPoint);
        assertEquals(0, finalSSDCacheMetrics.ssdCacheCachedEntries, "SSD Cache should be empty after cleanup.");
    }

    private Metrics collectCacheMetrics(Set<InternalNode> workerNodes, String endpoint)
            throws Exception
    {
        int memoryCacheHits = 0;
        int memoryCacheEntries = 0;
        int ssdCacheReadEntries = 0;
        int ssdCacheWriteEntries = 0;
        int ssdCacheCachedEntries = 0;

        for (InternalNode worker : workerNodes) {
            Map<String, Long> metrics = fetchScalarLongMetrics(worker.getInternalUri().toString(), endpoint, "GET");
            memoryCacheHits += metrics.get("velox_memory_cache_num_hits");
            memoryCacheEntries += metrics.get("velox_memory_cache_num_entries");
            ssdCacheReadEntries += metrics.get("velox_ssd_cache_read_entries");
            ssdCacheWriteEntries += metrics.get("velox_ssd_cache_written_entries");
            ssdCacheCachedEntries += metrics.get("velox_ssd_cache_cached_entries");
        }
        return new Metrics(memoryCacheHits, memoryCacheEntries, ssdCacheReadEntries, ssdCacheWriteEntries, ssdCacheCachedEntries);
    }

    private static class Metrics
    {
        final int memoryCacheHits;
        final int memoryCacheEntries;
        final int ssdCacheReadEntries;
        final int ssdCacheWriteEntries;
        final int ssdCacheCachedEntries;

        Metrics(int memoryCacheHits, int memoryCacheEntries, int ssdCacheReadEntries, int ssdCacheWriteEntries, int ssdCacheCachedEntries)
        {
            this.memoryCacheHits = memoryCacheHits;
            this.memoryCacheEntries = memoryCacheEntries;
            this.ssdCacheReadEntries = ssdCacheReadEntries;
            this.ssdCacheWriteEntries = ssdCacheWriteEntries;
            this.ssdCacheCachedEntries = ssdCacheCachedEntries;
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

    @Test(groups = {"async_data_cache"}, enabled = false)
    public void testAsyncDataCacheCleanupApiFormat()
    {
        QueryRunner queryRunner = getQueryRunner();
        DistributedQueryRunner distributedQueryRunner = (DistributedQueryRunner) queryRunner;
        Set<InternalNode> workerNodes = getWorkerNodes(distributedQueryRunner);
        InternalNode workerNode = workerNodes.iterator().next();

        int responseCode = sendWorkerRequest(workerNode.getInternalUri().toString(), memoryCacheCleanupEndPoint);
        assertEquals(responseCode, 200, "Expected a 200 OK response for valid shutdown request");

        String invalidEndPoint = "/v1/operation/server/clearCacheNonExisting?name=hive&id=hive";
        responseCode = sendWorkerRequest(workerNode.getInternalUri().toString(), invalidEndPoint);
        assertEquals(responseCode, 500);
    }
}
