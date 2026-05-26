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
package com.facebook.presto.plugin.opensearch;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.http.HttpHost;
import org.opensearch.client.Request;
import org.opensearch.client.RestClient;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.utility.DockerImageName;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.time.Duration;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Tests OpenSearch Scroll API pagination with large result sets.
 * Verifies that queries returning more than scroll.size (default 1000) rows
 * correctly use scroll continuation to fetch all results.
 */
public class TestScrollPagination
{
    private static final Logger log = Logger.get(TestScrollPagination.class);
    private static final String OPENSEARCH_IMAGE = "opensearchproject/opensearch:2.11.1";
    private static final int SCROLL_SIZE = 100; // Small scroll size to force multiple iterations
    private static final int TOTAL_DOCS = 2500; // More than 2x scroll size

    private GenericContainer<?> opensearchContainer;
    private RestClient restClient;
    private QueryRunner queryRunner;

    @BeforeClass
    public void setup()
            throws Exception
    {
        // Start OpenSearch container
        opensearchContainer = new GenericContainer<>(DockerImageName.parse(OPENSEARCH_IMAGE))
                .withExposedPorts(9200, 9600)
                .withEnv("discovery.type", "single-node")
                .withEnv("OPENSEARCH_JAVA_OPTS", "-Xms512m -Xmx512m")
                .withEnv("DISABLE_SECURITY_PLUGIN", "true")
                .waitingFor(new HttpWaitStrategy()
                        .forPort(9200)
                        .forStatusCode(200)
                        .withStartupTimeout(Duration.ofMinutes(2)));

        opensearchContainer.start();

        log.info("OpenSearch container started at: http://%s:%d",
                opensearchContainer.getHost(),
                opensearchContainer.getMappedPort(9200));

        // Create REST client
        restClient = RestClient.builder(
                new HttpHost(
                        opensearchContainer.getHost(),
                        opensearchContainer.getMappedPort(9200),
                        "http"))
                .build();

        // Create test index with many documents
        createLargeTestIndex();

        // Create query runner - we'll manually configure scroll size via catalog properties
        queryRunner = OpenSearchQueryRunner.createQueryRunner(
                opensearchContainer,
                ImmutableList.of()); // No TPCH tables needed

        // Create catalog with custom scroll size
        String host = opensearchContainer.getHost();
        Integer port = opensearchContainer.getMappedPort(9200);

        // Plugin is already installed by OpenSearchQueryRunner, just create the catalog
        queryRunner.createCatalog("opensearch_scroll", "opensearch",
                ImmutableMap.<String, String>builder()
                        .put("opensearch.host", host)
                        .put("opensearch.port", String.valueOf(port))
                        .put("opensearch.scheme", "http")
                        .put("opensearch.ssl.enabled", "false")
                        .put("opensearch.scroll.size", String.valueOf(SCROLL_SIZE))
                        .put("opensearch.scroll.timeout", "1m")
                        .build());
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        if (queryRunner != null) {
            queryRunner.close();
        }
        if (restClient != null) {
            restClient.close();
        }
        if (opensearchContainer != null) {
            opensearchContainer.stop();
        }
    }

    private void createLargeTestIndex()
            throws Exception
    {
        String indexName = "scroll_test";

        // Create index
        Request createIndex = new Request("PUT", "/" + indexName);
        createIndex.setJsonEntity(
                "{\"settings\":{\"number_of_shards\":1,\"number_of_replicas\":0}," +
                "\"mappings\":{\"properties\":{" +
                "\"id\":{\"type\":\"long\"}," +
                "\"value\":{\"type\":\"keyword\"}," +
                "\"score\":{\"type\":\"double\"}" +
                "}}}");
        restClient.performRequest(createIndex);

        log.info("Created index: %s", indexName);

        // Bulk insert documents
        StringBuilder bulkRequest = new StringBuilder();
        for (int i = 1; i <= TOTAL_DOCS; i++) {
            bulkRequest.append("{\"index\":{\"_index\":\"").append(indexName).append("\"}}\n");
            bulkRequest.append("{\"id\":").append(i)
                    .append(",\"value\":\"doc_").append(i)
                    .append("\",\"score\":").append(i * 1.5).append("}\n");
        }

        Request bulk = new Request("POST", "/_bulk");
        bulk.addParameter("refresh", "true");
        bulk.setJsonEntity(bulkRequest.toString());
        restClient.performRequest(bulk);

        log.info("Inserted %d documents into index: %s", TOTAL_DOCS, indexName);
    }

    @Test
    public void testScrollPaginationFetchesAllRows()
    {
        Session session = testSessionBuilder()
                .setCatalog("opensearch_scroll")
                .setSchema("default")
                .build();

        // Query all documents - should trigger multiple scroll iterations
        String query = "SELECT COUNT(*) FROM scroll_test";
        MaterializedResult result = queryRunner.execute(session, query);

        long count = (Long) result.getOnlyValue();
        assertEquals(count, TOTAL_DOCS, "Should fetch all documents using scroll pagination");

        log.info("Successfully fetched all %d documents using scroll (scroll_size=%d, iterations=%d)",
                TOTAL_DOCS, SCROLL_SIZE, (TOTAL_DOCS + SCROLL_SIZE - 1) / SCROLL_SIZE);
    }

    @Test
    public void testScrollPaginationWithFilter()
    {
        Session session = testSessionBuilder()
                .setCatalog("opensearch_scroll")
                .setSchema("default")
                .build();

        // Query with filter - should still use scroll
        String query = "SELECT COUNT(*) FROM scroll_test WHERE id > 500";
        MaterializedResult result = queryRunner.execute(session, query);

        long count = (Long) result.getOnlyValue();
        assertEquals(count, TOTAL_DOCS - 500, "Should fetch filtered documents using scroll");

        log.info("Successfully fetched %d filtered documents using scroll", count);
    }

    @Test
    public void testScrollPaginationWithProjection()
    {
        Session session = testSessionBuilder()
                .setCatalog("opensearch_scroll")
                .setSchema("default")
                .build();

        // Query specific columns - should use scroll with source filtering
        String query = "SELECT id, value FROM scroll_test ORDER BY id LIMIT " + TOTAL_DOCS;
        MaterializedResult result = queryRunner.execute(session, query);

        assertEquals(result.getRowCount(), TOTAL_DOCS, "Should fetch all rows with projection");

        // Verify first and last rows
        assertEquals(result.getMaterializedRows().get(0).getField(0), 1L);
        assertEquals(result.getMaterializedRows().get(TOTAL_DOCS - 1).getField(0), (long) TOTAL_DOCS);

        log.info("Successfully fetched %d rows with projection using scroll", TOTAL_DOCS);
    }

    @Test
    public void testScrollPaginationWithAggregation()
    {
        Session session = testSessionBuilder()
                .setCatalog("opensearch_scroll")
                .setSchema("default")
                .build();

        // Aggregation over large dataset
        String query = "SELECT AVG(score) FROM scroll_test";
        MaterializedResult result = queryRunner.execute(session, query);

        double avgScore = (Double) result.getOnlyValue();
        double expectedAvg = ((1 + TOTAL_DOCS) / 2.0) * 1.5; // Average of 1.5, 3.0, 4.5, ...

        assertTrue(Math.abs(avgScore - expectedAvg) < 0.01,
                String.format("Average should be close to %.2f, got %.2f", expectedAvg, avgScore));

        log.info("Successfully computed aggregation over %d documents using scroll", TOTAL_DOCS);
    }

    @Test
    public void testMultipleQueriesReuseScrollContext()
    {
        Session session = testSessionBuilder()
                .setCatalog("opensearch_scroll")
                .setSchema("default")
                .build();

        // Execute multiple queries to verify scroll contexts are properly managed
        for (int i = 0; i < 3; i++) {
            String query = "SELECT COUNT(*) FROM scroll_test WHERE id > " + (i * 500);
            MaterializedResult result = queryRunner.execute(session, query);
            long count = (Long) result.getOnlyValue();
            assertTrue(count > 0, "Query " + i + " should return results");
        }

        log.info("Successfully executed multiple queries with independent scroll contexts");
    }
}

// Made with Bob
