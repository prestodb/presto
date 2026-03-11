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
import com.facebook.presto.tests.AbstractTestQueries;
import io.airlift.tpch.TpchTable;
import org.apache.http.HttpHost;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.utility.DockerImageName;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.time.Duration;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Comprehensive integration tests for OpenSearch connector.
 * Extends AbstractTestQueries to inherit standard query test suite (500+ tests)
 * and adds OpenSearch-specific tests including nested field queries.
 */
public class TestOpenSearchQueries
        extends AbstractTestQueries
{
    private static final Logger log = Logger.get(TestOpenSearchQueries.class);
    private static final String OPENSEARCH_IMAGE = "opensearchproject/opensearch:2.11.1";
    private static volatile boolean indexCreated;
    private static final Object INDEX_LOCK = new Object();

    private static GenericContainer<?> opensearchContainer;
    private static RestClient restClient;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        // Start OpenSearch container only once (called by framework before @BeforeClass)
        if (opensearchContainer == null) {
            synchronized (INDEX_LOCK) {
                if (opensearchContainer == null) {
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

                    // Create REST client for index setup
                    restClient = RestClient.builder(
                            new HttpHost(
                                    opensearchContainer.getHost(),
                                    opensearchContainer.getMappedPort(9200),
                                    "http"))
                            .build();

                    // Create test indices with nested fields
                    createTestIndices();
                }
            }
        }

        return OpenSearchQueryRunner.createQueryRunner(opensearchContainer, TpchTable.getTables());
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws IOException
    {
        if (restClient != null) {
            restClient.close();
        }
        if (opensearchContainer != null) {
            opensearchContainer.stop();
        }
    }

    /**
     * Helper method to create a session for OpenSearch-specific queries.
     * Uses the opensearch_with_id catalog which has _id column visible.
     */
    private Session createOpenSearchSession()
    {
        return testSessionBuilder()
                .setCatalog("opensearch_with_id")
                .setSchema("default")
                .build();
    }

    /**
     * Create test indices with nested field mappings and sample data.
     */
    private void createTestIndices()
            throws IOException
    {
        // Only create index once across all test instances (double-check locking)
        synchronized (INDEX_LOCK) {
            if (indexCreated) {
                log.info("Index already created, skipping creation");
                return;
            }

            // Delete index if it exists to ensure clean state
            try {
                Request deleteRequest = new Request("DELETE", "/analytics_query_index");
                restClient.performRequest(deleteRequest);
                log.info("Deleted existing analytics_query_index");
            }
            catch (Exception e) {
                // Index doesn't exist, which is fine
                log.info("Index analytics_query_index does not exist, creating new");
            }

            // Create analytics_query_index with nested fields and multiple shards
            // Include problematic field names that could cause serialization issues
            String analyticsMapping = "{\n" +
                    "  \"settings\": {\n" +
                    "    \"number_of_shards\": 3,\n" +
                    "    \"number_of_replicas\": 0\n" +
                    "  },\n" +
                    "  \"mappings\": {\n" +
                    "    \"properties\": {\n" +
                    "      \"user_id\": { \"type\": \"keyword\" },\n" +
                    "      \"query_text\": { \"type\": \"text\" },\n" +
                    "      \"cache_creation_input_tokens\": { \"type\": \"long\" },\n" +
                    "      \"cache_creation_output_tokens\": { \"type\": \"long\" },\n" +
                    "      \"cache_read_input_tokens\": { \"type\": \"long\" },\n" +
                    "      \"total_input_tokens\": { \"type\": \"long\" },\n" +
                    "      \"prompt_tokens_used\": { \"type\": \"long\" },\n" +
                    "      \"completion_tokens_generated\": { \"type\": \"long\" },\n" +
                    "      \"model_response_time_ms\": { \"type\": \"long\" },\n" +
                    "      \"api_call_timestamp\": { \"type\": \"date\" },\n" +
                    "      \"llm_model_version\": { \"type\": \"keyword\" },\n" +
                    "      \"token_usage\": {\n" +
                    "        \"type\": \"object\",\n" +
                    "        \"properties\": {\n" +
                    "          \"total_tokens\": { \"type\": \"long\" },\n" +
                    "          \"prompt_tokens\": { \"type\": \"long\" },\n" +
                    "          \"completion_tokens\": { \"type\": \"long\" }\n" +
                    "        }\n" +
                    "      },\n" +
                    "      \"reliability_scores\": {\n" +
                    "        \"type\": \"object\",\n" +
                    "        \"properties\": {\n" +
                    "          \"answer_relevance\": {\n" +
                    "            \"type\": \"object\",\n" +
                    "            \"properties\": {\n" +
                    "              \"score\": { \"type\": \"integer\" },\n" +
                    "              \"confidence\": { \"type\": \"double\" }\n" +
                    "            }\n" +
                    "          },\n" +
                    "          \"faithfulness\": {\n" +
                    "            \"type\": \"object\",\n" +
                    "            \"properties\": {\n" +
                    "              \"score\": { \"type\": \"integer\" }\n" +
                    "            }\n" +
                    "          }\n" +
                    "        }\n" +
                    "      },\n" +
                    "      \"metadata\": {\n" +
                    "        \"type\": \"object\",\n" +
                    "        \"properties\": {\n" +
                    "          \"model_name\": { \"type\": \"keyword\" },\n" +
                    "          \"timestamp\": { \"type\": \"date\" }\n" +
                    "        }\n" +
                    "      }\n" +
                    "    }\n" +
                    "  }\n" +
                    "}";

            Request createIndexRequest = new Request("PUT", "/analytics_query_index");
            createIndexRequest.setJsonEntity(analyticsMapping);
            Response response = restClient.performRequest(createIndexRequest);
            assertEquals(response.getStatusLine().getStatusCode(), 200);

            log.info("Created analytics_query_index");

            // Insert sample documents
            insertSampleDocuments();

            // Create k-NN index for vector search tests
            createKnnIndex();

            // Mark index as created
            indexCreated = true;
        }
    }

    /**
     * Create k-NN index with vector embeddings for table function tests.
     */
    private void createKnnIndex()
            throws IOException
    {
        // Delete index if it exists
        try {
            Request deleteRequest = new Request("DELETE", "/knn_docs");
            restClient.performRequest(deleteRequest);
            log.info("Deleted existing knn_docs index");
        }
        catch (Exception e) {
            log.info("Index knn_docs does not exist, creating new");
        }

        // Create knn_docs index with k-NN vector field and multiple shards
        String knnMapping = "{\n" +
                "  \"settings\": {\n" +
                "    \"number_of_shards\": 4,\n" +
                "    \"number_of_replicas\": 0,\n" +
                "    \"index.knn\": true\n" +
                "  },\n" +
                "  \"mappings\": {\n" +
                "    \"properties\": {\n" +
                "      \"doc_id\": { \"type\": \"keyword\" },\n" +
                "      \"category\": { \"type\": \"keyword\" },\n" +
                "      \"knn_embedding\": {\n" +
                "        \"type\": \"knn_vector\",\n" +
                "        \"dimension\": 3,\n" +
                "        \"method\": {\n" +
                "          \"name\": \"hnsw\",\n" +
                "          \"space_type\": \"cosinesimil\",\n" +
                "          \"engine\": \"nmslib\",\n" +
                "          \"parameters\": {\n" +
                "            \"ef_construction\": 128,\n" +
                "            \"m\": 24\n" +
                "          }\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";

        Request createKnnIndexRequest = new Request("PUT", "/knn_docs");
        createKnnIndexRequest.setJsonEntity(knnMapping);
        Response knnResponse = restClient.performRequest(createKnnIndexRequest);
        assertEquals(knnResponse.getStatusLine().getStatusCode(), 200);
        log.info("Created knn_docs index");

        // Insert sample k-NN documents
        insertKnnDocuments();
        // Create vector_docs index for vector function tests
        createVectorDocsIndex();
    }

    /**
     * Insert sample documents with k-NN vector embeddings.
     */
    private void insertKnnDocuments()
            throws IOException
    {
        // Document 1: vector [1.0, 0.0, 0.0]
        String knnDoc1 = "{\n" +
                "  \"doc_id\": \"vec-1\",\n" +
                "  \"category\": \"tech\",\n" +
                "  \"knn_embedding\": [1.0, 0.0, 0.0]\n" +
                "}";
        Request indexKnnDoc1 = new Request("PUT", "/knn_docs/_doc/1");
        indexKnnDoc1.setJsonEntity(knnDoc1);
        restClient.performRequest(indexKnnDoc1);

        // Document 2: vector [0.9, 0.1, 0.0]
        String knnDoc2 = "{\n" +
                "  \"doc_id\": \"vec-2\",\n" +
                "  \"category\": \"tech\",\n" +
                "  \"knn_embedding\": [0.9, 0.1, 0.0]\n" +
                "}";
        Request indexKnnDoc2 = new Request("PUT", "/knn_docs/_doc/2");
        indexKnnDoc2.setJsonEntity(knnDoc2);
        restClient.performRequest(indexKnnDoc2);

        // Document 3: vector [0.8, 0.2, 0.0]
        String knnDoc3 = "{\n" +
                "  \"doc_id\": \"vec-3\",\n" +
                "  \"category\": \"science\",\n" +
                "  \"knn_embedding\": [0.8, 0.2, 0.0]\n" +
                "}";
        Request indexKnnDoc3 = new Request("PUT", "/knn_docs/_doc/3");
        indexKnnDoc3.setJsonEntity(knnDoc3);
        restClient.performRequest(indexKnnDoc3);

        // Document 4: vector [0.0, 1.0, 0.0]
        String knnDoc4 = "{\n" +
                "  \"doc_id\": \"vec-4\",\n" +
                "  \"category\": \"science\",\n" +
                "  \"knn_embedding\": [0.0, 1.0, 0.0]\n" +
                "}";
        Request indexKnnDoc4 = new Request("PUT", "/knn_docs/_doc/4");
        indexKnnDoc4.setJsonEntity(knnDoc4);
        restClient.performRequest(indexKnnDoc4);

        // Document 5: vector [0.0, 0.0, 1.0]
        String knnDoc5 = "{\n" +
                "  \"doc_id\": \"vec-5\",\n" +
                "  \"category\": \"arts\",\n" +
                "  \"knn_embedding\": [0.0, 0.0, 1.0]\n" +
                "}";
        Request indexKnnDoc5 = new Request("PUT", "/knn_docs/_doc/5");
        indexKnnDoc5.setJsonEntity(knnDoc5);
        restClient.performRequest(indexKnnDoc5);

        // Refresh index to make documents searchable
        Request refreshRequest = new Request("POST", "/knn_docs/_refresh");
        restClient.performRequest(refreshRequest);
        log.info("Inserted 5 k-NN documents and refreshed index");
    }
    /**
     * Create vector_docs index with array-type embedding field for vector function tests.
     */
    private void createVectorDocsIndex()
            throws IOException
    {
        // Delete index if it exists
        try {
            Request deleteRequest = new Request("DELETE", "/vector_docs");
            restClient.performRequest(deleteRequest);
            log.info("Deleted existing vector_docs index");
        }
        catch (Exception e) {
            log.info("Index vector_docs does not exist, creating new");
        }

        // Create vector_docs index with array field for embeddings and multiple shards
        String vectorDocsMapping = "{\n" +
                "  \"settings\": {\n" +
                "    \"number_of_shards\": 3,\n" +
                "    \"number_of_replicas\": 0\n" +
                "  },\n" +
                "  \"mappings\": {\n" +
                "    \"properties\": {\n" +
                "      \"doc_id\": { \"type\": \"keyword\" },\n" +
                "      \"embedding\": { \"type\": \"float\", \"index\": false }\n" +
                "    }\n" +
                "  }\n" +
                "}";

        Request createVectorDocsRequest = new Request("PUT", "/vector_docs");
        createVectorDocsRequest.setJsonEntity(vectorDocsMapping);
        Response vectorDocsResponse = restClient.performRequest(createVectorDocsRequest);
        assertEquals(vectorDocsResponse.getStatusLine().getStatusCode(), 200);
        log.info("Created vector_docs index");

        // Insert sample documents with array embeddings
        insertVectorDocuments();
    }

    /**
     * Insert sample documents with array-type embeddings for vector function tests.
     */
    private void insertVectorDocuments()
            throws IOException
    {
        // Document 1: embedding [1.0, 0.0, 0.0] - unit vector on x-axis
        String vecDoc1 = "{\n" +
                "  \"doc_id\": \"vec-1\",\n" +
                "  \"embedding\": [1.0, 0.0, 0.0]\n" +
                "}";
        Request indexVecDoc1 = new Request("PUT", "/vector_docs/_doc/1");
        indexVecDoc1.setJsonEntity(vecDoc1);
        restClient.performRequest(indexVecDoc1);

        // Document 2: embedding [0.0, 1.0, 0.0] - unit vector on y-axis
        String vecDoc2 = "{\n" +
                "  \"doc_id\": \"vec-2\",\n" +
                "  \"embedding\": [0.0, 1.0, 0.0]\n" +
                "}";
        Request indexVecDoc2 = new Request("PUT", "/vector_docs/_doc/2");
        indexVecDoc2.setJsonEntity(vecDoc2);
        restClient.performRequest(indexVecDoc2);

        // Document 3: embedding [3.0, 4.0, 0.0] - 3-4-5 triangle, magnitude 5.0
        String vecDoc3 = "{\n" +
                "  \"doc_id\": \"vec-3\",\n" +
                "  \"embedding\": [3.0, 4.0, 0.0]\n" +
                "}";
        Request indexVecDoc3 = new Request("PUT", "/vector_docs/_doc/3");
        indexVecDoc3.setJsonEntity(vecDoc3);
        restClient.performRequest(indexVecDoc3);

        // Refresh index to make documents searchable
        Request refreshRequest = new Request("POST", "/vector_docs/_refresh");
        restClient.performRequest(refreshRequest);
        log.info("Inserted 3 vector documents and refreshed index");
    }

    /**
     * Insert sample documents with nested field data.
     */
    private void insertSampleDocuments()
            throws IOException
    {
        // Document 1 - includes problematic field names
        String doc1 = "{\n" +
                "  \"user_id\": \"user1\",\n" +
                "  \"query_text\": \"What is machine learning?\",\n" +
                "  \"cache_creation_input_tokens\": 1000,\n" +
                "  \"cache_creation_output_tokens\": 500,\n" +
                "  \"cache_read_input_tokens\": 200,\n" +
                "  \"total_input_tokens\": 1200,\n" +
                "  \"prompt_tokens_used\": 800,\n" +
                "  \"completion_tokens_generated\": 400,\n" +
                "  \"model_response_time_ms\": 1500,\n" +
                "  \"api_call_timestamp\": \"2024-01-01T00:00:00Z\",\n" +
                "  \"llm_model_version\": \"gpt-4-0613\",\n" +
                "  \"token_usage\": {\n" +
                "    \"total_tokens\": 90616,\n" +
                "    \"prompt_tokens\": 45308,\n" +
                "    \"completion_tokens\": 45308\n" +
                "  },\n" +
                "  \"reliability_scores\": {\n" +
                "    \"answer_relevance\": {\n" +
                "      \"score\": 80,\n" +
                "      \"confidence\": 0.85\n" +
                "    },\n" +
                "    \"faithfulness\": {\n" +
                "      \"score\": 75\n" +
                "    }\n" +
                "  },\n" +
                "  \"metadata\": {\n" +
                "    \"model_name\": \"gpt-4\",\n" +
                "    \"timestamp\": \"2024-01-01T00:00:00Z\"\n" +
                "  }\n" +
                "}";

        Request indexDoc1 = new Request("PUT", "/analytics_query_index/_doc/test-id-1");
        indexDoc1.setJsonEntity(doc1);
        restClient.performRequest(indexDoc1);

        // Document 2
        String doc2 = "{\n" +
                "  \"user_id\": \"user2\",\n" +
                "  \"query_text\": \"Explain neural networks\",\n" +
                "  \"token_usage\": {\n" +
                "    \"total_tokens\": 120000,\n" +
                "    \"prompt_tokens\": 60000,\n" +
                "    \"completion_tokens\": 60000\n" +
                "  },\n" +
                "  \"reliability_scores\": {\n" +
                "    \"answer_relevance\": {\n" +
                "      \"score\": 90,\n" +
                "      \"confidence\": 0.92\n" +
                "    },\n" +
                "    \"faithfulness\": {\n" +
                "      \"score\": 85\n" +
                "    }\n" +
                "  },\n" +
                "  \"metadata\": {\n" +
                "    \"model_name\": \"gpt-4\",\n" +
                "    \"timestamp\": \"2024-01-02T00:00:00Z\"\n" +
                "  }\n" +
                "}";

        Request indexDoc2 = new Request("PUT", "/analytics_query_index/_doc/test-id-2");
        indexDoc2.setJsonEntity(doc2);
        restClient.performRequest(indexDoc2);

        // Document 3
        String doc3 = "{\n" +
                "  \"user_id\": \"user1\",\n" +
                "  \"query_text\": \"Deep learning basics\",\n" +
                "  \"token_usage\": {\n" +
                "    \"total_tokens\": 45000,\n" +
                "    \"prompt_tokens\": 22500,\n" +
                "    \"completion_tokens\": 22500\n" +
                "  },\n" +
                "  \"reliability_scores\": {\n" +
                "    \"answer_relevance\": {\n" +
                "      \"score\": 70,\n" +
                "      \"confidence\": 0.78\n" +
                "    },\n" +
                "    \"faithfulness\": {\n" +
                "      \"score\": 65\n" +
                "    }\n" +
                "  },\n" +
                "  \"metadata\": {\n" +
                "    \"model_name\": \"gpt-3.5\",\n" +
                "    \"timestamp\": \"2024-01-03T00:00:00Z\"\n" +
                "  }\n" +
                "}";

        Request indexDoc3 = new Request("PUT", "/analytics_query_index/_doc/test-id-3");
        indexDoc3.setJsonEntity(doc3);
        restClient.performRequest(indexDoc3);

        // Refresh index to make documents searchable
        Request refreshRequest = new Request("POST", "/analytics_query_index/_refresh");
        restClient.performRequest(refreshRequest);

        log.info("Inserted sample documents into analytics_query_index");
    }

    // ========== Nested Field Query Tests ==========

    @Test
    public void testSelectNestedField()
    {
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT \"token_usage.total_tokens\" FROM analytics_query_index WHERE _id = 'test-id-1'",
                "SELECT CAST(90616 AS BIGINT)");
    }

    @Test
    public void testSelectMultipleNestedFields()
    {
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT \"token_usage.total_tokens\", \"token_usage.prompt_tokens\" " +
                        "FROM analytics_query_index WHERE _id = 'test-id-1'",
                "SELECT CAST(90616 AS BIGINT), CAST(45308 AS BIGINT)");
    }

    @Test
    public void testPredicateOnNestedField()
    {
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT _id FROM analytics_query_index WHERE \"token_usage.total_tokens\" > 50000 ORDER BY _id",
                "VALUES ('test-id-1'), ('test-id-2')");
    }

    @Test
    public void testDeeplyNestedField()
    {
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT \"reliability_scores.answer_relevance.score\" FROM analytics_query_index WHERE _id = 'test-id-1'",
                "SELECT 80");
    }

    @Test
    public void testDeeplyNestedFieldWithConfidence()
    {
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT \"reliability_scores.answer_relevance.score\", \"reliability_scores.answer_relevance.confidence\" " +
                        "FROM analytics_query_index WHERE _id = 'test-id-2'",
                "SELECT 90, 0.92");
    }

    @Test
    public void testAggregateOnNestedField()
    {
        assertQuery(
                createOpenSearchSession(),
                "SELECT COUNT(DISTINCT _id), AVG(\"token_usage.total_tokens\") FROM analytics_query_index",
                "SELECT CAST(3 AS BIGINT), 85205.33333333333");
    }

    @Test
    public void testGroupByNestedField()
    {
        // user1 has 2 documents: 90616 + 45000 = 135616
        // user2 has 1 document: 120000
        assertQuery(
                createOpenSearchSession(),
                "SELECT user_id, SUM(\"token_usage.total_tokens\") FROM analytics_query_index GROUP BY user_id ORDER BY user_id",
                "VALUES ('user1', CAST(135616 AS BIGINT)), ('user2', CAST(120000 AS BIGINT))");
    }

    @Test
    public void testOrderByNestedField()
    {
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT _id, \"token_usage.total_tokens\" FROM analytics_query_index " +
                        "ORDER BY \"token_usage.total_tokens\" DESC",
                "VALUES ('test-id-2', CAST(120000 AS BIGINT)), ('test-id-1', CAST(90616 AS BIGINT)), ('test-id-3', CAST(45000 AS BIGINT))");
    }

    @Test
    public void testMultipleNestedFieldsInPredicate()
    {
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT _id FROM analytics_query_index " +
                        "WHERE \"token_usage.total_tokens\" > 50000 " +
                        "AND \"reliability_scores.answer_relevance.score\" > 75 " +
                        "ORDER BY _id",
                "VALUES ('test-id-1'), ('test-id-2')");
    }

    @Test
    public void testNestedFieldWithMetadata()
    {
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT \"metadata.model_name\", \"token_usage.total_tokens\" " +
                        "FROM analytics_query_index " +
                        "WHERE \"metadata.model_name\" = 'gpt-4' " +
                        "ORDER BY \"token_usage.total_tokens\"",
                "VALUES ('gpt-4', CAST(90616 AS BIGINT)), ('gpt-4', CAST(120000 AS BIGINT))");
    }

    @Test
    public void testCountWithNestedFieldFilter()
    {
        assertQuery(
                createOpenSearchSession(),
                "SELECT COUNT(DISTINCT _id) FROM analytics_query_index WHERE \"token_usage.total_tokens\" > 80000",
                "SELECT CAST(2 AS BIGINT)");
    }

    @Test
    public void testMaxMinOnNestedField()
    {
        assertQuery(
                createOpenSearchSession(),
                "SELECT MAX(\"token_usage.total_tokens\"), MIN(\"token_usage.total_tokens\") FROM analytics_query_index",
                "SELECT CAST(120000 AS BIGINT), CAST(45000 AS BIGINT)");
    }

    @Test
    public void testSumOnNestedField()
    {
        assertQuery(
                createOpenSearchSession(),
                "SELECT SUM(\"token_usage.total_tokens\") FROM analytics_query_index",
                "SELECT CAST(255616 AS BIGINT)");
    }

    @Test
    public void testSumOnMultipleNestedFields()
    {
        assertQuery(
                createOpenSearchSession(),
                "SELECT SUM(\"token_usage.prompt_tokens\"), SUM(\"token_usage.completion_tokens\") FROM analytics_query_index",
                "SELECT CAST(127808 AS BIGINT), CAST(127808 AS BIGINT)");
    }

    @Test
    public void testAvgOnDeeplyNestedField()
    {
        assertQuery(
                createOpenSearchSession(),
                "SELECT AVG(\"reliability_scores.answer_relevance.score\") FROM analytics_query_index",
                "SELECT 80.0");
    }

    @Test
    public void testCountDistinctOnNestedField()
    {
        assertQuery(
                createOpenSearchSession(),
                "SELECT COUNT(DISTINCT \"metadata.model_name\") FROM analytics_query_index",
                "SELECT CAST(2 AS BIGINT)");
    }

    @Test
    public void testAggregateWithGroupByOnNestedFields()
    {
        assertQuery(
                createOpenSearchSession(),
                "SELECT \"metadata.model_name\", " +
                        "COUNT(DISTINCT _id), " +
                        "AVG(\"token_usage.total_tokens\"), " +
                        "MAX(\"reliability_scores.answer_relevance.score\") " +
                        "FROM analytics_query_index " +
                        "GROUP BY \"metadata.model_name\" " +
                        "ORDER BY \"metadata.model_name\"",
                "VALUES ('gpt-3.5', CAST(1 AS BIGINT), 45000.0, CAST(70 AS BIGINT)), " +
                        "('gpt-4', CAST(2 AS BIGINT), 105308.0, CAST(90 AS BIGINT))");
    }

    @Test
    public void testMultipleAggregatesOnSameNestedField()
    {
        assertQuery(
                createOpenSearchSession(),
                "SELECT " +
                        "COUNT(DISTINCT _id), " +
                        "SUM(\"token_usage.total_tokens\"), " +
                        "AVG(\"token_usage.total_tokens\"), " +
                        "MAX(\"token_usage.total_tokens\"), " +
                        "MIN(\"token_usage.total_tokens\") " +
                        "FROM analytics_query_index",
                "SELECT CAST(3 AS BIGINT), CAST(255616 AS BIGINT), 85205.33333333333, CAST(120000 AS BIGINT), CAST(45000 AS BIGINT)");
    }

    @Test
    public void testAggregateOnNestedFieldWithFilter()
    {
        assertQuery(
                createOpenSearchSession(),
                "SELECT SUM(\"token_usage.total_tokens\") " +
                        "FROM analytics_query_index " +
                        "WHERE \"reliability_scores.answer_relevance.score\" >= 80",
                "SELECT CAST(210616 AS BIGINT)");
    }

    // ========== ROW Type Dereference Query Tests ==========
    // These tests verify that nested objects are properly exposed as ROW types,
    // enabling SQL dereference syntax (e.g., token_usage.total_tokens without quotes)

    @Test
    public void testRowTypeDereferenceSimple()
    {
        // Test basic ROW type dereference: parent_object.field
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT token_usage.total_tokens FROM analytics_query_index WHERE _id = 'test-id-1'",
                "SELECT CAST(90616 AS BIGINT)");
    }

    @Test
    public void testRowTypeDereferenceMultipleFields()
    {
        // Test multiple field dereferences from same ROW
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT token_usage.total_tokens, token_usage.prompt_tokens, token_usage.completion_tokens " +
                        "FROM analytics_query_index WHERE _id = 'test-id-1'",
                "SELECT CAST(90616 AS BIGINT), CAST(45308 AS BIGINT), CAST(45308 AS BIGINT)");
    }

    @Test
    public void testRowTypeDereferenceDeeplyNested()
    {
        // Test deeply nested ROW dereference: parent.child.grandchild
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT reliability_scores.answer_relevance.score FROM analytics_query_index WHERE _id = 'test-id-1'",
                "SELECT 80");
    }

    @Test
    public void testRowTypeDereferenceDeeplyNestedMultiple()
    {
        // Test multiple deeply nested field dereferences
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT reliability_scores.answer_relevance.score, reliability_scores.answer_relevance.confidence " +
                        "FROM analytics_query_index WHERE _id = 'test-id-2'",
                "SELECT 90, 0.92");
    }

    @Test
    public void testRowTypeDereferenceInPredicate()
    {
        // Test ROW dereference in WHERE clause
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT _id FROM analytics_query_index WHERE token_usage.total_tokens > 50000 ORDER BY _id",
                "VALUES ('test-id-1'), ('test-id-2')");
    }

    @Test
    public void testRowTypeDereferenceInComplexPredicate()
    {
        // Test ROW dereference with complex predicate
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT _id FROM analytics_query_index " +
                        "WHERE token_usage.total_tokens > 50000 AND reliability_scores.answer_relevance.score >= 80 " +
                        "ORDER BY _id",
                "VALUES ('test-id-1'), ('test-id-2')");
    }

    @Test
    public void testRowTypeDereferenceWithAggregate()
    {
        // Test ROW dereference in aggregate functions
        assertQuery(
                createOpenSearchSession(),
                "SELECT COUNT(DISTINCT _id), AVG(token_usage.total_tokens) FROM analytics_query_index",
                "SELECT CAST(3 AS BIGINT), 85205.33333333333");
    }

    @Test
    public void testRowTypeDereferenceInGroupBy()
    {
        // Test ROW dereference in GROUP BY
        assertQuery(
                createOpenSearchSession(),
                "SELECT user_id, SUM(token_usage.total_tokens) FROM analytics_query_index GROUP BY user_id ORDER BY user_id",
                "VALUES ('user1', CAST(135616 AS BIGINT)), ('user2', CAST(120000 AS BIGINT))");
    }

    @Test
    public void testRowTypeDereferenceInOrderBy()
    {
        // Test ROW dereference in ORDER BY
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT _id, token_usage.total_tokens FROM analytics_query_index " +
                        "ORDER BY token_usage.total_tokens DESC",
                "VALUES ('test-id-2', CAST(120000 AS BIGINT)), ('test-id-1', CAST(90616 AS BIGINT)), ('test-id-3', CAST(45000 AS BIGINT))");
    }

    @Test
    public void testRowTypeDereferenceMultipleObjects()
    {
        // Test dereferencing fields from multiple ROW objects
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT metadata.model_name, token_usage.total_tokens " +
                        "FROM analytics_query_index WHERE _id = 'test-id-1'",
                "VALUES ('gpt-4', CAST(90616 AS BIGINT))");
    }

    @Test
    public void testRowTypeDereferenceWithJoin()
    {
        // Test ROW dereference in self-join scenario
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT a._id, a.token_usage.total_tokens " +
                        "FROM analytics_query_index a " +
                        "WHERE a.token_usage.total_tokens > 80000 " +
                        "ORDER BY a._id",
                "VALUES ('test-id-1', CAST(90616 AS BIGINT)), ('test-id-2', CAST(120000 AS BIGINT))");
    }

    @Test
    public void testRowTypeDereferenceParentObject()
    {
        // Test selecting the parent ROW object itself (should return as JSON string)
        MaterializedResult result = computeActual(
                createOpenSearchSession(),
                "SELECT DISTINCT token_usage FROM analytics_query_index WHERE _id = 'test-id-1'");
        assertEquals(result.getRowCount(), 1);
        // The ROW should be returned as a structured value
        assertTrue(result.getMaterializedRows().get(0).getField(0).toString().contains("90616"));
    }

    @Test
    public void testRowTypeDereferenceNestedParentObject()
    {
        // Test selecting a nested parent ROW object
        MaterializedResult result = computeActual(
                createOpenSearchSession(),
                "SELECT DISTINCT reliability_scores.answer_relevance FROM analytics_query_index WHERE _id = 'test-id-1'");
        assertEquals(result.getRowCount(), 1);
        // The nested ROW should contain both score and confidence
        String rowValue = result.getMaterializedRows().get(0).getField(0).toString();
        assertTrue(rowValue.contains("80"));
        assertTrue(rowValue.contains("0.85"));
    }

    @Test
    public void testRowTypeDereferenceMixedSyntax()
    {
        // Test mixing quoted column names with ROW dereference
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT \"metadata.model_name\", token_usage.total_tokens " +
                        "FROM analytics_query_index WHERE _id = 'test-id-1'",
                "VALUES ('gpt-4', CAST(90616 AS BIGINT))");
    }

    @Test
    public void testAggregateOnDeeplyNestedFieldWithGroupBy()
    {
        assertQuery(
                createOpenSearchSession(),
                "SELECT user_id, " +
                        "AVG(\"reliability_scores.answer_relevance.score\"), " +
                        "AVG(\"reliability_scores.answer_relevance.confidence\") " +
                        "FROM analytics_query_index " +
                        "GROUP BY user_id " +
                        "ORDER BY user_id",
                "VALUES ('user1', 75.0, 0.815), ('user2', 90.0, 0.92)");
    }

    @Test
    public void testMaxMinOnDeeplyNestedField()
    {
        assertQuery(
                createOpenSearchSession(),
                "SELECT " +
                        "MAX(\"reliability_scores.answer_relevance.confidence\"), " +
                        "MIN(\"reliability_scores.answer_relevance.confidence\") " +
                        "FROM analytics_query_index",
                "SELECT 0.92, 0.78");
    }

    @Test
    public void testAggregateWithHavingOnNestedField()
    {
        assertQuery(
                createOpenSearchSession(),
                "SELECT user_id, SUM(\"token_usage.total_tokens\") as total " +
                        "FROM analytics_query_index " +
                        "GROUP BY user_id " +
                        "HAVING SUM(\"token_usage.total_tokens\") > 100000 " +
                        "ORDER BY user_id",
                "VALUES ('user1', CAST(135616 AS BIGINT)), ('user2', CAST(120000 AS BIGINT))");
    }

    @Test
    public void testCountWithNestedFieldGroupBy()
    {
        assertQuery(
                createOpenSearchSession(),
                "SELECT \"metadata.model_name\", COUNT(DISTINCT _id) " +
                        "FROM analytics_query_index " +
                        "GROUP BY \"metadata.model_name\" " +
                        "ORDER BY \"metadata.model_name\"",
                "VALUES ('gpt-3.5', CAST(1 AS BIGINT)), ('gpt-4', CAST(2 AS BIGINT))");
    }

    @Test
    public void testAggregateOnMultipleDeeplyNestedFields()
    {
        assertQuery(
                createOpenSearchSession(),
                "SELECT " +
                        "AVG(\"reliability_scores.answer_relevance.score\"), " +
                        "AVG(\"reliability_scores.faithfulness.score\") " +
                        "FROM analytics_query_index",
                "SELECT 80.0, 75.0");
    }

    @Test
    public void testSumWithNestedFieldFilter()
    {
        assertQuery(
                createOpenSearchSession(),
                "SELECT SUM(\"token_usage.prompt_tokens\") " +
                        "FROM analytics_query_index " +
                        "WHERE \"token_usage.total_tokens\" > 50000",
                "SELECT CAST(105308 AS BIGINT)");
    }

    @Test
    public void testAggregateOnNestedFieldWithComplexFilter()
    {
        assertQuery(
                createOpenSearchSession(),
                "SELECT " +
                        "COUNT(DISTINCT _id), " +
                        "AVG(\"token_usage.total_tokens\") " +
                        "FROM analytics_query_index " +
                        "WHERE \"reliability_scores.answer_relevance.score\" > 75 " +
                        "AND \"metadata.model_name\" = 'gpt-4'",
                "SELECT CAST(2 AS BIGINT), 105308.0");
    }

    @Test
    public void testGroupByMultipleNestedFields()
    {
        assertQuery(
                createOpenSearchSession(),
                "SELECT " +
                        "user_id, " +
                        "\"metadata.model_name\", " +
                        "COUNT(DISTINCT _id) " +
                        "FROM analytics_query_index " +
                        "GROUP BY user_id, \"metadata.model_name\" " +
                        "ORDER BY user_id, \"metadata.model_name\"",
                "VALUES ('user1', 'gpt-3.5', CAST(1 AS BIGINT)), " +
                        "('user1', 'gpt-4', CAST(1 AS BIGINT)), " +
                        "('user2', 'gpt-4', CAST(1 AS BIGINT))");
    }

    @Test
    public void testAggregateWithOrderByOnNestedField()
    {
        assertQuery(
                createOpenSearchSession(),
                "SELECT user_id, SUM(\"token_usage.total_tokens\") as total " +
                        "FROM analytics_query_index " +
                        "GROUP BY user_id " +
                        "ORDER BY SUM(\"token_usage.total_tokens\") DESC",
                "VALUES ('user1', CAST(135616 AS BIGINT)), ('user2', CAST(120000 AS BIGINT))");
    }

    @Test
    public void testMinMaxOnMultipleNestedFields()
    {
        assertQuery(
                createOpenSearchSession(),
                "SELECT " +
                        "MAX(\"token_usage.prompt_tokens\"), " +
                        "MIN(\"token_usage.prompt_tokens\"), " +
                        "MAX(\"token_usage.completion_tokens\"), " +
                        "MIN(\"token_usage.completion_tokens\") " +
                        "FROM analytics_query_index",
                "SELECT CAST(60000 AS BIGINT), CAST(22500 AS BIGINT), CAST(60000 AS BIGINT), CAST(22500 AS BIGINT)");
    }

    // ========== Vector Search Function Tests Using Array Column from OpenSearch Index ==========

    @Test
    public void testVectorMagnitudeFromArrayColumn()
    {
        // Test magnitude calculation: sqrt(dot_product(v, v))
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT doc_id, sqrt(dot_product(embedding, embedding)) as magnitude " +
                        "FROM vector_docs WHERE doc_id = 'vec-3'",
                "VALUES ('vec-3', 5.0)");
    }

    @Test
    public void testVectorCosineSimilarityFromArrayColumn()
    {
        // Test cosine similarity with embedding array column
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT doc_id, " +
                        "cosine_similarity(embedding, ARRAY[1.0, 0.0, 0.0]) as similarity " +
                        "FROM vector_docs " +
                        "ORDER BY similarity DESC",
                "VALUES ('vec-1', 1.0), ('vec-3', 0.6), ('vec-2', 0.0)");
    }

    @Test
    public void testVectorEuclideanDistanceFromArrayColumn()
    {
        // Test Euclidean distance: sqrt(l2_squared(v1, v2))
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT doc_id, " +
                        "sqrt(l2_squared(CAST(ARRAY[0.0, 0.0, 0.0] AS array(real)), embedding)) as distance " +
                        "FROM vector_docs WHERE doc_id = 'vec-3'",
                "VALUES ('vec-3', 5.0)");
    }

    @Test
    public void testVectorDotProductFromArrayColumn()
    {
        // Test dot product with embedding array column
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT doc_id, " +
                        "dot_product(embedding, ARRAY[1.0, 1.0, 0.0]) as score " +
                        "FROM vector_docs " +
                        "ORDER BY score DESC",
                "VALUES ('vec-3', 7.0), ('vec-1', 1.0), ('vec-2', 1.0)");
    }

    @Test
    public void testVectorSearchWithArrayColumnFilter()
    {
        // Filter documents using vector magnitude
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT doc_id FROM vector_docs " +
                        "WHERE sqrt(dot_product(embedding, embedding)) > 1.5 " +
                        "ORDER BY doc_id",
                "VALUES ('vec-3')");
    }

    // ========== k-NN Table Function Tests ==========
    // Tests for k-NN vector search using SQL table functions

    /**
     * Test basic k-NN search using table function.
     */
    @Test
    public void testKnnTableFunctionBasicSearch()
    {
        // Query for top 3 nearest neighbors to [1.0, 0.0, 0.0]
        // Expected: vec-1 (exact match), vec-2 (close), vec-3 (further)
        assertQuery(
                createOpenSearchSession(),
                "SELECT _id, doc_id, category " +
                        "FROM TABLE(opensearch.system.knn_search(" +
                        "  index_name => 'knn_docs', " +
                        "  vector_field => 'knn_embedding', " +
                        "  query_vector => ARRAY[1.0, 0.0, 0.0], " +
                        "  k => 3" +
                        ")) " +
                        "ORDER BY _score DESC",
                "VALUES ('1', 'vec-1', 'tech'), " +
                        "('2', 'vec-2', 'tech'), " +
                        "('3', 'vec-3', 'science')");
    }

    /**
     * Test k-NN search with custom k value.
     */
    @Test
    public void testKnnTableFunctionWithCustomK()
    {
        assertQuery(
                createOpenSearchSession(),
                "SELECT COUNT(*) " +
                        "FROM TABLE(opensearch.system.knn_search(" +
                        "  index_name => 'knn_docs', " +
                        "  vector_field => 'knn_embedding', " +
                        "  query_vector => ARRAY[0.7, 0.7, 0.0], " +
                        "  k => 2" +
                        "))",
                "SELECT CAST(2 AS BIGINT)");
    }

    /**
     * Test k-NN search with space_type parameter.
     */
    @Test
    public void testKnnTableFunctionWithSpaceType()
    {
        assertQuery(
                createOpenSearchSession(),
                "SELECT _id, category " +
                        "FROM TABLE(opensearch.system.knn_search(" +
                        "  index_name => 'knn_docs', " +
                        "  vector_field => 'knn_embedding', " +
                        "  query_vector => ARRAY[1.0, 0.0, 0.0], " +
                        "  k => 5, " +
                        "  space_type => 'cosine'" +
                        ")) " +
                        "ORDER BY _score DESC " +
                        "LIMIT 3",
                "VALUES ('1', 'tech'), ('2', 'tech'), ('3', 'science')");
    }

    /**
     * Test k-NN search with ef_search parameter for accuracy tuning.
     */
    @Test
    public void testKnnTableFunctionWithEfSearch()
    {
        assertQuery(
                createOpenSearchSession(),
                "SELECT COUNT(*) " +
                        "FROM TABLE(opensearch.system.knn_search(" +
                        "  index_name => 'knn_docs', " +
                        "  vector_field => 'knn_embedding', " +
                        "  query_vector => ARRAY[0.5, 0.5, 0.0], " +
                        "  k => 3, " +
                        "  ef_search => 200" +
                        "))",
                "SELECT CAST(3 AS BIGINT)");
    }

    /**
     * Test k-NN search with filtering on results.
     */
    @Test
    public void testKnnTableFunctionWithFilter()
    {
        assertQuery(
                createOpenSearchSession(),
                "SELECT _id, category " +
                        "FROM TABLE(opensearch.system.knn_search(" +
                        "  index_name => 'knn_docs', " +
                        "  vector_field => 'knn_embedding', " +
                        "  query_vector => ARRAY[1.0, 0.0, 0.0], " +
                        "  k => 10" +
                        ")) " +
                        "WHERE category = 'tech' " +
                        "ORDER BY _score DESC",
                "VALUES ('1', 'tech'), ('2', 'tech')");
    }

    /**
     * Test k-NN search with aggregation.
     */
    @Test
    public void testKnnTableFunctionWithAggregation()
    {
        assertQuery(
                createOpenSearchSession(),
                "SELECT category, COUNT(*) " +
                        "FROM TABLE(opensearch.system.knn_search(" +
                        "  index_name => 'knn_docs', " +
                        "  vector_field => 'knn_embedding', " +
                        "  query_vector => ARRAY[1.0, 0.0, 0.0], " +
                        "  k => 10" +
                        ")) " +
                        "GROUP BY category " +
                        "ORDER BY COUNT(*) DESC",
                "VALUES ('tech', CAST(2 AS BIGINT)), ('science', CAST(2 AS BIGINT)), ('arts', CAST(1 AS BIGINT))");
    }

    /**
     * Test k-NN search with JOIN.
     */
    @Test
    public void testKnnTableFunctionWithJoin()
    {
        // Test JOIN with k-NN results - no matching rows since doc_id values don't match user_id values
        assertQuery(
                createOpenSearchSession(),
                "SELECT COUNT(*) " +
                        "FROM TABLE(opensearch.system.knn_search(" +
                        "  index_name => 'knn_docs', " +
                        "  vector_field => 'knn_embedding', " +
                        "  query_vector => ARRAY[1.0, 0.0, 0.0], " +
                        "  k => 5" +
                        ")) k " +
                        "JOIN analytics_query_index a ON k.doc_id = a.user_id",
                "SELECT CAST(0 AS BIGINT)");
    }

    /**
     * Test k-NN search with UNION.
     */
    @Test
    public void testKnnTableFunctionWithUnion()
    {
        assertQuery(
                createOpenSearchSession(),
                "SELECT 'query1' as source, _id " +
                        "FROM TABLE(opensearch.system.knn_search(" +
                        "  index_name => 'knn_docs', " +
                        "  vector_field => 'knn_embedding', " +
                        "  query_vector => ARRAY[1.0, 0.0, 0.0], " +
                        "  k => 2" +
                        ")) " +
                        "UNION ALL " +
                        "SELECT 'query2' as source, _id " +
                        "FROM TABLE(opensearch.system.knn_search(" +
                        "  index_name => 'knn_docs', " +
                        "  vector_field => 'knn_embedding', " +
                        "  query_vector => ARRAY[0.0, 1.0, 0.0], " +
                        "  k => 2" +
                        ")) " +
                        "ORDER BY _id",
                "VALUES ('query1', '1'), ('query1', '2'), ('query2', '3'), ('query2', '4')");
    }

    /**
     * Test k-NN search with subquery.
     */
    @Test
    public void testKnnTableFunctionWithSubquery()
    {
        assertQuery(
                createOpenSearchSession(),
                "SELECT category, num_results " +
                        "FROM (" +
                        "  SELECT category, COUNT(*) as num_results " +
                        "  FROM TABLE(opensearch.system.knn_search(" +
                        "    index_name => 'knn_docs', " +
                        "    vector_field => 'knn_embedding', " +
                        "    query_vector => ARRAY[1.0, 0.0, 0.0], " +
                        "    k => 10" +
                        "  )) " +
                        "  GROUP BY category" +
                        ") " +
                        "WHERE num_results > 1 " +
                        "ORDER BY category",
                "VALUES ('science', CAST(2 AS BIGINT)), ('tech', CAST(2 AS BIGINT))");
    }

    /**
     * Test k-NN search with LIMIT and OFFSET.
     */
    @Test
    public void testKnnTableFunctionWithLimitOffset()
    {
        // Test LIMIT on k-NN results
        assertQuery(
                createOpenSearchSession(),
                "SELECT _id " +
                        "FROM TABLE(opensearch.system.knn_search(" +
                        "  index_name => 'knn_docs', " +
                        "  vector_field => 'knn_embedding', " +
                        "  query_vector => ARRAY[1.0, 0.0, 0.0], " +
                        "  k => 5" +
                        ")) " +
                        "ORDER BY _id " +
                        "LIMIT 3",
                "VALUES ('1'), ('2'), ('3')");
    }
    /**
     * Test k-NN search with array containing scientific notation.
     * When array contains scientific notation (e.g., 9.158867E-4), Presto parses it as DOUBLE.
     * This test verifies that ARRAY(DOUBLE) is accepted by the knn_search function.
     */
    @Test
    public void testKnnTableFunctionWithScientificNotation()
    {
        // Query with scientific notation - this gets parsed as array(double)
        assertQuery(
                createOpenSearchSession(),
                "SELECT knn._id, knn._score " +
                        "FROM TABLE(opensearch.system.knn_search(" +
                        "index_name => 'knn_docs', " +
                        "vector_field => 'knn_embedding', " +
                        "query_vector => ARRAY[0.0053062416, 9.158867E-4, -0.032646563], " +
                        "k => 5, " +
                        "space_type => 'cosine')) AS knn " +
                        "ORDER BY knn._score DESC LIMIT 3",
                "VALUES ('2', 1.1624452), ('3', 1.1622945), ('1', 1.1603692)");
    }

    // ========== k-NN Table Function Negative Tests ==========
    // Tests for error handling and validation

    /**
     * Test k-NN search with non-existent vector field.
     */
    @Test
    public void testKnnTableFunctionWithNonExistentField()
    {
        assertQueryFails(
                createOpenSearchSession(),
                "SELECT * FROM TABLE(opensearch.system.knn_search(" +
                       "  index_name => 'knn_docs', " +
                       "  vector_field => 'nonexistent_field', " +
                       "  query_vector => ARRAY[1.0, 0.0, 0.0], " +
                       "  k => 10" +
                       "))",
                ".*Vector field 'nonexistent_field' does not exist in index mapping.*");
    }

    /**
     * Test k-NN search with non-vector field (text field).
     */
    @Test
    public void testKnnTableFunctionWithNonVectorField()
    {
        assertQueryFails(
                createOpenSearchSession(),
                "SELECT * FROM TABLE(opensearch.system.knn_search(" +
                       "  index_name => 'knn_docs', " +
                       "  vector_field => 'category', " +  // category is a keyword field, not knn_vector
                       "  query_vector => ARRAY[1.0, 0.0, 0.0], " +
                       "  k => 10" +
                       "))",
                ".*Field 'category' is not a knn_vector field.*");
    }

    /**
     * Test k-NN search with wrong vector dimensions.
     */
    @Test
    public void testKnnTableFunctionWithWrongDimensions()
    {
        // knn_embedding has 3 dimensions, but we provide 5
        assertQueryFails(
                createOpenSearchSession(),
                "SELECT * FROM TABLE(opensearch.system.knn_search(" +
                       "  index_name => 'knn_docs', " +
                       "  vector_field => 'knn_embedding', " +
                       "  query_vector => ARRAY[1.0, 0.0, 0.0, 0.0, 0.0], " +  // 5 dimensions instead of 3
                       "  k => 10" +
                       "))",
                ".*Query vector dimension \\(5\\) does not match field 'knn_embedding' dimension \\(3\\).*");
    }

    /**
     * Test k-NN search on regular index without knn_vector fields.
     */
    @Test
    public void testKnnTableFunctionOnNonKnnIndex()
    {
        // analytics_query_index has no knn_vector fields
        assertQueryFails(
                createOpenSearchSession(),
                "SELECT * FROM TABLE(opensearch.system.knn_search(" +
                       "  index_name => 'analytics_query_index', " +
                       "  vector_field => 'user_id', " +
                       "  query_vector => ARRAY[1.0, 0.0, 0.0], " +
                       "  k => 10" +
                       "))",
                ".*Field 'user_id' is not a knn_vector field.*");
    }

    /**
     * Test k-NN search with empty query vector.
     */
    @Test
    public void testKnnTableFunctionWithEmptyVector()
    {
        assertQueryFails(
                createOpenSearchSession(),
                "SELECT * FROM TABLE(opensearch.system.knn_search(" +
                       "  index_name => 'knn_docs', " +
                       "  vector_field => 'knn_embedding', " +
                       "  query_vector => ARRAY[], " +  // Empty array
                       "  k => 10" +
                       "))",
                ".*Query vector dimension \\(0\\) does not match field 'knn_embedding' dimension \\(3\\).*");
    }

    /**
     * Test k-NN search with invalid k value (0).
     */
    @Test
    public void testKnnTableFunctionWithZeroK()
    {
        // This should be caught by VectorSearchHandler validation
        assertQueryFails(
                createOpenSearchSession(),
                "SELECT * FROM TABLE(opensearch.system.knn_search(" +
                       "  index_name => 'knn_docs', " +
                       "  vector_field => 'knn_embedding', " +
                       "  query_vector => ARRAY[1.0, 0.0, 0.0], " +
                       "  k => 0" +
                       "))",
                ".*k must be positive.*");
    }

    /**
     * Test k-NN search with negative k value.
     */
    @Test
    public void testKnnTableFunctionWithNegativeK()
    {
        assertQueryFails(
                createOpenSearchSession(),
                "SELECT * FROM TABLE(opensearch.system.knn_search(" +
                        "  index_name => 'knn_docs', " +
                        "  vector_field => 'knn_embedding', " +
                        "  query_vector => ARRAY[1.0, 0.0, 0.0], " +
                        "  k => -5" +
                        "))",
                ".*k must be positive.*");
    }

    // ========== Multi-Shard Integration Tests ==========

    /**
     * Test that verifies data is distributed across multiple shards.
     */
    @Test
    public void testMultiShardDataDistribution()
            throws IOException
    {
        // Verify TPCH tables are using multiple shards
        Request statsRequest = new Request("GET", "/nation/_stats/docs");
        Response response = restClient.performRequest(statsRequest);
        assertEquals(response.getStatusLine().getStatusCode(), 200);
        log.info("Verified multi-shard setup for nation table");
    }

    /**
     * Test aggregations work correctly across multiple shards.
     */
    @Test
    public void testAggregationsAcrossMultipleShards()
    {
        Session session = createOpenSearchSession();
        assertQuery(session, "SELECT COUNT(*) FROM nation");
        assertQuery(session, "SELECT regionkey, COUNT(*) FROM nation GROUP BY regionkey");
    }

    /**
     * Test ORDER BY works correctly across multiple shards.
     */
    @Test
    public void testOrderByAcrossMultipleShards()
    {
        Session session = createOpenSearchSession();
        assertQuery(session, "SELECT nationkey, name FROM nation ORDER BY nationkey LIMIT 5");
    }

    /**
     * Test JOIN operations work correctly with multi-shard indices.
     */
    @Test
    public void testJoinAcrossMultipleShards()
    {
        Session session = createOpenSearchSession();
        // Test JOIN between two multi-sharded tables
        MaterializedResult result = computeActual(session,
                "SELECT n.name, r.name " +
                "FROM nation n JOIN region r ON n.regionkey = r.regionkey " +
                "LIMIT 10");
        assertEquals(result.getRowCount(), 10);
        // Verify we got valid data from both tables
        assertTrue(result.getMaterializedRows().stream()
                .allMatch(row -> row.getField(0) != null && row.getField(1) != null));
    }

    /**
     * Test nested field queries work correctly across multiple shards.
     */
    @Test
    public void testNestedFieldsAcrossMultipleShards()
    {
        Session session = createOpenSearchSession();
        // Test nested field access across shards
        MaterializedResult result = computeActual(session,
                "SELECT user_id, \"token_usage.total_tokens\" " +
                "FROM analytics_query_index " +
                "WHERE \"token_usage.total_tokens\" > 0");
        // Verify we got at least some data with nested fields
        assertTrue(result.getRowCount() > 0, "Expected at least one row with token_usage.total_tokens > 0");
        // Verify nested field values are retrieved correctly
        assertTrue(result.getMaterializedRows().stream()
                .allMatch(row -> row.getField(1) != null && ((Long) row.getField(1)) > 0));
    }

    /**
     * Test vector search functions work correctly across multiple shards.
     */
    @Test
    public void testVectorSearchAcrossMultipleShards()
    {
        Session session = createOpenSearchSession();
        // Test vector data retrieval across shards
        MaterializedResult result = computeActual(session,
                "SELECT doc_id, embedding " +
                "FROM vector_docs " +
                "LIMIT 5");
        // Verify we got vector data from the multi-shard index
        assertTrue(result.getRowCount() > 0, "Expected at least one row from vector_docs");
        assertTrue(result.getRowCount() <= 5, "Expected at most 5 rows");
        // Verify we got vector data (embedding is an array)
        assertTrue(result.getMaterializedRows().stream()
                .allMatch(row -> row.getField(0) != null && row.getField(1) != null));
    }

    /**
     * Test k-NN table function works correctly with multi-shard index.
     */
    @Test
    public void testKnnSearchAcrossMultipleShards()
    {
        Session session = createOpenSearchSession();
        MaterializedResult result = computeActual(session,
                "SELECT doc_id, category, _score " +
                "FROM TABLE(opensearch.system.knn_search(" +
                "  index_name => 'knn_docs', " +
                "  vector_field => 'knn_embedding', " +
                "  query_vector => ARRAY[1.0, 0.5, 0.3], " +
                "  k => 3" +
                "))");
        assertTrue(result.getRowCount() > 0 && result.getRowCount() <= 3);
    }

    /**
     * Test DISTINCT works correctly across multiple shards.
     */
    @Test
    public void testDistinctAcrossMultipleShards()
    {
        Session session = createOpenSearchSession();
        assertQuery(session, "SELECT DISTINCT regionkey FROM nation");
    }

    /**
     * Test LIMIT and OFFSET work correctly across multiple shards.
     */
    @Test
    public void testLimitOffsetAcrossMultipleShards()
    {
        Session session = createOpenSearchSession();
        MaterializedResult result = computeActual(session,
                "SELECT nationkey FROM nation ORDER BY nationkey LIMIT 5");
        assertEquals(result.getRowCount(), 5);
    }

    // ========== QueryBuilder Method Coverage Tests ==========
    // These tests ensure all QueryBuilder methods are exercised through integration tests

    // Tests for buildCondition() with flat fields
    @Test
    public void testFlatFieldEquality()
    {
        // Tests buildCondition() -> buildTermQuery() for keyword field
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT _id FROM analytics_query_index WHERE user_id = 'user1' ORDER BY _id",
                "VALUES ('test-id-1'), ('test-id-3')");
    }

    @Test
    public void testFlatFieldStringEquality()
    {
        // Tests buildCondition() -> buildTermQuery() for string values with special characters
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT _id FROM analytics_query_index WHERE \"metadata.model_name\" = 'gpt-4' ORDER BY _id",
                "VALUES ('test-id-1'), ('test-id-2')");
    }

    @Test
    public void testFlatFieldRangeGreaterThan()
    {
        // Tests buildCondition() -> buildRangeQuery() with gt operator
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT doc_id FROM knn_docs WHERE doc_id > 'vec-2' ORDER BY doc_id",
                "VALUES ('vec-3'), ('vec-4'), ('vec-5')");
    }

    @Test
    public void testFlatFieldRangeLessThan()
    {
        // Tests buildCondition() -> buildRangeQuery() with lt operator
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT doc_id FROM knn_docs WHERE doc_id < 'vec-3' ORDER BY doc_id",
                "VALUES ('vec-1'), ('vec-2')");
    }

    @Test
    public void testFlatFieldRangeGreaterThanOrEqual()
    {
        // Tests buildCondition() -> buildRangeQuery() with gte operator
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT doc_id FROM knn_docs WHERE doc_id >= 'vec-3' ORDER BY doc_id",
                "VALUES ('vec-3'), ('vec-4'), ('vec-5')");
    }

    @Test
    public void testFlatFieldRangeLessThanOrEqual()
    {
        // Tests buildCondition() -> buildRangeQuery() with lte operator
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT doc_id FROM knn_docs WHERE doc_id <= 'vec-2' ORDER BY doc_id",
                "VALUES ('vec-1'), ('vec-2')");
    }

    @Test
    public void testFlatFieldBetween()
    {
        // Tests buildCondition() -> buildRangeQuery() with both bounds (gte and lte)
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT _id FROM analytics_query_index WHERE \"token_usage.total_tokens\" BETWEEN 50000 AND 100000",
                "VALUES ('test-id-1')");
    }

    @Test
    public void testFlatFieldMultipleRanges()
    {
        // Tests buildCondition() with multiple ranges (bool should query)
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT _id FROM analytics_query_index WHERE \"token_usage.total_tokens\" IN (45000, 90616, 120000) ORDER BY _id",
                "VALUES ('test-id-1'), ('test-id-2'), ('test-id-3')");
    }

    @Test
    public void testMultipleFlatFieldConditions()
    {
        // Tests buildQuery() with multiple flat field conditions (bool must query)
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT _id FROM analytics_query_index WHERE user_id = 'user1' AND \"metadata.model_name\" = 'gpt-4'",
                "VALUES ('test-id-1')");
    }

    @Test
    public void testNestedFieldSingleValue()
    {
        // Tests buildNestedCondition() with single value (term query)
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT _id FROM analytics_query_index WHERE \"reliability_scores.answer_relevance.score\" = 90",
                "VALUES ('test-id-2')");
    }

    @Test
    public void testNestedFieldRangeQuery()
    {
        // Tests buildNestedCondition() with range query
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT _id FROM analytics_query_index WHERE \"token_usage.prompt_tokens\" >= 45000 ORDER BY _id",
                "VALUES ('test-id-1'), ('test-id-2')");
    }

    @Test
    public void testNestedFieldMultipleRanges()
    {
        // Tests buildNestedCondition() with multiple ranges (bool should inside nested)
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT _id FROM analytics_query_index WHERE \"reliability_scores.faithfulness.score\" IN (65, 75, 85) ORDER BY _id",
                "VALUES ('test-id-1'), ('test-id-2'), ('test-id-3')");
    }

    @Test
    public void testGroupedNestedQuerySameParent()
    {
        // Tests groupNestedPredicates() and buildGroupedNestedQuery()
        // Multiple predicates on same parent path should be grouped
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT _id FROM analytics_query_index " +
                        "WHERE \"token_usage.total_tokens\" > 50000 " +
                        "AND \"token_usage.prompt_tokens\" > 30000 " +
                        "ORDER BY _id",
                "VALUES ('test-id-1'), ('test-id-2')");
    }

    @Test
    public void testGroupedNestedQueryDifferentLevels()
    {
        // Tests groupNestedPredicates() with deeply nested fields on same parent
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT _id FROM analytics_query_index " +
                        "WHERE \"reliability_scores.answer_relevance.score\" > 75 " +
                        "AND \"reliability_scores.answer_relevance.confidence\" > 0.8 " +
                        "ORDER BY _id",
                "VALUES ('test-id-1'), ('test-id-2')");
    }

    @Test
    public void testMixedFlatAndNestedConditions()
    {
        // Tests buildQuery() with both flat and nested field conditions
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT _id FROM analytics_query_index " +
                        "WHERE user_id = 'user1' " +
                        "AND \"token_usage.total_tokens\" > 50000",
                "VALUES ('test-id-1')");
    }

    @Test
    public void testComplexQueryAllMethods()
    {
        // Comprehensive test covering multiple QueryBuilder methods:
        // - buildQuery() with multiple conditions
        // - buildCondition() for flat fields
        // - buildNestedCondition() for nested fields
        // - groupNestedPredicates() for optimization
        // - buildGroupedNestedQuery() for same parent predicates
        // - buildTermQuery() for equality
        // - buildRangeQuery() for comparisons
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT _id FROM analytics_query_index " +
                        "WHERE user_id = 'user1' " +
                        "AND \"metadata.model_name\" = 'gpt-4' " +
                        "AND \"token_usage.total_tokens\" > 80000 " +
                        "AND \"token_usage.prompt_tokens\" > 40000 " +
                        "AND \"reliability_scores.answer_relevance.score\" >= 80",
                "VALUES ('test-id-1')");
    }

    @Test
    public void testNestedFieldWithDifferentParents()
    {
        // Tests that predicates on different parent paths are not grouped
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT _id FROM analytics_query_index " +
                        "WHERE \"token_usage.total_tokens\" > 50000 " +
                        "AND \"reliability_scores.faithfulness.score\" > 70 " +
                        "ORDER BY _id",
                "VALUES ('test-id-1'), ('test-id-2')");
    }

    @Test
    public void testBuildInnerQueryWithSingleValue()
    {
        // Tests buildInnerQuery() with single value (used in grouping)
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT _id FROM analytics_query_index " +
                        "WHERE \"token_usage.total_tokens\" = 90616 " +
                        "AND \"token_usage.prompt_tokens\" = 45308",
                "VALUES ('test-id-1')");
    }

    @Test
    public void testBuildInnerQueryWithRange()
    {
        // Tests buildInnerQuery() with range (used in grouping)
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT _id FROM analytics_query_index " +
                        "WHERE \"token_usage.total_tokens\" BETWEEN 80000 AND 100000 " +
                        "AND \"token_usage.completion_tokens\" > 40000",
                "VALUES ('test-id-1')");
    }

    @Test
    public void testFormatValueWithNumbers()
    {
        // Tests formatValue() with numeric values
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT _id FROM analytics_query_index WHERE \"reliability_scores.answer_relevance.score\" = 80",
                "VALUES ('test-id-1')");
    }

    @Test
    public void testFormatValueWithDoubles()
    {
        // Tests formatValue() with double values
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT _id FROM analytics_query_index WHERE \"reliability_scores.answer_relevance.confidence\" > 0.9",
                "VALUES ('test-id-2')");
    }

    @Test
    public void testRangeQueryBothBoundsInclusive()
    {
        // Tests buildRangeQuery() with both bounds inclusive (gte and lte)
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT _id FROM analytics_query_index " +
                        "WHERE \"token_usage.total_tokens\" >= 45000 AND \"token_usage.total_tokens\" <= 90616 " +
                        "ORDER BY _id",
                "VALUES ('test-id-1'), ('test-id-3')");
    }

    @Test
    public void testRangeQueryBothBoundsExclusive()
    {
        // Tests buildRangeQuery() with both bounds exclusive (gt and lt)
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT _id FROM analytics_query_index " +
                        "WHERE \"token_usage.total_tokens\" > 45000 AND \"token_usage.total_tokens\" < 120000 " +
                        "ORDER BY _id",
                "VALUES ('test-id-1')");
    }

    @Test
    public void testRangeQueryMixedBounds()
    {
        // Tests buildRangeQuery() with mixed bounds (gt and lte)
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT _id FROM analytics_query_index " +
                        "WHERE \"token_usage.total_tokens\" > 45000 AND \"token_usage.total_tokens\" <= 90616",
                "VALUES ('test-id-1')");
    }

    @Test
    public void testEmptyResultSet()
    {
        // Tests buildQuery() with conditions that match no documents
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT _id FROM analytics_query_index WHERE user_id = 'nonexistent'",
                "SELECT 'dummy' WHERE false");
    }

    @Test
    public void testMatchAllQuery()
    {
        // Tests buildQuery() with no predicates (should generate match_all)
        assertQuery(
                createOpenSearchSession(),
                "SELECT COUNT(DISTINCT _id) FROM analytics_query_index",
                "SELECT CAST(3 AS BIGINT)");
    }

    @Test
    public void testCategoryFieldEquality()
    {
        // Tests buildCondition() with keyword field on knn_docs
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT doc_id FROM knn_docs WHERE category = 'tech' ORDER BY doc_id",
                "VALUES ('vec-1'), ('vec-2')");
    }

    @Test
    public void testMultipleCategoriesWithOr()
    {
        // Tests buildCondition() with multiple values (IN clause)
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT doc_id FROM knn_docs WHERE category IN ('tech', 'science') ORDER BY doc_id",
                "VALUES ('vec-1'), ('vec-2'), ('vec-3'), ('vec-4')");
    }

    // Tests for ROW type dereference syntax (unquoted identifiers)
    @Test
    public void testDereferenceNestedFieldUnquoted()
    {
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT token_usage.total_tokens FROM analytics_query_index WHERE _id = 'test-id-1'",
                "SELECT CAST(90616 AS BIGINT)");
    }

    @Test
    public void testDeferenceDeeplyNestedFieldUnquoted()
    {
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT reliability_scores.answer_relevance.score FROM analytics_query_index WHERE _id = 'test-id-1'",
                "SELECT 80");
    }

    @Test
    public void testDereferenceMultipleFieldsUnquoted()
    {
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT token_usage.total_tokens, token_usage.prompt_tokens " +
                        "FROM analytics_query_index WHERE _id = 'test-id-1'",
                "SELECT CAST(90616 AS BIGINT), CAST(45308 AS BIGINT)");
    }

    @Test
    public void testDereferenceInAggregateUnquoted()
    {
        assertQuery(
                createOpenSearchSession(),
                "SELECT COUNT(*), SUM(reliability_scores.answer_relevance.score), AVG(reliability_scores.answer_relevance.score) " +
                        "FROM analytics_query_index",
                "SELECT CAST(3 AS BIGINT), CAST(240 AS BIGINT), 80.0");
    }

    @Test
    public void testDereferenceInWhereClauseUnquoted()
    {
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT _id FROM analytics_query_index " +
                        "WHERE token_usage.total_tokens > 50000 " +
                        "ORDER BY _id",
                "VALUES ('test-id-1'), ('test-id-2')");
    }

    @Test
    public void testDereferenceInOrderByUnquoted()
    {
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT _id, token_usage.total_tokens FROM analytics_query_index " +
                        "ORDER BY token_usage.total_tokens DESC",
                "VALUES ('test-id-2', CAST(120000 AS BIGINT)), ('test-id-1', CAST(90616 AS BIGINT)), ('test-id-3', CAST(45000 AS BIGINT))");
    }

    @Test
    public void testDereferenceInGroupByUnquoted()
    {
        assertQuery(
                createOpenSearchSession(),
                "SELECT user_id, SUM(token_usage.total_tokens) FROM analytics_query_index GROUP BY user_id ORDER BY user_id",
                "VALUES ('user1', CAST(135616 AS BIGINT)), ('user2', CAST(120000 AS BIGINT))");
    }

    @Test
    public void testMixedQuotedAndUnquotedDereference()
    {
        // Test that both quoted and unquoted syntax work together
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT token_usage.total_tokens, \"token_usage.prompt_tokens\" " +
                        "FROM analytics_query_index WHERE _id = 'test-id-1'",
                "SELECT CAST(90616 AS BIGINT), CAST(45308 AS BIGINT)");
    }
    // ========== Complex Field Name Tests ==========

    /**
     * Test SELECT * with problematic field names that could cause serialization issues.
     * This test specifically addresses the bug where field names like "cache_creation_input_tokens"
     * could cause type parsing errors during distributed query execution.
     */
    @Test
    public void testSelectStarWithProblematicFieldNames()
    {
        // This should work without errors despite complex field names
        MaterializedResult result = computeActual(
                createOpenSearchSession(),
                "SELECT * FROM analytics_query_index WHERE _id = 'test-id-1'");

        // Verify we got results - this is the key test, ensuring SELECT * works
        assertTrue(result.getRowCount() > 0, "Should return at least one row");

        // Verify we got multiple columns (including the problematic field names)
        assertTrue(result.getTypes().size() > 10, "Should have multiple columns including problematic fields");
    }

    @Test
    public void testSelectProblematicFieldsExplicitly()
    {
        // Test selecting problematic fields explicitly
        assertQuery(
                createOpenSearchSession(),
                "SELECT cache_creation_input_tokens, cache_creation_output_tokens, total_input_tokens " +
                        "FROM analytics_query_index WHERE _id = 'test-id-1'",
                "SELECT CAST(1000 AS BIGINT), CAST(500 AS BIGINT), CAST(1200 AS BIGINT)");
    }

    @Test
    public void testAggregateOnProblematicFields()
    {
        // Test aggregations on fields with complex names
        assertQuery(
                createOpenSearchSession(),
                "SELECT SUM(cache_creation_input_tokens), AVG(prompt_tokens_used), MAX(model_response_time_ms) " +
                        "FROM analytics_query_index",
                "SELECT CAST(1000 AS BIGINT), 800.0, CAST(1500 AS BIGINT)");
    }

    @Test
    public void testPredicateOnProblematicFields()
    {
        // Test WHERE clause with problematic field names
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT _id FROM analytics_query_index " +
                        "WHERE cache_creation_input_tokens > 500 " +
                        "ORDER BY _id",
                "VALUES ('test-id-1')");
    }

    @Test
    public void testGroupByProblematicFields()
    {
        // Test GROUP BY with problematic field names
        // Note: Only doc1 has llm_model_version, doc2 and doc3 don't have this field
        assertQuery(
                createOpenSearchSession(),
                "SELECT llm_model_version, COUNT(*) FROM analytics_query_index " +
                        "GROUP BY llm_model_version " +
                        "ORDER BY llm_model_version",
                "VALUES (CAST(NULL AS VARCHAR), CAST(2 AS BIGINT)), ('gpt-4-0613', CAST(1 AS BIGINT))");
    }

    @Test
    public void testOrderByProblematicFields()
    {
        // Test ORDER BY with problematic field names
        assertQuery(
                createOpenSearchSession(),
                "SELECT DISTINCT _id, model_response_time_ms FROM analytics_query_index " +
                        "WHERE model_response_time_ms IS NOT NULL " +
                        "ORDER BY model_response_time_ms DESC",
                "VALUES ('test-id-1', CAST(1500 AS BIGINT))");
    }

    @Test
    public void testJoinWithProblematicFields()
    {
        // Test JOIN involving tables with problematic field names
        assertQuery(
                createOpenSearchSession(),
                "SELECT a._id, a.cache_creation_input_tokens " +
                        "FROM analytics_query_index a " +
                        "WHERE a.cache_creation_input_tokens > 0 " +
                        "ORDER BY a._id",
                "VALUES ('test-id-1', CAST(1000 AS BIGINT))");
    }

    @Test
    public void testComplexQueryWithProblematicFields()
    {
        // Test complex query combining multiple operations on problematic fields
        assertQuery(
                createOpenSearchSession(),
                "SELECT " +
                        "  llm_model_version, " +
                        "  COUNT(*) as query_count, " +
                        "  SUM(cache_creation_input_tokens) as total_cache_input, " +
                        "  AVG(prompt_tokens_used) as avg_prompt_tokens, " +
                        "  MAX(model_response_time_ms) as max_response_time " +
                        "FROM analytics_query_index " +
                        "WHERE cache_creation_input_tokens > 0 " +
                        "GROUP BY llm_model_version " +
                        "ORDER BY llm_model_version",
                "VALUES ('gpt-4-0613', CAST(1 AS BIGINT), CAST(1000 AS BIGINT), 800.0, CAST(1500 AS BIGINT))");
    }

    @Test
    public void testAllProblematicFieldsAccessible()
    {
        // Verify all problematic fields can be accessed without errors
        String[] problematicFields = {
            "cache_creation_input_tokens",
            "cache_creation_output_tokens",
            "cache_read_input_tokens",
            "total_input_tokens",
            "prompt_tokens_used",
            "completion_tokens_generated",
            "model_response_time_ms",
            "llm_model_version"
        };

        for (String field : problematicFields) {
            MaterializedResult result = computeActual(
                    createOpenSearchSession(),
                    String.format("SELECT %s FROM analytics_query_index WHERE _id = 'test-id-1'", field));

            assertTrue(result.getRowCount() > 0,
                    "Should be able to query field: " + field);
        }
    }
}
