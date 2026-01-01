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

import com.facebook.presto.spi.PrestoException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * Tests for VectorSearchHandler k-NN query building functionality.
 */
@Test(singleThreaded = true)
public class TestVectorSearchHandler
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private VectorSearchHandler handler;
    private OpenSearchConfig config;

    @BeforeMethod
    public void setUp()
    {
        config = new OpenSearchConfig();
        config.setVectorSearchEnabled(true);
        config.setVectorSearchDefaultK(10);
        config.setVectorSearchDefaultSpaceType("cosine");
        config.setVectorSearchDefaultEfSearch(100);
        handler = new VectorSearchHandler(config);
    }

    @Test
    public void testBuildBasicKnnQuery()
            throws Exception
    {
        float[] queryVector = {0.1f, 0.2f, 0.3f, 0.4f};
        String query = handler.buildKnnQuery("embedding", queryVector, 10);

        assertNotNull(query);
        JsonNode root = OBJECT_MAPPER.readTree(query);

        assertTrue(root.has("knn"));
        JsonNode knn = root.get("knn");
        assertTrue(knn.has("embedding"));

        JsonNode embeddingQuery = knn.get("embedding");
        assertTrue(embeddingQuery.has("vector"));
        assertTrue(embeddingQuery.has("k"));
        assertEquals(embeddingQuery.get("k").asInt(), 10);

        JsonNode vector = embeddingQuery.get("vector");
        assertEquals(vector.size(), 4);
        assertEquals(vector.get(0).asDouble(), 0.1, 0.001);
    }

    @Test
    public void testBuildKnnQueryWithEfSearch()
            throws Exception
    {
        float[] queryVector = {0.1f, 0.2f, 0.3f};
        String query = handler.buildKnnQuery("embedding", queryVector, 20, "cosine", 200);

        JsonNode root = OBJECT_MAPPER.readTree(query);
        JsonNode embeddingQuery = root.get("knn").get("embedding");

        assertTrue(embeddingQuery.has("ef_search"));
        assertEquals(embeddingQuery.get("ef_search").asInt(), 200);
        assertEquals(embeddingQuery.get("k").asInt(), 20);
    }

    @Test
    public void testBuildKnnQueryWithDifferentSpaceTypes()
            throws Exception
    {
        float[] queryVector = {0.1f, 0.2f};

        // Test cosine
        String cosineQuery = handler.buildKnnQuery("embedding", queryVector, 10, "cosine", null);
        assertNotNull(cosineQuery);

        // Test l2
        String l2Query = handler.buildKnnQuery("embedding", queryVector, 10, "l2", null);
        assertNotNull(l2Query);

        // Test inner_product
        String ipQuery = handler.buildKnnQuery("embedding", queryVector, 10, "inner_product", null);
        assertNotNull(ipQuery);
    }

    @Test
    public void testBuildHybridSearchQuery()
            throws Exception
    {
        float[] queryVector = {0.1f, 0.2f, 0.3f};
        String filterQuery = "{\"term\":{\"category\":\"electronics\"}}";

        String query = handler.buildHybridSearchQuery("embedding", queryVector, 10, filterQuery);

        JsonNode root = OBJECT_MAPPER.readTree(query);
        JsonNode embeddingQuery = root.get("knn").get("embedding");

        assertTrue(embeddingQuery.has("filter"));
        JsonNode filter = embeddingQuery.get("filter");
        assertTrue(filter.has("term"));
    }

    @Test
    public void testBuildHybridSearchQueryWithEfSearch()
            throws Exception
    {
        float[] queryVector = {0.1f, 0.2f};
        String filterQuery = "{\"range\":{\"price\":{\"gte\":100,\"lte\":500}}}";

        String query = handler.buildHybridSearchQuery(
                "embedding", queryVector, 15, filterQuery, "l2", 150);

        JsonNode root = OBJECT_MAPPER.readTree(query);
        JsonNode embeddingQuery = root.get("knn").get("embedding");

        assertTrue(embeddingQuery.has("filter"));
        assertTrue(embeddingQuery.has("ef_search"));
        assertEquals(embeddingQuery.get("ef_search").asInt(), 150);
        assertEquals(embeddingQuery.get("k").asInt(), 15);
    }

    @Test
    public void testBuildScriptScoreQuery()
            throws Exception
    {
        float[] queryVector = {0.1f, 0.2f, 0.3f};

        String query = handler.buildScriptScoreQuery("embedding", queryVector, "cosine");

        JsonNode root = OBJECT_MAPPER.readTree(query);
        assertTrue(root.has("script_score"));

        JsonNode scriptScore = root.get("script_score");
        assertTrue(scriptScore.has("query"));
        assertTrue(scriptScore.has("script"));

        JsonNode script = scriptScore.get("script");
        assertTrue(script.has("source"));
        assertTrue(script.has("params"));

        JsonNode params = script.get("params");
        assertEquals(params.get("field").asText(), "embedding");
        assertTrue(params.has("query_value"));
    }

    @Test
    public void testScriptScoreQueryDifferentMetrics()
            throws Exception
    {
        float[] queryVector = {0.1f, 0.2f};

        // Test cosine
        String cosineQuery = handler.buildScriptScoreQuery("embedding", queryVector, "cosine");
        assertTrue(cosineQuery.contains("cosineSimilarity"));

        // Test l2
        String l2Query = handler.buildScriptScoreQuery("embedding", queryVector, "l2");
        assertTrue(l2Query.contains("l2norm"));

        // Test inner_product
        String ipQuery = handler.buildScriptScoreQuery("embedding", queryVector, "inner_product");
        assertTrue(ipQuery.contains("dotProduct"));
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testVectorSearchDisabled()
    {
        config.setVectorSearchEnabled(false);
        VectorSearchHandler disabledHandler = new VectorSearchHandler(config);

        float[] queryVector = {0.1f, 0.2f};
        disabledHandler.buildKnnQuery("embedding", queryVector, 10);
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testInvalidK()
    {
        float[] queryVector = {0.1f, 0.2f};
        handler.buildKnnQuery("embedding", queryVector, 0);
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testNegativeK()
    {
        float[] queryVector = {0.1f, 0.2f};
        handler.buildKnnQuery("embedding", queryVector, -5);
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testEmptyQueryVector()
    {
        float[] queryVector = {};
        handler.buildKnnQuery("embedding", queryVector, 10);
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testInvalidSpaceType()
    {
        float[] queryVector = {0.1f, 0.2f};
        handler.buildKnnQuery("embedding", queryVector, 10, "invalid_metric", null);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testNullVectorField()
    {
        float[] queryVector = {0.1f, 0.2f};
        handler.buildKnnQuery(null, queryVector, 10);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testNullQueryVector()
    {
        handler.buildKnnQuery("embedding", null, 10);
    }

    @Test
    public void testCosineSimilarityCalculation()
    {
        float[] vector1 = {1.0f, 0.0f, 0.0f};
        float[] vector2 = {1.0f, 0.0f, 0.0f};

        double similarity = VectorSearchHandler.cosineSimilarity(vector1, vector2);
        assertEquals(similarity, 1.0, 0.001);
    }

    @Test
    public void testCosineSimilarityOrthogonal()
    {
        float[] vector1 = {1.0f, 0.0f};
        float[] vector2 = {0.0f, 1.0f};

        double similarity = VectorSearchHandler.cosineSimilarity(vector1, vector2);
        assertEquals(similarity, 0.0, 0.001);
    }

    @Test
    public void testEuclideanDistance()
    {
        float[] vector1 = {0.0f, 0.0f};
        float[] vector2 = {3.0f, 4.0f};

        double distance = VectorSearchHandler.euclideanDistance(vector1, vector2);
        assertEquals(distance, 5.0, 0.001);
    }

    @Test
    public void testDotProduct()
    {
        float[] vector1 = {1.0f, 2.0f, 3.0f};
        float[] vector2 = {4.0f, 5.0f, 6.0f};

        double dotProduct = VectorSearchHandler.dotProduct(vector1, vector2);
        assertEquals(dotProduct, 32.0, 0.001); // 1*4 + 2*5 + 3*6 = 32
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testCosineSimilarityDimensionMismatch()
    {
        float[] vector1 = {1.0f, 2.0f};
        float[] vector2 = {1.0f, 2.0f, 3.0f};

        VectorSearchHandler.cosineSimilarity(vector1, vector2);
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testEuclideanDistanceDimensionMismatch()
    {
        float[] vector1 = {1.0f, 2.0f};
        float[] vector2 = {1.0f};

        VectorSearchHandler.euclideanDistance(vector1, vector2);
    }

    @Test
    public void testCosineSimilarityZeroVector()
    {
        float[] vector1 = {0.0f, 0.0f};
        float[] vector2 = {1.0f, 1.0f};

        double similarity = VectorSearchHandler.cosineSimilarity(vector1, vector2);
        assertEquals(similarity, 0.0, 0.001);
    }

    @Test
    public void testLargeVectorDimensions()
            throws Exception
    {
        // Test with 768-dimensional vector (common for embeddings)
        float[] queryVector = new float[768];
        for (int i = 0; i < 768; i++) {
            queryVector[i] = (float) Math.random();
        }

        String query = handler.buildKnnQuery("embedding", queryVector, 10);

        JsonNode root = OBJECT_MAPPER.readTree(query);
        JsonNode vector = root.get("knn").get("embedding").get("vector");
        assertEquals(vector.size(), 768);
    }

    @Test
    public void testMultipleSpaceTypeAliases()
            throws Exception
    {
        float[] queryVector = {0.1f, 0.2f};

        // Test that both "cosine" and "cosinesimil" work
        String query1 = handler.buildKnnQuery("embedding", queryVector, 10, "cosine", null);
        String query2 = handler.buildKnnQuery("embedding", queryVector, 10, "cosinesimil", null);
        assertNotNull(query1);
        assertNotNull(query2);

        // Test that both "l2" and "euclidean" work
        String query3 = handler.buildKnnQuery("embedding", queryVector, 10, "l2", null);
        String query4 = handler.buildKnnQuery("embedding", queryVector, 10, "euclidean", null);
        assertNotNull(query3);
        assertNotNull(query4);
    }
}
