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
import com.facebook.presto.spi.PrestoException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.inject.Inject;

import java.util.HashMap;
import java.util.Map;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

/**
 * Handles vector search operations for OpenSearch k-NN plugin.
 * Translates vector search requests into OpenSearch k-NN query DSL.
 *
 * Supports:
 * - Basic k-NN search with configurable distance metrics
 * - Hybrid search combining vector similarity with filters
 * - Efficient search with ef_search parameter
 * - Multiple distance metrics: cosine, euclidean (l2), dot_product (inner_product)
 */
public class VectorSearchHandler
{
    private static final Logger log = Logger.get(VectorSearchHandler.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final OpenSearchConfig config;

    @Inject
    public VectorSearchHandler(OpenSearchConfig config)
    {
        this.config = requireNonNull(config, "config is null");
    }

    /**
     * Builds a k-NN vector search query using OpenSearch k-NN plugin.
     *
     * @param vectorField The name of the vector field
     * @param queryVector The query vector as float array
     * @param k The number of nearest neighbors to return
     * @return JSON string representing the k-NN query
     */
    public String buildKnnQuery(String vectorField, float[] queryVector, int k)
    {
        return buildKnnQuery(vectorField, queryVector, k, "cosine", null);
    }

    /**
     * Builds a k-NN vector search query with specified distance metric.
     *
     * @param vectorField The name of the vector field
     * @param queryVector The query vector as float array
     * @param k The number of nearest neighbors to return
     * @param spaceType Distance metric: "cosine", "l2" (euclidean), "inner_product" (dot product)
     * @param efSearch Efficiency parameter for HNSW algorithm (null for default)
     * @return JSON string representing the k-NN query
     */
    public String buildKnnQuery(
            String vectorField,
            float[] queryVector,
            int k,
            String spaceType,
            Integer efSearch)
    {
        requireNonNull(vectorField, "vectorField is null");
        requireNonNull(queryVector, "queryVector is null");
        requireNonNull(spaceType, "spaceType is null");

        validateVectorSearchEnabled();
        validateK(k);
        validateQueryVector(queryVector);
        validateSpaceType(spaceType);

        try {
            Map<String, Object> knnQuery = new HashMap<>();
            Map<String, Object> fieldQuery = new HashMap<>();

            fieldQuery.put("vector", queryVector);
            fieldQuery.put("k", k);

            // Add ef_search if specified (for HNSW algorithm tuning)
            if (efSearch != null && efSearch > 0) {
                fieldQuery.put("ef_search", efSearch);
            }

            knnQuery.put(vectorField, fieldQuery);

            Map<String, Object> query = new HashMap<>();
            query.put("knn", knnQuery);

            String queryJson = OBJECT_MAPPER.writeValueAsString(query);
            log.debug("Built k-NN query for field %s with k=%d, space_type=%s: %s",
                    vectorField, k, spaceType, queryJson);

            return queryJson;
        }
        catch (JsonProcessingException e) {
            throw new PrestoException(NOT_SUPPORTED, "Failed to build k-NN query", e);
        }
    }

    /**
     * Builds a hybrid search query combining k-NN vector search with filters.
     * Uses the efficient_filter approach for better performance.
     *
     * @param vectorField The name of the vector field
     * @param queryVector The query vector as float array
     * @param k The number of nearest neighbors to return
     * @param filterQuery JSON string representing the filter query
     * @return JSON string representing the hybrid search query
     */
    public String buildHybridSearchQuery(
            String vectorField,
            float[] queryVector,
            int k,
            String filterQuery)
    {
        return buildHybridSearchQuery(vectorField, queryVector, k, filterQuery, "cosine", null);
    }

    /**
     * Builds a hybrid search query with specified distance metric.
     *
     * @param vectorField The name of the vector field
     * @param queryVector The query vector as float array
     * @param k The number of nearest neighbors to return
     * @param filterQuery JSON string representing the filter query
     * @param spaceType Distance metric
     * @param efSearch Efficiency parameter for HNSW algorithm
     * @return JSON string representing the hybrid search query
     */
    public String buildHybridSearchQuery(
            String vectorField,
            float[] queryVector,
            int k,
            String filterQuery,
            String spaceType,
            Integer efSearch)
    {
        requireNonNull(vectorField, "vectorField is null");
        requireNonNull(queryVector, "queryVector is null");
        requireNonNull(filterQuery, "filterQuery is null");
        requireNonNull(spaceType, "spaceType is null");

        validateVectorSearchEnabled();
        validateK(k);
        validateQueryVector(queryVector);
        validateSpaceType(spaceType);

        try {
            // Parse the filter query
            @SuppressWarnings("unchecked")
            Map<String, Object> filter = OBJECT_MAPPER.readValue(filterQuery, Map.class);

            // Build k-NN query with filter
            Map<String, Object> knnQuery = new HashMap<>();
            Map<String, Object> fieldQuery = new HashMap<>();

            fieldQuery.put("vector", queryVector);
            fieldQuery.put("k", k);
            fieldQuery.put("filter", filter);

            if (efSearch != null && efSearch > 0) {
                fieldQuery.put("ef_search", efSearch);
            }

            knnQuery.put(vectorField, fieldQuery);

            Map<String, Object> query = new HashMap<>();
            query.put("knn", knnQuery);

            String queryJson = OBJECT_MAPPER.writeValueAsString(query);
            log.debug("Built hybrid k-NN query for field %s with k=%d, space_type=%s, filter: %s",
                    vectorField, k, spaceType, filterQuery);

            return queryJson;
        }
        catch (JsonProcessingException e) {
            throw new PrestoException(NOT_SUPPORTED, "Failed to build hybrid k-NN query", e);
        }
    }

    /**
     * Builds a script score query for vector similarity.
     * Useful when you need more control over scoring or when k-NN is not available.
     *
     * @param vectorField The name of the vector field
     * @param queryVector The query vector as float array
     * @param spaceType Distance metric
     * @return JSON string representing the script score query
     */
    public String buildScriptScoreQuery(
            String vectorField,
            float[] queryVector,
            String spaceType)
    {
        requireNonNull(vectorField, "vectorField is null");
        requireNonNull(queryVector, "queryVector is null");
        requireNonNull(spaceType, "spaceType is null");

        validateVectorSearchEnabled();
        validateQueryVector(queryVector);
        validateSpaceType(spaceType);

        try {
            // Map space type to script function
            String scriptFunction = getScriptFunction(spaceType);

            Map<String, Object> scriptParams = new HashMap<>();
            scriptParams.put("field", vectorField);
            scriptParams.put("query_value", queryVector);

            Map<String, Object> script = new HashMap<>();
            script.put("source", scriptFunction);
            script.put("params", scriptParams);

            Map<String, Object> scriptScore = new HashMap<>();
            scriptScore.put("query", Map.of("match_all", Map.of()));
            scriptScore.put("script", script);

            Map<String, Object> query = new HashMap<>();
            query.put("script_score", scriptScore);

            String queryJson = OBJECT_MAPPER.writeValueAsString(query);
            log.debug("Built script score query for field %s with space_type=%s",
                    vectorField, spaceType);

            return queryJson;
        }
        catch (JsonProcessingException e) {
            throw new PrestoException(NOT_SUPPORTED, "Failed to build script score query", e);
        }
    }

    private String getScriptFunction(String spaceType)
    {
        switch (spaceType.toLowerCase(java.util.Locale.ENGLISH)) {
            case "cosine":
            case "cosinesimil":
                return "cosineSimilarity(params.query_value, params.field) + 1.0";
            case "l2":
            case "euclidean":
                return "1 / (1 + l2norm(params.query_value, params.field))";
            case "inner_product":
            case "dot_product":
                return "dotProduct(params.query_value, params.field) + 1.0";
            default:
                throw new PrestoException(NOT_SUPPORTED,
                        "Unsupported space type for script score: " + spaceType);
        }
    }

    private void validateVectorSearchEnabled()
    {
        if (!config.isVectorSearchEnabled()) {
            throw new PrestoException(
                    NOT_SUPPORTED,
                    "Vector search is not enabled. Set opensearch.vector-search.enabled=true");
        }
    }

    private void validateK(int k)
    {
        if (k <= 0) {
            throw new PrestoException(
                    NOT_SUPPORTED,
                    "k must be positive, got: " + k);
        }
    }

    private void validateQueryVector(float[] queryVector)
    {
        if (queryVector.length == 0) {
            throw new PrestoException(
                    NOT_SUPPORTED,
                    "Query vector cannot be empty");
        }
    }

    private void validateSpaceType(String spaceType)
    {
        String normalized = spaceType.toLowerCase(java.util.Locale.ENGLISH);
        if (!normalized.equals("cosine") &&
                !normalized.equals("cosinesimil") &&
                !normalized.equals("l2") &&
                !normalized.equals("euclidean") &&
                !normalized.equals("inner_product") &&
                !normalized.equals("dot_product")) {
            throw new PrestoException(
                    NOT_SUPPORTED,
                    "Unsupported space type: " + spaceType +
                    ". Supported types: cosine, l2, inner_product");
        }
    }

    /**
     * Calculates cosine similarity between two vectors.
     *
     * @param vector1 First vector
     * @param vector2 Second vector
     * @return Cosine similarity score between -1 and 1
     */
    public static double cosineSimilarity(float[] vector1, float[] vector2)
    {
        requireNonNull(vector1, "vector1 is null");
        requireNonNull(vector2, "vector2 is null");

        if (vector1.length != vector2.length) {
            throw new PrestoException(
                    NOT_SUPPORTED,
                    String.format("Vector dimensions must match: %d vs %d",
                            vector1.length, vector2.length));
        }

        if (vector1.length == 0) {
            throw new PrestoException(
                    NOT_SUPPORTED,
                    "Vectors cannot be empty");
        }

        double dotProduct = 0.0;
        double norm1 = 0.0;
        double norm2 = 0.0;

        for (int i = 0; i < vector1.length; i++) {
            dotProduct += vector1[i] * vector2[i];
            norm1 += vector1[i] * vector1[i];
            norm2 += vector2[i] * vector2[i];
        }

        if (norm1 == 0.0 || norm2 == 0.0) {
            return 0.0;
        }

        return dotProduct / (Math.sqrt(norm1) * Math.sqrt(norm2));
    }

    /**
     * Calculates Euclidean distance between two vectors.
     *
     * @param vector1 First vector
     * @param vector2 Second vector
     * @return Euclidean distance
     */
    public static double euclideanDistance(float[] vector1, float[] vector2)
    {
        requireNonNull(vector1, "vector1 is null");
        requireNonNull(vector2, "vector2 is null");

        if (vector1.length != vector2.length) {
            throw new PrestoException(
                    NOT_SUPPORTED,
                    String.format("Vector dimensions must match: %d vs %d",
                            vector1.length, vector2.length));
        }

        if (vector1.length == 0) {
            throw new PrestoException(
                    NOT_SUPPORTED,
                    "Vectors cannot be empty");
        }

        double sum = 0.0;
        for (int i = 0; i < vector1.length; i++) {
            double diff = vector1[i] - vector2[i];
            sum += diff * diff;
        }

        return Math.sqrt(sum);
    }

    /**
     * Calculates dot product between two vectors.
     *
     * @param vector1 First vector
     * @param vector2 Second vector
     * @return Dot product
     */
    public static double dotProduct(float[] vector1, float[] vector2)
    {
        requireNonNull(vector1, "vector1 is null");
        requireNonNull(vector2, "vector2 is null");

        if (vector1.length != vector2.length) {
            throw new PrestoException(
                    NOT_SUPPORTED,
                    String.format("Vector dimensions must match: %d vs %d",
                            vector1.length, vector2.length));
        }

        if (vector1.length == 0) {
            throw new PrestoException(
                    NOT_SUPPORTED,
                    "Vectors cannot be empty");
        }

        double result = 0.0;
        for (int i = 0; i < vector1.length; i++) {
            result += vector1[i] * vector2[i];
        }

        return result;
    }
}
