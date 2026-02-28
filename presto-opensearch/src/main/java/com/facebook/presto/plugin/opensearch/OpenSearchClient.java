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

import com.facebook.airlift.json.JsonObjectMapperProvider;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.PrestoException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import jakarta.inject.Inject;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.TrustAllStrategy;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.ssl.SSLContextBuilder;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;

import javax.net.ssl.SSLContext;

import java.io.IOException;
import java.io.InputStream;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

/**
 * Client for communicating with OpenSearch cluster.
 * Uses OpenSearch REST API for all operations.
 */
public class OpenSearchClient
{
    private static final Logger log = Logger.get(OpenSearchClient.class);
    private static final ObjectMapper OBJECT_MAPPER = new JsonObjectMapperProvider().get()
            .configure(DeserializationFeature.USE_JAVA_ARRAY_FOR_JSON_ARRAY, false);
    private static final String METADATA_INDEX = ".presto_metadata";

    private final OpenSearchConfig config;
    private final RestClient restClient;

    @Inject
    public OpenSearchClient(OpenSearchConfig config)
    {
        this.config = requireNonNull(config, "config is null");
        this.restClient = createRestClient();
        log.info("OpenSearch client initialized for %s:%d", config.getHost(), config.getPort());
    }

    private RestClient createRestClient()
    {
        HttpHost host = new HttpHost(
                config.getHost(),
                config.getPort(),
                config.isSslEnabled() ? "https" : "http");

        RestClientBuilder builder = RestClient.builder(host)
                .setRequestConfigCallback(requestConfigBuilder ->
                        requestConfigBuilder
                                .setConnectTimeout(config.getConnectTimeout())
                                .setSocketTimeout(config.getSocketTimeout()));

        // Configure SSL and authentication
        builder.setHttpClientConfigCallback(httpClientBuilder -> {
            // Add authentication if configured
            if (config.getUsername() != null && config.getPassword() != null) {
                CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                credentialsProvider.setCredentials(
                        AuthScope.ANY,
                        new UsernamePasswordCredentials(config.getUsername(), config.getPassword()));
                httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            }

            // Configure SSL certificate validation
            if (config.isSslEnabled() && config.isSslSkipCertificateValidation()) {
                try {
                    SSLContext sslContext = SSLContextBuilder.create()
                            .loadTrustMaterial(new TrustAllStrategy())
                            .build();
                    httpClientBuilder.setSSLContext(sslContext);
                    httpClientBuilder.setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE);
                    log.warn("SSL certificate validation is disabled. This should only be used for testing with expired certificates.");
                }
                catch (NoSuchAlgorithmException | KeyStoreException | KeyManagementException e) {
                    throw new RuntimeException("Failed to configure SSL context", e);
                }
            }

            return httpClientBuilder;
        });

        return builder.build();
    }

    public List<String> listIndices()
    {
        try {
            Request request = new Request("GET", "/_cat/indices?format=json&h=index");
            Response response = restClient.performRequest(request);

            try (InputStream content = response.getEntity().getContent()) {
                JsonNode root = OBJECT_MAPPER.readTree(content);
                ImmutableList.Builder<String> indices = ImmutableList.builder();

                if (root.isArray()) {
                    for (JsonNode node : root) {
                        String indexName = node.get("index").asText();
                        // Filter out system indices (starting with .)
                        if (!indexName.startsWith(".")) {
                            indices.add(indexName);
                        }
                    }
                }

                List<String> result = indices.build();
                log.debug("Found %d indices", result.size());
                return result;
            }
        }
        catch (IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to list OpenSearch indices", e);
        }
    }

    public Map<String, Object> getIndexMapping(String indexName)
    {
        try {
            Request request = new Request("GET", "/" + indexName + "/_mapping");
            Response response = restClient.performRequest(request);

            try (InputStream content = response.getEntity().getContent()) {
                JsonNode root = OBJECT_MAPPER.readTree(content);
                JsonNode indexNode = root.get(indexName);

                if (indexNode == null) {
                    return Collections.emptyMap();
                }

                JsonNode mappingsNode = indexNode.get("mappings");
                if (mappingsNode == null) {
                    return Collections.emptyMap();
                }

                JsonNode propertiesNode = mappingsNode.get("properties");
                if (propertiesNode == null) {
                    return Collections.emptyMap();
                }

                // Use LinkedHashMap to preserve field order from OpenSearch
                Map<String, Object> properties = new LinkedHashMap<>();
                Iterator<Map.Entry<String, JsonNode>> fields = propertiesNode.fields();
                while (fields.hasNext()) {
                    Map.Entry<String, JsonNode> field = fields.next();
                    // Convert to LinkedHashMap to preserve nested field order
                    properties.put(field.getKey(), OBJECT_MAPPER.convertValue(field.getValue(), LinkedHashMap.class));
                }

                log.debug("Retrieved mapping for index %s with %d fields", indexName, properties.size());
                return properties;
            }
        }
        catch (IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to get OpenSearch index mapping", e);
        }
    }

    public List<Map<String, Object>> executeQuery(String indexName, String query, List<String> fields, int from, int size)
    {
        return executeQuery(indexName, query, fields, from, size, null);
    }

    public List<Map<String, Object>> executeQuery(String indexName, String query, List<String> fields, int from, int size, Integer shardId)
    {
        try {
            // Build search request
            Map<String, Object> searchRequest = new HashMap<>();
            searchRequest.put("from", from);
            searchRequest.put("size", size);

            // Add query
            Map<String, Object> queryMap = OBJECT_MAPPER.readValue(query, Map.class);
            searchRequest.put("query", queryMap);

            // Add source filtering if fields specified
            if (fields != null && !fields.isEmpty()) {
                searchRequest.put("_source", fields);
            }

            String requestBody = OBJECT_MAPPER.writeValueAsString(searchRequest);

            Request request = new Request("POST", "/" + indexName + "/_search");
            request.setJsonEntity(requestBody);

            // Add preference parameter to route to specific shard if provided
            if (shardId != null) {
                request.addParameter("preference", "_shards:" + shardId);
            }

            Response response = restClient.performRequest(request);

            try (InputStream content = response.getEntity().getContent()) {
                JsonNode root = OBJECT_MAPPER.readTree(content);
                JsonNode hitsNode = root.get("hits").get("hits");

                List<Map<String, Object>> results = new ArrayList<>();
                if (hitsNode.isArray()) {
                    for (JsonNode hit : hitsNode) {
                        Map<String, Object> document = new HashMap<>();

                        // Add _id
                        document.put("_id", hit.get("_id").asText());

                        // Add _source fields
                        JsonNode sourceNode = hit.get("_source");
                        if (sourceNode != null) {
                            Map<String, Object> source = OBJECT_MAPPER.convertValue(sourceNode, Map.class);
                            document.putAll(source);
                        }

                        results.add(document);
                    }
                }

                log.debug("Query returned %d documents from index %s", results.size(), indexName);
                return results;
            }
        }
        catch (IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to execute OpenSearch query", e);
        }
    }

    /**
     * Execute a k-NN vector search query.
     *
     * @param indexName The index to search
     * @param vectorField The vector field name
     * @param queryVector The query vector
     * @param k Number of nearest neighbors
     * @return List of matching documents with scores
     */
    public List<Map<String, Object>> executeVectorSearch(String indexName, String vectorField, float[] queryVector, int k)
    {
        return executeVectorSearch(indexName, vectorField, queryVector, k, null, null, null);
    }

    /**
     * Execute a k-NN vector search query with advanced options.
     *
     * @param indexName The index to search
     * @param vectorField The vector field name
     * @param queryVector The query vector
     * @param k Number of nearest neighbors
     * @param spaceType Distance metric (cosine, l2, inner_product)
     * @param efSearch HNSW ef_search parameter for search quality/speed tradeoff
     * @param filterQuery Optional filter query to apply (JSON string)
     * @return List of matching documents with scores
     */
    public List<Map<String, Object>> executeVectorSearch(
            String indexName,
            String vectorField,
            float[] queryVector,
            int k,
            String spaceType,
            Integer efSearch,
            String filterQuery)
    {
        try {
            // Build k-NN search request for OpenSearch 2.x k-NN plugin
            // For HNSW indices, use script_score query approach with doc values
            Map<String, Object> searchRequest = new HashMap<>();
            searchRequest.put("size", k);

            // Build script score query for k-NN
            Map<String, Object> scriptParams = new HashMap<>();
            scriptParams.put("field", vectorField);
            scriptParams.put("query_value", queryVector);

            // Determine space type for script
            // Use doc[field] syntax to access k-NN vector fields
            String scriptSpaceType = spaceType != null ? spaceType : "cosinesimil";
            String scriptSource;
            if ("cosine".equals(scriptSpaceType) || "cosinesimil".equals(scriptSpaceType)) {
                scriptSource = "1.0 + cosineSimilarity(params.query_value, doc[params.field])";
            }
            else if ("l2".equals(scriptSpaceType)) {
                scriptSource = "1.0 / (1.0 + l2norm(params.query_value, doc[params.field]))";
            }
            else if ("inner_product".equals(scriptSpaceType) || "innerproduct".equals(scriptSpaceType)) {
                scriptSource = "dotProduct(params.query_value, doc[params.field]) + 1.0";
            }
            else {
                scriptSource = "1.0 + cosineSimilarity(params.query_value, doc[params.field])";
            }

            Map<String, Object> script = new HashMap<>();
            script.put("source", scriptSource);
            script.put("params", scriptParams);

            Map<String, Object> scriptScore = new HashMap<>();
            scriptScore.put("query", Collections.singletonMap("match_all", Collections.emptyMap()));
            scriptScore.put("script", script);

            Map<String, Object> query = new HashMap<>();
            query.put("script_score", scriptScore);

            searchRequest.put("query", query);

            String requestBody = OBJECT_MAPPER.writeValueAsString(searchRequest);
            log.debug("Executing k-NN search on index %s: %s", indexName, requestBody);

            // Use regular _search endpoint for OpenSearch 2.x
            Request request = new Request("POST", "/" + indexName + "/_search");
            request.setJsonEntity(requestBody);

            Response response = restClient.performRequest(request);

            try (InputStream content = response.getEntity().getContent()) {
                JsonNode root = OBJECT_MAPPER.readTree(content);
                JsonNode hitsNode = root.get("hits").get("hits");

                List<Map<String, Object>> results = new ArrayList<>();
                if (hitsNode.isArray()) {
                    for (JsonNode hit : hitsNode) {
                        Map<String, Object> document = new HashMap<>();

                        // Add _id and _score
                        document.put("_id", hit.get("_id").asText());
                        document.put("_score", hit.get("_score").asDouble());

                        // Add _source fields
                        JsonNode sourceNode = hit.get("_source");
                        if (sourceNode != null) {
                            Map<String, Object> source = OBJECT_MAPPER.convertValue(sourceNode, Map.class);
                            document.putAll(source);
                        }

                        results.add(document);
                    }
                }

                log.debug("Vector search returned %d documents from index %s", results.size(), indexName);
                return results;
            }
        }
        catch (IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to execute OpenSearch vector search", e);
        }
    }

    /**
     * Execute a raw k-NN query (provided as JSON string).
     * Useful for custom queries built by VectorSearchHandler.
     *
     * @param indexName The index to search
     * @param knnQueryJson The complete k-NN query as JSON
     * @param size Maximum number of results to return
     * @return List of matching documents with scores
     */
    public List<Map<String, Object>> executeKnnQuery(String indexName, String knnQueryJson, int size)
    {
        try {
            // Parse the k-NN query
            @SuppressWarnings("unchecked")
            Map<String, Object> queryMap = OBJECT_MAPPER.readValue(knnQueryJson, Map.class);

            // Build search request
            Map<String, Object> searchRequest = new HashMap<>();
            searchRequest.put("size", size);
            searchRequest.put("query", queryMap);

            String requestBody = OBJECT_MAPPER.writeValueAsString(searchRequest);
            log.debug("Executing k-NN query on index %s: %s", indexName, requestBody);

            Request request = new Request("POST", "/" + indexName + "/_search");
            request.setJsonEntity(requestBody);

            Response response = restClient.performRequest(request);

            try (InputStream content = response.getEntity().getContent()) {
                JsonNode root = OBJECT_MAPPER.readTree(content);
                JsonNode hitsNode = root.get("hits").get("hits");

                List<Map<String, Object>> results = new ArrayList<>();
                if (hitsNode.isArray()) {
                    for (JsonNode hit : hitsNode) {
                        Map<String, Object> document = new HashMap<>();

                        // Add _id and _score
                        document.put("_id", hit.get("_id").asText());
                        document.put("_score", hit.get("_score").asDouble());

                        // Add _source fields
                        JsonNode sourceNode = hit.get("_source");
                        if (sourceNode != null) {
                            Map<String, Object> source = OBJECT_MAPPER.convertValue(sourceNode, Map.class);
                            document.putAll(source);
                        }

                        results.add(document);
                    }
                }

                log.debug("k-NN query returned %d documents from index %s", results.size(), indexName);
                return results;
            }
        }
        catch (IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to execute OpenSearch k-NN query", e);
        }
    }

    public List<ShardInfo> getShardInfo(String indexName)
    {
        try {
            Request request = new Request("GET", "/_cat/shards/" + indexName + "?format=json&h=shard,node");
            Response response = restClient.performRequest(request);

            try (InputStream content = response.getEntity().getContent()) {
                JsonNode root = OBJECT_MAPPER.readTree(content);
                ImmutableList.Builder<ShardInfo> shards = ImmutableList.builder();

                if (root.isArray()) {
                    for (JsonNode node : root) {
                        int shardId = node.get("shard").asInt();
                        String nodeAddress = node.get("node").asText();
                        shards.add(new ShardInfo(shardId, nodeAddress));
                    }
                }

                List<ShardInfo> result = shards.build();
                log.debug("Found %d shards for index %s", result.size(), indexName);
                return result.isEmpty() ?
                        ImmutableList.of(new ShardInfo(0, config.getHost() + ":" + config.getPort())) :
                        result;
            }
        }
        catch (IOException e) {
            log.warn(e, "Failed to get shard info for index %s, using default", indexName);
            // Return default single shard on error
            return ImmutableList.of(new ShardInfo(0, config.getHost() + ":" + config.getPort()));
        }
    }
    /**
     * Store column order and type metadata for an index to preserve field order and types.
     * OpenSearch returns fields alphabetically and loses VARCHAR length info, so we store the original order and types.
     */
    public void storeColumnOrderMetadata(String indexName, List<String> columnNames, List<String> columnTypes)
    {
        try {
            // Create metadata index if it doesn't exist
            try {
                Request checkIndex = new Request("HEAD", "/" + METADATA_INDEX);
                restClient.performRequest(checkIndex);
            }
            catch (IOException e) {
                // Index doesn't exist, create it
                Request createIndex = new Request("PUT", "/" + METADATA_INDEX);
                createIndex.setJsonEntity("{\"settings\":{\"number_of_shards\":1,\"number_of_replicas\":0}}");
                restClient.performRequest(createIndex);
                log.info("Created metadata index: %s", METADATA_INDEX);
            }

            // Store column order and types as a document
            Map<String, Object> metadata = new HashMap<>();
            metadata.put("index_name", indexName);
            metadata.put("column_order", columnNames);
            if (columnTypes != null) {
                metadata.put("column_types", columnTypes);
            }
            metadata.put("timestamp", System.currentTimeMillis());

            String metadataJson = OBJECT_MAPPER.writeValueAsString(metadata);
            Request indexRequest = new Request("PUT", "/" + METADATA_INDEX + "/_doc/" + indexName);
            indexRequest.setJsonEntity(metadataJson);
            restClient.performRequest(indexRequest);

            log.debug("Stored column metadata for index %s: %d columns", indexName, columnNames.size());
        }
        catch (IOException e) {
            log.warn(e, "Failed to store column metadata for index %s", indexName);
            // Non-fatal - we can fall back to alphabetical order
        }
    }

    /**
     * Store column order metadata for an index (backward compatibility).
     */
    public void storeColumnOrderMetadata(String indexName, List<String> columnNames)
    {
        storeColumnOrderMetadata(indexName, columnNames, null);
    }

    /**
     * Retrieve stored column order for an index.
     * Returns empty list if metadata doesn't exist.
     */
    public List<String> getColumnOrderMetadata(String indexName)
    {
        Map<String, List<String>> metadata = getColumnMetadata(indexName);
        return metadata.getOrDefault("column_order", Collections.emptyList());
    }

    /**
     * Retrieve stored column metadata (order and types) for an index.
     * Returns map with "column_order" and "column_types" keys.
     */
    public Map<String, List<String>> getColumnMetadata(String indexName)
    {
        try {
            Request request = new Request("GET", "/" + METADATA_INDEX + "/_doc/" + indexName);
            Response response = restClient.performRequest(request);

            try (InputStream content = response.getEntity().getContent()) {
                JsonNode root = OBJECT_MAPPER.readTree(content);
                JsonNode source = root.get("_source");
                if (source != null) {
                    Map<String, List<String>> result = new HashMap<>();

                    // Get column order
                    if (source.has("column_order")) {
                        JsonNode columnOrderNode = source.get("column_order");
                        List<String> columnOrder = new ArrayList<>();
                        if (columnOrderNode.isArray()) {
                            for (JsonNode node : columnOrderNode) {
                                columnOrder.add(node.asText());
                            }
                        }
                        result.put("column_order", columnOrder);
                    }

                    // Get column types
                    if (source.has("column_types")) {
                        JsonNode columnTypesNode = source.get("column_types");
                        List<String> columnTypes = new ArrayList<>();
                        if (columnTypesNode.isArray()) {
                            for (JsonNode node : columnTypesNode) {
                                columnTypes.add(node.asText());
                            }
                        }
                        result.put("column_types", columnTypes);
                    }

                    log.debug("Retrieved column metadata for index %s", indexName);
                    return result;
                }
            }
        }
        catch (IOException e) {
            log.debug("No column metadata found for index %s", indexName);
        }
        return Collections.emptyMap();
    }

    public void close()
    {
        try {
            if (restClient != null) {
                restClient.close();
                log.info("OpenSearch client closed");
            }
        }
        catch (IOException e) {
            log.error(e, "Error closing OpenSearch client");
        }
    }

    /**
     * Information about an OpenSearch shard.
     */
    public static class ShardInfo
    {
        private final int shardId;
        private final String nodeAddress;

        public ShardInfo(int shardId, String nodeAddress)
        {
            this.shardId = shardId;
            this.nodeAddress = nodeAddress;
        }

        public int getShardId()
        {
            return shardId;
        }

        public String getNodeAddress()
        {
            return nodeAddress;
        }
    }
}
