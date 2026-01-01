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
import com.facebook.airlift.log.Logging;
import com.facebook.presto.Session;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableMap;
import io.airlift.tpch.TpchTable;
import org.apache.http.HttpHost;
import org.opensearch.client.Request;
import org.opensearch.client.RestClient;
import org.testcontainers.containers.GenericContainer;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;

/**
 * Query runner for OpenSearch connector tests.
 * Sets up a DistributedQueryRunner with OpenSearch catalog configured.
 */
public final class OpenSearchQueryRunner
{
    private static final Logger log = Logger.get(OpenSearchQueryRunner.class);
    private static final String OPENSEARCH_CATALOG = "opensearch";
    private static final String OPENSEARCH_SCHEMA = "default";

    private OpenSearchQueryRunner()
    {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    /**
     * Create a DistributedQueryRunner with OpenSearch connector.
     *
     * @param opensearchContainer The OpenSearch testcontainer instance
     * @param tables TPCH tables to load into OpenSearch
     * @return Configured DistributedQueryRunner
     * @throws Exception if setup fails
     */
    public static DistributedQueryRunner createQueryRunner(
            GenericContainer<?> opensearchContainer,
            Iterable<TpchTable<?>> tables)
            throws Exception
    {
        return createQueryRunner(opensearchContainer, tables, ImmutableMap.of(), ImmutableMap.of());
    }

    /**
     * Create a DistributedQueryRunner with OpenSearch connector and custom properties.
     *
     * @param opensearchContainer The OpenSearch testcontainer instance
     * @param tables TPCH tables to load into OpenSearch
     * @param extraProperties Extra properties for the query runner
     * @param coordinatorProperties Coordinator-specific properties
     * @return Configured DistributedQueryRunner
     * @throws Exception if setup fails
     */
    public static DistributedQueryRunner createQueryRunner(
            GenericContainer<?> opensearchContainer,
            Iterable<TpchTable<?>> tables,
            Map<String, String> extraProperties,
            Map<String, String> coordinatorProperties)
            throws Exception
    {
        // Use OpenSearch catalog as default for inherited AbstractTestQueries tests
        Session session = testSessionBuilder()
                .setCatalog(OPENSEARCH_CATALOG)
                .setSchema(OPENSEARCH_SCHEMA)
                .build();

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .setExtraProperties(extraProperties)
                .setCoordinatorProperties(coordinatorProperties)
                .build();

        try {
            // Install TPCH plugin first (needed for copying data)
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch", ImmutableMap.of());

            log.info("TPCH catalog created successfully");

            // Install OpenSearch plugin
            queryRunner.installPlugin(new OpenSearchPlugin());

            // Get OpenSearch connection details from container
            String host = opensearchContainer.getHost();
            Integer port = opensearchContainer.getMappedPort(9200);
            String opensearchUrl = String.format("http://%s:%d", host, port);

            log.info("Connecting to OpenSearch at: %s", opensearchUrl);

            // Create main catalog with _id hidden for TPCH compatibility (inherited AbstractTestQueries tests)
            Map<String, String> catalogProperties = ImmutableMap.<String, String>builder()
                    .put("opensearch.host", host)
                    .put("opensearch.port", String.valueOf(port))
                    .put("opensearch.scheme", "http")
                    .put("opensearch.ssl.enabled", "false")
                    .put("opensearch.scroll.size", "1000")
                    .put("opensearch.scroll.timeout", "5m")
                    .put("opensearch.socket-timeout", "60000")
                    .put("opensearch.connection-timeout", "10000")
                    // Enable nested field support
                    .put("opensearch.nested.enabled", "true")
                    .put("opensearch.nested.max-depth", "5")
                    // Hide _id column for TPCH compatibility
                    .put("opensearch.hide-document-id-column", "true")
                    .build();

            queryRunner.createCatalog(OPENSEARCH_CATALOG, "opensearch", catalogProperties);

            log.info("OpenSearch catalog created successfully");

            // Create second catalog with _id visible for OpenSearch-specific tests
            Map<String, String> catalogWithIdProperties = ImmutableMap.<String, String>builder()
                    .put("opensearch.host", host)
                    .put("opensearch.port", String.valueOf(port))
                    .put("opensearch.scheme", "http")
                    .put("opensearch.ssl.enabled", "false")
                    .put("opensearch.scroll.size", "1000")
                    .put("opensearch.scroll.timeout", "5m")
                    .put("opensearch.socket-timeout", "60000")
                    .put("opensearch.connection-timeout", "10000")
                    // Enable nested field support
                    .put("opensearch.nested.enabled", "true")
                    .put("opensearch.nested.max-depth", "5")
                    // Show _id column for OpenSearch-specific tests
                    .put("opensearch.hide-document-id-column", "false")
                    .build();

            queryRunner.createCatalog("opensearch_with_id", "opensearch", catalogWithIdProperties);

            log.info("OpenSearch catalog with _id created successfully");

            // Load TPCH data into OpenSearch via REST API
            log.info("Loading TPCH tables into OpenSearch...");
            loadTpchDataIntoOpenSearch(queryRunner, opensearchContainer, tables);
            log.info("TPCH tables loaded successfully");

            return queryRunner;
        }
        catch (Exception e) {
            queryRunner.close();
            throw new RuntimeException("Failed to create OpenSearchQueryRunner", e);
        }
    }

    /**
     * Load TPCH data into OpenSearch indices by querying TPCH catalog and indexing via REST API.
     * Uses proper type mapping to ensure DATE fields are stored correctly.
     */
    private static void loadTpchDataIntoOpenSearch(
            DistributedQueryRunner queryRunner,
            GenericContainer<?> opensearchContainer,
            Iterable<TpchTable<?>> tables)
            throws Exception
    {
        String host = opensearchContainer.getHost();
        Integer port = opensearchContainer.getMappedPort(9200);

        try (RestClient restClient = RestClient.builder(new HttpHost(host, port, "http")).build()) {
            for (TpchTable<?> table : tables) {
                String tableName = table.getTableName();
                log.info("Loading TPCH table: %s", tableName);

                // Query TPCH data
                Session tpchSession = testSessionBuilder()
                        .setCatalog("tpch")
                        .setSchema(TINY_SCHEMA_NAME)
                        .build();

                // Get column metadata from TPCH
                String describeQuery = "SELECT column_name, data_type FROM information_schema.columns " +
                        "WHERE table_schema = '" + TINY_SCHEMA_NAME + "' AND table_name = '" + tableName + "' " +
                        "ORDER BY ordinal_position";
                MaterializedResult describeResult = queryRunner.execute(tpchSession, describeQuery);

                List<String> columnNames = new ArrayList<>();
                List<String> columnTypes = new ArrayList<>();
                for (MaterializedRow row : describeResult.getMaterializedRows()) {
                    columnNames.add((String) row.getField(0));
                    columnTypes.add((String) row.getField(1));
                }

                // Create index with explicit mapping based on TPCH schema
                String indexName = tableName.toLowerCase(Locale.ENGLISH);

                // Delete index if it exists
                try {
                    Request deleteIndex = new Request("DELETE", "/" + indexName);
                    restClient.performRequest(deleteIndex);
                    log.info("Deleted existing index %s", indexName);
                }
                catch (Exception e) {
                    log.debug("Index %s doesn't exist, will create new one", indexName);
                }

                // Build OpenSearch mapping with multiple shards for testing distributed queries
                StringBuilder mapping = new StringBuilder();
                mapping.append("{\"settings\":{\"number_of_shards\":5,\"number_of_replicas\":0,\"index.max_result_window\":100000},");
                mapping.append("\"mappings\":{\"properties\":{");

                for (int i = 0; i < columnNames.size(); i++) {
                    if (i > 0) {
                        mapping.append(",");
                    }
                    String columnName = columnNames.get(i);
                    String prestoType = columnTypes.get(i);

                    mapping.append("\"").append(columnName).append("\":{");

                    // Map Presto types to OpenSearch types
                    if (prestoType.equals("bigint")) {
                        mapping.append("\"type\":\"long\"");
                    }
                    else if (prestoType.equals("integer")) {
                        mapping.append("\"type\":\"integer\"");
                    }
                    else if (prestoType.equals("double")) {
                        mapping.append("\"type\":\"double\"");
                    }
                    else if (prestoType.equals("real")) {
                        mapping.append("\"type\":\"float\"");
                    }
                    else if (prestoType.equals("date")) {
                        // Map date type to OpenSearch date with epoch_millis format
                        mapping.append("\"type\":\"date\",\"format\":\"epoch_millis\"");
                    }
                    else if (prestoType.equals("timestamp")) {
                        mapping.append("\"type\":\"date\",\"format\":\"epoch_millis\"");
                    }
                    else if (prestoType.startsWith("varchar") || prestoType.startsWith("char")) {
                        mapping.append("\"type\":\"keyword\"");
                    }
                    else {
                        mapping.append("\"type\":\"keyword\"");
                    }

                    mapping.append("}");
                }

                mapping.append("}}}");

                // Create index
                Request createIndex = new Request("PUT", "/" + indexName);
                createIndex.setJsonEntity(mapping.toString());
                restClient.performRequest(createIndex);
                log.info("Created index %s with explicit mapping", indexName);

                // Store column order and type metadata to preserve field order and VARCHAR lengths
                // OpenSearch returns fields alphabetically and loses VARCHAR length info
                OpenSearchConfig clientConfig = new OpenSearchConfig()
                        .setHost(opensearchContainer.getHost())
                        .setPort(opensearchContainer.getMappedPort(9200))
                        .setScheme("http")
                        .setSslEnabled(false);
                OpenSearchClient openSearchClient = new OpenSearchClient(clientConfig);
                openSearchClient.storeColumnOrderMetadata(indexName, columnNames, columnTypes);
                openSearchClient.close();

                // Get data with types
                MaterializedResult result = queryRunner.execute(tpchSession, "SELECT * FROM " + tableName);
                List<Type> types = result.getTypes();

                // Bulk index documents
                StringBuilder bulkRequest = new StringBuilder();
                int docCount = 0;
                for (MaterializedRow row : result.getMaterializedRows()) {
                    // Index action
                    bulkRequest.append(String.format("{\"index\":{\"_index\":\"%s\"}}\n", indexName));

                    // Document
                    bulkRequest.append("{");
                    for (int i = 0; i < columnNames.size(); i++) {
                        if (i > 0) {
                            bulkRequest.append(",");
                        }
                        String columnName = columnNames.get(i);
                        Object value = row.getField(i);
                        Type type = types.get(i);

                        bulkRequest.append("\"").append(columnName).append("\":");
                        if (value == null) {
                            bulkRequest.append("null");
                        }
                        else if (type.equals(DATE)) {
                            // DATE type: value can be LocalDate or days since epoch
                            long millis;
                            if (value instanceof java.time.LocalDate) {
                                java.time.LocalDate localDate = (java.time.LocalDate) value;
                                millis = localDate.toEpochDay() * 24 * 60 * 60 * 1000;
                            }
                            else {
                                // Days since epoch
                                long days = ((Number) value).longValue();
                                millis = days * 24 * 60 * 60 * 1000;
                            }
                            bulkRequest.append(millis);
                        }
                        else if (type.equals(TIMESTAMP)) {
                            // TIMESTAMP type: value is milliseconds since epoch or Instant
                            long millis;
                            if (value instanceof java.time.Instant) {
                                millis = ((java.time.Instant) value).toEpochMilli();
                            }
                            else {
                                millis = ((Number) value).longValue();
                            }
                            bulkRequest.append(millis);
                        }
                        else if (value instanceof String) {
                            bulkRequest.append("\"").append(value.toString().replace("\"", "\\\"")).append("\"");
                        }
                        else if (value instanceof Number || value instanceof Boolean) {
                            bulkRequest.append(value);
                        }
                        else {
                            bulkRequest.append("\"").append(value.toString().replace("\"", "\\\"")).append("\"");
                        }
                    }
                    bulkRequest.append("}\n");
                    docCount++;

                    // Send bulk request every 1000 documents
                    if (docCount % 1000 == 0) {
                        Request bulkReq = new Request("POST", "/_bulk");
                        bulkReq.setJsonEntity(bulkRequest.toString());
                        bulkReq.addParameter("refresh", "true");
                        restClient.performRequest(bulkReq);
                        bulkRequest = new StringBuilder();
                    }
                }

                // Send remaining documents
                if (bulkRequest.length() > 0) {
                    Request bulkReq = new Request("POST", "/_bulk");
                    bulkReq.setJsonEntity(bulkRequest.toString());
                    bulkReq.addParameter("refresh", "true");
                    restClient.performRequest(bulkReq);
                }

                log.info("Loaded %d documents into %s", docCount, indexName);
            }
        }
    }

    /**
     * Main method for manual testing.
     * Starts an OpenSearch container and query runner for interactive testing.
     */
    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();

        // Start OpenSearch container
        GenericContainer<?> opensearch = new GenericContainer<>(
                org.testcontainers.utility.DockerImageName.parse("opensearchproject/opensearch:2.11.1"))
                .withExposedPorts(9200, 9600)
                .withEnv("discovery.type", "single-node")
                .withEnv("OPENSEARCH_JAVA_OPTS", "-Xms512m -Xmx512m")
                .withEnv("DISABLE_SECURITY_PLUGIN", "true");

        opensearch.start();

        log.info("OpenSearch container started at: http://%s:%d",
                opensearch.getHost(),
                opensearch.getMappedPort(9200));

        // Create query runner with all TPCH tables
        DistributedQueryRunner queryRunner = createQueryRunner(
                opensearch,
                TpchTable.getTables(),
                ImmutableMap.of("http-server.http.port", "8080"),
                ImmutableMap.of());

        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());

        // Keep running until interrupted
        Thread.currentThread().join();
    }
}
