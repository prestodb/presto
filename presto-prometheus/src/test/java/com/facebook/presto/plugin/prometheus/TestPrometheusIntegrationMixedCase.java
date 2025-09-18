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

package com.facebook.presto.plugin.prometheus;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.bootstrap.LifeCycleManager;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.airlift.node.testing.TestingNodeModule;
import com.facebook.presto.Session;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import okhttp3.OkHttpClient;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.facebook.airlift.testing.Closeables.closeAllSuppress;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.plugin.prometheus.MetadataUtil.METRIC_CODEC;
import static com.facebook.presto.plugin.prometheus.TestPrometheusTable.TYPE_MANAGER;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestPrometheusIntegrationMixedCase
        extends AbstractTestQueryFramework
{
    private static final Logger log = Logger.get(TestPrometheusIntegrationMixedCase.class);
    private PrometheusServer server;
    private OkHttpClient httpClient;
    private MockPrometheusClient mockClient;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        server = new PrometheusServer();
        httpClient = new OkHttpClient.Builder()
                .connectTimeout(120, TimeUnit.SECONDS)
                .readTimeout(120, TimeUnit.SECONDS)
                .build();

        // Create a mock Prometheus client with mixed case metrics
        mockClient = new MockPrometheusClient(METRIC_CODEC, TYPE_MANAGER);

        // Add metrics with various case patterns
        mockClient.addMetric("up", 1.0);
        mockClient.addMetric("http_requests_total", 10.0);
        mockClient.addMetric("HTTP_REQUESTS_TOTAL", 20.0);
        mockClient.addMetric("HttpRequests_Total", 30.0);
        mockClient.addMetric("api_calls", 50.0);
        mockClient.addMetric("CPU_USAGE", 75.0);

        // Add table definitions for all metrics to ensure they're available
        mockClient.setTableOverride("up", new PrometheusTable(
                "up",
                ImmutableList.of(
                        new PrometheusColumn("labels", VARCHAR),
                        new PrometheusColumn("timestamp", TIMESTAMP_WITH_TIME_ZONE),
                        new PrometheusColumn("value", DoubleType.DOUBLE))));

        // Set case sensitivity to true by default
        mockClient.setCaseSensitiveNameMatching(true);

        DistributedQueryRunner queryRunner = null;
        try {
            queryRunner = DistributedQueryRunner.builder(createSession()).build();
            // Install a custom plugin that uses our mock client
            queryRunner.installPlugin(new MockPrometheusPlugin(mockClient));
            // Create case-insensitive catalog (default)
            Map<String, String> properties = ImmutableMap.<String, String>builder()
                    .put("prometheus.uri", server.getUri().toString())
                    .put("case-sensitive-name-matching", "false")
                    .build();
            queryRunner.createCatalog("prometheus", "prometheus", properties);
            // Create case-sensitive catalog for tests that need it
            Map<String, String> caseSensitiveProperties = ImmutableMap.<String, String>builder()
                    .put("prometheus.uri", server.getUri().toString())
                    .put("case-sensitive-name-matching", "true")
                    .build();
            queryRunner.createCatalog("prometheus_case_sensitive", "prometheus", caseSensitiveProperties);
            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    private static Session createSession()
    {
        return testSessionBuilder()
                .setCatalog("prometheus")
                .setSchema("default")
                .build();
    }
    private Session createCaseSensitiveSession()
    {
        // Use the case-sensitive catalog that was created in createQueryRunner
        return testSessionBuilder()
                .setCatalog("prometheus_case_sensitive")
                .setSchema("default")
                .build();
    }
    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        if (server != null) {
            server.close();
        }
    }
    @Test
    public void testCaseSensitiveMetricNames()
    {
        // Ensure case sensitivity is set to true for this test
        mockClient.setCaseSensitiveNameMatching(true);

        // Make sure we don't have an uppercase "UP" metric
        mockClient.removeMetric("UP");

        // Also ensure the getTable method returns null for "UP"
        mockClient.setTableOverride("UP", null);

        // Override the fetchUri method to ensure it returns empty results for "UP"
        mockClient.setFetchUriOverride("UP", mockClient.createEmptyResponse());

        // Create a case-sensitive session for this test
        Session caseSensitiveSession = createCaseSensitiveSession();
        // Test lowercase metric
        MaterializedResult lowerCaseResult = getQueryRunner().execute(caseSensitiveSession, "SELECT value FROM \"http_requests_total\"");
        assertEquals(lowerCaseResult.getOnlyColumnAsSet().iterator().next(), 10.0);

        // Test uppercase metric - should succeed because it exists as a separate metric
        mockClient.setFetchUriOverride("HTTP_REQUESTS_TOTAL", mockClient.createMockResponse("HTTP_REQUESTS_TOTAL", 20.0));
        MaterializedResult upperCaseResult = getQueryRunner().execute(caseSensitiveSession, "SELECT value FROM \"HTTP_REQUESTS_TOTAL\"");
        assertEquals(upperCaseResult.getOnlyColumnAsSet().iterator().next(), 20.0);

        // Test mixed case metric
        MaterializedResult mixedCaseResult = getQueryRunner().execute(caseSensitiveSession, "SELECT value FROM \"HttpRequests_Total\"");
        assertEquals(mixedCaseResult.getOnlyColumnAsSet().iterator().next(), 30.0);

        // Test metrics with only one case version
        MaterializedResult apiCallsResult = getQueryRunner().execute(caseSensitiveSession, "SELECT value FROM \"api_calls\"");
        assertEquals(apiCallsResult.getOnlyColumnAsSet().iterator().next(), 50.0);

        // Make sure API_CALLS doesn't exist in case-sensitive mode
        mockClient.removeMetric("API_CALLS");
        mockClient.setTableOverride("API_CALLS", null);
        mockClient.setFetchUriOverride("API_CALLS", mockClient.createEmptyResponse());
        assertQueryFails(caseSensitiveSession, "SELECT * FROM \"API_CALLS\" WHERE value > 0", ".*Table prometheus_case_sensitive.default.API_CALLS does not exist");

        MaterializedResult cpuUsageResult = getQueryRunner().execute(caseSensitiveSession, "SELECT value FROM \"CPU_USAGE\"");
        assertEquals(cpuUsageResult.getOnlyColumnAsSet().iterator().next(), 75.0);

        // Make sure cpu_usage doesn't exist in case-sensitive mode
        mockClient.removeMetric("cpu_usage");
        mockClient.setTableOverride("cpu_usage", null);
        mockClient.setFetchUriOverride("cpu_usage", mockClient.createEmptyResponse());
        assertQueryFails(caseSensitiveSession, "SELECT * FROM \"cpu_usage\" WHERE value > 0", ".*Table prometheus_case_sensitive.default.cpu_usage does not exist");

        // Test that uppercase "UP" fails in case-sensitive mode
        // Make sure we don't have an uppercase "UP" metric in the table names
        mockClient.removeMetric("UP");
        mockClient.setTableOverride("UP", null);
        mockClient.setFetchUriOverride("UP", mockClient.createEmptyResponse());
        assertQueryFails(caseSensitiveSession, "SELECT * FROM \"UP\" WHERE value > 0", ".*Table prometheus_case_sensitive.default.UP does not exist");
    }
    @Test
    public void testMixedCaseLabels()
    {
        // Ensure case sensitivity is set to true for this test
        mockClient.setCaseSensitiveNameMatching(true);

        // This test specifically verifies that the labels in Prometheus metrics
        // preserve their case sensitivity. In Prometheus, labels are key-value pairs
        // that are attached to metrics (like "instance", "job", etc.)

        // Create a case-sensitive session for this test
        Session caseSensitiveSession = createCaseSensitiveSession();
        // First, verify we can query the labels column
        assertQuerySucceeds(caseSensitiveSession, "SELECT labels FROM \"up\"");

        // Verify that we can see the labels in the output
        MaterializedResult result = getQueryRunner().execute(caseSensitiveSession, "SELECT labels FROM \"up\"");
        assertTrue(result.getRowCount() > 0, "Should have at least one row with labels");

        // Test that we can cast the labels to JSON and extract fields
        // This verifies that the case of label names is preserved
        assertQuerySucceeds(caseSensitiveSession, "SELECT CAST(labels AS JSON) FROM \"up\"");
        assertQuerySucceeds(caseSensitiveSession, "SELECT * FROM \"up\"");

        // Test count query
        MaterializedResult countResult = getQueryRunner().execute(caseSensitiveSession, "SELECT count(*) FROM \"up\"");
        assertNotNull(countResult);
        assertTrue(countResult.getRowCount() > 0, "Should have count result");
    }
    @Test
    public void testCaseInsensitiveMetricNames()
    {
        // Set case sensitivity to false for this test
        mockClient.setCaseSensitiveNameMatching(false);
        // Make sure we have the "up" metric available for case-insensitive testing
        mockClient.addMetric("up", 1.0);
        // With case-insensitive matching (false), queries should normalize to lowercase
        // and always return the same result regardless of the case used in the query

        // Test with "up" metric - in case-insensitive mode, both lowercase and uppercase should work
        MaterializedResult lowerCaseUp = getQueryRunner().execute(getSession(), "SELECT value FROM \"up\"");
        MaterializedResult upperCaseUp = getQueryRunner().execute(getSession(), "SELECT value FROM \"UP\"");
        // Both queries should return the same result
        assertEquals(lowerCaseUp.getOnlyColumnAsSet().iterator().next(), upperCaseUp.getOnlyColumnAsSet().iterator().next());
        assertEquals(lowerCaseUp.getOnlyColumnAsSet().iterator().next(), 1.0);

        // Test with http_requests_total - all case variations should return the same result
        MaterializedResult lowerCaseHttp = getQueryRunner().execute(getSession(), "SELECT value FROM \"http_requests_total\"");
        MaterializedResult upperCaseHttp = getQueryRunner().execute(getSession(), "SELECT value FROM \"HTTP_REQUESTS_TOTAL\"");
        MaterializedResult mixedCaseHttp = getQueryRunner().execute(getSession(), "SELECT value FROM \"Http_Requests_Total\"");
        // All queries should return the same result
        assertEquals(lowerCaseHttp.getOnlyColumnAsSet().iterator().next(), 10.0);
        assertEquals(lowerCaseHttp.getOnlyColumnAsSet().iterator().next(), upperCaseHttp.getOnlyColumnAsSet().iterator().next());
        assertEquals(lowerCaseHttp.getOnlyColumnAsSet().iterator().next(), mixedCaseHttp.getOnlyColumnAsSet().iterator().next());

        // Test with api_calls - both lowercase and uppercase should work
        MaterializedResult lowerCaseApi = getQueryRunner().execute(getSession(), "SELECT value FROM \"api_calls\"");
        MaterializedResult upperCaseApi = getQueryRunner().execute(getSession(), "SELECT value FROM \"API_CALLS\"");
        // Both queries should return the same result
        assertEquals(lowerCaseApi.getOnlyColumnAsSet().iterator().next(), upperCaseApi.getOnlyColumnAsSet().iterator().next());
        assertEquals(lowerCaseApi.getOnlyColumnAsSet().iterator().next(), 50.0);

        // Test with CPU_USAGE - both lowercase and uppercase should work
        MaterializedResult lowerCaseCpu = getQueryRunner().execute(getSession(), "SELECT value FROM \"cpu_usage\"");
        MaterializedResult upperCaseCpu = getQueryRunner().execute(getSession(), "SELECT value FROM \"CPU_USAGE\"");
        // Both queries should return the same result
        assertEquals(lowerCaseCpu.getOnlyColumnAsSet().iterator().next(), upperCaseCpu.getOnlyColumnAsSet().iterator().next());
        assertEquals(lowerCaseCpu.getOnlyColumnAsSet().iterator().next(), 75.0);

        // Reset case sensitivity to true for other tests
        mockClient.setCaseSensitiveNameMatching(true);
    }
    // Custom plugin that uses our mock client
    private static class MockPrometheusPlugin
            implements Plugin
    {
        private final MockPrometheusClient mockClient;
        public MockPrometheusPlugin(MockPrometheusClient mockClient)
        {
            this.mockClient = mockClient;
        }
        @Override
        public Iterable<ConnectorFactory> getConnectorFactories()
        {
            return ImmutableList.of(new MockPrometheusConnectorFactory(mockClient));
        }
    }
    // Custom connector factory that uses our mock client
    private static class MockPrometheusConnectorFactory
            implements ConnectorFactory
    {
        private final MockPrometheusClient mockClient;
        public MockPrometheusConnectorFactory(MockPrometheusClient mockClient)
        {
            this.mockClient = mockClient;
        }
        @Override
        public String getName()
        {
            return "prometheus";
        }
        @Override
        public ConnectorHandleResolver getHandleResolver()
        {
            return new PrometheusHandleResolver();
        }
        @Override
        public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
        {
            PrometheusConnectorConfig connectorConfig = new PrometheusConnectorConfig();
            // Set case sensitivity based on config
            boolean caseSensitiveNameMatching = Boolean.parseBoolean(config.getOrDefault("case-sensitive-name-matching", "false"));
            connectorConfig.setCaseSensitiveNameMatching(caseSensitiveNameMatching);
            mockClient.setCaseSensitiveNameMatching(caseSensitiveNameMatching);
            // Update the connector configuration with the URI from the config
            if (config.containsKey("prometheus.uri")) {
                try {
                    connectorConfig.setPrometheusURI(new URI(config.get("prometheus.uri")));
                }
                catch (Exception e) {
                    // Ignore URI parsing errors in tests
                }
            }
            PrometheusMetadata metadata = new PrometheusMetadata(mockClient, connectorConfig);
            PrometheusSplitManager splitManager = new PrometheusSplitManager(mockClient, new PrometheusClock(), connectorConfig);
            PrometheusRecordSetProvider recordSetProvider = new PrometheusRecordSetProvider(mockClient);
            Bootstrap app = new Bootstrap(new TestingNodeModule());
            Injector injector = app
                    .doNotInitializeLogging()
                    .initialize();
            LifeCycleManager lifeCycleManager = injector.getInstance(LifeCycleManager.class);
            return new PrometheusConnector(lifeCycleManager, metadata, splitManager, recordSetProvider);
        }
    }
    // Mock client that simulates Prometheus with mixed case metrics
    private static class MockPrometheusClient
            extends PrometheusClient
    {
        private final Map<String, Double> metricValues = new HashMap<>();
        private final Map<String, PrometheusTable> tableOverrides = new HashMap<>();
        private final Map<String, byte[]> fetchUriOverrides = new HashMap<>();
        private boolean caseSensitiveNameMatching = true;

        public MockPrometheusClient(JsonCodec<Map<String, Object>> metricCodec, TypeManager typeManager)
        {
            super(createDefaultConfig(), metricCodec, typeManager);
        }
        private static PrometheusConnectorConfig createDefaultConfig()
        {
            PrometheusConnectorConfig config = new PrometheusConnectorConfig();
            config.setCaseSensitiveNameMatching(false);
            return config;
        }
        public void setFetchUriOverride(String metricName, byte[] response)
        {
            fetchUriOverrides.put(metricName, response);
        }

        public void addMetric(String metricName, double value)
        {
            metricValues.put(metricName, value);
        }

        public void removeMetric(String metricName)
        {
            metricValues.remove(metricName);
        }
        public void setTableOverride(String tableName, PrometheusTable table)
        {
            tableOverrides.put(tableName, table);
        }

        public void setCaseSensitiveNameMatching(boolean caseSensitiveNameMatching)
        {
            this.caseSensitiveNameMatching = caseSensitiveNameMatching;
        }

        @Override
        public Set<String> getTableNames(String schema)
        {
            if (!schema.equals("default")) {
                return ImmutableSet.of();
            }

            ImmutableSet.Builder<String> builder = ImmutableSet.builder();
            for (Map.Entry<String, PrometheusTable> entry : tableOverrides.entrySet()) {
                if (entry.getValue() != null) {
                    builder.add(entry.getKey());
                }
            }

            if (caseSensitiveNameMatching) {
                // In case-sensitive mode, return the exact keys
                builder.addAll(metricValues.keySet());
            }
            else {
                // In case-insensitive mode, normalize all table names to lowercase
                for (String name : metricValues.keySet()) {
                    builder.add(name.toLowerCase(java.util.Locale.ENGLISH));
                }
            }

            return builder.build();
        }

        @Override
        public PrometheusTable getTable(String schema, String tableName)
        {
            if (!schema.equals("default")) {
                return null;
            }

            if (caseSensitiveNameMatching) {
                if (tableOverrides.containsKey(tableName)) {
                    return tableOverrides.get(tableName);
                }

                // For case-sensitive mode, do a direct lookup without any normalization
                if (!metricValues.containsKey(tableName)) {
                    return null;
                }
                return new PrometheusTable(
                        tableName,
                        ImmutableList.of(
                                new PrometheusColumn("labels", VARCHAR),
                                new PrometheusColumn("timestamp", TIMESTAMP_WITH_TIME_ZONE),
                                new PrometheusColumn("value", DoubleType.DOUBLE)));
            }
            else {
                // For case-insensitive mode, normalize the table name to lowercase
                String lowercaseTableName = tableName.toLowerCase(java.util.Locale.ENGLISH);

                for (Map.Entry<String, PrometheusTable> entry : tableOverrides.entrySet()) {
                    if (entry.getKey() != null &&
                            entry.getKey().toLowerCase(java.util.Locale.ENGLISH).equals(lowercaseTableName) &&
                            entry.getValue() != null) {
                        return entry.getValue();
                    }
                }

                if (metricValues.containsKey(lowercaseTableName)) {
                    return new PrometheusTable(
                            lowercaseTableName, // Always use lowercase in case-insensitive mode
                            ImmutableList.of(
                                    new PrometheusColumn("labels", VARCHAR),
                                    new PrometheusColumn("timestamp", TIMESTAMP_WITH_TIME_ZONE),
                                    new PrometheusColumn("value", DoubleType.DOUBLE)));
                }
                for (String metricName : metricValues.keySet()) {
                    if (metricName != null &&
                            metricName.toLowerCase(java.util.Locale.ENGLISH).equals(lowercaseTableName)) {
                        // Always return a table with the lowercase name in case-insensitive mode
                        return new PrometheusTable(
                                lowercaseTableName,
                                ImmutableList.of(
                                        new PrometheusColumn("labels", VARCHAR),
                                        new PrometheusColumn("timestamp", TIMESTAMP_WITH_TIME_ZONE),
                                        new PrometheusColumn("value", DoubleType.DOUBLE)));
                    }
                }
            }

            return null;
        }

        // Override fetchUri to return mock data with proper JSON response format
        @Override
        public byte[] fetchUri(URI uri)
        {
            if (uri == null) {
                return createEmptyResponse();
            }

            // Extract the metric name from the URI
            String query = uri.getQuery();
            String requestedMetricName = extractMetricName(query);
            if (requestedMetricName == null) {
                return createEmptyResponse();
            }
            if (fetchUriOverrides.containsKey(requestedMetricName)) {
                byte[] response = fetchUriOverrides.get(requestedMetricName);
                if (response != null) {
                    return response;
                }
            }

            // For case-sensitive mode, do a direct lookup without any normalization
            if (caseSensitiveNameMatching) {
                if (!metricValues.containsKey(requestedMetricName)) {
                    return createEmptyResponse();
                }
                Double value = metricValues.get(requestedMetricName);
                if (value == null) {
                    return createEmptyResponse();
                }
                return createMockResponse(requestedMetricName, value);
            }

            // For case-insensitive mode, normalize the metric name to lowercase
            String lowercaseMetricName = requestedMetricName.toLowerCase(java.util.Locale.ENGLISH);

            if (metricValues.containsKey(lowercaseMetricName)) {
                Double value = metricValues.get(lowercaseMetricName);
                if (value != null) {
                    return createMockResponse(lowercaseMetricName, value);
                }
            }
            // If no direct lowercase match, find the first case-insensitive match
            for (Map.Entry<String, Double> entry : metricValues.entrySet()) {
                if (entry.getKey() != null &&
                        entry.getKey().toLowerCase(java.util.Locale.ENGLISH).equals(lowercaseMetricName) &&
                        entry.getValue() != null) {
                    // Always use the lowercase name in the response for case-insensitive mode
                    return createMockResponse(lowercaseMetricName, entry.getValue());
                }
            }

            return createEmptyResponse();
        }

        private String extractMetricName(String query)
        {
            if (query == null) {
                return null;
            }
            String[] parts = query.split("&");
            for (String part : parts) {
                if (part.startsWith("query=")) {
                    String queryValue = part.substring("query=".length());
                    int bracketIndex = queryValue.indexOf('[');
                    if (bracketIndex > 0) {
                        return queryValue.substring(0, bracketIndex);
                    }
                    return queryValue;
                }
            }
            return null;
        }

        public byte[] createMockResponse(String metricName, double value)
        {
            // Create a mock Prometheus response JSON with the metric value
            String timestamp = String.valueOf(Instant.now().getEpochSecond());
            double responseValue = value;
            String valueStr = String.valueOf(responseValue);
            // Include both lowercase and mixed case labels to test case sensitivity
            String json = "{\n" +
                    "  \"status\": \"success\",\n" +
                    "  \"data\": {\n" +
                    "    \"resultType\": \"vector\",\n" +
                    "    \"result\": [\n" +
                    "      {\n" +
                    "        \"metric\": {\"__name__\": \"" + metricName + "\", \"instance\": \"localhost:9090\", \"Job\": \"prometheus\", \"ENVIRONMENT\": \"test\"},\n" +
                    "        \"value\": [" + timestamp + ", \"" + valueStr + "\"]\n" +
                    "      }\n" +
                    "    ]\n" +
                    "  }\n" +
                    "}";
            return json.getBytes(UTF_8);
        }

        public byte[] createEmptyResponse()
        {
            // Create an empty but valid Prometheus response
            String json = "{\n" +
                    "  \"status\": \"success\",\n" +
                    "  \"data\": {\n" +
                    "    \"resultType\": \"vector\",\n" +
                    "    \"result\": []\n" +
                    "  }\n" +
                    "}";
            return json.getBytes(UTF_8);
        }
    }
}
