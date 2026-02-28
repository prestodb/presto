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
import org.apache.http.HttpHost;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.TrustAllStrategy;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.ssl.SSLContextBuilder;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.net.ssl.SSLContext;

import java.io.IOException;
import java.time.Duration;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;

/**
 * Integration tests for SSL configuration in OpenSearch connector.
 * Tests connection to actual OpenSearch instances with and without SSL.
 */
public class TestOpenSearchSSLConnection
{
    private static final Logger log = Logger.get(TestOpenSearchSSLConnection.class);
    private static final String OPENSEARCH_IMAGE = "opensearchproject/opensearch:2.11.1";

    private static GenericContainer<?> opensearchContainerWithSSL;
    private static GenericContainer<?> opensearchContainerWithoutSSL;
    private static RestClient restClientSSL;
    private static RestClient restClientNoSSL;

    @BeforeClass
    public void setup()
            throws Exception
    {
        // Start OpenSearch container WITH SSL (uses self-signed certificate)
        opensearchContainerWithSSL = new GenericContainer<>(DockerImageName.parse(OPENSEARCH_IMAGE))
                .withExposedPorts(9200, 9600)
                .withEnv("discovery.type", "single-node")
                .withEnv("OPENSEARCH_JAVA_OPTS", "-Xms512m -Xmx512m")
                // Enable security plugin with self-signed certificate
                .withEnv("DISABLE_INSTALL_DEMO_CONFIG", "false")
                .waitingFor(Wait.forLogMessage(".*Node started.*", 1)
                        .withStartupTimeout(Duration.ofMinutes(3)));

        opensearchContainerWithSSL.start();

        log.info("OpenSearch container with SSL started at: https://%s:%d",
                opensearchContainerWithSSL.getHost(),
                opensearchContainerWithSSL.getMappedPort(9200));

        // Create REST client for SSL container (with certificate validation disabled for testing)
        SSLContext sslContext = SSLContextBuilder.create()
                .loadTrustMaterial(new TrustAllStrategy())
                .build();

        restClientSSL = RestClient.builder(
                        new HttpHost(
                                opensearchContainerWithSSL.getHost(),
                                opensearchContainerWithSSL.getMappedPort(9200),
                                "https"))
                .setHttpClientConfigCallback((HttpAsyncClientBuilder httpClientBuilder) ->
                        httpClientBuilder
                                .setSSLContext(sslContext)
                                .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE))
                .build();

        // Start OpenSearch container WITHOUT SSL for comparison
        opensearchContainerWithoutSSL = new GenericContainer<>(DockerImageName.parse(OPENSEARCH_IMAGE))
                .withExposedPorts(9200, 9600)
                .withEnv("discovery.type", "single-node")
                .withEnv("OPENSEARCH_JAVA_OPTS", "-Xms512m -Xmx512m")
                .withEnv("DISABLE_SECURITY_PLUGIN", "true")
                .waitingFor(Wait.forLogMessage(".*Node started.*", 1)
                        .withStartupTimeout(Duration.ofMinutes(3)));

        opensearchContainerWithoutSSL.start();

        log.info("OpenSearch container without SSL started at: http://%s:%d",
                opensearchContainerWithoutSSL.getHost(),
                opensearchContainerWithoutSSL.getMappedPort(9200));

        restClientNoSSL = RestClient.builder(
                        new HttpHost(
                                opensearchContainerWithoutSSL.getHost(),
                                opensearchContainerWithoutSSL.getMappedPort(9200),
                                "http"))
                .build();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws IOException
    {
        if (restClientSSL != null) {
            restClientSSL.close();
        }
        if (restClientNoSSL != null) {
            restClientNoSSL.close();
        }
        if (opensearchContainerWithSSL != null) {
            opensearchContainerWithSSL.stop();
        }
        if (opensearchContainerWithoutSSL != null) {
            opensearchContainerWithoutSSL.stop();
        }
    }

    @Test
    public void testConnectionToOpenSearchWithoutSSL()
            throws IOException
    {
        // Configure client to connect to non-SSL container
        OpenSearchConfig config = new OpenSearchConfig();
        config.setHost(opensearchContainerWithoutSSL.getHost());
        config.setPort(opensearchContainerWithoutSSL.getMappedPort(9200));
        config.setSslEnabled(false);

        // Create client and test connection
        OpenSearchClient client = new OpenSearchClient(config);
        List<String> indices = client.listIndices();

        assertNotNull(indices, "Should be able to list indices");
        log.info("Successfully connected to OpenSearch without SSL. Found %d indices", indices.size());
    }

    @Test
    public void testConnectionToSSLOpenSearchWithoutSkipValidation()
    {
        // Configure client to connect to SSL container WITHOUT skip validation
        // This should FAIL because the certificate is self-signed
        OpenSearchConfig config = new OpenSearchConfig();
        config.setHost(opensearchContainerWithSSL.getHost());
        config.setPort(opensearchContainerWithSSL.getMappedPort(9200));
        config.setSslEnabled(true);
        config.setSslSkipCertificateValidation(false);
        config.setUsername("admin");
        config.setPassword("admin");

        try {
            OpenSearchClient client = new OpenSearchClient(config);
            client.listIndices();
            fail("Should have failed with certificate validation error");
        }
        catch (Exception e) {
            // Expected - certificate validation should fail
            log.info("Expected failure with certificate validation: %s", e.getMessage());
            // The error message might be wrapped, so we just verify an exception was thrown
            assertNotNull(e, "Should have thrown an exception for invalid certificate");
        }
    }

    @Test
    public void testConnectionToSSLOpenSearchWithSkipValidation()
            throws IOException
    {
        // Configure client to connect to SSL container WITH skip validation
        // This should SUCCEED even with self-signed certificate
        OpenSearchConfig config = new OpenSearchConfig();
        config.setHost(opensearchContainerWithSSL.getHost());
        config.setPort(opensearchContainerWithSSL.getMappedPort(9200));
        config.setSslEnabled(true);
        config.setSslSkipCertificateValidation(true);
        config.setUsername("admin");
        config.setPassword("admin");

        // Create client and test connection - should work with skip validation
        OpenSearchClient client = new OpenSearchClient(config);
        List<String> indices = client.listIndices();

        assertNotNull(indices, "Should be able to list indices with skip validation enabled");
        log.info("Successfully connected to SSL OpenSearch with skip certificate validation. Found %d indices", indices.size());
    }

    @Test
    public void testClusterHealthCheckSSL()
            throws IOException
    {
        // Verify SSL OpenSearch cluster is healthy
        Request request = new Request("GET", "/_cluster/health");
        request.setOptions(request.getOptions().toBuilder()
                .addHeader("Authorization", "Basic YWRtaW46YWRtaW4="));  // admin:admin
        Response response = restClientSSL.performRequest(request);

        assertEquals(response.getStatusLine().getStatusCode(), 200, "SSL cluster health check should succeed");
        log.info("OpenSearch SSL cluster is healthy");
    }

    @Test
    public void testClusterHealthCheckNoSSL()
            throws IOException
    {
        // Verify non-SSL OpenSearch cluster is healthy
        Request request = new Request("GET", "/_cluster/health");
        Response response = restClientNoSSL.performRequest(request);

        assertEquals(response.getStatusLine().getStatusCode(), 200, "Non-SSL cluster health check should succeed");
        log.info("OpenSearch non-SSL cluster is healthy");
    }
}

// Made with Bob
