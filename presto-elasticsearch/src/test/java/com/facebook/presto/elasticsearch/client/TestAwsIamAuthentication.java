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
package com.facebook.presto.elasticsearch.client;

import co.elastic.clients.transport.rest5_client.low_level.Response;
import co.elastic.clients.transport.rest5_client.low_level.Rest5Client;
import com.facebook.presto.elasticsearch.ElasticsearchServer;
import com.facebook.presto.sql.query.QueryAssertions;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.HttpRequest;
import org.apache.hc.core5.http.io.entity.ByteArrayEntity;
import org.apache.hc.core5.http.message.BasicHttpRequest;
import org.apache.hc.core5.http.protocol.BasicHttpContext;
import org.apache.hc.core5.http.protocol.HttpContext;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

import java.io.IOException;

import static com.facebook.presto.elasticsearch.ElasticsearchQueryRunner.createElasticsearchQueryRunner;
import static com.facebook.presto.elasticsearch.client.ElasticSearchClientUtils.performRequest;
import static org.apache.http.protocol.HttpCoreContext.HTTP_TARGET_HOST;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * Integration test to validate AWS IAM authentication with OpenSearch/Elasticsearch
 * using AWS SDK v2.
 */
public class TestAwsIamAuthentication
{
    private static final String AWS_REGION = "us-east-1";
    private static final String ACCESS_KEY = "test-access-key";
    private static final String SECRET_KEY = "test-secret-key";

    private final String elasticsearchImage = "docker.elastic.co/elasticsearch/elasticsearch:9.1.0";
    private ElasticsearchServer elasticsearch;
    private Rest5Client client;
    private QueryAssertions assertions;

    @BeforeClass
    public void setup()
            throws Exception
    {
        elasticsearch = new ElasticsearchServer(elasticsearchImage,
                ImmutableMap.<String, String>builder()
                        .put("elasticsearch.yml", getElasticsearchConfig())
                        .build(),
                ImmutableMap.of());

        HostAndPort address = elasticsearch.getAddress();
        client = Rest5Client.builder(new HttpHost(address.getHost(), address.getPort())).build();

        DistributedQueryRunner runner = createElasticsearchQueryRunner(
                elasticsearch.getAddress(),
                ImmutableList.of(),
                ImmutableMap.of(),
                ImmutableMap.<String, String>builder()
                        .put("elasticsearch.security", "AWS")
                        .put("elasticsearch.aws.region", AWS_REGION)
                        .put("elasticsearch.aws.access-key", ACCESS_KEY)
                        .put("elasticsearch.aws.secret-key", SECRET_KEY)
                        .build());

        assertions = new QueryAssertions(runner);
    }

    @AfterClass(alwaysRun = true)
    public void cleanup()
            throws IOException
    {
        if (assertions != null) {
            assertions.close();
        }
        if (client != null) {
            client.close();
        }
        if (elasticsearch != null) {
            elasticsearch.stop();
        }
    }

    @Test
    public void testAwsRequestSigning()
            throws Exception
    {
        AwsRequestSigner signer = new AwsRequestSigner(
                AWS_REGION,
                StaticCredentialsProvider.create(AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)));

        HttpRequest request = new BasicHttpRequest("GET", "/_cluster/health");
        HttpContext context = new BasicHttpContext();
        HostAndPort address = elasticsearch.getAddress();
        context.setAttribute(HTTP_TARGET_HOST, new HttpHost(address.getHost(), address.getPort()));

        signer.process(request, null, context);

        assertNotNull(request.getFirstHeader("Authorization"), "Authorization header should be present");
        assertNotNull(request.getFirstHeader("X-Amz-Date"), "X-Amz-Date header should be present");

        String authHeader = request.getFirstHeader("Authorization").getValue();
        assertTrue(authHeader.startsWith("AWS4-HMAC-SHA256"), "Authorization header should use AWS4-HMAC-SHA256");
        assertTrue(authHeader.contains("Credential=" + ACCESS_KEY), "Authorization header should contain access key");
        assertTrue(authHeader.contains("SignedHeaders="), "Authorization header should contain signed headers");
        assertTrue(authHeader.contains("Signature="), "Authorization header should contain signature");
    }

    @Test
    public void testIamAuthenticatedQuery()
            throws IOException
    {
        String json = new ObjectMapper().writeValueAsString(ImmutableMap.<String, Object>builder()
                .put("id", 1L)
                .put("name", "test-document")
                .put("value", 100L)
                .build());

        performRequest(
                "POST",
                "/test-iam/_doc/1?refresh",
                ImmutableMap.of(),
                new ByteArrayEntity(json.getBytes(), ContentType.APPLICATION_JSON),
                client);

        assertions.assertQuery("SELECT * FROM \"test-iam\"",
                "VALUES (BIGINT '1', CAST('test-document' AS varchar), BIGINT '100')");
    }

    @Test
    public void testIamClusterAccess()
            throws IOException
    {
        Response response = performRequest(
                "GET",
                "/_cluster/health",
                ImmutableMap.of(),
                null,
                client);

        assertEquals(response.getStatusCode(), 200, "Cluster health check should return 200 OK");
        assertNotNull(response.getEntity(), "Response should contain cluster health data");
    }

    @Test
    public void testIamIndexOperations()
            throws IOException
    {
        String indexName = "test-iam-operations";

        Response createResponse = performRequest(
                "PUT",
                "/" + indexName,
                ImmutableMap.of(),
                new ByteArrayEntity("{\"settings\":{\"number_of_shards\":1}}".getBytes(), ContentType.APPLICATION_JSON),
                client);
        assertEquals(createResponse.getStatusCode(), 200, "Index creation should return 200 OK");

        Response getResponse = performRequest(
                "GET",
                "/" + indexName,
                ImmutableMap.of(),
                null,
                client);
        assertEquals(getResponse.getStatusCode(), 200, "Index retrieval should return 200 OK");

        Response deleteResponse = performRequest(
                "DELETE",
                "/" + indexName,
                ImmutableMap.of(),
                null,
                client);
        assertEquals(deleteResponse.getStatusCode(), 200, "Index deletion should return 200 OK");
    }

    private String getElasticsearchConfig()
    {
        return "# Basic cluster identification\n" +
                "cluster.name: test-cluster\n" +
                "node.name: test-node\n" +
                "# Allow external connections for testing (binds to all network interfaces)\n" +
                "network.host: 0.0.0.0\n" +
                "# Single node setup for testing - no cluster discovery needed\n" +
                "discovery.type: single-node\n" +
                "# Disable X-Pack security features for simplified testing environment\n" +
                "xpack.security.enabled: false\n" +
                "# Disable monitoring to reduce overhead in test environment\n" +
                "xpack.monitoring.enabled: false\n" +
                "# Disable watcher (alerting) as it's not needed for tests\n" +
                "xpack.watcher.enabled: false\n" +
                "# Disable machine learning features to reduce resource usage\n" +
                "xpack.ml.enabled: false";
    }
}
