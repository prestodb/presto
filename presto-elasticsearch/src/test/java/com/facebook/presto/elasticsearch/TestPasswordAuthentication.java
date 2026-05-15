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
package com.facebook.presto.elasticsearch;

import co.elastic.clients.transport.rest5_client.low_level.Request;
import co.elastic.clients.transport.rest5_client.low_level.RequestOptions;
import co.elastic.clients.transport.rest5_client.low_level.Rest5Client;
import com.amazonaws.util.Base64;
import com.facebook.presto.sql.query.QueryAssertions;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;
import com.google.common.net.HostAndPort;
import org.apache.hc.core5.http.HttpHost;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static com.facebook.presto.elasticsearch.ElasticsearchQueryRunner.createElasticsearchQueryRunner;
import static com.google.common.io.Resources.getResource;
import static java.nio.charset.StandardCharsets.UTF_8;

public class TestPasswordAuthentication
{
    private final String elasticsearchImage = "docker.elastic.co/elasticsearch/elasticsearch:9.1.0";
    private static final String USER = "elastic_user";
    private static final String PASSWORD = "123456";

    private final ElasticsearchServer elasticsearch;
    private final Rest5Client client;
    private final QueryAssertions assertions;

    public TestPasswordAuthentication()
            throws Exception
    {
        elasticsearch = new ElasticsearchServer(elasticsearchImage, ImmutableMap.<String, String>builder()
                .put("elasticsearch.yml", loadResource("elasticsearch.yml"))
                .put("users", loadResource("users"))
                .put("users_roles", loadResource("users_roles"))
                .put("roles.yml", loadResource("roles.yml"))
                .build(),
                ImmutableMap.of());

        HostAndPort address = elasticsearch.getAddress();
        client = Rest5Client.builder(new HttpHost(address.getHost(), address.getPort())).build();

        DistributedQueryRunner runner = createElasticsearchQueryRunner(
                elasticsearch.getAddress(),
                ImmutableList.of(),
                ImmutableMap.of(),
                ImmutableMap.<String, String>builder()
                        .put("elasticsearch.security", "PASSWORD")
                        .put("elasticsearch.auth.user", USER)
                        .put("elasticsearch.auth.password", PASSWORD)
                        .build());

        assertions = new QueryAssertions(runner);
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
            throws IOException
    {
        assertions.close();
        elasticsearch.stop();
        client.close();
    }

    @Test
    public void test()
            throws IOException
    {
        String json = new ObjectMapper().writeValueAsString(Map.of("value", 42L));

        Request request = new Request("POST", "/test/_doc");
        request.setJsonEntity(json);
        request.addParameter("refresh", "true");

        String auth = Base64.encodeAsString((USER + ":" + PASSWORD).getBytes(StandardCharsets.UTF_8));
        request.setOptions(RequestOptions.DEFAULT.toBuilder()
                .addHeader("Authorization", "Basic " + auth)
                .build());

        client.performRequest(request);

        assertions.assertQuery("SELECT * FROM test", "VALUES BIGINT '42'");
    }

    private static String loadResource(String file)
            throws IOException
    {
        return Resources.toString(getResource(file), UTF_8);
    }
}
