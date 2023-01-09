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

import com.amazonaws.util.Base64;
import com.facebook.presto.sql.query.QueryAssertions;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;
import com.google.common.net.HostAndPort;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.message.BasicHeader;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static com.facebook.presto.elasticsearch.ElasticsearchQueryRunner.createElasticsearchQueryRunner;
import static com.google.common.io.Resources.getResource;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

public class TestPasswordAuthentication
{
    // We use 7.8.0 because security became a non-commercial feature in recent versions
    private final String elasticsearchImage = "elasticsearch:7.8.0";
    private static final String USER = "elastic_user";
    private static final String PASSWORD = "123456";

    private final ElasticsearchServer elasticsearch;
    private final RestHighLevelClient client;
    private final QueryAssertions assertions;

    public TestPasswordAuthentication()
            throws Exception
    {
        elasticsearch = new ElasticsearchServer(elasticsearchImage, ImmutableMap.<String, String>builder()
                .put("elasticsearch.yml", loadResource("elasticsearch.yml"))
                .put("users", loadResource("users"))
                .put("users_roles", loadResource("users_roles"))
                .put("roles.yml", loadResource("roles.yml"))
                .build());

        HostAndPort address = elasticsearch.getAddress();
        client = new RestHighLevelClient(RestClient.builder(new HttpHost(address.getHost(), address.getPort())));

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
        String json = new ObjectMapper().writeValueAsString(ImmutableMap.<String, Object>builder()
                .put("value", 42L)
                .build());

        client.getLowLevelClient()
                .performRequest(
                        "POST",
                        "/test/_doc?refresh",
                        ImmutableMap.of(),
                        new NStringEntity(json, ContentType.APPLICATION_JSON),
                        new BasicHeader("Authorization", format("Basic %s", Base64.encodeAsString(format("%s:%s", USER, PASSWORD).getBytes(StandardCharsets.UTF_8)))));

        assertions.assertQuery("SELECT * FROM test",
                "VALUES BIGINT '42'");
    }

    private static String loadResource(String file)
            throws IOException
    {
        return Resources.toString(getResource(file), UTF_8);
    }
}
