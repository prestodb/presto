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

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest5_client.Rest5ClientTransport;
import co.elastic.clients.transport.rest5_client.low_level.Rest5Client;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import io.airlift.tpch.TpchTable;
import org.apache.hc.core5.http.HttpHost;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Map;

import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.elasticsearch.ElasticsearchQueryRunner.createElasticsearchQueryRunner;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.tests.QueryAssertions.assertContains;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test
public class TestElasticsearchMixedCaseTest
        extends AbstractTestQueryFramework
{
    private final String elasticsearchServer = "docker.elastic.co/elasticsearch/elasticsearch:7.17.27";
    private ElasticsearchServer elasticsearch;
    private ElasticsearchClient client;
    @Override
    protected com.facebook.presto.testing.QueryRunner createQueryRunner()
            throws Exception
    {
        elasticsearch = new ElasticsearchServer(elasticsearchServer, ImmutableMap.of(), ImmutableMap.of(
                "xpack.security.enabled", "false"));
        HostAndPort address = elasticsearch.getAddress();
        Rest5Client restClient = Rest5Client.builder(new HttpHost(address.getHost(), address.getPort())).build();
        Rest5ClientTransport transport = new Rest5ClientTransport(restClient, new JacksonJsonpMapper());
        client = new ElasticsearchClient(transport);

        return createElasticsearchQueryRunner(elasticsearch.getAddress(),
                TpchTable.getTables(),
                ImmutableMap.of(),
                ImmutableMap.of("case-sensitive-name-matching", "true", "elasticsearch.default-schema-name", "MySchema"));
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
            throws IOException
    {
        elasticsearch.stop();
        client.close();
    }
    private void index(String index, Map<String, Object> document)
            throws IOException
    {
        client.index(IndexRequest.of(i -> i
                .index(index)
                .document(document)
                .refresh(co.elastic.clients.elasticsearch._types.Refresh.True)));
    }

    @Test
    public void testShowColumns()
            throws IOException
    {
        String indexName = "mixed_case";
        index(indexName, ImmutableMap.<String, Object>builder()
                .put("NAME", "JOHN")
                .put("Profession", "Developer")
                .put("id", 2)
                .put("name", "john")
                .build());

        MaterializedResult actual = computeActual("SHOW columns FROM MySchema.mixed_case");
        assertEquals(actual.getMaterializedRows().get(0).getField(0), "NAME");
        assertEquals(actual.getMaterializedRows().get(1).getField(0), "Profession");
        assertEquals(actual.getMaterializedRows().get(2).getField(0), "id");
        assertEquals(actual.getMaterializedRows().get(3).getField(0), "name");
    }

    @Test
    public void testSelect()
            throws IOException
    {
        String indexName = "mixed_case_select";
        index(indexName, ImmutableMap.<String, Object>builder()
                .put("NAME", "JOHN")
                .put("Profession", "Developer")
                .put("name", "john")
                .build());

        MaterializedResult actualRow = computeActual("SELECT * from MySchema.mixed_case_select");
        MaterializedResult expectedRow = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR)
                .row("JOHN", "Developer", "john")
                .build();
        assertTrue(actualRow.equals(expectedRow));
    }

    @Test
    public void testSchema()
    {
        MaterializedResult actualRow = computeActual("SHOW schemas from elasticsearch");
        MaterializedResult expectedRow = resultBuilder(getSession(), VARCHAR)
                .row("MySchema")
                .build();
        assertContains(actualRow, expectedRow);
    }
}
