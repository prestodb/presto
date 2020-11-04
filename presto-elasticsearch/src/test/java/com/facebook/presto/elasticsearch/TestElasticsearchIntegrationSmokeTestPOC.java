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

import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestIntegrationSmokeTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closer;
import io.airlift.tpch.TpchTable;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Map;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.elasticsearch.ElasticsearchQueryRunner.createElasticsearchQueryRunner;
import static com.facebook.presto.elasticsearch.EmbeddedElasticsearchNode.createEmbeddedElasticsearchNode;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.elasticsearch.client.Requests.indexAliasesRequest;
import static org.elasticsearch.client.Requests.refreshRequest;

@Test(singleThreaded = true)
public class TestElasticsearchIntegrationSmokeTestPOC
        extends AbstractTestIntegrationSmokeTest
{
    private final EmbeddedElasticsearchNode embeddedElasticsearchNode;

    private QueryRunner queryRunner;

    public TestElasticsearchIntegrationSmokeTestPOC()
    {
        this(createEmbeddedElasticsearchNode());
    }

    public TestElasticsearchIntegrationSmokeTestPOC(EmbeddedElasticsearchNode embeddedElasticsearchNode)
    {
        super(() -> createElasticsearchQueryRunner(embeddedElasticsearchNode, TpchTable.getTables()));
        this.embeddedElasticsearchNode = embeddedElasticsearchNode;
    }

    @BeforeClass
    public void setUp()
    {
        queryRunner = getQueryRunner();
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
            throws IOException
    {
        try (Closer closer = Closer.create()) {
            closer.register(queryRunner);
            closer.register(embeddedElasticsearchNode);
        }
        queryRunner = null;
    }

    @Test
    public void testPoc()
    {
        String indexName = "test_poc";

        embeddedElasticsearchNode.getClient()
                .admin()
                .indices()
                .prepareCreate(indexName)
                .addMapping("doc", 
                    "{" +
                        "\"_meta\": {" +
                            "\"presto\": {" +
                                "\"document_rows\": true " +
                               "}" +
                         "}," +
                         "\"properties\": {" +
                             "\"rows\" : {" +
                                 "\"type\": \"nested\"," +
                                 "\"properties\": {" +
                                     "\"keyword_field\": {\"type\": \"keyword\"}," +
                                     "\"int_field\": {\"type\": \"integer\"}" +
                                   "}" +
                               "}" +
                          "}" +
                    "}",
                    XContentType.JSON)
                .get();

        index(indexName, 
            ImmutableMap.<String, Object>builder()
                .put("rows", ImmutableList.<Map<String, Object>>builder()
                        .add(ImmutableMap.<String, Object>builder()
                            .put("keyword_field", "a1")
                            .put("int_field", 1)
                            .build())
                        .add(ImmutableMap.<String, Object>builder()
                            .put("keyword_field", "a2")
                            .put("int_field", 2)
                            .build())
                .build())
            .build());
        embeddedElasticsearchNode.getClient()
          .admin()
          .indices()
          .refresh(refreshRequest(indexName))
          .actionGet();
        index(indexName, 
            ImmutableMap.<String, Object>builder()
                .put("rows", ImmutableList.<Map<String, Object>>builder()
                        .add(ImmutableMap.<String, Object>builder()
                            .put("keyword_field", "a3")
                            .put("int_field", 3)
                            .build())
                        .add(ImmutableMap.<String, Object>builder()
                            .put("keyword_field", "a4")
                            .put("int_field", 4)
                            .build())
                .build())
            .build());

        embeddedElasticsearchNode.getClient()
                .admin()
                .indices()
                .refresh(refreshRequest(indexName))
                .actionGet();

        assertQuery(
                "SELECT * FROM test_poc",
                "VALUES (1, 'a1'), (2, 'a2'), (3, 'a3'), (4, 'a4')");
    }

    @Test
    public void testFilters()
    {
        String indexName = "filter_pushdown";

        embeddedElasticsearchNode.getClient()
                .admin()
                .indices()
                .prepareCreate(indexName)
                .addMapping("doc",
                        "boolean_column", "type=boolean",
                        "byte_column", "type=byte",
                        "short_column", "type=short",
                        "integer_column", "type=integer",
                        "long_column", "type=long",
                        "float_column", "type=float",
                        "double_column", "type=double",
                        "keyword_column", "type=keyword",
                        "text_column", "type=text",
                        "binary_column", "type=binary",
                        "timestamp_column", "type=date",
                        "ipv4_column", "type=ip",
                        "ipv6_column", "type=ip")
                .get();

        index(indexName, ImmutableMap.<String, Object>builder()
                .put("boolean_column", true)
                .put("byte_column", 1)
                .put("short_column", 2)
                .put("integer_column", 3)
                .put("long_column", 4L)
                .put("float_column", 1.0f)
                .put("double_column", 1.0d)
                .put("keyword_column", "cool")
                .put("text_column", "some text")
                .put("binary_column", new byte[] {(byte) 0xCA, (byte) 0xFE})
                .put("timestamp_column", 1569888000000L)
                .put("ipv4_column", "192.0.2.4")
                .put("ipv6_column", "2001:db8:0:1:1:1:1:1")
                .build());

        embeddedElasticsearchNode.getClient()
                .admin()
                .indices()
                .refresh(refreshRequest(indexName))
                .actionGet();

        // boolean
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE boolean_column = true", "VALUES 1");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE boolean_column = false", "VALUES 0");

        // tinyint
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE byte_column = 1", "VALUES 1");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE byte_column = 0", "VALUES 0");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE byte_column > 1", "VALUES 0");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE byte_column < 1", "VALUES 0");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE byte_column > 0", "VALUES 1");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE byte_column < 10", "VALUES 1");

        // smallint
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE short_column = 2", "VALUES 1");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE short_column > 2", "VALUES 0");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE short_column < 2", "VALUES 0");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE short_column = 0", "VALUES 0");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE short_column > 0", "VALUES 1");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE short_column < 10", "VALUES 1");

        // integer
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE integer_column = 3", "VALUES 1");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE integer_column > 3", "VALUES 0");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE integer_column < 3", "VALUES 0");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE integer_column = 0", "VALUES 0");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE integer_column > 0", "VALUES 1");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE integer_column < 10", "VALUES 1");

        // bigint
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE long_column = 4", "VALUES 1");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE long_column > 4", "VALUES 0");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE long_column < 4", "VALUES 0");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE long_column = 0", "VALUES 0");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE long_column > 0", "VALUES 1");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE long_column < 10", "VALUES 1");

        // real
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE float_column = 1.0", "VALUES 1");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE float_column > 1.0", "VALUES 0");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE float_column < 1.0", "VALUES 0");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE float_column = 0.0", "VALUES 0");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE float_column > 0.0", "VALUES 1");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE float_column < 10.0", "VALUES 1");

        // double
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE double_column = 1.0", "VALUES 1");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE double_column > 1.0", "VALUES 0");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE double_column < 1.0", "VALUES 0");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE double_column = 0.0", "VALUES 0");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE double_column > 0.0", "VALUES 1");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE double_column < 10.0", "VALUES 1");

        // varchar
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE keyword_column = 'cool'", "VALUES 1");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE keyword_column = 'bar'", "VALUES 0");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE text_column = 'some'", "VALUES 0");

        // timestamp
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE timestamp_column = TIMESTAMP '2019-10-01 00:00:00'", "VALUES 1");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE timestamp_column > TIMESTAMP '2019-10-01 00:00:00'", "VALUES 0");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE timestamp_column < TIMESTAMP '2019-10-01 00:00:00'", "VALUES 0");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE timestamp_column = TIMESTAMP '2019-10-02 00:00:00'", "VALUES 0");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE timestamp_column > TIMESTAMP '2001-01-01 00:00:00'", "VALUES 1");
        assertQuery("SELECT count(*) FROM filter_pushdown WHERE timestamp_column < TIMESTAMP '2030-01-01 00:00:00'", "VALUES 1");

        // ipaddress
        assertQuery("SELECT count(ipv4_column) FROM filter_pushdown", "VALUES 1");
        assertQuery("SELECT count(ipv6_column) FROM filter_pushdown", "VALUES 1");
    }

    @Test
    public void testDataTypesNested()
    {
        String indexName = "types_nested";

        embeddedElasticsearchNode.getClient()
                .admin()
                .indices()
                .prepareCreate(indexName)
                .addMapping("doc", "" +
                                "{ " +
                                "    \"properties\": {\n" +
                                "        \"field\": {\n" +
                                "            \"properties\": {\n" +
                                "                \"boolean_column\":   { \"type\": \"boolean\" },\n" +
                                "                \"float_column\":     { \"type\": \"float\" },\n" +
                                "                \"double_column\":    { \"type\": \"double\" },\n" +
                                "                \"integer_column\":   { \"type\": \"integer\" },\n" +
                                "                \"long_column\":      { \"type\": \"long\" },\n" +
                                "                \"keyword_column\":   { \"type\": \"keyword\" },\n" +
                                "                \"text_column\":      { \"type\": \"text\" },\n" +
                                "                \"binary_column\":    { \"type\": \"binary\" },\n" +
                                "                \"timestamp_column\": { \"type\": \"date\" },\n" +
                                "                \"ipv4_column\":      { \"type\": \"ip\" },\n" +
                                "                \"ipv6_column\":      { \"type\": \"ip\" }\n" +
                                "            }\n" +
                                "        }\n" +
                                "    }" +
                                "}\n",
                        XContentType.JSON)
                .get();

        index(indexName, ImmutableMap.of(
                "field",
                ImmutableMap.<String, Object>builder()
                        .put("boolean_column", true)
                        .put("float_column", 1.0f)
                        .put("double_column", 1.0d)
                        .put("integer_column", 1)
                        .put("long_column", 1L)
                        .put("keyword_column", "cool")
                        .put("text_column", "some text")
                        .put("binary_column", new byte[] {(byte) 0xCA, (byte) 0xFE})
                        .put("timestamp_column", 0)
                        .put("ipv4_column", "192.0.2.4")
                        .put("ipv6_column", "2001:db8:0:1:1:1:1:1")
                        .build()));

        embeddedElasticsearchNode.getClient()
                .admin()
                .indices()
                .refresh(refreshRequest(indexName))
                .actionGet();

        MaterializedResult rows = computeActual("" +
                "SELECT " +
                "field.boolean_column, " +
                "field.float_column, " +
                "field.double_column, " +
                "field.integer_column, " +
                "field.long_column, " +
                "field.keyword_column, " +
                "field.text_column, " +
                "field.binary_column, " +
                "field.timestamp_column, " +
                "field.ipv4_column, " +
                "field.ipv6_column " +
                "FROM types_nested");

        MaterializedResult expected = resultBuilder(getSession(), rows.getTypes())
                .row(true, 1.0f, 1.0d, 1, 1L, "cool", "some text", new byte[] {(byte) 0xCA, (byte) 0xFE},
                        LocalDateTime.of(1970, 1, 1, 0, 0), "192.0.2.4", "2001:db8:0:1:1:1:1:1")
                .build();

        assertEquals(rows.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testNestedTypeDataTypesNested()
    {
        String indexName = "nested_type_nested";

        embeddedElasticsearchNode.getClient()
                .admin()
                .indices()
                .prepareCreate(indexName)
                .addMapping("doc", "" +
                                "{ " +
                                "    \"properties\": {\n" +
                                "        \"nested_field\": {" +
                                "          \"type\":\"nested\"," +
                                "          \"properties\": {" +
                                "            \"boolean_column\":   { \"type\": \"boolean\" },\n" +
                                "            \"float_column\":     { \"type\": \"float\" },\n" +
                                "            \"double_column\":    { \"type\": \"double\" },\n" +
                                "            \"integer_column\":   { \"type\": \"integer\" }\n," +
                                "            \"long_column\":      { \"type\": \"long\" },\n" +
                                "            \"keyword_column\":   { \"type\": \"keyword\" },\n" +
                                "            \"text_column\":      { \"type\": \"text\" },\n" +
                                "            \"binary_column\":    { \"type\": \"binary\" },\n" +
                                "            \"timestamp_column\": { \"type\": \"date\" },\n" +
                                "            \"ipv4_column\":      { \"type\": \"ip\" },\n" +
                                "            \"ipv6_column\":      { \"type\": \"ip\" }\n" +
                                "            }\n" +
                                "        }\n" +
                                "    }" +
                                "}\n",
                        XContentType.JSON)
                .get();

        index(indexName, ImmutableMap.of(
                "nested_field",
                ImmutableMap.<String, Object>builder()
                        .put("boolean_column", true)
                        .put("float_column", 1.0f)
                        .put("double_column", 1.0d)
                        .put("integer_column", 1)
                        .put("long_column", 1L)
                        .put("keyword_column", "cool")
                        .put("text_column", "some text")
                        .put("binary_column", new byte[] {(byte) 0xCA, (byte) 0xFE})
                        .put("timestamp_column", 0)
                        .put("ipv4_column", "192.0.2.4")
                        .put("ipv6_column", "2001:db8:0:1:1:1:1:1")
                        .build()));

        embeddedElasticsearchNode.getClient()
                .admin()
                .indices()
                .refresh(refreshRequest(indexName))
                .actionGet();

        MaterializedResult rows = computeActual("" +
                "SELECT " +
                "nested_field.boolean_column, " +
                "nested_field.float_column, " +
                "nested_field.double_column, " +
                "nested_field.integer_column, " +
                "nested_field.long_column, " +
                "nested_field.keyword_column, " +
                "nested_field.text_column, " +
                "nested_field.binary_column, " +
                "nested_field.timestamp_column, " +
                "nested_field.ipv4_column, " +
                "nested_field.ipv6_column " +
                "FROM nested_type_nested");

        MaterializedResult expected = resultBuilder(getSession(), rows.getTypes())
                .row(true, 1.0f, 1.0d, 1, 1L, "cool", "some text", new byte[] {(byte) 0xCA, (byte) 0xFE},
                        LocalDateTime.of(1970, 1, 1, 0, 0), "192.0.2.4", "2001:db8:0:1:1:1:1:1")
                .build();

        assertEquals(rows.getMaterializedRows(), expected.getMaterializedRows());
    }

    
    private void index(String indexName, Map<String, Object> document)
    {
        embeddedElasticsearchNode.getClient()
                .prepareIndex(indexName, "doc")
                .setSource(document)
                .get();
    }
}
