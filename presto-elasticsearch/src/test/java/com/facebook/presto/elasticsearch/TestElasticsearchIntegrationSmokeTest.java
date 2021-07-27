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
import com.google.common.io.BaseEncoding;
import com.google.common.net.HostAndPort;
import io.airlift.tpch.TpchTable;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Map;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.elasticsearch.ElasticsearchQueryRunner.createElasticsearchQueryRunner;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

@Test(singleThreaded = true)
public class TestElasticsearchIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    private ElasticsearchServer elasticsearch;
    private RestHighLevelClient client;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        elasticsearch = new ElasticsearchServer();

        HostAndPort address = elasticsearch.getAddress();
        client = new RestHighLevelClient(RestClient.builder(new HttpHost(address.getHost(), address.getPort())));

        return createElasticsearchQueryRunner(elasticsearch.getAddress(), TpchTable.getTables());
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
            throws IOException
    {
        elasticsearch.stop();
        client.close();
    }

    @Test
    public void testSelectAll()
    {
        // List columns explicitly, as there's no defined order in Elasticsearch
        assertQuery("SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment  FROM orders");
    }

    @Test
    public void testRangePredicate()
    {
        // List columns explicitly, as there's no defined order in Elasticsearch
        assertQuery("" +
                "SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment " +
                "FROM orders " +
                "WHERE orderkey BETWEEN 10 AND 50");
    }

    @Test
    public void testCountAll()
    {
        assertQuery("SELECT COUNT(*) FROM orders");
        assertQuery("SELECT count(*) FROM orders WHERE orderkey > 10");
        assertQuery("SELECT count(*) FROM (SELECT * FROM orders LIMIT 10)");
        assertQuery("SELECT count(*) FROM (SELECT * FROM orders WHERE orderkey > 10 LIMIT 10)");
    }

    @Test
    public void testMultipleRangesPredicate()
    {
        assertQuery("" +
                "SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment " +
                "FROM orders " +
                "WHERE orderkey BETWEEN 10 AND 50 OR orderkey BETWEEN 100 AND 150");
    }

    @Test
    @Override
    public void testDescribeTable()
    {
        MaterializedResult actualColumns = computeActual("DESC orders").toTestTypes();
        MaterializedResult.Builder builder = resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR);
        for (MaterializedRow row : actualColumns.getMaterializedRows()) {
            builder.row(row.getField(0), row.getField(1), "", "");
        }
        MaterializedResult actualResult = builder.build();
        builder = resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR);
        MaterializedResult expectedColumns = builder
                .row("clerk", "varchar", "", "")
                .row("comment", "varchar", "", "")
                .row("custkey", "bigint", "", "")
                .row("orderdate", "timestamp", "", "")
                .row("orderkey", "bigint", "", "")
                .row("orderpriority", "varchar", "", "")
                .row("orderstatus", "varchar", "", "")
                .row("shippriority", "bigint", "", "")
                .row("totalprice", "real", "", "")
                .build();
        assertEquals(actualResult, expectedColumns, format("%s != %s", actualResult, expectedColumns));
    }

    @Test
    public void testShowCreateTable()
    {
        assertThat(computeActual("SHOW CREATE TABLE orders").getOnlyValue())
                .isEqualTo("CREATE TABLE elasticsearch.tpch.orders (\n" +
                        "   clerk varchar,\n" +
                        "   comment varchar,\n" +
                        "   custkey bigint,\n" +
                        "   orderdate timestamp,\n" +
                        "   orderkey bigint,\n" +
                        "   orderpriority varchar,\n" +
                        "   orderstatus varchar,\n" +
                        "   shippriority bigint,\n" +
                        "   totalprice real\n" +
                        ")");
    }

    @Test
    public void testArrayFields()
            throws IOException
    {
        String indexName = "test_arrays";

        String mapping = "" +
                "{" +
                "  \"mappings\": {" +
                "    \"doc\": {" +
                "      \"_meta\": {" +
                "        \"presto\": {" +
                "          \"a\": {" +
                "            \"b\": {" +
                "              \"y\": {" +
                "                \"isArray\": true" +
                "              }" +
                "            }" +
                "          }," +
                "          \"c\": {" +
                "            \"f\": {" +
                "              \"g\": {" +
                "                \"isArray\": true" +
                "              }," +
                "              \"isArray\": true" +
                "            }" +
                "          }," +
                "          \"j\": {" +
                "            \"isArray\": true" +
                "          }," +
                "          \"k\": {" +
                "            \"isArray\": true" +
                "          }" +
                "        }" +
                "      }," +
                "      \"properties\":{" +
                "        \"a\": {" +
                "          \"type\": \"object\"," +
                "          \"properties\": {" +
                "            \"b\": {" +
                "              \"type\": \"object\"," +
                "              \"properties\": {" +
                "                \"x\": {" +
                "                  \"type\": \"integer\"" +
                "                }," +
                "                \"y\": {" +
                "                  \"type\": \"keyword\"" +
                "                }" +
                "              } " +
                "            }" +
                "          }" +
                "        }," +
                "        \"c\": {" +
                "          \"type\": \"object\"," +
                "          \"properties\": {" +
                "            \"d\": {" +
                "              \"type\": \"keyword\"" +
                "            }," +
                "            \"e\": {" +
                "              \"type\": \"keyword\"" +
                "            }," +
                "            \"f\": {" +
                "              \"type\": \"object\"," +
                "              \"properties\": {" +
                "                \"g\": {" +
                "                  \"type\": \"integer\"" +
                "                }," +
                "                \"h\": {" +
                "                  \"type\": \"integer\"" +
                "                }" +
                "              } " +
                "            }" +
                "          }" +
                "        }," +
                "        \"i\": {" +
                "          \"type\": \"long\"" +
                "        }," +
                "        \"j\": {" +
                "          \"type\": \"long\"" +
                "        }," +
                "        \"k\": {" +
                "          \"type\": \"long\"" +
                "        }" +
                "      }" +
                "    }" +
                "  }" +
                "}";

        createIndex(indexName, mapping);

        index(indexName, ImmutableMap.<String, Object>builder()
                .put("a", ImmutableMap.<String, Object>builder()
                        .put("b", ImmutableMap.<String, Object>builder()
                                .put("x", 1)
                                .put("y", ImmutableList.<String>builder()
                                        .add("hello")
                                        .add("world")
                                        .build())
                                .build())
                        .build())
                .put("c", ImmutableMap.<String, Object>builder()
                        .put("d", "foo")
                        .put("e", "bar")
                        .put("f", ImmutableList.<Map<String, Object>>builder()
                                .add(ImmutableMap.<String, Object>builder()
                                        .put("g", ImmutableList.<Integer>builder()
                                                .add(10)
                                                .add(20)
                                                .build())
                                        .put("h", 100)
                                        .build())
                                .add(ImmutableMap.<String, Object>builder()
                                        .put("g", ImmutableList.<Integer>builder()
                                                .add(30)
                                                .add(40)
                                                .build())
                                        .put("h", 200)
                                        .build())
                                .build())
                        .build())
                .put("j", ImmutableList.<Long>builder()
                        .add(50L)
                        .add(60L)
                        .build())
                .build());

        assertQuery(
                "SELECT a.b.y[1], c.f[1].g[2], c.f[2].g[1], j[2], k[1] FROM test_arrays",
                "VALUES ('hello', 20, 30, 60, NULL)");
    }

    @Test
    public void testEmptyObjectFields()
            throws IOException
    {
        String indexName = "emptyobject";
        index(indexName, ImmutableMap.<String, Object>builder()
                .put("name", "stringfield")
                .put("emptyobject", ImmutableMap.of())
                .put("fields.fielda", 32)
                .put("fields.fieldb", ImmutableMap.of())
                .build());

        assertQuery(
                "SELECT name, fields.fielda FROM emptyobject",
                "VALUES ('stringfield', 32)");
    }

    @Test
    public void testNestedFields()
            throws IOException
    {
        String indexName = "data";
        index(indexName, ImmutableMap.<String, Object>builder()
                .put("name", "nestfield")
                .put("fields.fielda", 32)
                .put("fields.fieldb", "valueb")
                .build());

        assertQuery(
                "SELECT name, fields.fielda, fields.fieldb FROM data",
                "VALUES ('nestfield', 32, 'valueb')");
    }

    @Test
    public void testNestedVariants()
            throws IOException
    {
        String indexName = "nested_variants";

        index(indexName,
                ImmutableMap.of("a",
                        ImmutableMap.of("b",
                                ImmutableMap.of("c",
                                        "value1"))));

        index(indexName,
                ImmutableMap.of("a.b",
                        ImmutableMap.of("c",
                                "value2")));

        index(indexName,
                ImmutableMap.of("a",
                        ImmutableMap.of("b.c",
                                "value3")));

        index(indexName,
                ImmutableMap.of("a.b.c", "value4"));

        assertQuery(
                "SELECT a.b.c FROM nested_variants",
                "VALUES 'value1', 'value2', 'value3', 'value4'");
    }

    @Test
    public void testDataTypes()
            throws IOException
    {
        String indexName = "types";

        String mapping = "" +
                "{" +
                "  \"mappings\": {" +
                "    \"doc\": {" +
                "      \"properties\": {" +
                "        \"boolean_column\":   { \"type\": \"boolean\" }," +
                "        \"float_column\":     { \"type\": \"float\" }," +
                "        \"double_column\":    { \"type\": \"double\" }," +
                "        \"integer_column\":   { \"type\": \"integer\" }," +
                "        \"long_column\":      { \"type\": \"long\" }," +
                "        \"keyword_column\":   { \"type\": \"keyword\" }," +
                "        \"text_column\":      { \"type\": \"text\" }," +
                "        \"binary_column\":    { \"type\": \"binary\" }," +
                "        \"timestamp_column\": { \"type\": \"date\" }" +
                "      }" +
                "    }" +
                "  }" +
                "}";

        createIndex(indexName, mapping);

        index(indexName, ImmutableMap.<String, Object>builder()
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
                .build());

        MaterializedResult rows = computeActual("" +
                "SELECT " +
                "boolean_column, " +
                "float_column, " +
                "double_column, " +
                "integer_column, " +
                "long_column, " +
                "keyword_column, " +
                "text_column, " +
                "binary_column, " +
                "timestamp_column, " +
                "ipv4_column, " +
                "ipv6_column " +
                "FROM types");

        MaterializedResult expected = resultBuilder(getSession(), rows.getTypes())
                .row(true, 1.0f, 1.0d, 1, 1L, "cool", "some text", new byte[] {(byte) 0xCA, (byte) 0xFE},
                        LocalDateTime.of(1970, 1, 1, 0, 0), "192.0.2.4", "2001:db8:0:1:1:1:1:1")
                .build();

        assertEquals(rows.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Test
    public void testFilters()
            throws IOException
    {
        String indexName = "filter_pushdown";

        String mapping = "" +
                "{" +
                "  \"mappings\": {" +
                "    \"doc\": {" +
                "      \"properties\": {" +
                "        \"boolean_column\":   { \"type\": \"boolean\" }," +
                "        \"float_column\":     { \"type\": \"float\" }," +
                "        \"double_column\":    { \"type\": \"double\" }," +
                "        \"integer_column\":   { \"type\": \"integer\" }," +
                "        \"long_column\":      { \"type\": \"long\" }," +
                "        \"keyword_column\":   { \"type\": \"keyword\" }," +
                "        \"text_column\":      { \"type\": \"text\" }," +
                "        \"binary_column\":    { \"type\": \"binary\" }," +
                "        \"timestamp_column\": { \"type\": \"date\" }" +
                "      }" +
                "    }" +
                "  }" +
                "}";

        createIndex(indexName, mapping);

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
            throws IOException
    {
        String indexName = "types_nested";

        String mapping = "" +
                "{" +
                "  \"mappings\": {" +
                "    \"doc\": {" +
                "      \"properties\": {" +
                "        \"field\": {" +
                "          \"properties\": {" +
                "            \"boolean_column\":   { \"type\": \"boolean\" }," +
                "            \"float_column\":     { \"type\": \"float\" }," +
                "            \"double_column\":    { \"type\": \"double\" }," +
                "            \"integer_column\":   { \"type\": \"integer\" }," +
                "            \"long_column\":      { \"type\": \"long\" }," +
                "            \"keyword_column\":   { \"type\": \"keyword\" }," +
                "            \"text_column\":      { \"type\": \"text\" }," +
                "            \"binary_column\":    { \"type\": \"binary\" }," +
                "            \"timestamp_column\": { \"type\": \"date\" }" +
                "          }" +
                "        }" +
                "      }" +
                "    }" +
                "  }" +
                "}";

        createIndex(indexName, mapping);

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
            throws IOException
    {
        String indexName = "nested_type_nested";

        String mapping = "" +
                "{" +
                "  \"mappings\": {" +
                "    \"doc\": {" +
                "      \"properties\": {" +
                "        \"nested_field\": {" +
                "          \"type\":\"nested\"," +
                "          \"properties\": {" +
                "            \"boolean_column\":   { \"type\": \"boolean\" }," +
                "            \"float_column\":     { \"type\": \"float\" }," +
                "            \"double_column\":    { \"type\": \"double\" }," +
                "            \"integer_column\":   { \"type\": \"integer\" }," +
                "            \"long_column\":      { \"type\": \"long\" }," +
                "            \"keyword_column\":   { \"type\": \"keyword\" }," +
                "            \"text_column\":      { \"type\": \"text\" }," +
                "            \"binary_column\":    { \"type\": \"binary\" }," +
                "            \"timestamp_column\": { \"type\": \"date\" }," +
                "            \"ipv4_column\":      { \"type\": \"ip\" }," +
                "            \"ipv6_column\":      { \"type\": \"ip\" }" +
                "          }" +
                "        }" +
                "      }" +
                "    }" +
                "  }" +
                "}";

        createIndex(indexName, mapping);

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

    @Test
    public void testQueryString()
    {
        MaterializedResult actual = computeActual("SELECT count(*) FROM \"orders: +packages -slyly\"");

        MaterializedResult expected = resultBuilder(getSession(), ImmutableList.of(BIGINT))
                .row(1639L)
                .build();

        assertEquals(actual, expected);
    }

    @Test
    public void testMixedCase()
            throws IOException
    {
        String indexName = "mixed_case";
        index(indexName, ImmutableMap.<String, Object>builder()
                .put("Name", "john")
                .put("AGE", 32)
                .build());

        assertQuery(
                "SELECT name, age FROM mixed_case",
                "VALUES ('john', 32)");

        assertQuery(
                "SELECT name, age FROM mixed_case WHERE name = 'john'",
                "VALUES ('john', 32)");
    }

    @Test
    public void testNumericKeyword()
            throws IOException
    {
        String indexName = "numeric_keyword";

        index(indexName, ImmutableMap.<String, Object>builder()
                .put("numeric_column", 20)
                .build());

        assertQuery(
                "SELECT numeric_column FROM numeric_keyword",
                "VALUES 20");
        assertQuery(
                "SELECT numeric_column FROM numeric_keyword where numeric_column = 20",
                "VALUES 20");
    }

    @Test
    public void testQueryStringError()
    {
        assertQueryFails("SELECT orderkey FROM \"orders: ++foo AND\"", "\\QFailed to parse query [ ++foo and]\\E");
        assertQueryFails("SELECT count(*) FROM \"orders: ++foo AND\"", "\\QFailed to parse query [ ++foo and]\\E");
    }

    @Test
    public void testAlias()
            throws IOException
    {
        addAlias("orders", "orders_alias");

        assertQuery(
                "SELECT count(*) FROM orders_alias",
                "SELECT count(*) FROM orders");

        embeddedElasticsearchNode.getClient()
                .admin()
                .indices()
                .aliases(indexAliasesRequest()
                        .addAliasAction(IndicesAliasesRequest.AliasActions.remove()
                                .index("orders")
                                .alias("orders_alias")))
                .actionGet();
    }

    @Test(enabled = false)
    public void testMultiIndexAlias()
            throws IOException
    {
        addAlias("nation", "multi_alias");
        addAlias("region", "multi_alias");

        assertQuery(
                "SELECT count(*) FROM multi_alias",
                "SELECT (SELECT count(*) FROM region) + (SELECT count(*) FROM nation)");
    }

    private void index(String index, Map<String, Object> document)
            throws IOException
    {
        client.index(new IndexRequest(index, "doc")
                .source(document)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE));
    }

    @Test
    public void testPassthroughQuery()
    {
        @Language("JSON")
        String query = "{\n" +
                "    \"size\": 0,\n" +
                "    \"aggs\" : {\n" +
                "        \"max_orderkey\" : { \"max\" : { \"field\" : \"orderkey\" } },\n" +
                "        \"sum_orderkey\" : { \"sum\" : { \"field\" : \"orderkey\" } }\n" +
                "    }\n" +
                "}";

        assertQuery(
                format("WITH data(r) AS (" +
                        "   SELECT CAST(result AS ROW(aggregations ROW(max_orderkey ROW(value BIGINT), sum_orderkey ROW(value BIGINT)))) " +
                        "   FROM \"orders$query:%s\") " +
                        "SELECT r.aggregations.max_orderkey.value, r.aggregations.sum_orderkey.value " +
                        "FROM data", BaseEncoding.base32().encode(query.getBytes(UTF_8))),
                "VALUES (60000, 449872500)");

        assertQueryFails(
                "SELECT * FROM \"orders$query:invalid-base32-encoding\"",
                "Elasticsearch query for 'orders' is not base32-encoded correctly");
        assertQueryFails(
                format("SELECT * FROM \"orders$query:%s\"", BaseEncoding.base32().encode("invalid json".getBytes(UTF_8))),
                "Elasticsearch query for 'orders' is not valid JSON");
    }

    private void addAlias(String index, String alias)
            throws IOException
    {
        client.getLowLevelClient()
                .performRequest("PUT", format("/%s/_alias/%s", index, alias));
    }

    private void createIndex(String indexName, @Language("JSON") String mapping)
            throws IOException
    {
        client.getLowLevelClient()
                .performRequest("PUT", "/" + indexName, ImmutableMap.of(), new NStringEntity(mapping, ContentType.APPLICATION_JSON));
    }
}
