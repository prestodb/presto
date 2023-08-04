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
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import io.airlift.tpch.TpchTable;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.IOException;

import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.elasticsearch.ElasticsearchQueryRunner.createElasticsearchQueryRunner;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static java.lang.String.format;

@Test(singleThreaded = true)
public class ElasticsearchConnectorTest
        extends AbstractTestIntegrationSmokeTest
{
    private final String elasticsearchServer = "docker.elastic.co/elasticsearch/elasticsearch-oss:6.0.0";
    private ElasticsearchServer elasticsearch;
    private RestHighLevelClient client;

    @AfterClass(alwaysRun = true)
    public final void destroy()
            throws IOException
    {
        elasticsearch.stop();
        client.close();
    }

    @Test
    public void testSelectInformationSchemaForMultiIndexAlias()
            throws IOException
    {
        addAlias("nation", "multi_alias");
        addAlias("region", "multi_alias");

        // No duplicate entries should be found in information_schema.tables or information_schema.columns.
        testSelectInformationSchemaTables();
        testSelectInformationSchemaColumns();
    }

    @Test
    public void testSelectInformationSchemaTables()
    {
        String catalog = getSession().getCatalog().get();
        String schema = getSession().getSchema().get();
        String schemaPattern = schema.replaceAll("^.", "_");

        @Language("SQL") String expectedTables = "VALUES " +
                "('customer'), " +
                "('lineitem'), " +
                "('nation'), " +
                "('part'), " +
                "('orders'), " +
                "('partsupp'), " +
                "('region'), " +
                "('supplier'), " +
                "('multi_alias')";

        assertQuery("SELECT table_name FROM information_schema.tables WHERE table_schema = '" + schema + "'", expectedTables);

        assertQuery("SELECT table_name FROM information_schema.tables WHERE table_schema = '" + schema + "' AND table_name = 'orders'", "VALUES 'orders'");
        assertQuery("SELECT table_name FROM information_schema.tables WHERE table_schema LIKE '" + schema + "' AND table_name LIKE '%rders'", "VALUES 'orders'");
        assertQuery("SELECT table_name FROM information_schema.tables WHERE table_schema LIKE '" + schemaPattern + "' AND table_name LIKE '%rders'", "VALUES 'orders'");
        assertQuery(
                "SELECT table_name FROM information_schema.tables " +
                        "WHERE table_catalog = '" + catalog + "' AND table_schema LIKE '" + schema + "' AND table_name LIKE '%orders'",
                "VALUES 'orders'");
        assertQuery("SELECT table_name FROM information_schema.tables WHERE table_catalog = 'something_else'", "SELECT '' WHERE false");

        assertQuery(
                "SELECT DISTINCT table_name FROM information_schema.tables WHERE table_schema = 'information_schema' OR rand() = 42 ORDER BY 1",
                "VALUES " +
                        "('applicable_roles'), " +
                        "('columns'), " +
                        "('enabled_roles'), " +
                        "('roles'), " +
                        "('schemata'), " +
                        "('table_privileges'), " +
                        "('tables'), " +
                        "('views')");
    }

    @Test
    public void testSelectInformationSchemaColumns()
    {
        String catalog = getSession().getCatalog().get();
        String schema = getSession().getSchema().get();
        String schemaPattern = schema.replaceAll(".$", "_");

        @Language("SQL") String ordersTableWithColumns = "VALUES " +
                "('orders', 'orderkey'), " +
                "('orders', 'custkey'), " +
                "('orders', 'orderstatus'), " +
                "('orders', 'totalprice'), " +
                "('orders', 'orderdate'), " +
                "('orders', 'orderpriority'), " +
                "('orders', 'clerk'), " +
                "('orders', 'shippriority'), " +
                "('orders', 'comment')";

        assertQuery("SELECT table_schema FROM information_schema.columns WHERE table_schema = '" + schema + "' GROUP BY table_schema", "VALUES '" + schema + "'");
        assertQuery("SELECT table_name FROM information_schema.columns WHERE table_name = 'orders' GROUP BY table_name", "VALUES 'orders'");
        assertQuery("SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = '" + schema + "' AND table_name = 'orders'", ordersTableWithColumns);
        assertQuery("SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = '" + schema + "' AND table_name LIKE '%rders'", ordersTableWithColumns);
        assertQuery("SELECT table_name, column_name FROM information_schema.columns WHERE table_schema LIKE '" + schemaPattern + "' AND table_name LIKE '_rder_'", ordersTableWithColumns);

        assertQuerySucceeds("SELECT * FROM information_schema.columns");
        assertQuery("SELECT DISTINCT table_name, column_name FROM information_schema.columns WHERE table_name LIKE '_rders'", ordersTableWithColumns);
        assertQuerySucceeds("SELECT * FROM information_schema.columns WHERE table_catalog = '" + catalog + "'");
        assertQuerySucceeds("SELECT * FROM information_schema.columns WHERE table_catalog = '" + catalog + "' AND table_schema = '" + schema + "'");
        assertQuery("SELECT table_name, column_name FROM information_schema.columns WHERE table_catalog = '" + catalog + "' AND table_schema = '" + schema + "' AND table_name LIKE '_rders'", ordersTableWithColumns);
        assertQuerySucceeds("SELECT * FROM information_schema.columns WHERE table_catalog = '" + catalog + "' AND table_name LIKE '%'");
        assertQuery("SELECT column_name FROM information_schema.columns WHERE table_catalog = 'something_else'", "SELECT '' WHERE false");

        assertQuery(
                "SELECT DISTINCT table_name FROM information_schema.columns WHERE table_schema = 'information_schema' OR rand() = 42 ORDER BY 1",
                "VALUES " +
                        "('applicable_roles'), " +
                        "('columns'), " +
                        "('enabled_roles'), " +
                        "('roles'), " +
                        "('schemata'), " +
                        "('table_privileges'), " +
                        "('tables'), " +
                        "('views')");
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        elasticsearch = new ElasticsearchServer(elasticsearchServer, ImmutableMap.of());
        HostAndPort address = elasticsearch.getAddress();
        client = new RestHighLevelClient(RestClient.builder(new HttpHost(address.getHost(), address.getPort())));

        return createElasticsearchQueryRunner(elasticsearch.getAddress(),
                TpchTable.getTables(),
                ImmutableMap.of(),
                ImmutableMap.of());
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
    public void testMultipleRangesPredicate()
    {
        assertQuery("" +
                "SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment " +
                "FROM orders " +
                "WHERE orderkey BETWEEN 10 AND 50 OR orderkey BETWEEN 100 AND 150");
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
    public void testSelectAll()
    {
        // List columns explicitly, as there's no defined order in Elasticsearch
        assertQuery("SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment  FROM orders");
    }

    private void addAlias(String index, String alias)
            throws IOException
    {
        client.getLowLevelClient()
                .performRequest("PUT", format("/%s/_alias/%s", index, alias));
    }
}
