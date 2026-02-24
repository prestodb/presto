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
package com.facebook.presto.execution;

import com.facebook.airlift.units.Duration;
import com.facebook.presto.Session;
import com.facebook.presto.client.ClientSession;
import com.facebook.presto.client.Column;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.client.StatementClient;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import okhttp3.OkHttpClient;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Locale;
import java.util.Optional;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.client.StatementClientFactory.newStatementClient;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

@Test(singleThreaded = true)
public class TestPrestoClient
{
    private DistributedQueryRunner queryRunner;

    @BeforeClass
    public void setup()
            throws Exception
    {
        queryRunner = createQueryRunner(TEST_SESSION);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        if (queryRunner != null) {
            queryRunner.close();
            queryRunner = null;
        }
    }

    @Test
    public void testNormalResultsWithPrimitiveTypes()
    {
        QueryResultData result = executeQuery(
                "SELECT * FROM (VALUES (CAST(123 AS BIGINT), 'hello', true, CAST(42 AS INTEGER), CAST(3.14 AS DOUBLE))) AS t(col1, col2, col3, col4, col5)",
                false);

        assertEquals(result.rows.size(), 1);
        assertEquals(result.columns.size(), 5);
        assertEquals(result.columns.get(0).getType(), "bigint");
        assertEquals(result.columns.get(1).getType(), "varchar(5)");
        assertEquals(result.columns.get(2).getType(), "boolean");
        assertEquals(result.columns.get(3).getType(), "integer");
        assertEquals(result.columns.get(4).getType(), "double");

        List<Object> row = result.rows.get(0);
        assertEquals(row.get(0), 123L);
        assertEquals(row.get(1), "hello");
        assertEquals(row.get(2), true);
        assertEquals(row.get(3), 42);
        assertEquals(row.get(4), 3.14);
    }

    @Test
    public void testBinaryResultsWithPrimitiveTypes()
    {
        QueryResultData result = executeQuery(
                "SELECT * FROM (VALUES (CAST(123 AS BIGINT), 'hello', true, CAST(42 AS INTEGER), CAST(3.14 AS DOUBLE))) AS t(col1, col2, col3, col4, col5)",
                true);

        assertEquals(result.rows.size(), 1);
        assertEquals(result.columns.size(), 5);
        assertEquals(result.columns.get(0).getType(), "bigint");
        assertEquals(result.columns.get(1).getType(), "varchar(5)");
        assertEquals(result.columns.get(2).getType(), "boolean");
        assertEquals(result.columns.get(3).getType(), "integer");
        assertEquals(result.columns.get(4).getType(), "double");

        List<Object> row = result.rows.get(0);
        assertEquals(row.get(0), 123L);
        assertEquals(row.get(1), "hello");
        assertEquals(row.get(2), true);
        assertEquals(row.get(3), 42);
        assertEquals(row.get(4), 3.14);
    }

    @Test
    public void testNormalResultsWithMultipleRows()
    {
        QueryResultData result = executeQuery(
                "SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(id, name)",
                false);

        assertEquals(result.rows.size(), 3);
        assertEquals(result.columns.size(), 2);

        assertEquals(result.rows.get(0).get(0), 1);
        assertEquals(result.rows.get(0).get(1), "a");
        assertEquals(result.rows.get(1).get(0), 2);
        assertEquals(result.rows.get(1).get(1), "b");
        assertEquals(result.rows.get(2).get(0), 3);
        assertEquals(result.rows.get(2).get(1), "c");
    }

    @Test
    public void testBinaryResultsWithMultipleRows()
    {
        QueryResultData result = executeQuery(
                "SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(id, name)",
                true);

        assertEquals(result.rows.size(), 3);
        assertEquals(result.columns.size(), 2);

        assertEquals(result.rows.get(0).get(0), 1);
        assertEquals(result.rows.get(0).get(1), "a");
        assertEquals(result.rows.get(1).get(0), 2);
        assertEquals(result.rows.get(1).get(1), "b");
        assertEquals(result.rows.get(2).get(0), 3);
        assertEquals(result.rows.get(2).get(1), "c");
    }

    @Test
    public void testNormalResultsWithNullValues()
    {
        QueryResultData result = executeQuery(
                "SELECT CAST(NULL AS BIGINT), VARCHAR 'test', CAST(NULL AS BOOLEAN)",
                false);

        assertEquals(result.rows.size(), 1);
        assertEquals(result.columns.size(), 3);

        List<Object> row = result.rows.get(0);
        assertNull(row.get(0));
        assertEquals(row.get(1), "test");
        assertNull(row.get(2));
    }

    @Test
    public void testBinaryResultsWithNullValues()
    {
        QueryResultData result = executeQuery(
                "SELECT CAST(NULL AS BIGINT), VARCHAR 'test', CAST(NULL AS BOOLEAN)",
                true);

        assertEquals(result.rows.size(), 1);
        assertEquals(result.columns.size(), 3);

        List<Object> row = result.rows.get(0);
        assertNull(row.get(0));
        assertEquals(row.get(1), "test");
        assertNull(row.get(2));
    }

    @Test
    public void testBinaryAndNormalResultsMatch()
    {
        String query = "SELECT * FROM (VALUES (CAST(999 AS BIGINT), 'world', false, CAST(88 AS INTEGER), CAST(2.718 AS DOUBLE))) AS t(col1, col2, col3, col4, col5)";

        QueryResultData normalResult = executeQuery(query, false);
        QueryResultData binaryResult = executeQuery(query, true);

        assertEquals(normalResult.rows.size(), binaryResult.rows.size());
        assertEquals(normalResult.columns.size(), binaryResult.columns.size());

        for (int i = 0; i < normalResult.rows.size(); i++) {
            List<Object> normalRow = normalResult.rows.get(i);
            List<Object> binaryRow = binaryResult.rows.get(i);

            assertEquals(normalRow.size(), binaryRow.size());
            for (int j = 0; j < normalRow.size(); j++) {
                Object normalValue = normalRow.get(j);
                Object binaryValue = binaryRow.get(j);
                if (normalValue instanceof Double && binaryValue instanceof Double) {
                    assertEquals((Double) normalValue, (Double) binaryValue, 0.0001);
                }
                else {
                    assertEquals(normalValue, binaryValue);
                }
            }
        }
    }

    @Test
    public void testBinaryResultsWithTpchQuery()
    {
        QueryResultData result = executeQuery("SELECT COUNT(*) FROM tpch.tiny.nation", true);

        assertEquals(result.rows.size(), 1);
        assertEquals(result.columns.get(0).getType(), "bigint");
        assertEquals(result.rows.get(0).get(0), 25L);
    }

    @Test
    public void testNormalResultsWithTpchQuery()
    {
        QueryResultData result = executeQuery("SELECT COUNT(*) FROM tpch.tiny.nation", false);

        assertEquals(result.rows.size(), 1);
        assertEquals(result.columns.get(0).getType(), "bigint");
        assertEquals(result.rows.get(0).get(0), 25L);
    }

    private QueryResultData executeQuery(String sql, boolean binaryResults)
    {
        OkHttpClient httpClient = new OkHttpClient();
        try {
            ClientSession clientSession = new ClientSession(
                    queryRunner.getCoordinator().getBaseUrl(),
                    "test-user",
                    "test-source",
                    Optional.empty(),
                    ImmutableSet.of(),
                    null,
                    null,
                    null,
                    "America/Los_Angeles",
                    Locale.ENGLISH,
                    ImmutableMap.of(),
                    ImmutableMap.of(),
                    ImmutableMap.of(),
                    ImmutableMap.of(),
                    ImmutableMap.of(),
                    null,
                    new Duration(2, MINUTES),
                    false,
                    ImmutableMap.of(),
                    ImmutableMap.of(),
                    false,
                    binaryResults);

            StatementClient client = newStatementClient(httpClient, clientSession, sql);

            List<Column> columns = null;
            ImmutableList.Builder<List<Object>> rowsBuilder = ImmutableList.builder();

            while (client.isRunning()) {
                Iterable<List<Object>> data = client.currentData().getData();
                if (data != null) {
                    for (List<Object> row : data) {
                        rowsBuilder.add(row);
                    }
                }

                if (columns == null) {
                    QueryResults results = (QueryResults) client.currentStatusInfo();
                    if (results.getColumns() != null) {
                        columns = results.getColumns();
                    }
                }

                client.advance();
            }

            assertNotNull(columns, "Columns should not be null");

            ImmutableList<List<Object>> rows = rowsBuilder.build();
            return new QueryResultData(columns, rows);
        }
        finally {
            httpClient.dispatcher().executorService().shutdown();
            httpClient.connectionPool().evictAll();
        }
    }

    private static class QueryResultData
    {
        public final List<Column> columns;
        public final List<List<Object>> rows;

        public QueryResultData(List<Column> columns, List<List<Object>> rows)
        {
            this.columns = columns;
            this.rows = rows;
        }
    }

    private static DistributedQueryRunner createQueryRunner(Session session)
            throws Exception
    {
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .setNodeCount(2)
                .build();

        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");
            return queryRunner;
        }
        catch (Exception e) {
            queryRunner.close();
            throw e;
        }
    }
}
