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
package com.facebook.presto.iceberg;

import com.facebook.presto.Session;
import com.facebook.presto.common.type.JsonType;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.facebook.presto.iceberg.CatalogType.HIVE;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

/**
 * Integration tests for reading Iceberg V3 tables with a VARIANT column.
 *
 * <p>The golden table ({@code iceberg_v3/variant_type_read}) was produced by
 * Apache Spark 4.1 with the Iceberg 1.10.1 runtime.  It contains 10 rows spread
 * across <b>three</b> Parquet files to exercise distinct read patterns:
 *
 * <table>
 *   <tr><th>File</th><th>Physical row order</th><th>Pattern</th></tr>
 *   <tr><td>1</td><td>all non-null multi-row batch</td>
 *       <td>
 *         id=1 {@code {"active":true,"age":30,"user":"alice"}}<br>
 *         id=2 {@code {"age":25,"tags":["admin","dev"],"user":"bob"}}<br>
 *         id=3 {@code {"active":true,"age":35,"score":99.5,"user":"charlie"}}
 *       </td></tr>
 *   <tr><td>2</td><td>null along with non-null values</td>
 *       <td>
 *         id=4 {@code {"age":28,"scores":[95,87,92],"user":"diana"}}<br>
 *         id=8 {@code NULL}<br>
 *         id=5 {@code {"active":false,"age":42,"role":"manager","user":"reve"}}<br>
 *         id=6 {@code {"age":25,"tags":["admin","dev"],"user":"bob"}}<br>
 *         id=7 {@code {"age":28,"scores":[95,87,92],"user":"diana"}}
 *       </td></tr>
 *   <tr><td>3</td><td>all-null file</td>
 *       <td>
 *         id=9  {@code NULL}<br>
 *         id=10 {@code NULL}
 *       </td></tr>
 * </table>
 */
@Test(singleThreaded = true)
public class TestIcebergVariantTypeRead
        extends IcebergImportedTableTestBase
{
    private static final String TEST_NAME = "variant_type_read";

    private String tablePath;
    private Session session;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        session = testSessionBuilder()
                .setCatalog(CATALOGNAME)
                .setSchema(SCHEMANAME)
                .build();

        return IcebergQueryRunner.builder()
                .setCatalogType(HIVE)
                .setSchemaName(SCHEMANAME)
                .setCreateTpchTables(false)
                .build().getQueryRunner();
    }

    @BeforeMethod
    public void setup()
    {
        tablePath = setupAndRegisterTable(TEST_NAME);
    }

    @AfterMethod(alwaysRun = true)
    public void teardown()
    {
        dropAndCleanupTable(TEST_NAME, tablePath);
    }

    @Test
    public void testVariantColumnIsJson()
    {
        String query = format("SELECT data FROM %s.%s.%s LIMIT 1", CATALOGNAME, SCHEMANAME, TEST_NAME);
        MaterializedResult result = computeActual(session, query);
        assertEquals(result.getTypes().get(0), JsonType.JSON);
    }

    @Test
    public void testVariantSchema()
    {
        String query = format("SHOW COLUMNS FROM %s.%s.%s", CATALOGNAME, SCHEMANAME, TEST_NAME);
        MaterializedResult result = computeActual(session, query);
        assertEquals(result.getMaterializedRows().size(), 2);
        assertEquals(result.getMaterializedRows().get(0).getField(0), "id");
        assertEquals(result.getMaterializedRows().get(0).getField(1), "integer");
        assertEquals(result.getMaterializedRows().get(1).getField(0), "data");
        assertEquals(result.getMaterializedRows().get(1).getField(1), "json");
    }

    @Test
    public void testVariantSelectAll()
    {
        String query = format(
                "SELECT id, data FROM %s.%s.%s ORDER BY id",
                CATALOGNAME, SCHEMANAME, TEST_NAME);
        MaterializedResult result = computeActual(session, query);
        assertEquals(result.getMaterializedRows().size(), 10);

        assertEquals(result.getMaterializedRows().get(0).getField(0), 1);
        assertEquals(result.getMaterializedRows().get(0).getField(1), "{\"active\":true,\"age\":30,\"user\":\"alice\"}");

        assertEquals(result.getMaterializedRows().get(1).getField(0), 2);
        assertEquals(result.getMaterializedRows().get(1).getField(1), "{\"age\":25,\"tags\":[\"admin\",\"dev\"],\"user\":\"bob\"}");

        assertEquals(result.getMaterializedRows().get(2).getField(0), 3);
        assertEquals(result.getMaterializedRows().get(2).getField(1), "{\"active\":true,\"age\":35,\"score\":99.5,\"user\":\"charlie\"}");

        assertEquals(result.getMaterializedRows().get(3).getField(0), 4);
        assertEquals(result.getMaterializedRows().get(3).getField(1), "{\"age\":28,\"scores\":[95,87,92],\"user\":\"diana\"}");

        assertEquals(result.getMaterializedRows().get(4).getField(0), 5);
        assertEquals(result.getMaterializedRows().get(4).getField(1), "{\"active\":false,\"age\":42,\"role\":\"manager\",\"user\":\"reve\"}");

        assertEquals(result.getMaterializedRows().get(5).getField(0), 6);
        assertEquals(result.getMaterializedRows().get(5).getField(1), "{\"age\":25,\"tags\":[\"admin\",\"dev\"],\"user\":\"bob\"}");

        assertEquals(result.getMaterializedRows().get(6).getField(0), 7);
        assertEquals(result.getMaterializedRows().get(6).getField(1), "{\"age\":28,\"scores\":[95,87,92],\"user\":\"diana\"}");

        assertEquals(result.getMaterializedRows().get(7).getField(0), 8);
        assertNull(result.getMaterializedRows().get(7).getField(1));

        assertEquals(result.getMaterializedRows().get(8).getField(0), 9);
        assertNull(result.getMaterializedRows().get(8).getField(1));

        assertEquals(result.getMaterializedRows().get(9).getField(0), 10);
        assertNull(result.getMaterializedRows().get(9).getField(1));
    }

    @Test
    public void testVariantCount()
    {
        String query = format("SELECT COUNT(*) FROM %s.%s.%s", CATALOGNAME, SCHEMANAME, TEST_NAME);
        MaterializedResult result = computeActual(session, query);
        assertEquals(result.getMaterializedRows().get(0).getField(0), 10L);
    }

    @Test
    public void testVariantNullRows()
    {
        String query = format(
                "SELECT id, data FROM %s.%s.%s WHERE id IN (8, 9, 10) ORDER BY id",
                CATALOGNAME, SCHEMANAME, TEST_NAME);
        MaterializedResult result = computeActual(session, query);
        assertEquals(result.getMaterializedRows().size(), 3);
        assertEquals(result.getMaterializedRows().get(0).getField(0), 8);
        assertNull(result.getMaterializedRows().get(0).getField(1));
        assertEquals(result.getMaterializedRows().get(1).getField(0), 9);
        assertNull(result.getMaterializedRows().get(1).getField(1));
        assertEquals(result.getMaterializedRows().get(2).getField(0), 10);
        assertNull(result.getMaterializedRows().get(2).getField(1));
    }

    @Test
    public void testVariantJsonExtractScalar()
    {
        String query = format(
                "SELECT id,"
                        + " json_extract_scalar(data, '$.user') AS user_name,"
                        + " CAST(json_extract_scalar(data, '$.age') AS INTEGER) AS age,"
                        + " CAST(json_extract_scalar(data, '$.active') AS BOOLEAN) AS is_active"
                        + " FROM %s.%s.%s ORDER BY id",
                CATALOGNAME, SCHEMANAME, TEST_NAME);
        MaterializedResult result = computeActual(session, query);
        assertEquals(result.getMaterializedRows().size(), 10);

        MaterializedRow alice = result.getMaterializedRows().get(0);
        assertEquals(alice.getField(0), 1);
        assertEquals(alice.getField(1), "alice");
        assertEquals(alice.getField(2), 30);
        assertEquals(alice.getField(3), true);

        MaterializedRow bob = result.getMaterializedRows().get(1);
        assertEquals(bob.getField(0), 2);
        assertEquals(bob.getField(1), "bob");
        assertEquals(bob.getField(2), 25);
        assertNull(bob.getField(3));

        MaterializedRow charlie = result.getMaterializedRows().get(2);
        assertEquals(charlie.getField(0), 3);
        assertEquals(charlie.getField(1), "charlie");
        assertEquals(charlie.getField(2), 35);
        assertEquals(charlie.getField(3), true);

        MaterializedRow diana = result.getMaterializedRows().get(3);
        assertEquals(diana.getField(0), 4);
        assertEquals(diana.getField(1), "diana");
        assertEquals(diana.getField(2), 28);
        assertNull(diana.getField(3));

        MaterializedRow reve = result.getMaterializedRows().get(4);
        assertEquals(reve.getField(0), 5);
        assertEquals(reve.getField(1), "reve");
        assertEquals(reve.getField(2), 42);
        assertEquals(reve.getField(3), false);

        MaterializedRow bob2 = result.getMaterializedRows().get(5);
        assertEquals(bob2.getField(0), 6);
        assertEquals(bob2.getField(1), "bob");
        assertEquals(bob2.getField(2), 25);
        assertNull(bob2.getField(3));

        MaterializedRow diana2 = result.getMaterializedRows().get(6);
        assertEquals(diana2.getField(0), 7);
        assertEquals(diana2.getField(1), "diana");
        assertEquals(diana2.getField(2), 28);
        assertNull(diana2.getField(3));

        for (int i = 7; i < 10; i++) {
            MaterializedRow nullRow = result.getMaterializedRows().get(i);
            assertNull(nullRow.getField(1));
            assertNull(nullRow.getField(2));
            assertNull(nullRow.getField(3));
        }
    }

    @Test
    public void testVariantJsonExtractArrays()
    {
        String query = format(
                "SELECT id,"
                        + " json_extract_scalar(data, '$.user') AS user_name,"
                        + " json_extract(data, '$.tags') AS all_tags,"
                        + " json_extract(data, '$.scores') AS all_scores"
                        + " FROM %s.%s.%s ORDER BY id",
                CATALOGNAME, SCHEMANAME, TEST_NAME);
        MaterializedResult result = computeActual(session, query);
        assertEquals(result.getMaterializedRows().size(), 10);

        assertEquals(result.getMaterializedRows().get(0).getField(1), "alice");
        assertNull(result.getMaterializedRows().get(0).getField(2));
        assertNull(result.getMaterializedRows().get(0).getField(3));

        assertEquals(result.getMaterializedRows().get(1).getField(1), "bob");
        assertEquals(result.getMaterializedRows().get(1).getField(2), "[\"admin\",\"dev\"]");
        assertNull(result.getMaterializedRows().get(1).getField(3));

        assertEquals(result.getMaterializedRows().get(2).getField(1), "charlie");
        assertNull(result.getMaterializedRows().get(2).getField(2));
        assertNull(result.getMaterializedRows().get(2).getField(3));

        assertEquals(result.getMaterializedRows().get(3).getField(1), "diana");
        assertNull(result.getMaterializedRows().get(3).getField(2));
        assertEquals(result.getMaterializedRows().get(3).getField(3), "[95,87,92]");

        assertEquals(result.getMaterializedRows().get(5).getField(1), "bob");
        assertEquals(result.getMaterializedRows().get(5).getField(2), "[\"admin\",\"dev\"]");
        assertNull(result.getMaterializedRows().get(5).getField(3));

        assertEquals(result.getMaterializedRows().get(6).getField(1), "diana");
        assertNull(result.getMaterializedRows().get(6).getField(2));
        assertEquals(result.getMaterializedRows().get(6).getField(3), "[95,87,92]");

        for (int i = 7; i < 10; i++) {
            assertNull(result.getMaterializedRows().get(i).getField(1));
            assertNull(result.getMaterializedRows().get(i).getField(2));
            assertNull(result.getMaterializedRows().get(i).getField(3));
        }
    }

    @Test
    public void testVariantUnnestTags()
    {
        String query = format(
                "SELECT id,"
                        + " json_extract_scalar(data, '$.user') AS user_name,"
                        + " tag"
                        + " FROM %s.%s.%s"
                        + " CROSS JOIN UNNEST(CAST(json_extract(data, '$.tags') AS ARRAY(VARCHAR))) AS t(tag)"
                        + " ORDER BY id, tag",
                CATALOGNAME, SCHEMANAME, TEST_NAME);
        MaterializedResult result = computeActual(session, query);

        assertEquals(result.getMaterializedRows().size(), 4);

        assertEquals(result.getMaterializedRows().get(0).getField(0), 2);
        assertEquals(result.getMaterializedRows().get(0).getField(1), "bob");
        assertEquals(result.getMaterializedRows().get(0).getField(2), "admin");

        assertEquals(result.getMaterializedRows().get(1).getField(0), 2);
        assertEquals(result.getMaterializedRows().get(1).getField(1), "bob");
        assertEquals(result.getMaterializedRows().get(1).getField(2), "dev");

        assertEquals(result.getMaterializedRows().get(2).getField(0), 6);
        assertEquals(result.getMaterializedRows().get(2).getField(1), "bob");
        assertEquals(result.getMaterializedRows().get(2).getField(2), "admin");

        assertEquals(result.getMaterializedRows().get(3).getField(0), 6);
        assertEquals(result.getMaterializedRows().get(3).getField(1), "bob");
        assertEquals(result.getMaterializedRows().get(3).getField(2), "dev");
    }

    @Test
    public void testVariantUnnestScores()
    {
        String query = format(
                "SELECT id,"
                        + " json_extract_scalar(data, '$.user') AS user_name,"
                        + " CAST(score AS INTEGER) AS score"
                        + " FROM %s.%s.%s"
                        + " CROSS JOIN UNNEST(CAST(json_extract(data, '$.scores') AS ARRAY(VARCHAR))) AS t(score)"
                        + " ORDER BY id, score",
                CATALOGNAME, SCHEMANAME, TEST_NAME);
        MaterializedResult result = computeActual(session, query);

        assertEquals(result.getMaterializedRows().size(), 6);

        int[] expectedScores = {87, 92, 95};
        for (int occurrence = 0; occurrence < 2; occurrence++) {
            int expectedId = occurrence == 0 ? 4 : 7;
            for (int s = 0; s < 3; s++) {
                MaterializedRow row = result.getMaterializedRows().get(occurrence * 3 + s);
                assertEquals(row.getField(0), expectedId);
                assertEquals(row.getField(1), "diana");
                assertEquals(row.getField(2), expectedScores[s]);
            }
        }
    }

    @Test
    public void testVariantFilter()
    {
        String query = format(
                "SELECT id FROM %s.%s.%s"
                        + " WHERE CAST(json_extract_scalar(data, '$.age') AS INTEGER) > 28"
                        + " ORDER BY id",
                CATALOGNAME, SCHEMANAME, TEST_NAME);
        MaterializedResult result = computeActual(session, query);
        assertEquals(result.getMaterializedRows().size(), 3);
        assertEquals(result.getMaterializedRows().get(0).getField(0), 1);
        assertEquals(result.getMaterializedRows().get(1).getField(0), 3);
        assertEquals(result.getMaterializedRows().get(2).getField(0), 5);
    }

    @Test
    public void testVariantFilterBoolean()
    {
        String query = format(
                "SELECT id, json_extract_scalar(data, '$.user') AS user_name"
                        + " FROM %s.%s.%s"
                        + " WHERE CAST(json_extract_scalar(data, '$.active') AS BOOLEAN) = true"
                        + " ORDER BY id",
                CATALOGNAME, SCHEMANAME, TEST_NAME);
        MaterializedResult result = computeActual(session, query);
        assertEquals(result.getMaterializedRows().size(), 2);
        assertEquals(result.getMaterializedRows().get(0).getField(0), 1);
        assertEquals(result.getMaterializedRows().get(0).getField(1), "alice");
        assertEquals(result.getMaterializedRows().get(1).getField(0), 3);
        assertEquals(result.getMaterializedRows().get(1).getField(1), "charlie");
    }

    @Test
    public void testVariantAggregationCount()
    {
        String query = format(
                "SELECT json_extract_scalar(data, '$.user') AS user_name,"
                        + " COUNT(*) AS cnt"
                        + " FROM %s.%s.%s"
                        + " GROUP BY json_extract_scalar(data, '$.user')"
                        + " ORDER BY user_name NULLS LAST",
                CATALOGNAME, SCHEMANAME, TEST_NAME);
        MaterializedResult result = computeActual(session, query);

        assertEquals(result.getMaterializedRows().size(), 6);

        assertEquals(result.getMaterializedRows().get(0).getField(0), "alice");
        assertEquals(result.getMaterializedRows().get(0).getField(1), 1L);

        assertEquals(result.getMaterializedRows().get(1).getField(0), "bob");
        assertEquals(result.getMaterializedRows().get(1).getField(1), 2L);

        assertEquals(result.getMaterializedRows().get(2).getField(0), "charlie");
        assertEquals(result.getMaterializedRows().get(2).getField(1), 1L);

        assertEquals(result.getMaterializedRows().get(3).getField(0), "diana");
        assertEquals(result.getMaterializedRows().get(3).getField(1), 2L);

        assertEquals(result.getMaterializedRows().get(4).getField(0), "reve");
        assertEquals(result.getMaterializedRows().get(4).getField(1), 1L);

        assertNull(result.getMaterializedRows().get(5).getField(0));
        assertEquals(result.getMaterializedRows().get(5).getField(1), 3L);
    }

    @Test
    public void testVariantAggregationAverage()
    {
        String query = format(
                "SELECT json_extract_scalar(data, '$.user') AS user_name,"
                        + " AVG(CAST(json_extract_scalar(data, '$.age') AS INTEGER)) AS avg_age,"
                        + " MIN(CAST(json_extract_scalar(data, '$.age') AS INTEGER)) AS min_age,"
                        + " MAX(CAST(json_extract_scalar(data, '$.age') AS INTEGER)) AS max_age"
                        + " FROM %s.%s.%s"
                        + " GROUP BY json_extract_scalar(data, '$.user')"
                        + " ORDER BY user_name NULLS LAST",
                CATALOGNAME, SCHEMANAME, TEST_NAME);
        MaterializedResult result = computeActual(session, query);

        assertEquals(result.getMaterializedRows().size(), 6);

        assertEquals(result.getMaterializedRows().get(0).getField(0), "alice");
        assertEquals(result.getMaterializedRows().get(0).getField(1), 30.0);
        assertEquals(result.getMaterializedRows().get(0).getField(2), 30);
        assertEquals(result.getMaterializedRows().get(0).getField(3), 30);

        assertEquals(result.getMaterializedRows().get(1).getField(0), "bob");
        assertEquals(result.getMaterializedRows().get(1).getField(1), 25.0);
        assertEquals(result.getMaterializedRows().get(1).getField(2), 25);
        assertEquals(result.getMaterializedRows().get(1).getField(3), 25);

        assertEquals(result.getMaterializedRows().get(2).getField(0), "charlie");
        assertEquals(result.getMaterializedRows().get(2).getField(1), 35.0);
        assertEquals(result.getMaterializedRows().get(2).getField(2), 35);
        assertEquals(result.getMaterializedRows().get(2).getField(3), 35);

        assertEquals(result.getMaterializedRows().get(3).getField(0), "diana");
        assertEquals(result.getMaterializedRows().get(3).getField(1), 28.0);
        assertEquals(result.getMaterializedRows().get(3).getField(2), 28);
        assertEquals(result.getMaterializedRows().get(3).getField(3), 28);

        assertEquals(result.getMaterializedRows().get(4).getField(0), "reve");
        assertEquals(result.getMaterializedRows().get(4).getField(1), 42.0);
        assertEquals(result.getMaterializedRows().get(4).getField(2), 42);
        assertEquals(result.getMaterializedRows().get(4).getField(3), 42);

        assertNull(result.getMaterializedRows().get(5).getField(0));
        assertNull(result.getMaterializedRows().get(5).getField(1));
        assertNull(result.getMaterializedRows().get(5).getField(2));
        assertNull(result.getMaterializedRows().get(5).getField(3));
    }

    @Test
    public void testVariantGroupByWithHaving()
    {
        String query = format(
                "SELECT json_extract_scalar(data, '$.user') AS user_name,"
                        + " COUNT(*) AS cnt"
                        + " FROM %s.%s.%s"
                        + " GROUP BY json_extract_scalar(data, '$.user')"
                        + " HAVING COUNT(*) > 1"
                        + " ORDER BY user_name",
                CATALOGNAME, SCHEMANAME, TEST_NAME);
        MaterializedResult result = computeActual(session, query);

        assertEquals(result.getMaterializedRows().size(), 3);

        assertEquals(result.getMaterializedRows().get(0).getField(0), "bob");
        assertEquals(result.getMaterializedRows().get(0).getField(1), 2L);

        assertEquals(result.getMaterializedRows().get(1).getField(0), "diana");
        assertEquals(result.getMaterializedRows().get(1).getField(1), 2L);

        assertNull(result.getMaterializedRows().get(2).getField(0));
        assertEquals(result.getMaterializedRows().get(2).getField(1), 3L);
    }

    @Test
    public void testVariantCountDistinct()
    {
        String query = format(
                "SELECT COUNT(DISTINCT json_extract_scalar(data, '$.user')) AS unique_users,"
                        + " COUNT(DISTINCT CAST(json_extract_scalar(data, '$.age') AS INTEGER)) AS unique_ages"
                        + " FROM %s.%s.%s",
                CATALOGNAME, SCHEMANAME, TEST_NAME);
        MaterializedResult result = computeActual(session, query);

        assertEquals(result.getMaterializedRows().size(), 1);
        assertEquals(result.getMaterializedRows().get(0).getField(0), 5L);
        assertEquals(result.getMaterializedRows().get(0).getField(1), 5L);
    }

    @Test
    public void testVariantSelfJoin()
    {
        String query = format(
                "SELECT t1.id AS id1, json_extract_scalar(t1.data, '$.user') AS user1,"
                        + " t2.id AS id2, json_extract_scalar(t2.data, '$.user') AS user2"
                        + " FROM %s.%s.%s t1"
                        + " JOIN %s.%s.%s t2"
                        + "   ON json_extract_scalar(t1.data, '$.user') = json_extract_scalar(t2.data, '$.user')"
                        + "   AND t1.id < t2.id"
                        + " ORDER BY t1.id, t2.id",
                CATALOGNAME, SCHEMANAME, TEST_NAME,
                CATALOGNAME, SCHEMANAME, TEST_NAME);
        MaterializedResult result = computeActual(session, query);

        assertEquals(result.getMaterializedRows().size(), 2);

        assertEquals(result.getMaterializedRows().get(0).getField(0), 2);
        assertEquals(result.getMaterializedRows().get(0).getField(1), "bob");
        assertEquals(result.getMaterializedRows().get(0).getField(2), 6);
        assertEquals(result.getMaterializedRows().get(0).getField(3), "bob");

        assertEquals(result.getMaterializedRows().get(1).getField(0), 4);
        assertEquals(result.getMaterializedRows().get(1).getField(1), "diana");
        assertEquals(result.getMaterializedRows().get(1).getField(2), 7);
        assertEquals(result.getMaterializedRows().get(1).getField(3), "diana");
    }

    @Test
    public void testVariantJoinOnAge()
    {
        String query = format(
                "SELECT t1.id AS id1, json_extract_scalar(t1.data, '$.user') AS user1,"
                        + " t2.id AS id2, json_extract_scalar(t2.data, '$.user') AS user2,"
                        + " CAST(json_extract_scalar(t1.data, '$.age') AS INTEGER) AS age"
                        + " FROM %s.%s.%s t1"
                        + " JOIN %s.%s.%s t2"
                        + "   ON json_extract_scalar(t1.data, '$.age') = json_extract_scalar(t2.data, '$.age')"
                        + "   AND t1.id < t2.id"
                        + " ORDER BY age, t1.id",
                CATALOGNAME, SCHEMANAME, TEST_NAME,
                CATALOGNAME, SCHEMANAME, TEST_NAME);
        MaterializedResult result = computeActual(session, query);

        assertEquals(result.getMaterializedRows().size(), 2);

        assertEquals(result.getMaterializedRows().get(0).getField(0), 2);
        assertEquals(result.getMaterializedRows().get(0).getField(1), "bob");
        assertEquals(result.getMaterializedRows().get(0).getField(2), 6);
        assertEquals(result.getMaterializedRows().get(0).getField(3), "bob");
        assertEquals(result.getMaterializedRows().get(0).getField(4), 25);

        assertEquals(result.getMaterializedRows().get(1).getField(0), 4);
        assertEquals(result.getMaterializedRows().get(1).getField(1), "diana");
        assertEquals(result.getMaterializedRows().get(1).getField(2), 7);
        assertEquals(result.getMaterializedRows().get(1).getField(3), "diana");
        assertEquals(result.getMaterializedRows().get(1).getField(4), 28);
    }
}
