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
package com.facebook.presto.delta;

import com.facebook.presto.Session;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import org.testng.annotations.Test;

import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

/**
 * Integration tests for reading Delta tables whose {@code data} column has type {@code variant}.
 *
 * <p>The {@code test_variant} table (delta_v3/test_variant) consists of three Parquet files:
 *
 * <p><b>File 1</b> — {@code part-00000-0ff28d95-...c000.snappy.parquet} (5 non-null rows):
 * <pre>
 *   id=1:  {"active":true,  "age":30, "user":"alice"}
 *   id=2:  {"age":25, "tags":["admin","dev"], "user":"bob"}
 *   id=3:  {"address":{"city":"NYC","zip":"10001"}, "age":35, "user":"charlie"}
 *   id=4:  {"age":28, "scores":[95,87,92], "user":"diana"}
 *   id=5:  {"active":false, "age":42, "role":"manager", "user":"eve"}
 * </pre>
 *
 * <p><b>File 2</b> — {@code part-00001-variant-mixed-null.c000.snappy.parquet} (3 non-null + 2 NULL rows):
 * <pre>
 *   id=6:  {"active":false, "age":22, "user":"frank"}
 *   id=7:  NULL
 *   id=8:  {"age":25, "tags":["admin","dev"], "user":"bob"}   (bob repeated)
 *   id=9:  NULL
 *   id=10: {"age":28, "scores":[95,87,92], "user":"diana"}   (diana repeated)
 * </pre>
 *
 * <p><b>File 3</b> — {@code part-00002-variant-all-null.c000.snappy.parquet} (all NULL rows):
 * <pre>
 *   id=11: NULL
 *   id=12: NULL
 * </pre>
 *
 * <p>Total: 12 rows — 8 non-null variants and 4 NULLs.
 * The repeated bob (age=25, tags) and diana (age=28, scores) rows make aggregate
 * and join tests produce non-trivial results.
 */
public class TestDeltaVariantType
        extends AbstractDeltaDistributedQueryTestBase
{
    @Test
    public void testVariantTypeSchema()
    {
        Session session = Session.builder(getSession()).build();
        String query = format("SHOW COLUMNS FROM \"%s\".\"%s\"", PATH_SCHEMA,
                goldenTablePathWithPrefix(DELTA_V3, "test_variant"));
        MaterializedResult result = computeActual(session, query);
        assertEquals(result.getMaterializedRows().size(), 2);
        assertEquals(result.getMaterializedRows().get(0).getField(0), "id");
        assertEquals(result.getMaterializedRows().get(1).getField(0), "data");
    }

    @Test
    public void testVariantTypeSelectAll()
    {
        Session session = Session.builder(getSession()).build();
        String query = format("SELECT * FROM \"%s\".\"%s\" ORDER BY id", PATH_SCHEMA,
                goldenTablePathWithPrefix(DELTA_V3, "test_variant"));
        MaterializedResult result = computeActual(session, query);
        // 12 rows total: 5 non-null (file 1) + 3 non-null + 2 null (file 2) + 2 null (file 3)
        assertEquals(result.getMaterializedRows().size(), 12);

        // File 1 — 5 non-null rows
        assertEquals(result.getMaterializedRows().get(0).getField(0), 1);
        assertEquals(result.getMaterializedRows().get(0).getField(1), "{\"active\":true,\"age\":30,\"user\":\"alice\"}");

        assertEquals(result.getMaterializedRows().get(1).getField(0), 2);
        assertEquals(result.getMaterializedRows().get(1).getField(1), "{\"age\":25,\"tags\":[\"admin\",\"dev\"],\"user\":\"bob\"}");

        assertEquals(result.getMaterializedRows().get(2).getField(0), 3);
        assertEquals(result.getMaterializedRows().get(2).getField(1), "{\"address\":{\"city\":\"NYC\",\"zip\":\"10001\"},\"age\":35,\"user\":\"charlie\"}");

        assertEquals(result.getMaterializedRows().get(3).getField(0), 4);
        assertEquals(result.getMaterializedRows().get(3).getField(1), "{\"age\":28,\"scores\":[95,87,92],\"user\":\"diana\"}");

        assertEquals(result.getMaterializedRows().get(4).getField(0), 5);
        assertEquals(result.getMaterializedRows().get(4).getField(1), "{\"active\":false,\"age\":42,\"role\":\"manager\",\"user\":\"eve\"}");

        // File 2 — non-null, NULL, non-null (bob repeat), NULL, non-null (diana repeat)
        assertEquals(result.getMaterializedRows().get(5).getField(0), 6);
        assertEquals(result.getMaterializedRows().get(5).getField(1), "{\"active\":false,\"age\":22,\"user\":\"frank\"}");

        assertEquals(result.getMaterializedRows().get(6).getField(0), 7);
        assertNull(result.getMaterializedRows().get(6).getField(1));

        assertEquals(result.getMaterializedRows().get(7).getField(0), 8);
        assertEquals(result.getMaterializedRows().get(7).getField(1), "{\"age\":25,\"tags\":[\"admin\",\"dev\"],\"user\":\"bob\"}");

        assertEquals(result.getMaterializedRows().get(8).getField(0), 9);
        assertNull(result.getMaterializedRows().get(8).getField(1));

        assertEquals(result.getMaterializedRows().get(9).getField(0), 10);
        assertEquals(result.getMaterializedRows().get(9).getField(1), "{\"age\":28,\"scores\":[95,87,92],\"user\":\"diana\"}");

        // File 3 — all NULL
        assertEquals(result.getMaterializedRows().get(10).getField(0), 11);
        assertNull(result.getMaterializedRows().get(10).getField(1));

        assertEquals(result.getMaterializedRows().get(11).getField(0), 12);
        assertNull(result.getMaterializedRows().get(11).getField(1));
    }

    @Test
    public void testVariantTypeCount()
    {
        Session session = Session.builder(getSession()).build();
        String query = format("SELECT COUNT(*) FROM \"%s\".\"%s\"", PATH_SCHEMA,
                goldenTablePathWithPrefix(DELTA_V3, "test_variant"));
        MaterializedResult result = computeActual(session, query);
        assertEquals(result.getMaterializedRows().size(), 1);
        assertEquals(result.getMaterializedRows().get(0).getField(0), 12L);
    }

    @Test
    public void testVariantTypeJsonExtractScalar()
    {
        Session session = Session.builder(getSession()).build();
        String query = format(
                "SELECT " +
                "    id, " +
                "    json_extract_scalar(data, '$.user') AS user_name, " +
                "    CAST(json_extract_scalar(data, '$.age') AS INTEGER) AS age, " +
                "    CAST(json_extract_scalar(data, '$.active') AS BOOLEAN) AS is_active " +
                "FROM \"%s\".\"%s\" ORDER BY id",
                PATH_SCHEMA,
                goldenTablePathWithPrefix(DELTA_V3, "test_variant"));
        MaterializedResult result = computeActual(session, query);

        // 12 total rows; json_extract_scalar on NULL variant rows returns NULL for all fields
        assertEquals(result.getMaterializedRows().size(), 12);

        // File 1 — non-null rows (id=1..5)
        MaterializedRow row1 = result.getMaterializedRows().get(0);
        assertEquals(row1.getField(0), 1);
        assertEquals(row1.getField(1), "alice");
        assertEquals(row1.getField(2), 30);
        assertEquals(row1.getField(3), true);

        MaterializedRow row2 = result.getMaterializedRows().get(1);
        assertEquals(row2.getField(0), 2);
        assertEquals(row2.getField(1), "bob");
        assertEquals(row2.getField(2), 25);
        assertNull(row2.getField(3));

        MaterializedRow row3 = result.getMaterializedRows().get(2);
        assertEquals(row3.getField(0), 3);
        assertEquals(row3.getField(1), "charlie");
        assertEquals(row3.getField(2), 35);
        assertNull(row3.getField(3));

        MaterializedRow row4 = result.getMaterializedRows().get(3);
        assertEquals(row4.getField(0), 4);
        assertEquals(row4.getField(1), "diana");
        assertEquals(row4.getField(2), 28);
        assertNull(row4.getField(3));

        MaterializedRow row5 = result.getMaterializedRows().get(4);
        assertEquals(row5.getField(0), 5);
        assertEquals(row5.getField(1), "eve");
        assertEquals(row5.getField(2), 42);
        assertEquals(row5.getField(3), false);

        // File 2 — frank (non-null), NULL, bob repeat (non-null), NULL, diana repeat (non-null)
        MaterializedRow row6 = result.getMaterializedRows().get(5);
        assertEquals(row6.getField(0), 6);
        assertEquals(row6.getField(1), "frank");
        assertEquals(row6.getField(2), 22);
        assertEquals(row6.getField(3), false);

        MaterializedRow row7 = result.getMaterializedRows().get(6);
        assertEquals(row7.getField(0), 7);
        assertNull(row7.getField(1));
        assertNull(row7.getField(2));
        assertNull(row7.getField(3));

        MaterializedRow row8 = result.getMaterializedRows().get(7);
        assertEquals(row8.getField(0), 8);
        assertEquals(row8.getField(1), "bob");
        assertEquals(row8.getField(2), 25);
        assertNull(row8.getField(3));

        MaterializedRow row9 = result.getMaterializedRows().get(8);
        assertEquals(row9.getField(0), 9);
        assertNull(row9.getField(1));
        assertNull(row9.getField(2));
        assertNull(row9.getField(3));

        MaterializedRow row10 = result.getMaterializedRows().get(9);
        assertEquals(row10.getField(0), 10);
        assertEquals(row10.getField(1), "diana");
        assertEquals(row10.getField(2), 28);
        assertNull(row10.getField(3));

        // File 3 — all NULL (id=11..12)
        MaterializedRow row11 = result.getMaterializedRows().get(10);
        assertEquals(row11.getField(0), 11);
        assertNull(row11.getField(1));
        assertNull(row11.getField(2));
        assertNull(row11.getField(3));

        MaterializedRow row12 = result.getMaterializedRows().get(11);
        assertEquals(row12.getField(0), 12);
        assertNull(row12.getField(1));
        assertNull(row12.getField(2));
        assertNull(row12.getField(3));
    }

    @Test
    public void testVariantTypeJsonExtractArrays()
    {
        Session session = Session.builder(getSession()).build();
        String query = format(
                "SELECT " +
                "    id, " +
                "    json_extract_scalar(data, '$.user') AS user_name, " +
                "    json_extract_scalar(data, '$.age') AS age, " +
                "    json_extract_scalar(data, '$.active') AS active, " +
                "    json_extract(data, '$.tags') AS all_tags, " +
                "    json_extract(data, '$.scores') AS all_scores " +
                "FROM \"%s\".\"%s\" ORDER BY id",
                PATH_SCHEMA,
                goldenTablePathWithPrefix(DELTA_V3, "test_variant"));
        MaterializedResult result = computeActual(session, query);

        // 12 total; json_extract on NULL variant returns NULL for all extracted fields
        assertEquals(result.getMaterializedRows().size(), 12);

        // File 1 — id=1..5, non-null
        MaterializedRow row1 = result.getMaterializedRows().get(0);
        assertEquals(row1.getField(0), 1);
        assertEquals(row1.getField(1), "alice");
        assertEquals(row1.getField(2), "30");
        assertEquals(row1.getField(3), "true");
        assertNull(row1.getField(4));
        assertNull(row1.getField(5));

        MaterializedRow row2 = result.getMaterializedRows().get(1);
        assertEquals(row2.getField(0), 2);
        assertEquals(row2.getField(1), "bob");
        assertEquals(row2.getField(2), "25");
        assertNull(row2.getField(3));
        assertEquals(row2.getField(4), "[\"admin\",\"dev\"]");
        assertNull(row2.getField(5));

        MaterializedRow row3 = result.getMaterializedRows().get(2);
        assertEquals(row3.getField(0), 3);
        assertEquals(row3.getField(1), "charlie");
        assertEquals(row3.getField(2), "35");
        assertNull(row3.getField(3));
        assertNull(row3.getField(4));
        assertNull(row3.getField(5));

        MaterializedRow row4 = result.getMaterializedRows().get(3);
        assertEquals(row4.getField(0), 4);
        assertEquals(row4.getField(1), "diana");
        assertEquals(row4.getField(2), "28");
        assertNull(row4.getField(3));
        assertNull(row4.getField(4));
        assertEquals(row4.getField(5), "[95,87,92]");

        MaterializedRow row5 = result.getMaterializedRows().get(4);
        assertEquals(row5.getField(0), 5);
        assertEquals(row5.getField(1), "eve");
        assertEquals(row5.getField(2), "42");
        assertEquals(row5.getField(3), "false");
        assertNull(row5.getField(4));
        assertNull(row5.getField(5));

        // File 2 — id=6 frank, id=7 NULL, id=8 bob, id=9 NULL, id=10 diana
        MaterializedRow row6 = result.getMaterializedRows().get(5);
        assertEquals(row6.getField(0), 6);
        assertEquals(row6.getField(1), "frank");
        assertEquals(row6.getField(2), "22");
        assertEquals(row6.getField(3), "false");
        assertNull(row6.getField(4));
        assertNull(row6.getField(5));

        MaterializedRow row7 = result.getMaterializedRows().get(6);
        assertEquals(row7.getField(0), 7);
        assertNull(row7.getField(1));
        assertNull(row7.getField(2));
        assertNull(row7.getField(3));
        assertNull(row7.getField(4));
        assertNull(row7.getField(5));

        MaterializedRow row8 = result.getMaterializedRows().get(7);
        assertEquals(row8.getField(0), 8);
        assertEquals(row8.getField(1), "bob");
        assertEquals(row8.getField(2), "25");
        assertNull(row8.getField(3));
        assertEquals(row8.getField(4), "[\"admin\",\"dev\"]");
        assertNull(row8.getField(5));

        MaterializedRow row9 = result.getMaterializedRows().get(8);
        assertEquals(row9.getField(0), 9);
        assertNull(row9.getField(1));
        assertNull(row9.getField(2));
        assertNull(row9.getField(3));
        assertNull(row9.getField(4));
        assertNull(row9.getField(5));

        MaterializedRow row10 = result.getMaterializedRows().get(9);
        assertEquals(row10.getField(0), 10);
        assertEquals(row10.getField(1), "diana");
        assertEquals(row10.getField(2), "28");
        assertNull(row10.getField(3));
        assertNull(row10.getField(4));
        assertEquals(row10.getField(5), "[95,87,92]");

        // File 3 — id=11..12, all NULL
        MaterializedRow row11 = result.getMaterializedRows().get(10);
        assertEquals(row11.getField(0), 11);
        assertNull(row11.getField(1));
        assertNull(row11.getField(2));
        assertNull(row11.getField(3));
        assertNull(row11.getField(4));
        assertNull(row11.getField(5));

        MaterializedRow row12 = result.getMaterializedRows().get(11);
        assertEquals(row12.getField(0), 12);
        assertNull(row12.getField(1));
        assertNull(row12.getField(2));
        assertNull(row12.getField(3));
        assertNull(row12.getField(4));
        assertNull(row12.getField(5));
    }

    @Test
    public void testVariantTypeUnnestTags()
    {
        Session session = Session.builder(getSession()).build();
        String query = format(
                "SELECT " +
                "    id, " +
                "    json_extract_scalar(data, '$.user') AS user_name, " +
                "    tag " +
                "FROM \"%s\".\"%s\" " +
                "CROSS JOIN UNNEST(CAST(json_extract(data, '$.tags') AS ARRAY(VARCHAR))) AS t(tag) " +
                "ORDER BY id, tag",
                PATH_SCHEMA,
                goldenTablePathWithPrefix(DELTA_V3, "test_variant"));
        MaterializedResult result = computeActual(session, query);

        // id=2 (bob) and id=8 (bob repeat) both have tags — 2 tags × 2 rows = 4
        assertEquals(result.getMaterializedRows().size(), 4);

        assertEquals(result.getMaterializedRows().get(0).getField(0), 2);
        assertEquals(result.getMaterializedRows().get(0).getField(1), "bob");
        assertEquals(result.getMaterializedRows().get(0).getField(2), "admin");

        assertEquals(result.getMaterializedRows().get(1).getField(0), 2);
        assertEquals(result.getMaterializedRows().get(1).getField(1), "bob");
        assertEquals(result.getMaterializedRows().get(1).getField(2), "dev");

        assertEquals(result.getMaterializedRows().get(2).getField(0), 8);
        assertEquals(result.getMaterializedRows().get(2).getField(1), "bob");
        assertEquals(result.getMaterializedRows().get(2).getField(2), "admin");

        assertEquals(result.getMaterializedRows().get(3).getField(0), 8);
        assertEquals(result.getMaterializedRows().get(3).getField(1), "bob");
        assertEquals(result.getMaterializedRows().get(3).getField(2), "dev");
    }

    @Test
    public void testVariantTypeAggregationCount()
    {
        Session session = Session.builder(getSession()).build();
        String query = format(
                "SELECT " +
                "    json_extract_scalar(data, '$.user') AS user_name, " +
                "    COUNT(*) AS user_count " +
                "FROM \"%s\".\"%s\" " +
                "GROUP BY json_extract_scalar(data, '$.user') " +
                "ORDER BY user_name",
                PATH_SCHEMA,
                goldenTablePathWithPrefix(DELTA_V3, "test_variant"));
        MaterializedResult result = computeActual(session, query);

        // 7 groups: alice, bob (id=2+8 count=2), charlie, diana (id=4+10 count=2), eve, frank, NULL (id=7,9,11,12 count=4)
        // ORDER BY user_name: NULLs sort last in Presto default ASC ordering
        assertEquals(result.getMaterializedRows().size(), 7);

        assertEquals(result.getMaterializedRows().get(0).getField(0), "alice");
        assertEquals(result.getMaterializedRows().get(0).getField(1), 1L);

        assertEquals(result.getMaterializedRows().get(1).getField(0), "bob");
        assertEquals(result.getMaterializedRows().get(1).getField(1), 2L);

        assertEquals(result.getMaterializedRows().get(2).getField(0), "charlie");
        assertEquals(result.getMaterializedRows().get(2).getField(1), 1L);

        assertEquals(result.getMaterializedRows().get(3).getField(0), "diana");
        assertEquals(result.getMaterializedRows().get(3).getField(1), 2L);

        assertEquals(result.getMaterializedRows().get(4).getField(0), "eve");
        assertEquals(result.getMaterializedRows().get(4).getField(1), 1L);

        assertEquals(result.getMaterializedRows().get(5).getField(0), "frank");
        assertEquals(result.getMaterializedRows().get(5).getField(1), 1L);

        assertNull(result.getMaterializedRows().get(6).getField(0));
        assertEquals(result.getMaterializedRows().get(6).getField(1), 4L);
    }

    @Test
    public void testVariantTypeAggregationAverage()
    {
        Session session = Session.builder(getSession()).build();
        String query = format(
                "SELECT " +
                "    json_extract_scalar(data, '$.user') AS user_name, " +
                "    AVG(CAST(json_extract_scalar(data, '$.age') AS INTEGER)) AS avg_age, " +
                "    MIN(CAST(json_extract_scalar(data, '$.age') AS INTEGER)) AS min_age, " +
                "    MAX(CAST(json_extract_scalar(data, '$.age') AS INTEGER)) AS max_age " +
                "FROM \"%s\".\"%s\" " +
                "GROUP BY json_extract_scalar(data, '$.user') " +
                "ORDER BY user_name",
                PATH_SCHEMA,
                goldenTablePathWithPrefix(DELTA_V3, "test_variant"));
        MaterializedResult result = computeActual(session, query);

        // 7 groups including NULL (from NULL-variant rows); NULL user has NULL age → AVG/MIN/MAX are all NULL
        // bob (id=2,8): avg/min/max=25; diana (id=4,10): avg/min/max=28; NULLs sort last
        assertEquals(result.getMaterializedRows().size(), 7);

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

        assertEquals(result.getMaterializedRows().get(4).getField(0), "eve");
        assertEquals(result.getMaterializedRows().get(4).getField(1), 42.0);
        assertEquals(result.getMaterializedRows().get(4).getField(2), 42);
        assertEquals(result.getMaterializedRows().get(4).getField(3), 42);

        assertEquals(result.getMaterializedRows().get(5).getField(0), "frank");
        assertEquals(result.getMaterializedRows().get(5).getField(1), 22.0);
        assertEquals(result.getMaterializedRows().get(5).getField(2), 22);
        assertEquals(result.getMaterializedRows().get(5).getField(3), 22);

        assertNull(result.getMaterializedRows().get(6).getField(0));
        assertNull(result.getMaterializedRows().get(6).getField(1));
        assertNull(result.getMaterializedRows().get(6).getField(2));
        assertNull(result.getMaterializedRows().get(6).getField(3));
    }

    @Test
    public void testVariantTypeGroupByWithHaving()
    {
        Session session = Session.builder(getSession()).build();
        String query = format(
                "SELECT " +
                "    json_extract_scalar(data, '$.user') AS user_name, " +
                "    COUNT(*) AS user_count " +
                "FROM \"%s\".\"%s\" " +
                "GROUP BY json_extract_scalar(data, '$.user') " +
                "HAVING COUNT(*) > 1 " +
                "ORDER BY user_name",
                PATH_SCHEMA,
                goldenTablePathWithPrefix(DELTA_V3, "test_variant"));
        MaterializedResult result = computeActual(session, query);

        // bob (id=2,8 count=2), diana (id=4,10 count=2), and NULL (id=7,9,11,12 count=4) have count > 1
        // NULLs sort last in Presto default ASC ordering
        assertEquals(result.getMaterializedRows().size(), 3);

        assertEquals(result.getMaterializedRows().get(0).getField(0), "bob");
        assertEquals(result.getMaterializedRows().get(0).getField(1), 2L);

        assertEquals(result.getMaterializedRows().get(1).getField(0), "diana");
        assertEquals(result.getMaterializedRows().get(1).getField(1), 2L);

        assertNull(result.getMaterializedRows().get(2).getField(0));
        assertEquals(result.getMaterializedRows().get(2).getField(1), 4L);
    }

    @Test
    public void testVariantTypeGroupByAgeWithHaving()
    {
        Session session = Session.builder(getSession()).build();
        String query = format(
                "SELECT " +
                "    CAST(json_extract_scalar(data, '$.age') AS INTEGER) AS age, " +
                "    COUNT(*) AS count " +
                "FROM \"%s\".\"%s\" " +
                "GROUP BY CAST(json_extract_scalar(data, '$.age') AS INTEGER) " +
                "HAVING AVG(CAST(json_extract_scalar(data, '$.age') AS INTEGER)) >= 25 " +
                "ORDER BY age",
                PATH_SCHEMA,
                goldenTablePathWithPrefix(DELTA_V3, "test_variant"));
        MaterializedResult result = computeActual(session, query);

        // ages >= 25: 22 is excluded (frank); bob age=25 appears twice (id=2,8); diana age=28 appears twice (id=4,10)
        assertEquals(result.getMaterializedRows().size(), 5);

        assertEquals(result.getMaterializedRows().get(0).getField(0), 25);
        assertEquals(result.getMaterializedRows().get(0).getField(1), 2L);  // bob x2

        assertEquals(result.getMaterializedRows().get(1).getField(0), 28);
        assertEquals(result.getMaterializedRows().get(1).getField(1), 2L);  // diana x2

        assertEquals(result.getMaterializedRows().get(2).getField(0), 30);
        assertEquals(result.getMaterializedRows().get(2).getField(1), 1L);  // alice

        assertEquals(result.getMaterializedRows().get(3).getField(0), 35);
        assertEquals(result.getMaterializedRows().get(3).getField(1), 1L);  // charlie

        assertEquals(result.getMaterializedRows().get(4).getField(0), 42);
        assertEquals(result.getMaterializedRows().get(4).getField(1), 1L);  // eve
    }

    @Test
    public void testVariantTypeSelfJoin()
    {
        Session session = Session.builder(getSession()).build();
        String query = format(
                "SELECT " +
                "    t1.id AS id1, " +
                "    json_extract_scalar(t1.data, '$.user') AS user1, " +
                "    t2.id AS id2, " +
                "    json_extract_scalar(t2.data, '$.user') AS user2 " +
                "FROM \"%s\".\"%s\" t1 " +
                "JOIN \"%s\".\"%s\" t2 " +
                "    ON json_extract_scalar(t1.data, '$.user') = json_extract_scalar(t2.data, '$.user') " +
                "    AND t1.id < t2.id " +
                "ORDER BY t1.id, t2.id",
                PATH_SCHEMA,
                goldenTablePathWithPrefix(DELTA_V3, "test_variant"),
                PATH_SCHEMA,
                goldenTablePathWithPrefix(DELTA_V3, "test_variant"));
        MaterializedResult result = computeActual(session, query);

        // bob: id=2 pairs with id=8; diana: id=4 pairs with id=10
        assertEquals(result.getMaterializedRows().size(), 2);

        assertEquals(result.getMaterializedRows().get(0).getField(0), 2);
        assertEquals(result.getMaterializedRows().get(0).getField(1), "bob");
        assertEquals(result.getMaterializedRows().get(0).getField(2), 8);
        assertEquals(result.getMaterializedRows().get(0).getField(3), "bob");

        assertEquals(result.getMaterializedRows().get(1).getField(0), 4);
        assertEquals(result.getMaterializedRows().get(1).getField(1), "diana");
        assertEquals(result.getMaterializedRows().get(1).getField(2), 10);
        assertEquals(result.getMaterializedRows().get(1).getField(3), "diana");
    }

    @Test
    public void testVariantTypeJoinOnAge()
    {
        Session session = Session.builder(getSession()).build();
        String query = format(
                "SELECT " +
                "    t1.id AS id1, " +
                "    json_extract_scalar(t1.data, '$.user') AS user1, " +
                "    t2.id AS id2, " +
                "    json_extract_scalar(t2.data, '$.user') AS user2, " +
                "    CAST(json_extract_scalar(t1.data, '$.age') AS INTEGER) AS age " +
                "FROM \"%s\".\"%s\" t1 " +
                "JOIN \"%s\".\"%s\" t2 " +
                "    ON json_extract_scalar(t1.data, '$.age') = json_extract_scalar(t2.data, '$.age') " +
                "    AND t1.id < t2.id " +
                "ORDER BY age, t1.id, t2.id",
                PATH_SCHEMA,
                goldenTablePathWithPrefix(DELTA_V3, "test_variant"),
                PATH_SCHEMA,
                goldenTablePathWithPrefix(DELTA_V3, "test_variant"));
        MaterializedResult result = computeActual(session, query);

        // age=25: bob id=2 joins bob id=8; age=28: diana id=4 joins diana id=10
        assertEquals(result.getMaterializedRows().size(), 2);

        assertEquals(result.getMaterializedRows().get(0).getField(0), 2);
        assertEquals(result.getMaterializedRows().get(0).getField(1), "bob");
        assertEquals(result.getMaterializedRows().get(0).getField(2), 8);
        assertEquals(result.getMaterializedRows().get(0).getField(3), "bob");
        assertEquals(result.getMaterializedRows().get(0).getField(4), 25);

        assertEquals(result.getMaterializedRows().get(1).getField(0), 4);
        assertEquals(result.getMaterializedRows().get(1).getField(1), "diana");
        assertEquals(result.getMaterializedRows().get(1).getField(2), 10);
        assertEquals(result.getMaterializedRows().get(1).getField(3), "diana");
        assertEquals(result.getMaterializedRows().get(1).getField(4), 28);
    }

    @Test
    public void testVariantTypeCountDistinct()
    {
        Session session = Session.builder(getSession()).build();
        String query = format(
                "SELECT " +
                "    COUNT(DISTINCT json_extract_scalar(data, '$.user')) AS unique_users, " +
                "    COUNT(DISTINCT CAST(json_extract_scalar(data, '$.age') AS INTEGER)) AS unique_ages " +
                "FROM \"%s\".\"%s\"",
                PATH_SCHEMA,
                goldenTablePathWithPrefix(DELTA_V3, "test_variant"));
        MaterializedResult result = computeActual(session, query);

        assertEquals(result.getMaterializedRows().size(), 1);
        // 6 distinct users: alice, bob, charlie, diana, eve, frank (NULL rows excluded by COUNT DISTINCT)
        // 6 distinct ages: 22, 25, 28, 30, 35, 42
        assertEquals(result.getMaterializedRows().get(0).getField(0), 6L);
        assertEquals(result.getMaterializedRows().get(0).getField(1), 6L);
    }
}
