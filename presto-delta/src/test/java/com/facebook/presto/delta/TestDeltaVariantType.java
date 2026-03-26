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
        assertEquals(result.getMaterializedRows().size(), 5);

        // Verify the data values
        assertEquals(result.getMaterializedRows().get(0).getField(0), 1);
        assertEquals(result.getMaterializedRows().get(0).getField(1), "{\"active\":true,\"age\":30,\"user\":\"alice\"}");

        assertEquals(result.getMaterializedRows().get(1).getField(0), 2);
        assertEquals(result.getMaterializedRows().get(1).getField(1), "{\"age\":25,\"tags\":[\"admin\",\"dev\"],\"user\":\"bob\"}");

        assertEquals(result.getMaterializedRows().get(3).getField(0), 4);
        assertEquals(result.getMaterializedRows().get(3).getField(1), "{\"age\":28,\"scores\":[95,87,92],\"user\":\"diana\"}");
    }

    @Test
    public void testVariantTypeCount()
    {
        Session session = Session.builder(getSession()).build();
        String query = format("SELECT COUNT(*) FROM \"%s\".\"%s\"", PATH_SCHEMA,
                goldenTablePathWithPrefix(DELTA_V3, "test_variant"));
        MaterializedResult result = computeActual(session, query);
        assertEquals(result.getMaterializedRows().size(), 1);
        assertEquals(result.getMaterializedRows().get(0).getField(0), 5L);
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

        assertEquals(result.getMaterializedRows().size(), 5);

        // Row 1: alice with active flag
        MaterializedRow row1 = result.getMaterializedRows().get(0);
        assertEquals(row1.getField(0), 1);
        assertEquals(row1.getField(1), "alice");
        assertEquals(row1.getField(2), 30);
        assertEquals(row1.getField(3), true);

        // Row 2: bob without active flag
        MaterializedRow row2 = result.getMaterializedRows().get(1);
        assertEquals(row2.getField(0), 2);
        assertEquals(row2.getField(1), "bob");
        assertEquals(row2.getField(2), 25);
        assertNull(row2.getField(3));

        // Row 4: diana without active flag
        MaterializedRow row4 = result.getMaterializedRows().get(3);
        assertEquals(row4.getField(0), 4);
        assertEquals(row4.getField(1), "diana");
        assertEquals(row4.getField(2), 28);
        assertNull(row4.getField(3));
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

        assertEquals(result.getMaterializedRows().size(), 5);

        // Row 1: alice - no tags or scores
        MaterializedRow row1 = result.getMaterializedRows().get(0);
        assertEquals(row1.getField(0), 1);
        assertEquals(row1.getField(1), "alice");
        assertEquals(row1.getField(2), "30");
        assertEquals(row1.getField(3), "true");
        assertNull(row1.getField(4));
        assertNull(row1.getField(5));

        // Row 2: bob - has tags
        MaterializedRow row2 = result.getMaterializedRows().get(1);
        assertEquals(row2.getField(0), 2);
        assertEquals(row2.getField(1), "bob");
        assertEquals(row2.getField(2), "25");
        assertNull(row2.getField(3));
        assertEquals(row2.getField(4), "[\"admin\",\"dev\"]");
        assertNull(row2.getField(5));

        // Row 4: diana - has scores
        MaterializedRow row4 = result.getMaterializedRows().get(3);
        assertEquals(row4.getField(0), 4);
        assertEquals(row4.getField(1), "diana");
        assertEquals(row4.getField(2), "28");
        assertNull(row4.getField(3));
        assertNull(row4.getField(4));
        assertEquals(row4.getField(5), "[95,87,92]");
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

        // Only rows 2 and 3 have tags (bob)
        assertEquals(result.getMaterializedRows().size(), 4);

        // Row 1: bob id=2, tag=admin
        assertEquals(result.getMaterializedRows().get(0).getField(0), 2);
        assertEquals(result.getMaterializedRows().get(0).getField(1), "bob");
        assertEquals(result.getMaterializedRows().get(0).getField(2), "admin");

        // Row 2: bob id=2, tag=dev
        assertEquals(result.getMaterializedRows().get(1).getField(0), 2);
        assertEquals(result.getMaterializedRows().get(1).getField(1), "bob");
        assertEquals(result.getMaterializedRows().get(1).getField(2), "dev");

        // Row 3: bob id=3, tag=admin
        assertEquals(result.getMaterializedRows().get(2).getField(0), 3);
        assertEquals(result.getMaterializedRows().get(2).getField(1), "bob");
        assertEquals(result.getMaterializedRows().get(2).getField(2), "admin");

        // Row 4: bob id=3, tag=dev
        assertEquals(result.getMaterializedRows().get(3).getField(0), 3);
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

        assertEquals(result.getMaterializedRows().size(), 3);

        // alice: 1 occurrence
        assertEquals(result.getMaterializedRows().get(0).getField(0), "alice");
        assertEquals(result.getMaterializedRows().get(0).getField(1), 1L);

        // bob: 2 occurrences
        assertEquals(result.getMaterializedRows().get(1).getField(0), "bob");
        assertEquals(result.getMaterializedRows().get(1).getField(1), 2L);

        // diana: 2 occurrences
        assertEquals(result.getMaterializedRows().get(2).getField(0), "diana");
        assertEquals(result.getMaterializedRows().get(2).getField(1), 2L);
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

        assertEquals(result.getMaterializedRows().size(), 3);

        // alice: age 30
        assertEquals(result.getMaterializedRows().get(0).getField(0), "alice");
        assertEquals(result.getMaterializedRows().get(0).getField(1), 30.0);
        assertEquals(result.getMaterializedRows().get(0).getField(2), 30);
        assertEquals(result.getMaterializedRows().get(0).getField(3), 30);

        // bob: age 25
        assertEquals(result.getMaterializedRows().get(1).getField(0), "bob");
        assertEquals(result.getMaterializedRows().get(1).getField(1), 25.0);
        assertEquals(result.getMaterializedRows().get(1).getField(2), 25);
        assertEquals(result.getMaterializedRows().get(1).getField(3), 25);

        // diana: age 28
        assertEquals(result.getMaterializedRows().get(2).getField(0), "diana");
        assertEquals(result.getMaterializedRows().get(2).getField(1), 28.0);
        assertEquals(result.getMaterializedRows().get(2).getField(2), 28);
        assertEquals(result.getMaterializedRows().get(2).getField(3), 28);
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

        // Only bob and diana have count > 1
        assertEquals(result.getMaterializedRows().size(), 2);

        assertEquals(result.getMaterializedRows().get(0).getField(0), "bob");
        assertEquals(result.getMaterializedRows().get(0).getField(1), 2L);

        assertEquals(result.getMaterializedRows().get(1).getField(0), "diana");
        assertEquals(result.getMaterializedRows().get(1).getField(1), 2L);
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

        assertEquals(result.getMaterializedRows().size(), 3);

        // Age 25: 2 occurrences
        assertEquals(result.getMaterializedRows().get(0).getField(0), 25);
        assertEquals(result.getMaterializedRows().get(0).getField(1), 2L);

        // Age 28: 2 occurrences
        assertEquals(result.getMaterializedRows().get(1).getField(0), 28);
        assertEquals(result.getMaterializedRows().get(1).getField(1), 2L);

        // Age 30: 1 occurrence
        assertEquals(result.getMaterializedRows().get(2).getField(0), 30);
        assertEquals(result.getMaterializedRows().get(2).getField(1), 1L);
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

        // bob: id 2 and 3
        // diana: id 4 and 5
        assertEquals(result.getMaterializedRows().size(), 2);

        // bob 2-3
        assertEquals(result.getMaterializedRows().get(0).getField(0), 2);
        assertEquals(result.getMaterializedRows().get(0).getField(1), "bob");
        assertEquals(result.getMaterializedRows().get(0).getField(2), 3);
        assertEquals(result.getMaterializedRows().get(0).getField(3), "bob");

        // diana 4-5
        assertEquals(result.getMaterializedRows().get(1).getField(0), 4);
        assertEquals(result.getMaterializedRows().get(1).getField(1), "diana");
        assertEquals(result.getMaterializedRows().get(1).getField(2), 5);
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

        // age 25: bob 2-3
        // age 28: diana 4-5
        assertEquals(result.getMaterializedRows().size(), 2);

        // bob 2-3, age 25
        assertEquals(result.getMaterializedRows().get(0).getField(0), 2);
        assertEquals(result.getMaterializedRows().get(0).getField(1), "bob");
        assertEquals(result.getMaterializedRows().get(0).getField(2), 3);
        assertEquals(result.getMaterializedRows().get(0).getField(3), "bob");
        assertEquals(result.getMaterializedRows().get(0).getField(4), 25);

        // diana 4-5, age 28
        assertEquals(result.getMaterializedRows().get(1).getField(0), 4);
        assertEquals(result.getMaterializedRows().get(1).getField(1), "diana");
        assertEquals(result.getMaterializedRows().get(1).getField(2), 5);
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
        assertEquals(result.getMaterializedRows().get(0).getField(0), 3L); // alice, bob, diana
        assertEquals(result.getMaterializedRows().get(0).getField(1), 3L); // 25, 28, 30
    }
}
