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
package io.prestosql.sql.query;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestOrderedAggregation
{
    private QueryAssertions assertions;

    @BeforeClass
    public void init()
    {
        assertions = new QueryAssertions();
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testAggregationWithOrderBy()
    {
        assertions.assertQuery(
                "SELECT sum(x ORDER BY y) FROM (VALUES (1, 2), (3, 5), (4, 1)) t(x, y)",
                "VALUES (BIGINT '8')");
        assertions.assertQuery(
                "SELECT array_agg(x ORDER BY y) FROM (VALUES (1, 2), (3, 5), (4, 1)) t(x, y)",
                "VALUES ARRAY[4, 1, 3]");

        assertions.assertQuery(
                "SELECT array_agg(x ORDER BY y DESC) FROM (VALUES (1, 2), (3, 5), (4, 1)) t(x, y)",
                "VALUES ARRAY[3, 1, 4]");

        assertions.assertQuery(
                "SELECT array_agg(x ORDER BY x DESC) FROM (VALUES (1, 2), (3, 5), (4, 1)) t(x, y)",
                "VALUES ARRAY[4, 3, 1]");

        assertions.assertQuery(
                "SELECT array_agg(x ORDER BY x) FROM (VALUES ('a', 2), ('bcd', 5), ('abcd', 1)) t(x, y)",
                "VALUES ARRAY['a', 'abcd', 'bcd']");

        assertions.assertQuery(
                "SELECT array_agg(y ORDER BY x) FROM (VALUES ('a', 2), ('bcd', 5), ('abcd', 1)) t(x, y)",
                "VALUES ARRAY[2, 1, 5]");

        assertions.assertQuery(
                "SELECT array_agg(y ORDER BY x) FROM (VALUES ((1, 2), 2), ((3, 4), 5), ((1, 1), 1)) t(x, y)",
                "VALUES ARRAY[1, 2, 5]");

        assertions.assertQuery(
                "SELECT array_agg(z ORDER BY x, y DESC) FROM (VALUES (1, 2, 2), (2, 2, 3), (2, 4, 5), (3, 4, 4), (1, 1, 1)) t(x, y, z)",
                "VALUES ARRAY[2, 1, 5, 3, 4]");

        assertions.assertQuery(
                "SELECT x, array_agg(z ORDER BY y + z DESC) FROM (VALUES (1, 2, 2), (2, 2, 3), (2, 4, 5), (3, 4, 4), (3, 2, 1), (1, 1, 1)) t(x, y, z) GROUP BY x",
                "VALUES (1, ARRAY[2, 1]), (2, ARRAY[5, 3]), (3, ARRAY[4, 1])");

        assertions.assertQuery(
                "SELECT array_agg(y ORDER BY x.a DESC) FROM (VALUES (CAST(ROW(1) AS ROW(a BIGINT)), 1), (CAST(ROW(2) AS ROW(a BIGINT)), 2)) t(x, y)",
                "VALUES ARRAY[2, 1]");

        assertions.assertQuery(
                "SELECT x, y, array_agg(z ORDER BY z DESC NULLS FIRST) FROM (VALUES (1, 2, NULL), (1, 2, 1), (1, 2, 2), (2, 1, 3), (2, 1, 4), (2, 1, NULL)) t(x, y, z) GROUP BY x, y",
                "VALUES (1, 2, ARRAY[NULL, 2, 1]), (2, 1, ARRAY[NULL, 4, 3])");

        assertions.assertQuery(
                "SELECT x, y, array_agg(z ORDER BY z DESC NULLS LAST) FROM (VALUES (1, 2, 3), (1, 2, 1), (1, 2, 2), (2, 1, 3), (2, 1, 4), (2, 1, NULL)) t(x, y, z) GROUP BY GROUPING SETS ((x), (x, y))",
                "VALUES (1, 2, ARRAY[3, 2, 1]), (1, NULL, ARRAY[3, 2, 1]), (2, 1, ARRAY[4, 3, NULL]), (2, NULL, ARRAY[4, 3, NULL])");

        assertions.assertQuery(
                "SELECT x, y, array_agg(z ORDER BY z DESC NULLS LAST) FROM (VALUES (1, 2, 3), (1, 2, 1), (1, 2, 2), (2, 1, 3), (2, 1, 4), (2, 1, NULL)) t(x, y, z) GROUP BY GROUPING SETS ((x), (x, y))",
                "VALUES (1, 2, ARRAY[3, 2, 1]), (1, NULL, ARRAY[3, 2, 1]), (2, 1, ARRAY[4, 3, NULL]), (2, NULL, ARRAY[4, 3, NULL])");

        assertions.assertQuery(
                "SELECT x, array_agg(DISTINCT z + y ORDER BY z + y DESC) FROM (VALUES (1, 2, 2), (2, 2, 3), (2, 4, 5), (3, 4, 4), (3, 2, 1), (1, 1, 1)) t(x, y, z) GROUP BY x",
                "VALUES (1, ARRAY[4, 2]), (2, ARRAY[9, 5]), (3, ARRAY[8, 3])");

        assertions.assertQuery(
                "SELECT x, sum(cast(x AS double))\n" +
                        "FROM (VALUES '1.0') t(x)\n" +
                        "GROUP BY x\n" +
                        "ORDER BY sum(cast(t.x AS double) ORDER BY t.x)",
                "VALUES ('1.0', 1e0)");

        assertions.assertQuery(
                "SELECT x, y, array_agg(z ORDER BY z) FROM (VALUES (1, 2, 3), (1, 2, 1), (2, 1, 3), (2, 1, 4)) t(x, y, z) GROUP BY GROUPING SETS ((x), (x, y))",
                "VALUES (1, NULL, ARRAY[1, 3]), (2, NULL, ARRAY[3, 4]), (1, 2, ARRAY[1, 3]), (2, 1, ARRAY[3, 4])");

        assertions.assertFails(
                "SELECT array_agg(z ORDER BY z) OVER (PARTITION BY x) FROM (VALUES (1, 2, 3), (1, 2, 1), (2, 1, 3), (2, 1, 4)) t(x, y, z) GROUP BY x, z",
                ".* Window function with ORDER BY is not supported");

        assertions.assertFails(
                "SELECT array_agg(DISTINCT x ORDER BY y) FROM (VALUES (1, 2), (3, 5), (4, 1)) t(x, y)",
                ".* For aggregate function with DISTINCT, ORDER BY expressions must appear in arguments");

        assertions.assertFails(
                "SELECT array_agg(DISTINCT x+y ORDER BY y) FROM (VALUES (1, 2), (3, 5), (4, 1)) t(x, y)",
                ".* For aggregate function with DISTINCT, ORDER BY expressions must appear in arguments");

        assertions.assertFails(
                "SELECT x, array_agg(DISTINCT y ORDER BY z + y DESC) FROM (VALUES (1, 2, 2), (2, 2, 3), (2, 4, 5), (3, 4, 4), (3, 2, 1), (1, 1, 1)) t(x, y, z) GROUP BY x",
                ".* For aggregate function with DISTINCT, ORDER BY expressions must appear in arguments");

        assertions.assertQuery(
                "SELECT multimap_agg(x, y ORDER BY z) FROM (VALUES (1, 2, 2), (1, 5, 5), (2, 1, 5), (3, 4, 4), (2, 5, 1), (1, 1, 1)) t(x, y, z)",
                "VALUES map_from_entries(ARRAY[row(1, ARRAY[1, 2, 5]), row(2, ARRAY[5, 1]), row(3, ARRAY[4])])");
    }

    @Test
    public void testGroupingSets()
    {
        assertions.assertQuery(
                "SELECT x, array_agg(y ORDER BY y), array_agg(y ORDER BY y) FILTER (WHERE y > 1), count(*) FROM (" +
                        "VALUES " +
                        "   (1, 3), " +
                        "   (1, 1), " +
                        "   (2, 3), " +
                        "   (2, 4)) t(x, y) " +
                        "GROUP BY GROUPING SETS ((), (x))",
                "VALUES " +
                        "   (1, ARRAY[1, 3], ARRAY[3], BIGINT '2'), " +
                        "   (2, ARRAY[3, 4], ARRAY[3, 4], BIGINT '2'), " +
                        "   (NULL, ARRAY[1, 3, 3, 4], ARRAY[3, 3, 4], BIGINT '4')");

        assertions.assertQuery(
                "SELECT x, array_agg(DISTINCT y ORDER BY y), count(*) FROM (" +
                        "VALUES " +
                        "   (1, 3), " +
                        "   (1, 1), " +
                        "   (1, 3), " +
                        "   (2, 3), " +
                        "   (2, 4)) t(x, y) " +
                        "GROUP BY GROUPING SETS ((), (x))",
                "VALUES " +
                        "   (1, ARRAY[1, 3], BIGINT '3'), " +
                        "   (2, ARRAY[3, 4], BIGINT '2'), " +
                        "   (NULL, ARRAY[1, 3, 4], BIGINT '5')");
    }
}
