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
package com.facebook.presto.sql.query;

import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.presto.SystemSessionProperties.OPTIMIZE_CONDITIONAL_AGGREGATION_ENABLED;

public class TestConditionalAggregations
        extends BasePlanTest
{
    private static final Map<String, String> sessionProperties = ImmutableMap.of(OPTIMIZE_CONDITIONAL_AGGREGATION_ENABLED, "true");
    private QueryAssertions assertions;

    public TestConditionalAggregations()
    {
        super(sessionProperties);
    }

    @BeforeClass
    public void init()
    {
        assertions = new QueryAssertions(sessionProperties);
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testAggregationSkipped()
    {
        String sql = "select y, grouping(y) grouping_y, IF(grouping(y)!=1, sum(x)) from (VALUES (1,1,1), (9223372036854775807,2,1)) AS T(x,y,z)"
                + " group by grouping sets ((y), (z)) ORDER BY 1, 2";
        QueryAssertions asserionNoOptimization = new QueryAssertions();
        asserionNoOptimization.assertFails(sql, "bigint addition overflow:.*");
        assertions.assertQuery(sql, "VALUES (1, 0, 1), (2, 0, 9223372036854775807), (NULL, 1, NULL)");
    }

    @Test
    public void testPredicateOnGroupBy()
    {
        assertions.assertQuery(
                "SELECT y, IF(y>1, sum(x)) FROM (VALUES (1, 2), (5, 2), (1,1), (3, 1)) t(x, y) GROUP BY y ORDER BY y",
                "VALUES (INTEGER '1', CAST(NULL AS BIGINT)), (INTEGER '2', BIGINT '6')");
        assertions.assertQuery(
                "SELECT y, SUM(IF(y>1, x)) FROM (VALUES (1, 2), (5, 2), (1,1), (3, 1)) t(x, y) GROUP BY y ORDER BY y",
                "VALUES (INTEGER '1', CAST(NULL AS BIGINT)), (INTEGER '2', BIGINT '6')");

        assertions.assertQuery(
                "SELECT z, IF(z<2, MAX_BY(x, y)) FROM (VALUES (1, 2, 1), (5, 3, 1), (1, 4, 2), (3, 1, 2)) t(x, y, z) GROUP BY z ORDER BY z",
                "VALUES (INTEGER '1', INTEGER '5'), (INTEGER '2', CAST(NULL AS INTEGER))");
        assertions.assertQuery(
                "SELECT z, MAX_BY(IF(z<2, x), y) FROM (VALUES (1, 2, 1), (5, 3, 1), (1, 4, 2), (3, 1, 2)) t(x, y, z) GROUP BY z ORDER BY z",
                "VALUES (INTEGER '1', INTEGER '5'), (INTEGER '2', CAST(NULL AS INTEGER))");

        assertions.assertQuery(
                "SELECT z, IF(z<2, MIN_BY(x, y)) FROM (VALUES (1, 2, 1), (5, 3, 1), (1, 4, 2), (3, 1, 2)) t(x, y, z) GROUP BY z ORDER BY z",
                "VALUES (INTEGER '1', INTEGER '1'), (INTEGER '2', CAST(NULL AS INTEGER))");
        assertions.assertQuery(
                "SELECT z, MIN_BY(IF(z<2, x), y) FROM (VALUES (1, 2, 1), (5, 3, 1), (1, 4, 2), (3, 1, 2)) t(x, y, z) GROUP BY z ORDER BY z",
                "VALUES (INTEGER '1', INTEGER '1'), (INTEGER '2', CAST(NULL AS INTEGER))");

        assertions.assertQuery(
                "SELECT y, IF(y>1, REDUCE_AGG(x, 0, (a, b) -> a+b, (a, b) -> a+b)) FROM (VALUES (1, 2), (5, 2), (1,1), (3, 1)) t(x, y) GROUP BY y ORDER BY y",
                "VALUES (INTEGER '1', CAST(NULL AS INTEGER)), (INTEGER '2', INTEGER '6')");
        assertions.assertQuery(
                "SELECT y, REDUCE_AGG(IF(y>1, x), 0, (a, b) -> a+b, (a, b) -> a+b) FROM (VALUES (1, 2), (5, 2), (1,1), (3, 1)) t(x, y) GROUP BY y ORDER BY y",
                "VALUES (INTEGER '1', CAST(NULL AS INTEGER)), (INTEGER '2', INTEGER '6')");

        assertions.assertQuery(
                "SELECT max_by(IF(x > 1, x), y), max_by(x, y) FROM (VALUES (1, 2), (1, 3), (0, 5), (0,6), (3, 0), (3, 1)) t(x, y)",
                "VALUES (CAST(NULL AS INTEGER), INTEGER '0')");
    }

    @Test
    public void testPredicateOnGroupingSet()
    {
        assertions.assertQuery(
                "SELECT y, z, IF(grouping(y)=0, sum(x)), if(grouping(z) = 0, count(x)) FROM (VALUES (1, 2, 8), (5, 2, 0), (1,1, 4), (3, 1, 0)) t(x, y, z) " +
                        "GROUP BY grouping sets ((y), (z)) ORDER BY y, z",
                "VALUES (INTEGER '1', CAST(NULL AS INTEGER), BIGINT '4', CAST(NULL AS BIGINT)), " +
                        "(INTEGER '2', CAST(NULL AS INTEGER),  BIGINT '6', CAST(NULL AS BIGINT)), " +
                        "(CAST(NULL AS INTEGER), INTEGER '0', CAST(NULL AS BIGINT), BIGINT '2'), " +
                        "(CAST(NULL AS INTEGER), INTEGER '4', CAST(NULL AS BIGINT), BIGINT '1'), " +
                        "(CAST(NULL AS INTEGER), INTEGER '8', CAST(NULL AS BIGINT), BIGINT '1')");

        assertions.assertQuery(
                "SELECT z, p, IF(grouping(z)=0, MAX_BY(x, y)), IF(grouping(p)=0, MAX_BY(y, x)) FROM (VALUES (1, 2, 1, 9), (5, 3, 1, 4), (1, 4, 2, 4), (3, 1, 2, 9)) t(x, y, z, p) GROUP BY grouping sets ((z), (p)) ORDER BY z, p",
                "VALUES (INTEGER '1', CAST(NULL AS INTEGER), INTEGER '5', CAST(NULL AS INTEGER)), " +
                        "(INTEGER '2', CAST(NULL AS INTEGER),  INTEGER '1', CAST(NULL AS INTEGER)), " +
                        "(CAST(NULL AS INTEGER), INTEGER '4', CAST(NULL AS INTEGER), INTEGER '3'), " +
                        "(CAST(NULL AS INTEGER), INTEGER '9', CAST(NULL AS INTEGER), INTEGER '1')");

        assertions.assertQuery(
                "SELECT z, p, IF(grouping(z)=0, MIN_BY(x, y)), IF(grouping(p)=0, MIN_BY(y, x)) FROM (VALUES (1, 2, 1, 9), (5, 3, 1, 4), (1, 4, 2, 4), (3, 1, 2, 9)) t(x, y, z, p) GROUP BY grouping sets ((z), (p)) ORDER BY z, p",
                "VALUES (INTEGER '1', CAST(NULL AS INTEGER), INTEGER '1', CAST(NULL AS INTEGER)), " +
                        "(INTEGER '2', CAST(NULL AS INTEGER),  INTEGER '3', CAST(NULL AS INTEGER)), " +
                        "(CAST(NULL AS INTEGER), INTEGER '4', CAST(NULL AS INTEGER), INTEGER '4'), " +
                        "(CAST(NULL AS INTEGER), INTEGER '9', CAST(NULL AS INTEGER), INTEGER '2')");

        assertions.assertQuery(
                "SELECT y, z, IF(grouping(y)=0, REDUCE_AGG(x, 0, (a, b) -> a+b, (a, b) -> a+b)), if(grouping(z) = 0, REDUCE_AGG(x, 0, (a, b) -> a*b, (a, b) -> a*b)) FROM (VALUES (1, 2, 8), (5, 2, 0), (1,1, 4), (3, 1, 0)) t(x, y, z) " +
                        "GROUP BY grouping sets ((y), (z)) ORDER BY y, z",
                "VALUES (INTEGER '1', CAST(NULL AS INTEGER), INTEGER '4', CAST(NULL AS INTEGER)), " +
                        "(INTEGER '2', CAST(NULL AS INTEGER),  INTEGER '6', CAST(NULL AS INTEGER)), " +
                        "(CAST(NULL AS INTEGER), INTEGER '0', CAST(NULL AS INTEGER), INTEGER '0'), " +
                        "(CAST(NULL AS INTEGER), INTEGER '4', CAST(NULL AS INTEGER), INTEGER '0'), " +
                        "(CAST(NULL AS INTEGER), INTEGER '8', CAST(NULL AS INTEGER), INTEGER '0')");
    }
}
