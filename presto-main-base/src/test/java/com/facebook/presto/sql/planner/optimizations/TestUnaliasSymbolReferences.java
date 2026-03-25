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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;

public class TestUnaliasSymbolReferences
        extends BasePlanTest
{
    @Test
    public void testJoinTimestampsWithTimezonesPreservesAssignments()
    {
        // Although the two timestamps are the same point in time, they are in different timezones,
        // so the optimizer should not collapse the two assignments.
        assertPlan("WITH source AS (" +
                        "SELECT * FROM (" +
                        "    VALUES" +
                        "        (TIMESTAMP '2001-08-22 03:04:05.321 America/Los_Angeles')," +
                        "        (TIMESTAMP '2001-08-22 06:04:05.321 America/New_York')" +
                        ") AS tbl (tstz)" +
                        ")" +
                        "SELECT * FROM source a JOIN source b ON a.tstz = b.tstz",
                anyTree(
                        join(INNER, ImmutableList.of(equiJoinClause("LEFT_BAR", "RIGHT_BAR")),
                                anyTree(values("LEFT_BAR")),
                                anyTree(values("RIGHT_BAR")))
                                .withNumberOfOutputColumns(2)
                                .withExactOutputs("LEFT_BAR", "RIGHT_BAR")));
    }

    @Test(timeOut = 30_000)
    public void testDuplicateConstantsAcrossNestedProjectionsDontHang()
    {
        // Regression test: when the same constant (e.g., '2026-02-04') is assigned to multiple
        // variables across nested subquery levels, UnaliasSymbolReferences could create a cycle
        // in its variable mapping (A -> B -> A) causing an infinite loop. This happens when:
        //   1. A child ProjectNode deduplicates two constant assignments, mapping $b -> $a
        //   2. A parent ProjectNode also has both $a and $b assigned to the same constant
        //   3. The parent stores $b in computedExpressions (original key, not canonical)
        //   4. Processing $a finds $b and calls map($a, $b), completing the cycle
        assertPlan(
                "SELECT metric_ds, ds " +
                        "FROM (" +
                        "  SELECT metric_ds, calls, '2026-02-04' AS ds " +
                        "  FROM (" +
                        "    SELECT ds, metric_ds, sum(calls) AS calls " +
                        "    FROM (" +
                        "      SELECT '2026-02-04' AS metric_ds, '2026-02-04' AS ds, 1 AS calls " +
                        "      FROM (VALUES 1) t(x)" +
                        "    ) GROUP BY ds, metric_ds" +
                        "  )" +
                        ")",
                anyTree(values()));
    }

    @Test
    public void testIdenticalValuesCollapseAssignments()
    {
        // We take the same table as in #testJoinTimestampsWithTimezonesPreservesAssignments but convert the
        // values to UTC epoch time as a double.  The optimizer should collapse the two identical values into
        // a single assignment, because doubles are ensured to be represented only a single way.
        assertPlan("WITH source AS (" +
                        "SELECT * FROM (" +
                        "    VALUES" +
                        "        (to_unixtime(TIMESTAMP '2001-08-22 03:04:05.321 America/Los_Angeles'))," +
                        "        (to_unixtime(TIMESTAMP '2001-08-22 06:04:05.321 America/New_York'))" +
                        ") AS tbl (tstz)" +
                        ")" +
                        "SELECT * FROM source a JOIN source b ON a.tstz = b.tstz",
                anyTree(
                        join(INNER, ImmutableList.of(equiJoinClause("LEFT_BAR", "RIGHT_BAR")),
                                anyTree(values("LEFT_BAR")),
                                anyTree(values("RIGHT_BAR")))
                                .withNumberOfOutputColumns(1)
                                .withExactOutputs("LEFT_BAR")));
    }
}
