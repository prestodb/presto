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
