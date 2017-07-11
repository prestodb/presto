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
package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;

public class TestCanonicalize
        extends BasePlanTest
{
    @Test
    public void testJoin()
            throws Exception
    {
        assertPlan("SELECT *\n" +
                        "FROM (\n" +
                        "    SELECT EXTRACT(DAY FROM DATE '2017-01-01')\n" +
                        ") t\n" +
                        "CROSS JOIN (VALUES 1)",
                anyTree(
                        join(INNER, ImmutableList.of(), Optional.empty(),
                                project(
                                        ImmutableMap.of("X", expression("BIGINT '1'")),
                                        values(ImmutableMap.of())),
                                values(ImmutableMap.of()))));
    }
}
