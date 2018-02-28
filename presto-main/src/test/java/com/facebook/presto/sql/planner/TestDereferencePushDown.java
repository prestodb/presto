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

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.output;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;

public class TestDereferencePushDown
        extends BasePlanTest
{
    private static final String VALUES = "(values ROW(CAST(ROW(1, 2.0) AS ROW(x BIGINT, y DOUBLE))))";

    @Test
    public void testPushDownDereferencesThroughJoin()
    {
        assertPlan(" with t1 as ( select * from " + VALUES + " as t (msg) ) select b.msg.x from t1 a, t1 b where a.msg.y = b.msg.y",
                output(ImmutableList.of("x"),
                        join(INNER, ImmutableList.of(equiJoinClause("left_y", "right_y")),
                                anyTree(
                                        project(ImmutableMap.of("left_y", expression("field.y")),
                                                values("field"))
                                ), anyTree(
                                        project(ImmutableMap.of("right_y", expression("field1.y"), "x", expression("field1.x")),
                                                values("field1"))))));
    }

    @Test
    public void testPushDownDereferencesInCase()
    {
        // Test dereferences in then clause will not be eagerly evaluated.
        String statement = "with t as (select * from (values cast(array[CAST(ROW(1, 2.0) AS ROW(x BIGINT, y DOUBLE)),ROW(1, 2.0)] as array<ROW(x BIGINT, y DOUBLE)>)) as t (arr) ) " +
                "select case when cardinality(arr) > cast(0 as bigint) then arr[cast(1 as bigint)].x end from t";
        assertPlan(statement,
                output(ImmutableList.of("x"),
                        project(ImmutableMap.of("x", expression("case when cardinality(field) > bigint '0' then field[bigint '1'].x end")), values("field"))));
    }

    @Test
    public void testPushDownDereferencesThroughFilter()
    {
        assertPlan(" with t1 as ( select * from " + VALUES + " as t (msg) ) select a.msg.y from t1 a join t1 b on a.msg.y = b.msg.y where a.msg.x > bigint '5'",
                output(ImmutableList.of("left_y"),
                        join(INNER, ImmutableList.of(equiJoinClause("left_y", "right_y")),
                                anyTree(
                                        project(ImmutableMap.of("left_y", expression("field.y")),
                                                filter("field.x > bigint '5'", values("field")))
                                ), anyTree(
                                        project(ImmutableMap.of("right_y", expression("field1.y")),
                                                values("field1"))))));
    }
}
