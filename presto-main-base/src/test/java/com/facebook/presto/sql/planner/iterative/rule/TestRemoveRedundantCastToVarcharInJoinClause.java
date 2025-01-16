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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.REMOVE_REDUNDANT_CAST_TO_VARCHAR_IN_JOIN;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.assignment;

public class TestRemoveRedundantCastToVarcharInJoinClause
        extends BaseRuleTest
{
    @Test
    public void testCastBigIntToVarchar()
    {
        tester().assertThat(new RemoveRedundantCastToVarcharInJoinClause(getFunctionManager()))
                .setSystemProperty(REMOVE_REDUNDANT_CAST_TO_VARCHAR_IN_JOIN, "true")
                .on(p -> {
                    VariableReferenceExpression leftBigint = p.variable("left_bigint");
                    VariableReferenceExpression rightBigint = p.variable("right_bigint");
                    VariableReferenceExpression leftCast = p.variable("left_cast", VARCHAR);
                    VariableReferenceExpression rightCast = p.variable("right_cast", VARCHAR);
                    return p.join(
                            JoinType.INNER,
                            p.project(assignment(leftCast, p.rowExpression("cast(left_bigint as varchar)")),
                                    p.values(leftBigint)),
                            p.project(assignment(rightCast, p.rowExpression("cast(right_bigint as varchar)")),
                                    p.values(rightBigint)),
                            new EquiJoinClause(leftCast, rightCast));
                })
                .matches(
                        join(
                                JoinType.INNER,
                                ImmutableList.of(equiJoinClause("new_left", "new_right")),
                                project(
                                        ImmutableMap.of("new_left", expression("left_bigint")),
                                        values("left_bigint")),
                                project(
                                        ImmutableMap.of("new_right", expression("right_bigint")),
                                        values("right_bigint"))));
    }

    @Test
    public void testCastIntToBigint()
    {
        tester().assertThat(new RemoveRedundantCastToVarcharInJoinClause(getFunctionManager()))
                .setSystemProperty(REMOVE_REDUNDANT_CAST_TO_VARCHAR_IN_JOIN, "true")
                .on(p -> {
                    VariableReferenceExpression leftBigint = p.variable("left_bigint", INTEGER);
                    VariableReferenceExpression rightBigint = p.variable("right_bigint", SMALLINT);
                    VariableReferenceExpression leftCast = p.variable("left_cast", BIGINT);
                    VariableReferenceExpression rightCast = p.variable("right_cast", BIGINT);
                    return p.join(
                            JoinType.INNER,
                            p.project(assignment(leftCast, p.rowExpression("cast(left_bigint as varchar)")),
                                    p.values(leftBigint)),
                            p.project(assignment(rightCast, p.rowExpression("cast(right_bigint as varchar)")),
                                    p.values(rightBigint)),
                            new EquiJoinClause(leftCast, rightCast));
                }).doesNotFire();
    }

    @Test
    public void testNoCast()
    {
        tester().assertThat(new RemoveRedundantCastToVarcharInJoinClause(getFunctionManager()))
                .setSystemProperty(REMOVE_REDUNDANT_CAST_TO_VARCHAR_IN_JOIN, "true")
                .on(p -> {
                    VariableReferenceExpression rightBigint = p.variable("right_bigint");
                    VariableReferenceExpression leftCast = p.variable("left_cast", VARCHAR);
                    VariableReferenceExpression rightCast = p.variable("right_cast", VARCHAR);
                    return p.join(
                            JoinType.INNER,
                            p.values(leftCast),
                            p.project(assignment(rightCast, p.rowExpression("cast(right_bigint as varchar)")),
                                    p.values(rightBigint)),
                            new EquiJoinClause(leftCast, rightCast));
                }).doesNotFire();
    }

    @Test
    public void testCastDoubleToVarchar()
    {
        tester().assertThat(new RemoveRedundantCastToVarcharInJoinClause(getFunctionManager()))
                .setSystemProperty(REMOVE_REDUNDANT_CAST_TO_VARCHAR_IN_JOIN, "true")
                .on(p -> {
                    VariableReferenceExpression leftBigint = p.variable("left_bigint", DOUBLE);
                    VariableReferenceExpression rightBigint = p.variable("right_bigint", DOUBLE);
                    VariableReferenceExpression leftCast = p.variable("left_cast", VARCHAR);
                    VariableReferenceExpression rightCast = p.variable("right_cast", VARCHAR);
                    return p.join(
                            JoinType.INNER,
                            p.project(assignment(leftCast, p.rowExpression("cast(left_bigint as varchar)")),
                                    p.values(leftBigint)),
                            p.project(assignment(rightCast, p.rowExpression("cast(right_bigint as varchar)")),
                                    p.values(rightBigint)),
                            new EquiJoinClause(leftCast, rightCast));
                }).doesNotFire();
    }
}
