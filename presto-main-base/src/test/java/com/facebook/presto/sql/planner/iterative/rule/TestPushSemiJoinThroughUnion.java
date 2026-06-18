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

import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.PUSH_SEMI_JOIN_THROUGH_UNION;
import static com.facebook.presto.common.function.OperatorType.MULTIPLY;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.semiJoin;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.union;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.assignment;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;

public class TestPushSemiJoinThroughUnion
        extends BaseRuleTest
{
    @Test
    public void testDoesNotFireWhenSourceIsNotUnion()
    {
        tester().assertThat(new PushSemiJoinThroughUnion())
                .setSystemProperty(PUSH_SEMI_JOIN_THROUGH_UNION, "true")
                .on(p -> {
                    VariableReferenceExpression sourceJoinVar = p.variable("sourceJoinVar");
                    VariableReferenceExpression filterJoinVar = p.variable("filterJoinVar");
                    VariableReferenceExpression semiJoinOutput = p.variable("semiJoinOutput", BOOLEAN);
                    return p.semiJoin(
                            sourceJoinVar,
                            filterJoinVar,
                            semiJoinOutput,
                            Optional.empty(),
                            Optional.empty(),
                            p.values(sourceJoinVar),
                            p.values(filterJoinVar));
                })
                .doesNotFire();
    }

    @Test
    public void testPushThroughTwoBranchUnion()
    {
        tester().assertThat(new PushSemiJoinThroughUnion())
                .setSystemProperty(PUSH_SEMI_JOIN_THROUGH_UNION, "true")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a");
                    VariableReferenceExpression b = p.variable("b");
                    VariableReferenceExpression c = p.variable("c");
                    VariableReferenceExpression filterJoinVar = p.variable("filterJoinVar");
                    VariableReferenceExpression semiJoinOutput = p.variable("semiJoinOutput", BOOLEAN);
                    return p.semiJoin(
                            c,
                            filterJoinVar,
                            semiJoinOutput,
                            Optional.empty(),
                            Optional.empty(),
                            p.union(
                                    ImmutableListMultimap.<VariableReferenceExpression, VariableReferenceExpression>builder()
                                            .put(c, a)
                                            .put(c, b)
                                            .build(),
                                    ImmutableList.of(
                                            p.values(a),
                                            p.values(b))),
                            p.values(filterJoinVar));
                })
                .matches(
                        union(
                                semiJoin("a", "filterJoinVar", "semiJoinOutput_0",
                                        values("a"),
                                        values("filterJoinVar")),
                                semiJoin("b", "filterJoinVar", "semiJoinOutput_1",
                                        values("b"),
                                        values("filterJoinVar"))));
    }

    @Test
    public void testPushThroughThreeBranchUnion()
    {
        tester().assertThat(new PushSemiJoinThroughUnion())
                .setSystemProperty(PUSH_SEMI_JOIN_THROUGH_UNION, "true")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a");
                    VariableReferenceExpression b = p.variable("b");
                    VariableReferenceExpression d = p.variable("d");
                    VariableReferenceExpression c = p.variable("c");
                    VariableReferenceExpression filterJoinVar = p.variable("filterJoinVar");
                    VariableReferenceExpression semiJoinOutput = p.variable("semiJoinOutput", BOOLEAN);
                    return p.semiJoin(
                            c,
                            filterJoinVar,
                            semiJoinOutput,
                            Optional.empty(),
                            Optional.empty(),
                            p.union(
                                    ImmutableListMultimap.<VariableReferenceExpression, VariableReferenceExpression>builder()
                                            .put(c, a)
                                            .put(c, b)
                                            .put(c, d)
                                            .build(),
                                    ImmutableList.of(
                                            p.values(a),
                                            p.values(b),
                                            p.values(d))),
                            p.values(filterJoinVar));
                })
                .matches(
                        union(
                                semiJoin("a", "filterJoinVar", "semiJoinOutput_0",
                                        values("a"),
                                        values("filterJoinVar")),
                                semiJoin("b", "filterJoinVar", "semiJoinOutput_1",
                                        values("b"),
                                        values("filterJoinVar")),
                                semiJoin("d", "filterJoinVar", "semiJoinOutput_2",
                                        values("d"),
                                        values("filterJoinVar"))));
    }

    @Test
    public void testPushThroughProjectOverUnion()
    {
        FunctionResolution functionResolution = new FunctionResolution(tester().getMetadata().getFunctionAndTypeManager().getFunctionAndTypeResolver());
        tester().assertThat(new PushSemiJoinThroughUnion())
                .setSystemProperty(PUSH_SEMI_JOIN_THROUGH_UNION, "true")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a");
                    VariableReferenceExpression b = p.variable("b");
                    VariableReferenceExpression c = p.variable("c");
                    VariableReferenceExpression cTimes3 = p.variable("c_times_3");
                    VariableReferenceExpression filterJoinVar = p.variable("filterJoinVar");
                    VariableReferenceExpression semiJoinOutput = p.variable("semiJoinOutput", BOOLEAN);
                    return p.semiJoin(
                            cTimes3,
                            filterJoinVar,
                            semiJoinOutput,
                            Optional.empty(),
                            Optional.empty(),
                            p.project(
                                    assignment(
                                            cTimes3,
                                            call("c * 3", functionResolution.arithmeticFunction(MULTIPLY, BIGINT, BIGINT), BIGINT, c, constant(3L, BIGINT))),
                                    p.union(
                                            ImmutableListMultimap.<VariableReferenceExpression, VariableReferenceExpression>builder()
                                                    .put(c, a)
                                                    .put(c, b)
                                                    .build(),
                                            ImmutableList.of(
                                                    p.values(a),
                                                    p.values(b)))),
                            p.values(filterJoinVar));
                })
                .matches(
                        union(
                                semiJoin(
                                        project(
                                                ImmutableMap.of("a_times_3", expression("a * 3")),
                                                values("a")),
                                        values("filterJoinVar")),
                                semiJoin(
                                        project(
                                                ImmutableMap.of("b_times_3", expression("b * 3")),
                                                values("b")),
                                        values("filterJoinVar"))));
    }

    @Test
    public void testPushThroughUnionWithHashVariables()
    {
        tester().assertThat(new PushSemiJoinThroughUnion())
                .setSystemProperty(PUSH_SEMI_JOIN_THROUGH_UNION, "true")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a");
                    VariableReferenceExpression b = p.variable("b");
                    VariableReferenceExpression c = p.variable("c");
                    VariableReferenceExpression aHash = p.variable("aHash");
                    VariableReferenceExpression bHash = p.variable("bHash");
                    VariableReferenceExpression cHash = p.variable("cHash");
                    VariableReferenceExpression filterJoinVar = p.variable("filterJoinVar");
                    VariableReferenceExpression filterHash = p.variable("filterHash");
                    VariableReferenceExpression semiJoinOutput = p.variable("semiJoinOutput", BOOLEAN);
                    return p.semiJoin(
                            c,
                            filterJoinVar,
                            semiJoinOutput,
                            Optional.of(cHash),
                            Optional.of(filterHash),
                            p.union(
                                    ImmutableListMultimap.<VariableReferenceExpression, VariableReferenceExpression>builder()
                                            .put(c, a)
                                            .put(c, b)
                                            .put(cHash, aHash)
                                            .put(cHash, bHash)
                                            .build(),
                                    ImmutableList.of(
                                            p.values(a, aHash),
                                            p.values(b, bHash))),
                            p.values(filterJoinVar, filterHash));
                })
                .matches(
                        union(
                                semiJoin("a", "filterJoinVar", "semiJoinOutput_0",
                                        values("a", "aHash"),
                                        values("filterJoinVar", "filterHash")),
                                semiJoin("b", "filterJoinVar", "semiJoinOutput_1",
                                        values("b", "bHash"),
                                        values("filterJoinVar", "filterHash"))));
    }

    @Test
    public void testDoesNotFireWhenDisabled()
    {
        tester().assertThat(new PushSemiJoinThroughUnion())
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a");
                    VariableReferenceExpression b = p.variable("b");
                    VariableReferenceExpression c = p.variable("c");
                    VariableReferenceExpression filterJoinVar = p.variable("filterJoinVar");
                    VariableReferenceExpression semiJoinOutput = p.variable("semiJoinOutput", BOOLEAN);
                    return p.semiJoin(
                            c,
                            filterJoinVar,
                            semiJoinOutput,
                            Optional.empty(),
                            Optional.empty(),
                            p.union(
                                    ImmutableListMultimap.<VariableReferenceExpression, VariableReferenceExpression>builder()
                                            .put(c, a)
                                            .put(c, b)
                                            .build(),
                                    ImmutableList.of(
                                            p.values(a),
                                            p.values(b))),
                            p.values(filterJoinVar));
                })
                .doesNotFire();
    }
}
