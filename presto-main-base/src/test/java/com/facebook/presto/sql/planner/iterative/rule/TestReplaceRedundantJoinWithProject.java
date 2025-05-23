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

import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.tree.NullLiteral;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.plan.JoinType.FULL;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.spi.plan.JoinType.LEFT;
import static com.facebook.presto.spi.plan.JoinType.RIGHT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static java.util.Collections.nCopies;

public class TestReplaceRedundantJoinWithProject
        extends BaseRuleTest
{
    @Test
    public void testDoesNotFireOnInnerJoin()
    {
        tester().assertThat(new ReplaceRedundantJoinWithProject())
                .on(p ->
                        p.join(
                                INNER,
                                p.values(0, p.variable("a")),
                                p.values(0, p.variable("b"))))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenOuterSourceEmpty()
    {
        tester().assertThat(new ReplaceRedundantJoinWithProject())
                .on(p ->
                        p.join(
                                LEFT,
                                p.values(0, p.variable("a")),
                                p.values(0, p.variable("b"))))
                .doesNotFire();

        tester().assertThat(new ReplaceRedundantJoinWithProject())
                .on(p ->
                        p.join(
                                RIGHT,
                                p.values(0, p.variable("a")),
                                p.values(0, p.variable("b"))))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireOnFullJoinWithBothSourcesEmpty()
    {
        tester().assertThat(new ReplaceRedundantJoinWithProject())
                .on(p ->
                        p.join(
                                FULL,
                                p.values(0, p.variable("a")),
                                p.values(0, p.variable("b"))))
                .doesNotFire();
    }

    @Test
    public void testReplaceLeftJoin()
    {
        tester().assertThat(new ReplaceRedundantJoinWithProject())
                .on(p ->
                        p.join(
                                LEFT,
                                p.values(10, p.variable("a")),
                                p.values(0, p.variable("b"))))
                .matches(
                        project(
                                ImmutableMap.of(
                                        "a", expression("a"),
                                        "b", expression("null")),
                                values(ImmutableList.of("a"), nCopies(10, ImmutableList.of(new NullLiteral())))));
    }

    @Test
    public void testReplaceRightJoin()
    {
        tester().assertThat(new ReplaceRedundantJoinWithProject())
                .on(p ->
                        p.join(
                                RIGHT,
                                p.values(0, p.variable("a")),
                                p.values(10, p.variable("b"))))
                .matches(
                        project(
                                ImmutableMap.of(
                                        "a", expression("null"),
                                        "b", expression("b")),
                                values(ImmutableList.of("b"), nCopies(10, ImmutableList.of(new NullLiteral())))));
    }

    @Test
    public void testReplaceFULLJoin()
    {
        tester().assertThat(new ReplaceRedundantJoinWithProject())
                .on(p ->
                        p.join(
                                FULL,
                                p.values(10, p.variable("a")),
                                p.values(0, p.variable("b"))))
                .matches(
                        project(
                                ImmutableMap.of(
                                        "a", expression("a"),
                                        "b", expression("null")),
                                values(ImmutableList.of("a"), nCopies(10, ImmutableList.of(new NullLiteral())))));

        tester().assertThat(new ReplaceRedundantJoinWithProject())
                .on(p ->
                        p.join(
                                FULL,
                                p.values(0, p.variable("a")),
                                p.values(10, p.variable("b"))))
                .matches(
                        project(
                                ImmutableMap.of(
                                        "a", expression("null"),
                                        "b", expression("b")),
                                values(ImmutableList.of("b"), nCopies(10, ImmutableList.of(new NullLiteral())))));
    }
}
