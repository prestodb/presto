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

import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.assertions.SymbolMatcher;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.facebook.presto.sql.planner.plan.AssignUniqueId;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.MarkDistinctNode;
import com.facebook.presto.sql.planner.plan.SampleNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.except;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.exchange;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.intersect;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.union;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.plan.SampleNode.Type.BERNOULLI;

public class TestPushFilter
{
    private final RuleTester tester = new RuleTester();
    private final Rule rule = new PushFilter();

    @Test
    public void testDoesNotFire()
    {
        tester.assertThat(rule)
                .on(p ->
                        p.filter(
                                p.expression("uid"),
                                p.assignUniqueId(p.symbol("uid", BOOLEAN),
                                        p.values(p.symbol("a", BOOLEAN)))))
                .doesNotFire();
    }

    @Test
    public void testUnion()
    {
        tester.assertThat(rule)
                .on(p ->
                        p.filter(
                                p.expression("o1 or o2"),
                                p.union(
                                        ImmutableListMultimap.<Symbol, Symbol>builder()
                                                .putAll(p.symbol("o1", BOOLEAN), p.symbol("i11", BOOLEAN), p.symbol("i12", BOOLEAN))
                                                .putAll(p.symbol("o2", BOOLEAN), p.symbol("i21", BOOLEAN), p.symbol("i22", BOOLEAN))
                                                .build(),
                                        p.values(p.symbol("i11", BOOLEAN), p.symbol("i21", BOOLEAN)),
                                        p.values(p.symbol("i12", BOOLEAN), p.symbol("i22", BOOLEAN)))))
                .matches(
                        union(
                                filter("i11 or i21",
                                        values(ImmutableMap.of("i11", 0, "i21", 1))),
                                filter("i12 or i22",
                                        values(ImmutableMap.of("i12", 0, "i22", 1)))));
    }

    @Test
    public void testIntersect()
    {
        tester.assertThat(rule)
                .on(p ->
                        p.filter(
                                p.expression("o1 or o2"),
                                p.intersect(
                                        ImmutableListMultimap.<Symbol, Symbol>builder()
                                                .putAll(p.symbol("o1", BOOLEAN), p.symbol("i11", BOOLEAN), p.symbol("i12", BOOLEAN))
                                                .putAll(p.symbol("o2", BOOLEAN), p.symbol("i21", BOOLEAN), p.symbol("i22", BOOLEAN))
                                                .build(),
                                        p.values(p.symbol("i11", BOOLEAN), p.symbol("i21", BOOLEAN)),
                                        p.values(p.symbol("i12", BOOLEAN), p.symbol("i22", BOOLEAN)))))
                .matches(
                        intersect(
                                filter("i11 or i21",
                                        values(ImmutableMap.of("i11", 0, "i21", 1))),
                                filter("i12 or i22",
                                        values(ImmutableMap.of("i12", 0, "i22", 1)))));
    }

    @Test
    public void testIntersectWithNonDeterministicPredicate()
    {
        tester.assertThat(rule)
                .on(p ->
                        p.filter(
                                p.expression("o1 and o2 = random()"),
                                p.intersect(
                                        ImmutableListMultimap.<Symbol, Symbol>builder()
                                                .putAll(p.symbol("o1", BOOLEAN), p.symbol("i11", BOOLEAN), p.symbol("i12", BOOLEAN))
                                                .putAll(p.symbol("o2", BOOLEAN), p.symbol("i21", BOOLEAN), p.symbol("i22", BOOLEAN))
                                                .build(),
                                        p.values(p.symbol("i11", BOOLEAN), p.symbol("i21", BOOLEAN)),
                                        p.values(p.symbol("i12", BOOLEAN), p.symbol("i22", BOOLEAN)))))
                .matches(
                        filter("o2 = random()",
                                intersect(
                                        filter("i11",
                                                values(ImmutableMap.of("i11", 0, "i21", 1))),
                                        filter("i12",
                                                values(ImmutableMap.of("i12", 0, "i22", 1))))
                                        .withAlias("o2", new SymbolMatcher(1))));
    }

    @Test
    public void testExcept()
    {
        tester.assertThat(rule)
                .on(p ->
                        p.filter(
                                p.expression("o1 or o2"),
                                p.except(
                                        ImmutableListMultimap.<Symbol, Symbol>builder()
                                                .putAll(p.symbol("o1", BOOLEAN), p.symbol("i11", BOOLEAN), p.symbol("i12", BOOLEAN))
                                                .putAll(p.symbol("o2", BOOLEAN), p.symbol("i21", BOOLEAN), p.symbol("i22", BOOLEAN))
                                                .build(),
                                        p.values(p.symbol("i11", BOOLEAN), p.symbol("i21", BOOLEAN)),
                                        p.values(p.symbol("i12", BOOLEAN), p.symbol("i22", BOOLEAN)))))
                .matches(
                        except(
                                filter("i11 or i21",
                                        values(ImmutableMap.of("i11", 0, "i21", 1))),
                                filter("i12 or i22",
                                        values(ImmutableMap.of("i12", 0, "i22", 1)))));
    }

    @Test
    public void testExceptWithNonDeterministicPredicate()
    {
        tester.assertThat(rule)
                .on(p ->
                        p.filter(
                                p.expression("o1 and o2 = random()"),
                                p.except(
                                        ImmutableListMultimap.<Symbol, Symbol>builder()
                                                .putAll(p.symbol("o1", BOOLEAN), p.symbol("i11", BOOLEAN), p.symbol("i12", BOOLEAN))
                                                .putAll(p.symbol("o2", BOOLEAN), p.symbol("i21", BOOLEAN), p.symbol("i22", BOOLEAN))
                                                .build(),
                                        p.values(p.symbol("i11", BOOLEAN), p.symbol("i21", BOOLEAN)),
                                        p.values(p.symbol("i12", BOOLEAN), p.symbol("i22", BOOLEAN)))))
                .matches(
                        filter("o2 = random()",
                                except(
                                        filter("i11",
                                                values(ImmutableMap.of("i11", 0, "i21", 1))),
                                        filter("i12",
                                                values(ImmutableMap.of("i12", 0, "i22", 1))))
                                        .withAlias("o2", new SymbolMatcher(1))));
    }

    @Test
    public void testExchange()
    {
        tester.assertThat(rule)
                .on(p ->
                        p.filter(
                                p.expression("o1 or o2"),
                                p.exchange(
                                        ImmutableListMultimap.<Symbol, Symbol>builder()
                                                .putAll(p.symbol("o1", BOOLEAN), p.symbol("i11", BOOLEAN), p.symbol("i12", BOOLEAN))
                                                .putAll(p.symbol("o2", BOOLEAN), p.symbol("i21", BOOLEAN), p.symbol("i22", BOOLEAN))
                                                .build(),
                                        p.values(p.symbol("i11", BOOLEAN), p.symbol("i21", BOOLEAN)),
                                        p.values(p.symbol("i12", BOOLEAN), p.symbol("i22", BOOLEAN)))))
                .matches(
                        exchange(
                                filter("i11 or i21",
                                        values(ImmutableMap.of("i11", 0, "i21", 1))),
                                filter("i12 or i22",
                                        values(ImmutableMap.of("i12", 0, "i22", 1)))));
    }

    @Test
    public void testAssignUniqueId()
            throws Exception
    {
        tester.assertThat(rule)
                .on(p ->
                        p.filter(p.expression("a"),
                                p.assignUniqueId(p.symbol("uid", BOOLEAN),
                                        p.values(p.symbol("a", BOOLEAN)))))
                .matches(node(AssignUniqueId.class,
                        filter("a",
                                values(ImmutableMap.of("a", 0)))));

        tester.assertThat(rule)
                .on(p ->
                        p.filter(p.expression("a AND uid"),
                                p.assignUniqueId(p.symbol("uid", BOOLEAN),
                                        p.values(p.symbol("a", BOOLEAN)))))
                .matches(
                        filter("uid",
                                node(AssignUniqueId.class,
                                        filter("a",
                                                values(ImmutableMap.of("a", 0))))
                                        .withAlias("uid", new SymbolMatcher(1))));
    }

    @Test
    public void testSample()
            throws Exception
    {
        tester.assertThat(rule)
                .on(p ->
                        p.filter(p.expression("a"),
                                p.sample(1.0, BERNOULLI, p.values(p.symbol("a", BIGINT)))))
                .matches(node(SampleNode.class,
                        filter("a",
                                values(ImmutableMap.of("a", 0)))));
    }

    @Test
    public void testSort()
            throws Exception
    {
        tester.assertThat(rule)
                .on(p ->
                        p.filter(p.expression("a"),
                                p.sort(ImmutableList.of(p.symbol("a", BIGINT)), p.values(p.symbol("a", BIGINT)))))
                .matches(node(SortNode.class,
                        filter("a",
                                values(ImmutableMap.of("a", 0)))));
    }

    @Test
    public void testMarkDistinct()
            throws Exception
    {
        tester.assertThat(rule)
                .on(p ->
                        p.filter(p.expression("a"),
                                p.markDistinct(p.symbol("marker", BOOLEAN),
                                        p.values(p.symbol("a", BOOLEAN)))))
                .matches(node(MarkDistinctNode.class,
                        filter("a",
                                values(ImmutableMap.of("a", 0)))));

        tester.assertThat(rule)
                .on(p ->
                        p.filter(p.expression("a AND marker"),
                                p.markDistinct(p.symbol("marker", BOOLEAN),
                                        p.values(p.symbol("a", BOOLEAN)))))
                .matches(
                        filter("uid",
                                node(MarkDistinctNode.class,
                                        filter("a",
                                                values(ImmutableMap.of("a", 0))))
                                        .withAlias("uid", new SymbolMatcher(1))));
    }

    @Test
    public void testProject()
            throws Exception
    {
        tester.assertThat(rule)
                .on(p ->
                        p.filter(p.expression("r AND d"),
                                p.project(
                                        Assignments.of(
                                                p.symbol("r", BOOLEAN), p.expression("random()"),
                                                p.symbol("d", BOOLEAN), p.expression("a = 1")),
                                        p.values(p.symbol("a", BIGINT)))))
                .matches(
                        filter("r",
                                project(
                                        ImmutableMap.of("r", expression("random()"), "d", expression("a = 1")),
                                        filter("a = 1",
                                        values(ImmutableMap.of("a", 0))))));
    }
}
