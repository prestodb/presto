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

import com.facebook.presto.sql.planner.DynamicFilter;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DeferredSymbolReference;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.sql.ExpressionUtils.combineConjuncts;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.EQUAL;
import static io.airlift.testing.Closeables.closeAllRuntimeException;

public class TestApplyDynamicFilter
{
    private RuleTester tester;

    @BeforeClass
    public void setUp()
    {
        tester = new RuleTester();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        closeAllRuntimeException(tester);
        tester = null;
    }

    @Test
    public void testNotApplicable()
            throws Exception
    {
        tester.assertThat(new ApplyDynamicFilter())
                .on(p -> p.values(p.symbol("a", BIGINT)))
                .doesNotFire();
    }

    @Test
    public void testNonInnerJoin()
            throws Exception
    {
        tester.assertThat(new ApplyDynamicFilter())
                .on(p -> p.join(
                        JoinNode.Type.LEFT,
                        p.values(p.symbol("COL1", BIGINT)),
                        p.values(p.symbol("COL2", BIGINT)),
                        ImmutableList.of(new JoinNode.EquiJoinClause(new Symbol("COL1"), new Symbol("COL2"))),
                        ImmutableList.of(new Symbol("COL1"), new Symbol("COL2")),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()))
                .doesNotFire();
    }

    @Test
    public void testEmptyJoinCriteria()
            throws Exception
    {
        tester.assertThat(new ApplyDynamicFilter())
                .on(p -> p.join(
                        INNER,
                        p.values(p.symbol("COL1", BIGINT)),
                        p.values(p.symbol("COL2", BIGINT)),
                        ImmutableList.of(),
                        ImmutableList.of(new Symbol("COL1"), new Symbol("COL2")),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()))
                .doesNotFire();
    }

    @Test
    public void testAlreadyProcessed()
            throws Exception
    {
        tester.assertThat(new ApplyDynamicFilter())
                .on(p -> p.join(
                        INNER,
                        p.values(p.symbol("COL1", BIGINT)),
                        p.values(p.symbol("COL2", BIGINT)),
                        ImmutableList.of(new JoinNode.EquiJoinClause(new Symbol("COL1"), new Symbol("COL2"))),
                        ImmutableList.of(new Symbol("COL1"), new Symbol("COL2")),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        new DynamicFilter("dynamic_filter", ImmutableMap.of(new Symbol("COL2"), new Symbol("COL2_ALIAS")))))
                .doesNotFire();
    }

    @Test
    public void testSingleJoinCondition()
            throws Exception
    {
        tester.assertThat(new ApplyDynamicFilter())
                .on(p -> p.join(
                        INNER,
                        p.values(p.symbol("COL1", BIGINT)),
                        p.values(p.symbol("COL2", BIGINT)),
                        ImmutableList.of(new JoinNode.EquiJoinClause(new Symbol("COL1"), new Symbol("COL2"))),
                        ImmutableList.of(new Symbol("COL1"), new Symbol("COL2")),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        new DynamicFilter("dynamic_filter", ImmutableMap.of())))
                .matches(
                        join(
                                INNER,
                                ImmutableList.of(equiJoinClause("COL1", "COL2")),
                                filter(combineConjuncts(
                                        new ComparisonExpression(
                                                EQUAL,
                                                new SymbolReference("COL1"),
                                                new DeferredSymbolReference("dynamic_filter", "wat"))),
                                        values("COL1")),
                                values("COL2")));
    }
}
