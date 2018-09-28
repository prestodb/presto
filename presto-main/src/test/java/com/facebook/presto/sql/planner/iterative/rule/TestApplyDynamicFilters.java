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
import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.ENABLE_DYNAMIC_FILTERING;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static io.airlift.testing.Closeables.closeAllRuntimeException;

public class TestApplyDynamicFilters
{
    private RuleTester tester;

    @BeforeClass
    public void setUp()
    {
        tester = new RuleTester(Collections.emptyList(), ImmutableMap.of(ENABLE_DYNAMIC_FILTERING, "true"));
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        closeAllRuntimeException(tester);
        tester = null;
    }

    @Test
    public void testNotApplicable()
    {
        tester.assertThat(new ApplyDynamicFilters())
                .on(p -> p.values(p.symbol("a", BIGINT)))
                .doesNotFire();
    }

    @Test
    public void testNonInnerJoin()
    {
        tester.assertThat(new ApplyDynamicFilters())
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
    {
        tester.assertThat(new ApplyDynamicFilters())
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
    {
        tester.assertThat(new ApplyDynamicFilters())
                .on(p -> p.join(
                        INNER,
                        p.values(p.symbol("COL1", BIGINT)),
                        p.values(p.symbol("COL2", BIGINT)),
                        ImmutableList.of(new JoinNode.EquiJoinClause(new Symbol("COL1"), new Symbol("COL2"))),
                        ImmutableList.of(new Symbol("COL1"), new Symbol("COL2")),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableMap.of("COL2", new Symbol("COL2_ALIAS"))))
                .doesNotFire();
    }

    @Test
    public void testSingleDynamicFilterCondition()
    {
        tester.assertThat(new ApplyDynamicFilters())
                .on(p -> p.join(
                        INNER,
                        p.values(p.symbol("COL1", BIGINT)),
                        p.values(p.symbol("COL2", BIGINT)),
                        ImmutableList.of(new JoinNode.EquiJoinClause(new Symbol("COL1"), new Symbol("COL2"))),
                        ImmutableList.of(new Symbol("COL1"), new Symbol("COL2")),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()))
                .matches(
                        join(
                                INNER,
                                ImmutableList.of(equiJoinClause("COL1", "COL2")),
                                ImmutableMap.of("COL1", "COL2"),
                                Optional.empty(),
                                values("COL1"),
                                values("COL2")));
    }

    @Test
    public void testMultipleDynamicFilterConditions()
    {
        tester.assertThat(new ApplyDynamicFilters())
                .on(p -> p.join(
                        INNER,
                        p.values(p.symbol("L_COL1", BIGINT), p.symbol("L_COL2", BIGINT)),
                        p.values(p.symbol("R_COL1", BIGINT), p.symbol("R_COL2", BIGINT)),
                        ImmutableList.of(
                                new JoinNode.EquiJoinClause(new Symbol("L_COL1"), new Symbol("R_COL1")),
                                new JoinNode.EquiJoinClause(new Symbol("L_COL2"), new Symbol("R_COL2"))),
                        ImmutableList.of(new Symbol("L_COL1"), new Symbol("R_COL1")),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()))
                .matches(
                        join(
                                INNER,
                                ImmutableList.of(
                                        equiJoinClause("L_COL1", "R_COL1"),
                                        equiJoinClause("L_COL2", "R_COL2")),
                                ImmutableMap.of("L_COL1", "R_COL1", "L_COL2", "R_COL2"),
                                Optional.empty(),
                                values("L_COL1", "L_COL2"),
                                values("R_COL1", "R_COL2")));
    }
}
