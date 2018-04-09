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
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.JoinNode.DistributionType;
import com.facebook.presto.sql.planner.plan.JoinNode.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.DISTRIBUTED_JOIN;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.enforceSingleRow;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.expressions;
import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.PARTITIONED;
import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.REPLICATED;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.FULL;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.LEFT;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.RIGHT;

@Test(singleThreaded = true)
public class TestDetermineJoinDistributionType
        extends BaseRuleTest
{
    @Test
    public void testDetermineDistributionType()
    {
        testDetermineDistributionType(true, INNER, PARTITIONED);
        testDetermineDistributionType(false, INNER, REPLICATED);
    }

    @Test
    public void testDetermineDistributionTypeForLeftOuter()
    {
        testDetermineDistributionType(true, LEFT, PARTITIONED);
        testDetermineDistributionType(false, LEFT, REPLICATED);
    }

    private void testDetermineDistributionType(boolean sessionDistributedJoin, Type joinType, DistributionType expectedDistribution)
    {
        tester().assertThat(new DetermineJoinDistributionType())
                .on(p ->
                        p.join(
                                joinType,
                                p.values(ImmutableList.of(p.symbol("A1")), ImmutableList.of(expressions("10"), expressions("11"))),
                                p.values(ImmutableList.of(p.symbol("B1")), ImmutableList.of(expressions("50"), expressions("11"))),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT))),
                                ImmutableList.of(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT)),
                                Optional.empty()))
                .setSystemProperty(DISTRIBUTED_JOIN, Boolean.toString(sessionDistributedJoin))
                .matches(join(
                        joinType,
                        ImmutableList.of(equiJoinClause("B1", "A1")),
                        Optional.empty(),
                        Optional.of(expectedDistribution),
                        values(ImmutableMap.of("B1", 0)),
                        values(ImmutableMap.of("A1", 0))));
    }

    @Test
    public void testRepartitionRightOuter()
    {
        testRepartitionRightOuter(true, FULL);
        testRepartitionRightOuter(true, RIGHT);
        testRepartitionRightOuter(false, FULL);
        testRepartitionRightOuter(false, RIGHT);
    }

    private void testRepartitionRightOuter(boolean sessionDistributedJoin, Type joinType)
    {
        tester().assertThat(new DetermineJoinDistributionType())
                .on(p ->
                        p.join(
                                joinType,
                                p.values(ImmutableList.of(p.symbol("A1")), ImmutableList.of(expressions("10"), expressions("11"))),
                                p.values(ImmutableList.of(p.symbol("B1")), ImmutableList.of(expressions("50"), expressions("11"))),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT))),
                                ImmutableList.of(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT)),
                                Optional.empty()))
                .setSystemProperty(DISTRIBUTED_JOIN, Boolean.toString(sessionDistributedJoin))
                .matches(join(
                        joinType,
                        ImmutableList.of(equiJoinClause("A1", "B1")),
                        Optional.empty(),
                        Optional.of(PARTITIONED),
                        values(ImmutableMap.of("A1", 0)),
                        values(ImmutableMap.of("B1", 0))));
    }

    @Test
    public void testReplicateScalar()
    {
        tester().assertThat(new DetermineJoinDistributionType())
                .on(p ->
                        p.join(
                                INNER,
                                p.values(ImmutableList.of(p.symbol("A1")), ImmutableList.of(expressions("10"), expressions("11"))),
                                p.enforceSingleRow(
                                        p.values(ImmutableList.of(p.symbol("B1")), ImmutableList.of(expressions("50"), expressions("11")))),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT))),
                                ImmutableList.of(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT)),
                                Optional.empty()))
                .setSystemProperty(DISTRIBUTED_JOIN, "true")
                .matches(join(
                        INNER,
                        ImmutableList.of(equiJoinClause("A1", "B1")),
                        Optional.empty(),
                        Optional.of(REPLICATED),
                        values(ImmutableMap.of("A1", 0)),
                        enforceSingleRow(values(ImmutableMap.of("B1", 0)))));
    }

    @Test
    public void testReplicateNoEquiCriteria()
    {
        testReplicateNoEquiCriteria(INNER);
        testReplicateNoEquiCriteria(LEFT);
    }

    private void testReplicateNoEquiCriteria(Type joinType)
    {
        tester().assertThat(new DetermineJoinDistributionType())
                .on(p ->
                        p.join(
                                joinType,
                                p.values(ImmutableList.of(p.symbol("A1")), ImmutableList.of(expressions("10"), expressions("11"))),
                                p.values(ImmutableList.of(p.symbol("B1")), ImmutableList.of(expressions("50"), expressions("11"))),
                                ImmutableList.of(),
                                ImmutableList.of(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT)),
                                Optional.of(expression("A1 * B1 > 100"))))
                .setSystemProperty(DISTRIBUTED_JOIN, "true")
                .matches(join(
                        joinType,
                        ImmutableList.of(),
                        Optional.of("A1 * B1 > 100"),
                        Optional.of(REPLICATED),
                        values(ImmutableMap.of("A1", 0)),
                        values(ImmutableMap.of("B1", 0))));
    }

    @Test
    public void testRetainDistributionType()
    {
        tester().assertThat(new DetermineJoinDistributionType())
                .on(p ->
                        p.join(
                                INNER,
                                p.values(ImmutableList.of(p.symbol("A1")), ImmutableList.of(expressions("10"), expressions("11"))),
                                p.values(ImmutableList.of(p.symbol("B1")), ImmutableList.of(expressions("50"), expressions("11"))),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT))),
                                ImmutableList.of(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT)),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.of(REPLICATED)))
                .doesNotFire();
    }
}
