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
import com.facebook.presto.sql.tree.SortItem.NullOrdering;
import com.facebook.presto.sql.tree.SortItem.Ordering;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.PREFER_SORT_MERGE_JOIN;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.mergeJoin;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.sort;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.PARTITIONED;
import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.REPLICATED;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.LEFT;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.RIGHT;

public class TestConvertHashJoinToSortMergeJoin
        extends BaseRuleTest
{
    @Test
    public void testConvertsHashJoinToSortMergeJoin()
    {
        tester().assertThat(new ConvertHashJoinToSortMergeJoin())
                .setSystemProperty(PREFER_SORT_MERGE_JOIN, "true")
                .on(p ->
                        p.join(
                                INNER,
                                p.values(p.variable("A1")),
                                p.values(p.variable("B1")),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.variable("A1", BIGINT), p.variable("B1", BIGINT))),
                                ImmutableList.of(p.variable("A1", BIGINT), p.variable("B1", BIGINT)),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.of(PARTITIONED),
                                ImmutableMap.of()))
                .matches(
                        mergeJoin(
                                INNER,
                                ImmutableList.of(equiJoinClause("A1", "B1")),
                                Optional.empty(),
                                sort(
                                        ImmutableList.of(sort("A1", Ordering.ASCENDING, NullOrdering.FIRST)),
                                        values("A1")),
                                sort(
                                        ImmutableList.of(sort("B1", Ordering.ASCENDING, NullOrdering.FIRST)),
                                        values("B1"))));
    }

    @Test
    public void testDoesNotFireWhenDisabled()
    {
        tester().assertThat(new ConvertHashJoinToSortMergeJoin())
                .setSystemProperty(PREFER_SORT_MERGE_JOIN, "false")
                .on(p ->
                        p.join(
                                INNER,
                                p.values(p.variable("A1")),
                                p.values(p.variable("B1")),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.variable("A1", BIGINT), p.variable("B1", BIGINT))),
                                ImmutableList.of(p.variable("A1", BIGINT), p.variable("B1", BIGINT)),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.of(PARTITIONED),
                                ImmutableMap.of()))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForCrossJoin()
    {
        tester().assertThat(new ConvertHashJoinToSortMergeJoin())
                .setSystemProperty(PREFER_SORT_MERGE_JOIN, "true")
                .on(p ->
                        p.join(
                                RIGHT,
                                p.values(p.variable("A1")),
                                p.values(p.variable("B1")),
                                ImmutableList.of(),
                                ImmutableList.of(p.variable("A1", BIGINT), p.variable("B1", BIGINT)),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.of(PARTITIONED),
                                ImmutableMap.of()))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForBroadcastJoin()
    {
        tester().assertThat(new ConvertHashJoinToSortMergeJoin())
                .setSystemProperty(PREFER_SORT_MERGE_JOIN, "true")
                .on(p ->
                        p.join(
                                INNER,
                                p.values(p.variable("A1")),
                                p.values(p.variable("B1")),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.variable("A1", BIGINT), p.variable("B1", BIGINT))),
                                ImmutableList.of(p.variable("A1", BIGINT), p.variable("B1", BIGINT)),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.of(REPLICATED),
                                ImmutableMap.of()))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForOuterJoin()
    {
        tester().assertThat(new ConvertHashJoinToSortMergeJoin())
                .setSystemProperty(PREFER_SORT_MERGE_JOIN, "true")
                .on(p ->
                        p.join(
                                LEFT,
                                p.values(p.variable("A1")),
                                p.values(p.variable("B1")),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.variable("A1", BIGINT), p.variable("B1", BIGINT))),
                                ImmutableList.of(p.variable("A1", BIGINT), p.variable("B1", BIGINT)),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.of(PARTITIONED),
                                ImmutableMap.of()))
                .doesNotFire();
    }
}
