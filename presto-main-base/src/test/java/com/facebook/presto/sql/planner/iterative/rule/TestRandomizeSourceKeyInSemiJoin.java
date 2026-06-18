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

import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static com.facebook.presto.SystemSessionProperties.RANDOMIZE_NULL_SOURCE_KEY_IN_SEMI_JOIN_STRATEGY;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.semiJoin;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;

public class TestRandomizeSourceKeyInSemiJoin
        extends BaseRuleTest
{
    @Test
    public void testSemiJoinWithSupportedTypes()
    {
        tester().assertThat(new RandomizeSourceKeyInSemiJoin(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(RANDOMIZE_NULL_SOURCE_KEY_IN_SEMI_JOIN_STRATEGY, "ALWAYS")
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "PARTITIONED")
                .on(p -> {
                    return p.semiJoin(
                            p.variable("left_k1"),
                            p.variable("right_k1"),
                            p.variable("semi_output", BOOLEAN),
                            Optional.empty(),
                            Optional.empty(),
                            p.values(p.variable("left_k1")),
                            p.values(p.variable("right_k1")));
                })
                .matches(
                        project(
                                ImmutableMap.of("left_k1", expression("left_k1"), "semi_output", expression("semi_join_output_randomized OR if(left_k1 IS NULL, CAST(null AS boolean), false)")),
                                semiJoin(
                                        "randomized_left", "cast_right", "semi_join_output_randomized",
                                        project(
                                                ImmutableMap.of("left_k1", expression("left_k1"), "randomized_left", expression("coalesce(cast(left_k1 as varchar), 'l' || cast(random(100) as varchar))")),
                                                values("left_k1")),
                                        project(
                                                ImmutableMap.of("right_k1", expression("right_k1"), "cast_right", expression("cast(right_k1 as varchar)")),
                                                values("right_k1")))));
    }

    @Test
    public void testSemiJoinWithIntegerType()
    {
        tester().assertThat(new RandomizeSourceKeyInSemiJoin(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(RANDOMIZE_NULL_SOURCE_KEY_IN_SEMI_JOIN_STRATEGY, "ALWAYS")
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "PARTITIONED")
                .on(p -> {
                    return p.semiJoin(
                            p.variable("left_k1", INTEGER),
                            p.variable("right_k1", INTEGER),
                            p.variable("semi_output", BOOLEAN),
                            Optional.empty(),
                            Optional.empty(),
                            p.values(p.variable("left_k1", INTEGER)),
                            p.values(p.variable("right_k1", INTEGER)));
                })
                .matches(
                        project(
                                ImmutableMap.of("left_k1", expression("left_k1"), "semi_output", expression("semi_join_output_randomized OR if(left_k1 IS NULL, CAST(null AS boolean), false)")),
                                semiJoin(
                                        "randomized_left", "cast_right", "semi_join_output_randomized",
                                        project(
                                                ImmutableMap.of("left_k1", expression("left_k1"), "randomized_left", expression("coalesce(cast(left_k1 as varchar), 'l' || cast(random(100) as varchar))")),
                                                values("left_k1")),
                                        project(
                                                ImmutableMap.of("right_k1", expression("right_k1"), "cast_right", expression("cast(right_k1 as varchar)")),
                                                values("right_k1")))));
    }

    @Test
    public void testSemiJoinWithUnsupportedType()
    {
        // VARCHAR type is not supported - rule should not fire
        tester().assertThat(new RandomizeSourceKeyInSemiJoin(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(RANDOMIZE_NULL_SOURCE_KEY_IN_SEMI_JOIN_STRATEGY, "ALWAYS")
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "PARTITIONED")
                .on(p -> {
                    return p.semiJoin(
                            p.variable("left_k1", VARCHAR),
                            p.variable("right_k1", VARCHAR),
                            p.variable("semi_output", BOOLEAN),
                            Optional.empty(),
                            Optional.empty(),
                            p.values(p.variable("left_k1", VARCHAR)),
                            p.values(p.variable("right_k1", VARCHAR)));
                })
                .doesNotFire();
    }

    @Test
    public void testSemiJoinWithReplicatedDistribution()
    {
        // Replicated distribution should not be optimized
        tester().assertThat(new RandomizeSourceKeyInSemiJoin(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(RANDOMIZE_NULL_SOURCE_KEY_IN_SEMI_JOIN_STRATEGY, "ALWAYS")
                .on(p -> {
                    return new SemiJoinNode(
                            Optional.empty(),
                            new PlanNodeId("semi"),
                            Optional.empty(),
                            p.values(p.variable("left_k1")),
                            p.values(p.variable("right_k1")),
                            p.variable("left_k1"),
                            p.variable("right_k1"),
                            p.variable("semi_output", BOOLEAN),
                            Optional.empty(),
                            Optional.empty(),
                            Optional.of(SemiJoinNode.DistributionType.REPLICATED),
                            ImmutableMap.of());
                })
                .doesNotFire();
    }

    @Test
    public void testDisabledWhenStrategyNotAlways()
    {
        // Rule should not fire when strategy is not ALWAYS
        tester().assertThat(new RandomizeSourceKeyInSemiJoin(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(RANDOMIZE_NULL_SOURCE_KEY_IN_SEMI_JOIN_STRATEGY, "DISABLED")
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "PARTITIONED")
                .on(p -> {
                    p.variable("left_k1", BIGINT);
                    p.variable("right_k1", BIGINT);
                    return p.semiJoin(
                            p.variable("left_k1"),
                            p.variable("right_k1"),
                            p.variable("semi_output", BOOLEAN),
                            Optional.empty(),
                            Optional.empty(),
                            p.values(p.variable("left_k1")),
                            p.values(p.variable("right_k1")));
                })
                .doesNotFire();
    }
}
