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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.semiJoin;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.union;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;

public class TestPushSemiJoinThroughUnion
        extends BaseRuleTest
{
    @Test
    public void testPushThroughUnion()
    {
        tester().assertThat(new PushSemiJoinThroughUnion())
                .on(p ->
                        p.semiJoin(
                                p.variable("A"),
                                p.variable("B"),
                                p.variable("match"),
                                Optional.of(p.variable("leftKeyHash")),
                                Optional.empty(),
                                p.union(
                                        ImmutableListMultimap.<VariableReferenceExpression, VariableReferenceExpression>builder()
                                                .putAll(p.variable("A", BIGINT), p.variable("A", BIGINT), p.variable("A", BIGINT))
                                                .putAll(p.variable("B", BIGINT), p.variable("B", BIGINT), p.variable("B", BIGINT))
                                                .build(),
                                        ImmutableList.of(
                                                p.values(p.variable("A", BIGINT), p.variable("B", BIGINT)),
                                                p.values(p.variable("A", BIGINT), p.variable("B", BIGINT)))),
                                p.values(p.variable("B"))))
                .matches(
                        union(
                                semiJoin("A", "B", "match0", values("A", "B"), values("B")),
                                semiJoin("A", "B", "match1", values("A", "B"), values("B"))));
    }

    @Test
    public void testPushThroughUnionSessionVariable()
    {
        tester.assertThat(new PushSemiJoinThroughUnion())
                .setSystemProperty("push_semi_join_through_union", "false")
                .on(p ->
                        p.semiJoin(
                                p.variable("A"),
                                p.variable("B"),
                                p.variable("match"),
                                Optional.of(p.variable("leftKeyHash")),
                                Optional.empty(),
                                p.union(
                                        ImmutableListMultimap.<VariableReferenceExpression, VariableReferenceExpression>builder()
                                                .putAll(p.variable("A", BIGINT), p.variable("A", BIGINT), p.variable("A", BIGINT))
                                                .putAll(p.variable("B", BIGINT), p.variable("B", BIGINT), p.variable("B", BIGINT))
                                                .build(),
                                        ImmutableList.of(
                                                p.values(p.variable("A", BIGINT), p.variable("B", BIGINT)),
                                                p.values(p.variable("A", BIGINT), p.variable("B", BIGINT)))),
                                p.values(p.variable("B"))))
                .doesNotFire();
    }
}
