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

import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.plan.Ordering;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.specification;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.window;
import static com.facebook.presto.sql.planner.plan.WindowNode.Frame.BoundType.CURRENT_ROW;
import static com.facebook.presto.sql.planner.plan.WindowNode.Frame.BoundType.UNBOUNDED_PRECEDING;
import static com.facebook.presto.sql.planner.plan.WindowNode.Frame.WindowType.RANGE;
import static com.facebook.presto.sql.relational.Expressions.call;
import static java.util.Collections.emptyList;

public class TestRemoveRedundantOrderByInWindowNode
        extends BaseRuleTest
{
    private final CallExpression call;
    private final WindowNode.Frame frame;

    public TestRemoveRedundantOrderByInWindowNode()
    {
        FunctionHandle functionHandle = createTestMetadataManager().getFunctionAndTypeManager().lookupFunction("row_number", ImmutableList.of());
        call = call("row_number", functionHandle, BIGINT);
        frame = new WindowNode.Frame(
                RANGE,
                UNBOUNDED_PRECEDING,
                Optional.empty(),
                CURRENT_ROW,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    @BeforeClass
    public final void setUp()
    {
        tester = new RuleTester(emptyList(), ImmutableMap.of("remove_redundant_order_by_in_window_enabled", "true"));
    }

    @Test
    public void testOrderBySameAsPartitionBy()
    {
        tester().assertThat(new RemoveRedundantOrderByInWindowNode())
                .on(p -> p.window(
                        new WindowNode.Specification(
                                ImmutableList.of(p.variable("orderkey", BIGINT)),
                                Optional.of(new OrderingScheme(ImmutableList.of(new Ordering(p.variable("orderkey", BIGINT), SortOrder.ASC_NULLS_LAST))))),
                        ImmutableMap.of(
                                p.variable("row_number"),
                                new WindowNode.Function(call, frame, false)),
                        p.values(p.variable("orderkey", BIGINT))))
                .matches(
                        window(windowMatcherBuilder -> windowMatcherBuilder
                                        .specification(specification(ImmutableList.of("orderkey"), ImmutableList.of(), ImmutableMap.of()))
                                        .addFunction(functionCall("row_number", Optional.empty(), ImmutableList.of())),
                                values(ImmutableMap.of("orderkey", 0))));
    }

    @Test
    public void testOrderBySubSetOfPartitionBy()
    {
        tester().assertThat(new RemoveRedundantOrderByInWindowNode())
                .on(p -> p.window(
                        new WindowNode.Specification(
                                ImmutableList.of(p.variable("orderkey", BIGINT), p.variable("partkey", BIGINT)),
                                Optional.of(new OrderingScheme(ImmutableList.of(new Ordering(p.variable("orderkey", BIGINT), SortOrder.ASC_NULLS_LAST))))),
                        ImmutableMap.of(
                                p.variable("row_number"),
                                new WindowNode.Function(call, frame, false)),
                        p.values(p.variable("orderkey", BIGINT), p.variable("partkey", BIGINT))))
                .matches(
                        window(windowMatcherBuilder -> windowMatcherBuilder
                                        .specification(specification(ImmutableList.of("orderkey", "partkey"), ImmutableList.of(), ImmutableMap.of()))
                                        .addFunction(functionCall("row_number", Optional.empty(), ImmutableList.of())),
                                values(ImmutableMap.of("orderkey", 0, "partkey", 1))));
    }

    @Test
    public void testPartitionBySubSetOfOrderBy()
    {
        tester().assertThat(new RemoveRedundantOrderByInWindowNode())
                .on(p -> p.window(
                        new WindowNode.Specification(
                                ImmutableList.of(p.variable("orderkey", BIGINT)),
                                Optional.of(new OrderingScheme(ImmutableList.of(new Ordering(p.variable("orderkey", BIGINT), SortOrder.ASC_NULLS_LAST), new Ordering(p.variable("partkey", BIGINT), SortOrder.ASC_NULLS_LAST))))),
                        ImmutableMap.of(
                                p.variable("row_number"),
                                new WindowNode.Function(call, frame, false)),
                        p.values(p.variable("orderkey", BIGINT), p.variable("partkey", BIGINT)))).doesNotFire();
    }

    @Test
    public void testOrderByDifferentWithPartitionBy()
    {
        tester().assertThat(new RemoveRedundantOrderByInWindowNode())
                .on(p -> p.window(
                        new WindowNode.Specification(
                                ImmutableList.of(p.variable("orderkey", BIGINT)),
                                Optional.of(new OrderingScheme(ImmutableList.of(new Ordering(p.variable("partkey", BIGINT), SortOrder.ASC_NULLS_LAST))))),
                        ImmutableMap.of(
                                p.variable("row_number"),
                                new WindowNode.Function(call, frame, false)),
                        p.values(p.variable("orderkey", BIGINT), p.variable("partkey", BIGINT)))).doesNotFire();
    }
}
