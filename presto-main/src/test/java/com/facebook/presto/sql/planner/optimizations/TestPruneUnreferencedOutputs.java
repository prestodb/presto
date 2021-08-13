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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.Ordering;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.sql.planner.assertions.OptimizerAssert;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.output;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.plan.WindowNode.Frame.BoundType.UNBOUNDED_FOLLOWING;
import static com.facebook.presto.sql.planner.plan.WindowNode.Frame.BoundType.UNBOUNDED_PRECEDING;
import static com.facebook.presto.sql.planner.plan.WindowNode.Frame.WindowType.RANGE;
import static com.facebook.presto.sql.relational.Expressions.call;

public class TestPruneUnreferencedOutputs
        extends BaseRuleTest
{
    /**
     * Test that the unreferenced output pruning works correctly when WindowNode is pruned as no downstream operators are consuming the window function output
     */
    @Test
    public void windowNodePruning()
    {
        FunctionHandle functionHandle = createTestMetadataManager().getFunctionAndTypeManager().lookupFunction("rank", ImmutableList.of());
        CallExpression call = call("rank", functionHandle, BIGINT);
        WindowNode.Frame frame = new WindowNode.Frame(
                RANGE,
                UNBOUNDED_PRECEDING,
                Optional.empty(),
                UNBOUNDED_FOLLOWING,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        assertRuleApplication()
                .on(p ->
                        p.output(ImmutableList.of("user_uuid"), ImmutableList.of(p.variable("user_uuid", VARCHAR)),
                                p.project(Assignments.of(p.variable("user_uuid", VARCHAR), p.variable("user_uuid", VARCHAR)),
                                        p.window(
                                                new WindowNode.Specification(
                                                        ImmutableList.of(p.variable("user_uuid", VARCHAR)),
                                                        Optional.of(new OrderingScheme(
                                                                ImmutableList.of(
                                                                        new Ordering(p.variable("expr"), SortOrder.ASC_NULLS_LAST),
                                                                        new Ordering(p.variable("random"), SortOrder.ASC_NULLS_LAST))))),
                                                ImmutableMap.of(
                                                        p.variable("rank"),
                                                        new WindowNode.Function(call, frame, false)),
                                                p.project(Assignments.builder()
                                                                .put(p.variable("user_uuid", VARCHAR), p.variable("user_uuid", VARCHAR))
                                                                .put(p.variable("expr", BIGINT), p.variable("expr", BIGINT))
                                                                .put(p.variable("random", BIGINT), p.rowExpression("random()"))
                                                                .build(),
                                                        p.values(p.variable("user_uuid", VARCHAR), p.variable("expr", BIGINT)))))))
                .matches(
                        output(
                                project(
                                        project(
                                                values("user_uuid")))));
    }

    private OptimizerAssert assertRuleApplication()
    {
        RuleTester tester = tester();
        return tester.assertThat(new PruneUnreferencedOutputs());
    }
}
