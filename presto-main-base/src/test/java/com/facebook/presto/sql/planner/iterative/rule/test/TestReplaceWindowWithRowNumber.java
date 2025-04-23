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
package com.facebook.presto.sql.planner.iterative.rule.test;

import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.plan.DataOrganizationSpecification;
import com.facebook.presto.spi.plan.Ordering;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.WindowNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.rule.ReplaceWindowWithRowNumber;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.spi.plan.WindowNode.Frame.BoundType.UNBOUNDED_FOLLOWING;
import static com.facebook.presto.spi.plan.WindowNode.Frame.BoundType.UNBOUNDED_PRECEDING;
import static com.facebook.presto.spi.plan.WindowNode.Frame.WindowType.RANGE;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.rowNumber;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.relational.Expressions.call;

public class TestReplaceWindowWithRowNumber
        extends BaseRuleTest
{
    @Test
    public void test()
    {
        FunctionHandle rowNumberHandle = tester.getMetadata().getFunctionAndTypeManager().lookupFunction("row_number", ImmutableList.of());
        CallExpression rowNumber = call("row_number", rowNumberHandle, BIGINT, ImmutableList.of(new VariableReferenceExpression(Optional.empty(), "a", BIGINT)));
        tester().assertThat(new ReplaceWindowWithRowNumber())
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a");
                    VariableReferenceExpression rowNumberSymbol = p.variable("row_number_1");
                    return p.window(
                            new DataOrganizationSpecification(ImmutableList.of(a), Optional.empty()),
                            ImmutableMap.of(rowNumberSymbol, newWindowNodeFunction(rowNumber)),
                            p.values(a));
                })
                .matches(rowNumber(
                        pattern -> pattern
                                .maxRowCountPerPartition(Optional.empty())
                                .partitionBy(ImmutableList.of("a")),
                        values("a")));

        tester().assertThat(new ReplaceWindowWithRowNumber())
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a");
                    VariableReferenceExpression rowNumberSymbol = p.variable("row_number_1");
                    return p.window(
                            new DataOrganizationSpecification(ImmutableList.of(), Optional.empty()),
                            ImmutableMap.of(rowNumberSymbol, newWindowNodeFunction(rowNumber)),
                            p.values(a));
                })
                .matches(rowNumber(
                        pattern -> pattern
                                .maxRowCountPerPartition(Optional.empty())
                                .partitionBy(ImmutableList.of()),
                        values("a")));
    }

    @Test
    public void testDoNotFire()
    {
        FunctionHandle rankHandle = tester.getMetadata().getFunctionAndTypeManager().lookupFunction("rank", ImmutableList.of());
        CallExpression rank = call("rank", rankHandle, BIGINT, ImmutableList.of(new VariableReferenceExpression(Optional.empty(), "a", BIGINT)));
        tester().assertThat(new ReplaceWindowWithRowNumber())
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a");
                    VariableReferenceExpression rank1 = p.variable("rank_1");
                    return p.window(
                            new DataOrganizationSpecification(ImmutableList.of(a), Optional.empty()),
                            ImmutableMap.of(rank1, newWindowNodeFunction(rank)),
                            p.values(a));
                })
                .doesNotFire();

        FunctionHandle rowNumberHandle = tester.getMetadata().getFunctionAndTypeManager().lookupFunction("row_number", ImmutableList.of());
        CallExpression rowNumber = call("row_number", rowNumberHandle, BIGINT, ImmutableList.of(new VariableReferenceExpression(Optional.empty(), "a", BIGINT)));
        tester().assertThat(new ReplaceWindowWithRowNumber())
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a");
                    VariableReferenceExpression rowNumber1 = p.variable("row_number_1");
                    VariableReferenceExpression rank1 = p.variable("rank_1");
                    return p.window(
                            new DataOrganizationSpecification(ImmutableList.of(a), Optional.empty()),
                            ImmutableMap.of(rowNumber1, newWindowNodeFunction(rowNumber), rank1, newWindowNodeFunction(rank)),
                            p.values(a));
                })
                .doesNotFire();

        tester().assertThat(new ReplaceWindowWithRowNumber())
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a");
                    OrderingScheme orderingScheme = new OrderingScheme(ImmutableList.of(new Ordering(a, SortOrder.ASC_NULLS_FIRST)));
                    VariableReferenceExpression rowNumber1 = p.variable("row_number_1");
                    return p.window(
                            new DataOrganizationSpecification(ImmutableList.of(a), Optional.of(orderingScheme)),
                            ImmutableMap.of(rowNumber1, newWindowNodeFunction(rowNumber)),
                            p.values(a));
                })
                .doesNotFire();
    }

    private static WindowNode.Function newWindowNodeFunction(CallExpression callExpression)
    {
        WindowNode.Frame frame = new WindowNode.Frame(
                RANGE,
                UNBOUNDED_PRECEDING,
                Optional.empty(),
                Optional.empty(),
                UNBOUNDED_FOLLOWING,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
        return new WindowNode.Function(
                callExpression,
                frame,
                false);
    }
}
