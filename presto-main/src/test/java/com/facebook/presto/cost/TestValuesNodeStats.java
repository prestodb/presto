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
package com.facebook.presto.cost;

import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.UnknownType.UNKNOWN;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.constantExpressions;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.Expressions.constantNull;
import static com.facebook.presto.sql.tree.ArithmeticBinaryExpression.Operator.ADD;
import static io.airlift.slice.Slices.utf8Slice;

public class TestValuesNodeStats
        extends BaseStatsCalculatorTest
{
    @Test
    public void testStatsForValuesNode()
    {
        FunctionResolution resolution = new FunctionResolution(tester().getMetadata().getFunctionAndTypeManager());
        tester().assertStatsFor(pb -> pb
                .values(
                        ImmutableList.of(pb.variable("a", BIGINT), pb.variable("b", DOUBLE)),
                        ImmutableList.of(
                                ImmutableList.of(call(ADD.name(), resolution.arithmeticFunction(ADD, BIGINT, BIGINT), BIGINT, constantExpressions(BIGINT, 3L, 3L)), constant(13.5, DOUBLE)),
                                ImmutableList.of(constant(55L, BIGINT), constantNull(DOUBLE)),
                                ImmutableList.of(constant(6L, BIGINT), constant(13.5, DOUBLE)))))
                .check(outputStats -> outputStats.equalTo(
                        PlanNodeStatsEstimate.builder()
                                .setOutputRowCount(3)
                                .addVariableStatistics(
                                        new VariableReferenceExpression("a", BIGINT),
                                        VariableStatsEstimate.builder()
                                                .setNullsFraction(0)
                                                .setLowValue(6)
                                                .setHighValue(55)
                                                .setDistinctValuesCount(2)
                                                .build())
                                .addVariableStatistics(
                                        new VariableReferenceExpression("b", DOUBLE),
                                        VariableStatsEstimate.builder()
                                                .setNullsFraction(0.33333333333333333)
                                                .setLowValue(13.5)
                                                .setHighValue(13.5)
                                                .setDistinctValuesCount(1)
                                                .build())
                                .build()));

        tester().assertStatsFor(pb -> pb
                .values(
                        ImmutableList.of(pb.variable("v", createVarcharType(30))),
                        ImmutableList.of(
                                constantExpressions(VARCHAR, utf8Slice("Alice")),
                                constantExpressions(VARCHAR, utf8Slice("has")),
                                constantExpressions(VARCHAR, utf8Slice("a cat")),
                                ImmutableList.of(constantNull(VARCHAR)))))
                .check(outputStats -> outputStats.equalTo(
                        PlanNodeStatsEstimate.builder()
                                .setOutputRowCount(4)
                                .addVariableStatistics(
                                        new VariableReferenceExpression("v", createVarcharType(30)),
                                        VariableStatsEstimate.builder()
                                                .setNullsFraction(0.25)
                                                .setDistinctValuesCount(3)
                                                // TODO .setAverageRowSize(4 + 1. / 3)
                                                .build())
                                .build()));
    }

    @Test
    public void testStatsForValuesNodeWithJustNulls()
    {
        FunctionResolution resolution = new FunctionResolution(tester().getMetadata().getFunctionAndTypeManager());
        PlanNodeStatsEstimate bigintNullAStats = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(1)
                .addVariableStatistics(new VariableReferenceExpression("a", BIGINT), VariableStatsEstimate.zero())
                .build();

        tester().assertStatsFor(pb -> pb
                .values(
                        ImmutableList.of(pb.variable("a", BIGINT)),
                        ImmutableList.of(
                                ImmutableList.of(call(ADD.name(), resolution.arithmeticFunction(ADD, BIGINT, BIGINT), BIGINT, constant(3L, BIGINT), constantNull(BIGINT))))))
                .check(outputStats -> outputStats.equalTo(bigintNullAStats));

        tester().assertStatsFor(pb -> pb
                .values(
                        ImmutableList.of(pb.variable("a", BIGINT)),
                        ImmutableList.of(ImmutableList.of(constantNull(BIGINT)))))
                .check(outputStats -> outputStats.equalTo(bigintNullAStats));

        PlanNodeStatsEstimate unknownNullAStats = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(1)
                .addVariableStatistics(new VariableReferenceExpression("a", UNKNOWN), VariableStatsEstimate.zero())
                .build();

        tester().assertStatsFor(pb -> pb
                .values(
                        ImmutableList.of(pb.variable("a", UNKNOWN)),
                        ImmutableList.of(ImmutableList.of(constantNull(UNKNOWN)))))
                .check(outputStats -> outputStats.equalTo(unknownNullAStats));
    }

    @Test
    public void testStatsForEmptyValues()
    {
        tester().assertStatsFor(pb -> pb
                .values(
                        ImmutableList.of(pb.variable("a", BIGINT)),
                        ImmutableList.of()))
                .check(outputStats -> outputStats.equalTo(
                        PlanNodeStatsEstimate.builder()
                                .setOutputRowCount(0)
                                .addVariableStatistics(new VariableReferenceExpression("a", BIGINT), VariableStatsEstimate.zero())
                                .build()));
    }
}
