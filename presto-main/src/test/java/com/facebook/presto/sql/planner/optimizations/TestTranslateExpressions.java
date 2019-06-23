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

import com.facebook.presto.metadata.FunctionManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.type.FunctionType;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.OriginalExpressionUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static com.facebook.presto.sql.planner.plan.AggregationNode.Aggregation;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.Expressions.variable;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToRowExpression;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestTranslateExpressions
{
    private static final Metadata METADATA = createTestMetadataManager();
    private static final FunctionManager FUNCTION_MANAGER = METADATA.getFunctionManager();
    private static final FunctionResolution FUNCTION_RESOLUTION = new FunctionResolution(FUNCTION_MANAGER);
    private static final FunctionHandle REDUCE_AGG = FUNCTION_MANAGER.lookupFunction(
            "reduce_agg",
            fromTypes(
                    INTEGER,
                    INTEGER,
                    new FunctionType(ImmutableList.of(INTEGER, INTEGER), INTEGER),
                    new FunctionType(ImmutableList.of(INTEGER, INTEGER), INTEGER)));

    @Test
    public void testTranslateAggregationWithLambda()
    {
        TranslateExpressions translator = new TranslateExpressions(METADATA, new SqlParser());
        Aggregation aggregation = new Aggregation(
                new CallExpression(
                        "reduce_agg",
                        REDUCE_AGG,
                        INTEGER,
                        ImmutableList.of(
                                castToRowExpression(expression("input")),
                                castToRowExpression(expression("0")),
                                castToRowExpression(expression("(x,y) -> x*y")),
                                castToRowExpression(expression("(a,b) -> a*b")))),
                Optional.of(castToRowExpression(expression("input > 10"))),
                Optional.empty(),
                false,
                Optional.empty());
        Aggregation translated = translator.translateAggregation(aggregation, TEST_SESSION, TypeProvider.viewOf(ImmutableMap.of("input", INTEGER)));
        assertEquals(translated, new Aggregation(
                new CallExpression(
                        "reduce_agg",
                        REDUCE_AGG,
                        INTEGER,
                        ImmutableList.of(
                                variable("input", INTEGER),
                                constant(0L, INTEGER),
                                new LambdaDefinitionExpression(
                                        ImmutableList.of(INTEGER, INTEGER),
                                        ImmutableList.of("x", "y"),
                                        multiply(variable("x", INTEGER), variable("y", INTEGER))),
                                new LambdaDefinitionExpression(
                                        ImmutableList.of(INTEGER, INTEGER),
                                        ImmutableList.of("a", "b"),
                                        multiply(variable("a", INTEGER), variable("b", INTEGER))))),
                Optional.of(greaterThan(variable("input", INTEGER), constant(10L, INTEGER))),
                Optional.empty(),
                false,
                Optional.empty()));
        assertFalse(isUntranslated(translated));
    }

    @Test
    public void testTranslateIntermediateAggregationWithLambda()
    {
        TranslateExpressions translator = new TranslateExpressions(METADATA, new SqlParser());
        Aggregation aggregation = new Aggregation(
                new CallExpression(
                        "reduce_agg",
                        REDUCE_AGG,
                        INTEGER,
                        ImmutableList.of(
                                castToRowExpression(expression("input")),
                                castToRowExpression(expression("(x,y) -> x*y")),
                                castToRowExpression(expression("(a,b) -> a*b")))),
                Optional.of(castToRowExpression(expression("input > 10"))),
                Optional.empty(),
                false,
                Optional.empty());
        Aggregation translated = translator.translateAggregation(aggregation, TEST_SESSION, TypeProvider.viewOf(ImmutableMap.of("input", INTEGER)));
        assertEquals(translated, new Aggregation(
                new CallExpression(
                        "reduce_agg",
                        REDUCE_AGG,
                        INTEGER,
                        ImmutableList.of(
                                variable("input", INTEGER),
                                new LambdaDefinitionExpression(
                                        ImmutableList.of(INTEGER, INTEGER),
                                        ImmutableList.of("x", "y"),
                                        multiply(variable("x", INTEGER), variable("y", INTEGER))),
                                new LambdaDefinitionExpression(
                                        ImmutableList.of(INTEGER, INTEGER),
                                        ImmutableList.of("a", "b"),
                                        multiply(variable("a", INTEGER), variable("b", INTEGER))))),
                Optional.of(greaterThan(variable("input", INTEGER), constant(10L, INTEGER))),
                Optional.empty(),
                false,
                Optional.empty()));
        assertFalse(isUntranslated(translated));
    }

    private CallExpression greaterThan(RowExpression left, RowExpression right)
    {
        return call("GREATER_THAN", FUNCTION_RESOLUTION.comparisonFunction(OperatorType.GREATER_THAN, left.getType(), right.getType()), BOOLEAN, ImmutableList.of(left, right));
    }

    private CallExpression multiply(RowExpression left, RowExpression right)
    {
        return call("MULTIPLY", FUNCTION_RESOLUTION.arithmeticFunction(OperatorType.MULTIPLY, left.getType(), right.getType()), left.getType(), ImmutableList.of(left, right));
    }

    private static boolean isUntranslated(AggregationNode.Aggregation aggregation)
    {
        return aggregation.getCall().getArguments().stream().anyMatch(OriginalExpressionUtils::isExpression) ||
                aggregation.getFilter().map(OriginalExpressionUtils::isExpression).orElse(false);
    }
}
