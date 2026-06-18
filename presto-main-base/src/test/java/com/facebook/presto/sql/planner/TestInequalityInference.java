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
package com.facebook.presto.sql.planner;

import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.optimizations.ExpressionEquivalence;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.common.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.common.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.common.function.OperatorType.LESS_THAN;
import static com.facebook.presto.common.function.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.facebook.presto.sql.planner.TestEqualityInference.add;
import static com.facebook.presto.sql.planner.TestEqualityInference.compare;
import static com.facebook.presto.sql.planner.TestEqualityInference.multiply;
import static com.facebook.presto.sql.planner.TestEqualityInference.number;
import static com.facebook.presto.sql.planner.TestEqualityInference.variable;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

public class TestInequalityInference
{
    private static final FunctionAndTypeManager FUNCTION_AND_TYPE_MANAGER = createTestFunctionAndTypeManager();
    private static final SqlParser SQL_PARSER = new SqlParser();
    private static final MetadataManager METADATA = MetadataManager.createTestMetadataManager();
    private static final ExpressionEquivalence EXPRESSION_EQUIVALENCE = new ExpressionEquivalence(METADATA, SQL_PARSER);

    @Test
    public void testCanonicalization()
    {
        InequalityInference.Builder builder = new InequalityInference.Builder(FUNCTION_AND_TYPE_MANAGER, EXPRESSION_EQUIVALENCE, Optional.empty());
        RowExpression canonicalExpression = compare(LESS_THAN, variable("a1"), variable("a2"));
        RowExpression expression = compare(GREATER_THAN, variable("a2"), variable("a1"));
        assertEquals(builder.canonicalizeInequality(expression), canonicalExpression);

        canonicalExpression = compare(LESS_THAN_OR_EQUAL, variable("a1"), variable("a2"));
        expression = compare(GREATER_THAN_OR_EQUAL, variable("a2"), variable("a1"));
        assertEquals(builder.canonicalizeInequality(expression), canonicalExpression);

        canonicalExpression = compare(LESS_THAN, add("a1", "a2"), variable("a3"));
        expression = compare(GREATER_THAN, variable("a3"), add("a1", "a2"));
        assertEquals(builder.canonicalizeInequality(expression), canonicalExpression);

        canonicalExpression = compare(LESS_THAN, multiply("a1", "a2"), variable("a3"));
        expression = compare(GREATER_THAN, variable("a3"), multiply("a1", "a2"));
        assertEquals(builder.canonicalizeInequality(expression), canonicalExpression);

        // a1+a2 != a2+a1 wrt this function. Expression equivalence matching happens in compareAndExtractInequalities()
        canonicalExpression = compare(LESS_THAN, add("a1", "a2"), variable("a3"));
        expression = compare(GREATER_THAN, variable("a3"), add("a2", "a1"));
        assertNotEquals(builder.canonicalizeInequality(expression), canonicalExpression);
    }

    @Test
    public void testSimpleTransitivity()
    {
        InequalityInference.Builder builder = new InequalityInference.Builder(FUNCTION_AND_TYPE_MANAGER, EXPRESSION_EQUIVALENCE, Optional.empty());
        RowExpression exp1 = compare(LESS_THAN, variable("a1"), number(5));
        RowExpression exp2 = compare(LESS_THAN, variable("a2"), variable("a1"));
        ImmutableSet<RowExpression> toBeInferred = ImmutableSet.of(compare(LESS_THAN, variable("a2"), number(5)));
        assertInferredInequalites(toBeInferred, builder, exp1, exp2);

        builder = new InequalityInference.Builder(FUNCTION_AND_TYPE_MANAGER, EXPRESSION_EQUIVALENCE, Optional.empty());
        exp1 = compare(GREATER_THAN, variable("a1"), number(5));
        exp2 = compare(GREATER_THAN, variable("a2"), variable("a1"));
        toBeInferred = ImmutableSet.of(compare(LESS_THAN, number(5), variable("a2")));
        assertInferredInequalites(toBeInferred, builder, exp1, exp2);

        builder = new InequalityInference.Builder(FUNCTION_AND_TYPE_MANAGER, EXPRESSION_EQUIVALENCE, Optional.empty());
        exp1 = compare(LESS_THAN_OR_EQUAL, variable("a1"), number(5));
        exp2 = compare(LESS_THAN_OR_EQUAL, variable("a2"), variable("a1"));
        toBeInferred = ImmutableSet.of(compare(LESS_THAN_OR_EQUAL, variable("a2"), number(5)));
        assertInferredInequalites(toBeInferred, builder, exp1, exp2);

        builder = new InequalityInference.Builder(FUNCTION_AND_TYPE_MANAGER, EXPRESSION_EQUIVALENCE, Optional.empty());
        exp1 = compare(GREATER_THAN_OR_EQUAL, variable("a1"), number(5));
        exp2 = compare(GREATER_THAN_OR_EQUAL, variable("a2"), variable("a1"));
        toBeInferred = ImmutableSet.of(compare(LESS_THAN_OR_EQUAL, number(5), variable("a2")));
        assertInferredInequalites(toBeInferred, builder, exp1, exp2);

        // Simple transitivity after canonicalization
        builder = new InequalityInference.Builder(FUNCTION_AND_TYPE_MANAGER, EXPRESSION_EQUIVALENCE, Optional.empty());
        exp1 = compare(LESS_THAN, variable("a1"), number(5));
        exp2 = compare(GREATER_THAN, variable("a1"), variable("a2"));
        toBeInferred = ImmutableSet.of(compare(LESS_THAN, variable("a2"), number(5)));
        assertInferredInequalites(toBeInferred, builder, exp1, exp2);

        builder = new InequalityInference.Builder(FUNCTION_AND_TYPE_MANAGER, EXPRESSION_EQUIVALENCE, Optional.empty());
        exp1 = compare(GREATER_THAN, variable("a1"), number(5));
        exp2 = compare(LESS_THAN, variable("a1"), variable("a2"));
        toBeInferred = ImmutableSet.of(compare(LESS_THAN, number(5), variable("a2")));
        assertInferredInequalites(toBeInferred, builder, exp1, exp2);

        builder = new InequalityInference.Builder(FUNCTION_AND_TYPE_MANAGER, EXPRESSION_EQUIVALENCE, Optional.empty());
        exp1 = compare(GREATER_THAN_OR_EQUAL, variable("a1"), number(5));
        exp2 = compare(LESS_THAN, variable("a1"), variable("a2"));
        toBeInferred = ImmutableSet.of(compare(LESS_THAN, number(5), variable("a2")));
        assertInferredInequalites(toBeInferred, builder, exp1, exp2);

        builder = new InequalityInference.Builder(FUNCTION_AND_TYPE_MANAGER, EXPRESSION_EQUIVALENCE, Optional.empty());
        exp1 = compare(LESS_THAN, variable("a1"), number(5));
        exp2 = compare(GREATER_THAN_OR_EQUAL, variable("a1"), variable("a2"));
        toBeInferred = ImmutableSet.of(compare(LESS_THAN, variable("a2"), number(5)));
        assertInferredInequalites(toBeInferred, builder, exp1, exp2);

        builder = new InequalityInference.Builder(FUNCTION_AND_TYPE_MANAGER, EXPRESSION_EQUIVALENCE, Optional.of(ImmutableList.of(variable("a2"))));
        exp1 = compare(LESS_THAN, variable("a1"), number(5));
        exp2 = compare(LESS_THAN, variable("a2"), variable("a1"));
        toBeInferred = ImmutableSet.of();
        assertInferredInequalites(toBeInferred, builder, exp1, exp2);
    }

    @Test
    public void testChainOfTransitivity()
    {
        InequalityInference.Builder builder = new InequalityInference.Builder(FUNCTION_AND_TYPE_MANAGER, EXPRESSION_EQUIVALENCE, Optional.empty());
        RowExpression exp1 = compare(LESS_THAN, variable("a1"), number(5));
        RowExpression exp2 = compare(LESS_THAN, variable("a2"), variable("a1"));
        RowExpression exp3 = compare(LESS_THAN, variable("a3"), variable("a2"));
        RowExpression exp4 = compare(LESS_THAN, variable("a4"), variable("a3"));
        ImmutableSet<RowExpression> toBeInferred = ImmutableSet.of(
                compare(LESS_THAN, variable("a2"), number(5)),
                compare(LESS_THAN, variable("a3"), number(5)),
                compare(LESS_THAN, variable("a4"), number(5)));
        assertInferredInequalites(toBeInferred, builder, exp1, exp2, exp3, exp4);

        // Break inference chain with an equality
        builder = new InequalityInference.Builder(FUNCTION_AND_TYPE_MANAGER, EXPRESSION_EQUIVALENCE, Optional.empty());
        exp1 = compare(LESS_THAN, variable("a1"), number(10));
        exp2 = compare(GREATER_THAN_OR_EQUAL, variable("a1"), variable("a2"));
        exp3 = compare(EQUAL, variable("a3"), variable("a2"));
        exp4 = compare(LESS_THAN, variable("a4"), variable("a3"));
        toBeInferred = ImmutableSet.of(compare(LESS_THAN, variable("a2"), number(10)));
        assertInferredInequalites(toBeInferred, builder, exp1, exp2, exp3, exp4);

        builder = new InequalityInference.Builder(FUNCTION_AND_TYPE_MANAGER, EXPRESSION_EQUIVALENCE, Optional.empty());
        exp1 = compare(LESS_THAN, variable("a1"), number(10));
        exp2 = compare(GREATER_THAN_OR_EQUAL, variable("a1"), variable("a2"));
        exp3 = compare(LESS_THAN_OR_EQUAL, variable("a3"), variable("a2"));
        exp4 = compare(LESS_THAN, variable("a4"), variable("a3"));
        RowExpression exp5 = compare(GREATER_THAN, variable("a4"), number(1));
        RowExpression exp6 = compare(GREATER_THAN_OR_EQUAL, variable("a5"), variable("a4"));
        RowExpression exp7 = compare(LESS_THAN_OR_EQUAL, variable("a5"), variable("a6"));
        toBeInferred = ImmutableSet.of(
                compare(LESS_THAN, variable("a2"), number(10)),
                compare(LESS_THAN, variable("a3"), number(10)),
                compare(LESS_THAN, variable("a4"), number(10)),
                compare(LESS_THAN, number(1), variable("a5")),
                compare(LESS_THAN, number(1), variable("a6")),
                compare(LESS_THAN, number(1), variable("a3")),
                compare(LESS_THAN, number(1), variable("a2")),
                compare(LESS_THAN, number(1), variable("a1")));
        assertInferredInequalites(toBeInferred, builder, exp1, exp2, exp3, exp4, exp5, exp6, exp7);

        // Same as previous, but with a4 & a5 as outer variables. Only infer inequalities on the other variables
        builder = new InequalityInference.Builder(FUNCTION_AND_TYPE_MANAGER, EXPRESSION_EQUIVALENCE, Optional.of(ImmutableList.of(variable("a5"), variable("a4"))));
        exp1 = compare(LESS_THAN, variable("a1"), number(10));
        exp2 = compare(GREATER_THAN_OR_EQUAL, variable("a1"), variable("a2"));
        exp3 = compare(LESS_THAN_OR_EQUAL, variable("a3"), variable("a2"));
        exp4 = compare(LESS_THAN, variable("a4"), variable("a3"));
        exp5 = compare(GREATER_THAN, variable("a4"), number(1));
        exp6 = compare(GREATER_THAN_OR_EQUAL, variable("a5"), variable("a4"));
        exp7 = compare(LESS_THAN_OR_EQUAL, variable("a5"), variable("a6"));
        toBeInferred = ImmutableSet.of(
                compare(LESS_THAN, variable("a2"), number(10)),
                compare(LESS_THAN, variable("a3"), number(10)),
                compare(LESS_THAN, number(1), variable("a3")),
                compare(LESS_THAN, number(1), variable("a2")),
                compare(LESS_THAN, number(1), variable("a1")));
        assertInferredInequalites(toBeInferred, builder, exp1, exp2, exp3, exp4, exp5, exp6, exp7);
    }

    @Test
    public void testTransitivityWithExpressions()
    {
        RowExpression a1Plusa2 = add("a1", "a2");
        RowExpression a2Plusa1 = add("a2", "a1");
        RowExpression a3Multiplya4 = multiply("a3", "a4");
        RowExpression a4Multiplya3 = multiply("a4", "a3");
        InequalityInference.Builder builder = new InequalityInference.Builder(FUNCTION_AND_TYPE_MANAGER, EXPRESSION_EQUIVALENCE, Optional.empty());
        RowExpression exp1 = compare(LESS_THAN, a1Plusa2, number(5));
        RowExpression exp2 = compare(LESS_THAN, variable("a5"), a2Plusa1);
        RowExpression exp3 = compare(LESS_THAN, a3Multiplya4, number(100));
        RowExpression exp4 = compare(LESS_THAN, variable("a6"), a4Multiplya3);
        ImmutableSet<RowExpression> toBeInferred = ImmutableSet.of(
                compare(LESS_THAN, variable("a5"), number(5)),
                compare(LESS_THAN, variable("a6"), number(100)));
        assertInferredInequalites(toBeInferred, builder, exp1, exp2, exp3, exp4);

        RowExpression a1Plusa2Multiplya3 = multiply(add("a1", "a2"), variable("a3"));
        RowExpression a3Multiplya2Plusa1 = multiply(variable("a3"), add("a1", "a2"));
        builder = new InequalityInference.Builder(FUNCTION_AND_TYPE_MANAGER, EXPRESSION_EQUIVALENCE, Optional.empty());
        exp1 = compare(LESS_THAN, a1Plusa2Multiplya3, number(5));
        exp2 = compare(LESS_THAN, variable("a5"), a3Multiplya2Plusa1);
        toBeInferred = ImmutableSet.of(compare(LESS_THAN, variable("a5"), number(5)));
        assertInferredInequalites(toBeInferred, builder, exp1, exp2);
    }

    private void assertInferredInequalites(ImmutableSet<RowExpression> expectedInequalities, InequalityInference.Builder builder, RowExpression... expressions)
    {
        assertEquals(builder.addInequalityInferences(expressions).build().inferInequalities(), expectedInequalities);
    }
}
