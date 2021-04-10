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

import com.facebook.presto.expressions.RowExpressionRewriter;
import com.facebook.presto.expressions.RowExpressionTreeRewriter;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.common.function.OperatorType.ADD;
import static com.facebook.presto.common.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.expressions.RowExpressionNodeInliner.replaceExpression;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.planner.TestRowExpressionRewriter.NegationExpressionRewriter.rewrite;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestRowExpressionRewriter
{
    private FunctionAndTypeManager functionAndTypeManager;

    @BeforeClass
    public void setup()
    {
        this.functionAndTypeManager = createTestFunctionAndTypeManager();
    }

    @Test
    public void testSimple()
    {
        // successful rewrite
        RowExpression predicate = call(
                GREATER_THAN.name(),
                functionAndTypeManager.resolveOperator(GREATER_THAN, fromTypes(BIGINT, BIGINT)),
                BOOLEAN,
                constant(1L, BIGINT),
                constant(2L, BIGINT));

        RowExpression negatedPredicate = rewrite(predicate);
        assertEquals(negatedPredicate.getType(), BOOLEAN);
        assertTrue(negatedPredicate instanceof CallExpression);
        assertTrue(((CallExpression) negatedPredicate).getArguments().get(0) instanceof CallExpression);
        assertEquals(((CallExpression) negatedPredicate).getDisplayName(), "not");
        assertEquals(((CallExpression) ((CallExpression) negatedPredicate).getArguments().get(0)).getDisplayName(), GREATER_THAN.name());

        // no rewrite
        RowExpression nonPredicate = call(
                ADD.name(),
                functionAndTypeManager.resolveOperator(ADD, fromTypes(BIGINT, BIGINT)),
                BIGINT,
                constant(1L, BIGINT),
                constant(2L, BIGINT));

        RowExpression samePredicate = rewrite(nonPredicate);
        assertEquals(samePredicate, nonPredicate);
    }

    @Test
    public void testInliner()
    {
        RowExpression predicate = call(
                GREATER_THAN.name(),
                functionAndTypeManager.resolveOperator(GREATER_THAN, fromTypes(BIGINT, BIGINT)),
                BOOLEAN,
                constant(1L, BIGINT),
                constant(2L, BIGINT));

        // no rewrite
        RowExpression nonPredicate = call(
                ADD.name(),
                functionAndTypeManager.resolveOperator(ADD, fromTypes(BIGINT, BIGINT)),
                BIGINT,
                constant(1L, BIGINT),
                constant(2L, BIGINT));

        RowExpression samePredicate = replaceExpression(predicate, ImmutableMap.of(predicate, nonPredicate));
        assertEquals(samePredicate, nonPredicate);
    }

    public static class NegationExpressionRewriter
    {
        private NegationExpressionRewriter() {}

        public static RowExpression rewrite(RowExpression expression)
        {
            return RowExpressionTreeRewriter.rewriteWith(new Visitor(), expression);
        }

        private static class Visitor
                extends RowExpressionRewriter<Void>
        {
            private final FunctionAndTypeManager functionAndTypeManager;
            private final StandardFunctionResolution functionResolution;

            Visitor()
            {
                this.functionAndTypeManager = createTestFunctionAndTypeManager();
                this.functionResolution = new FunctionResolution(functionAndTypeManager);
            }

            @Override
            public RowExpression rewriteCall(CallExpression node, Void context, RowExpressionTreeRewriter<Void> treeRewriter)
            {
                FunctionMetadata metadata = functionAndTypeManager.getFunctionMetadata(node.getFunctionHandle());
                if (metadata.getOperatorType().isPresent() && metadata.getOperatorType().get().isComparisonOperator()) {
                    return call("not", functionResolution.notFunction(), BOOLEAN, node);
                }
                return null;
            }
        }
    }
}
