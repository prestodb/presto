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

import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.metadata.FunctionManager;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionRewriter;
import com.facebook.presto.spi.relation.RowExpressionTreeRewriter;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.function.OperatorType.ADD;
import static com.facebook.presto.spi.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.spi.relation.RowExpressionNodeInliner.replaceExpression;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.planner.TestRowExpressionRewriter.NegationExpressionRewriter.rewrite;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestRowExpressionRewriter
{
    private FunctionManager functionManager;

    @BeforeClass
    public void setup()
    {
        TypeRegistry typeManager = new TypeRegistry();
        this.functionManager = new FunctionManager(typeManager, new BlockEncodingManager(typeManager), new FeaturesConfig());
    }

    @Test
    public void testSimple()
    {
        // successful rewrite
        RowExpression predicate = call(
                GREATER_THAN.name(),
                functionManager.resolveOperator(GREATER_THAN, fromTypes(BIGINT, BIGINT)),
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
                functionManager.resolveOperator(ADD, fromTypes(BIGINT, BIGINT)),
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
                functionManager.resolveOperator(GREATER_THAN, fromTypes(BIGINT, BIGINT)),
                BOOLEAN,
                constant(1L, BIGINT),
                constant(2L, BIGINT));

        // no rewrite
        RowExpression nonPredicate = call(
                ADD.name(),
                functionManager.resolveOperator(ADD, fromTypes(BIGINT, BIGINT)),
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
            private final FunctionManager functionManager;
            private final StandardFunctionResolution functionResolution;

            Visitor()
            {
                TypeRegistry typeManager = new TypeRegistry();
                this.functionManager = new FunctionManager(typeManager, new BlockEncodingManager(typeManager), new FeaturesConfig());
                this.functionResolution = new FunctionResolution(functionManager);
            }

            @Override
            public RowExpression rewriteCall(CallExpression node, Void context, RowExpressionTreeRewriter<Void> treeRewriter)
            {
                FunctionMetadata metadata = functionManager.getFunctionMetadata(node.getFunctionHandle());
                if (metadata.getOperatorType().isPresent() && metadata.getOperatorType().get().isComparisonOperator()) {
                    return call("not", functionResolution.notFunction(), BOOLEAN, node);
                }
                return null;
            }
        }
    }
}
