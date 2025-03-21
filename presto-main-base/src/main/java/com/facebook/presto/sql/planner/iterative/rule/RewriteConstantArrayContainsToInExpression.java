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

import com.facebook.presto.Session;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.expressions.RowExpressionRewriter;
import com.facebook.presto.expressions.RowExpressionTreeRewriter;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.isRwriteConstantArrayContainsToInExpressionEnabled;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP_MICROSECONDS;
import static com.facebook.presto.common.type.TypeUtils.isExactNumericType;
import static com.facebook.presto.common.type.TypeUtils.readNativeValue;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class RewriteConstantArrayContainsToInExpression
        extends RowExpressionRewriteRuleSet
{
    public RewriteConstantArrayContainsToInExpression(FunctionAndTypeManager functionAndTypeManager)
    {
        super(new Rewriter(functionAndTypeManager));
    }

    @Override
    public boolean isRewriterEnabled(Session session)
    {
        return isRwriteConstantArrayContainsToInExpressionEnabled(session);
    }

    @Override
    public Set<Rule<?>> rules()
    {
        return ImmutableSet.of(filterRowExpressionRewriteRule(), projectRowExpressionRewriteRule());
    }

    private static class Rewriter
            implements PlanRowExpressionRewriter
    {
        private final ContainsToInRewriter containsToInRewriter;

        public Rewriter(FunctionAndTypeManager functionAndTypeManager)
        {
            requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
            this.containsToInRewriter = new ContainsToInRewriter(functionAndTypeManager);
        }

        @Override
        public RowExpression rewrite(RowExpression expression, Rule.Context context)
        {
            return RowExpressionTreeRewriter.rewriteWith(containsToInRewriter, expression);
        }
    }

    private static class ContainsToInRewriter
            extends RowExpressionRewriter<Void>
    {
        private final FunctionResolution functionResolution;

        private ContainsToInRewriter(FunctionAndTypeManager functionAndTypeManager)
        {
            this.functionResolution = new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver());
        }

        // contains(array[array[1, null]], array[1, null]) throw exception, but array[1, null] IN (array[1, null]) returns NULL.
        // We limit the optimization for simple primitive type here for safety and simplicity
        private boolean isSupportedType(Type type)
        {
            return isExactNumericType(type) || type instanceof VarcharType || type.equals(DATE) || type.equals(TIMESTAMP) || type.equals(TIMESTAMP_MICROSECONDS);
        }

        @Override
        public RowExpression rewriteCall(CallExpression node, Void context, RowExpressionTreeRewriter<Void> treeRewriter)
        {
            if (functionResolution.isArrayContainsFunction(node.getFunctionHandle()) && node.getArguments().get(0) instanceof ConstantExpression && !((ConstantExpression) node.getArguments().get(0)).isNull()) {
                checkState(((ConstantExpression) node.getArguments().get(0)).getValue() instanceof Block && node.getArguments().get(0).getType() instanceof ArrayType);
                Block arrayValue = (Block) ((ConstantExpression) node.getArguments().get(0)).getValue();
                if (arrayValue.getPositionCount() == 0) {
                    return null;
                }
                Type arrayElementType = ((ArrayType) node.getArguments().get(0).getType()).getElementType();
                if (!isSupportedType(arrayElementType)) {
                    return null;
                }
                ImmutableList.Builder arguments = ImmutableList.builder();
                arguments.add(node.getArguments().get(1));
                for (int i = 0; i < arrayValue.getPositionCount(); ++i) {
                    arguments.add(constant(readNativeValue(arrayElementType, arrayValue, i), arrayElementType));
                }
                return new SpecialFormExpression(node.getSourceLocation(), SpecialFormExpression.Form.IN, node.getType(), arguments.build());
            }
            return null;
        }
    }
}
