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
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.expressions.RowExpressionRewriter;
import com.facebook.presto.expressions.RowExpressionTreeRewriter;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.isRemoveMapCastEnabled;
import static com.facebook.presto.common.function.OperatorType.SUBSCRIPT;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.castToInteger;
import static com.facebook.presto.sql.relational.Expressions.tryCast;
import static java.util.Objects.requireNonNull;

/**
 * Remove cast on map if possible. Currently it only supports subscript and element_at function, and only works when map key is of type integer and index is bigint. For example:
 * Input: cast(feature as map<bigint, float>)[key], where feature is of type map<integer, float> and key is of type bigint
 * Output: feature[cast(key as integer)]
 *
 * Input: element_at(cast(feature as map<bigint, float>), key), where feature is of type map<integer, float> and key is of type bigint
 * Output: element_at(feature, try_cast(key as integer))
 *
 * Notice that here when it's accessing the map using subscript function, we use CAST function in index, and when it's element_at function, we use TRY_CAST function, so that
 * when the key is out of integer range, for feature[key] it will fail both with and without optimization, fail with map key not exists before optimization and with cast failure after optimization
 * when the key is out of integer range, for element_at(feature, key) it will return NULL both before and after optimization
 */
public class RemoveMapCastRule
        extends RowExpressionRewriteRuleSet
{
    public RemoveMapCastRule(FunctionAndTypeManager functionAndTypeManager)
    {
        super(new RemoveMapCastRule.Rewriter(functionAndTypeManager));
    }

    @Override
    public boolean isRewriterEnabled(Session session)
    {
        return isRemoveMapCastEnabled(session);
    }

    @Override
    public Set<Rule<?>> rules()
    {
        return ImmutableSet.of(filterRowExpressionRewriteRule(), projectRowExpressionRewriteRule());
    }

    private static class Rewriter
            implements PlanRowExpressionRewriter
    {
        private final RemoveMapCastRewriter removeMapCastRewriter;

        public Rewriter(FunctionAndTypeManager functionAndTypeManager)
        {
            requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
            this.removeMapCastRewriter = new RemoveMapCastRewriter(functionAndTypeManager);
        }

        @Override
        public RowExpression rewrite(RowExpression expression, Rule.Context context)
        {
            return RowExpressionTreeRewriter.rewriteWith(removeMapCastRewriter, expression);
        }
    }

    private static class RemoveMapCastRewriter
            extends RowExpressionRewriter<Void>
    {
        private final FunctionAndTypeManager functionAndTypeManager;
        private final FunctionResolution functionResolution;

        private RemoveMapCastRewriter(FunctionAndTypeManager functionAndTypeManager)
        {
            this.functionAndTypeManager = functionAndTypeManager;
            this.functionResolution = new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver());
        }

        @Override
        public RowExpression rewriteCall(CallExpression node, Void context, RowExpressionTreeRewriter<Void> treeRewriter)
        {
            if ((functionResolution.isSubscriptFunction(node.getFunctionHandle()) || functionResolution.isElementAtFunction(node.getFunctionHandle())) && node.getArguments().get(0) instanceof CallExpression
                    && functionResolution.isCastFunction(((CallExpression) node.getArguments().get(0)).getFunctionHandle())
                    && ((CallExpression) node.getArguments().get(0)).getArguments().get(0).getType() instanceof MapType) {
                CallExpression castExpression = (CallExpression) node.getArguments().get(0);
                RowExpression castInput = castExpression.getArguments().get(0);
                Type fromKeyType = ((MapType) castInput.getType()).getKeyType();
                Type fromValueType = ((MapType) castInput.getType()).getValueType();
                Type toKeyType = ((MapType) castExpression.getType()).getKeyType();
                Type toValueType = ((MapType) castExpression.getType()).getValueType();

                if (canRemoveMapCast(fromKeyType, fromValueType, toKeyType, toValueType, node.getArguments().get(1).getType())) {
                    if (functionResolution.isSubscriptFunction(node.getFunctionHandle())) {
                        RowExpression newIndex = castToInteger(functionAndTypeManager, node.getArguments().get(1));
                        return call(SUBSCRIPT.name(), functionResolution.subscriptFunction(castInput.getType(), newIndex.getType()), node.getType(), castInput, newIndex);
                    }
                    else {
                        RowExpression newIndex = tryCast(functionAndTypeManager, node.getArguments().get(1), INTEGER);
                        return call(functionAndTypeManager, "element_at", node.getType(), castInput, newIndex);
                    }
                }
            }
            return null;
        }

        private static boolean canRemoveMapCast(Type fromKeyType, Type fromValueType, Type toKeyType, Type toValueType, Type indexType)
        {
            return fromValueType.equals(toValueType) && fromKeyType.equals(INTEGER) && toKeyType.equals(BIGINT) && indexType.equals(BIGINT);
        }
    }
}
