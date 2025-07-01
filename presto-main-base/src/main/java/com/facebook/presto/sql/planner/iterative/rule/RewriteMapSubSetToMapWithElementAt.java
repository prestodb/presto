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
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.expressions.RowExpressionRewriter;
import com.facebook.presto.expressions.RowExpressionTreeRewriter;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.lang.invoke.MethodHandle;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.isRewriteMapSubsetWithConstantWithElementAt;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.TypeUtils.readNativeValue;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

/**
 * Rewrite the map_subset function which has a constant array of index with map filter and element_at
 * For example:
 * map_subset(feature, array[1, 4, 9]) will be rewritten to
 * map_filter(map(array[1, 4, 9], array[element_at(feature, 1), element_at(feature, 4), element_at(feature, 9)]), (k, v) -> v is not null)
 * With this rewrite, the PushdownSubfields can identify that for feature map, only element with key 1, 4, 9 are accessed, hence can
 * do field pushdown and avoid reading the whole map.
 */
public class RewriteMapSubSetToMapWithElementAt
        extends RowExpressionRewriteRuleSet
{
    public RewriteMapSubSetToMapWithElementAt(FunctionAndTypeManager functionAndTypeManager)
    {
        super(new RewriteMapSubSetToMapWithElementAt.Rewriter(functionAndTypeManager));
    }

    @Override
    public Set<Rule<?>> rules()
    {
        return ImmutableSet.of(projectRowExpressionRewriteRule());
    }

    @Override
    public boolean isRewriterEnabled(Session session)
    {
        return isRewriteMapSubsetWithConstantWithElementAt(session);
    }

    private static class Rewriter
            implements PlanRowExpressionRewriter
    {
        private final RewriteMapSubSetToMapWithElementAt.RewriteMapSubSetToMapWithElementAtRewriter removeMapCastRewriter;

        public Rewriter(FunctionAndTypeManager functionAndTypeManager)
        {
            requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
            this.removeMapCastRewriter = new RewriteMapSubSetToMapWithElementAt.RewriteMapSubSetToMapWithElementAtRewriter(functionAndTypeManager);
        }

        @Override
        public RowExpression rewrite(RowExpression expression, Rule.Context context)
        {
            return RowExpressionTreeRewriter.rewriteWith(removeMapCastRewriter, expression);
        }
    }

    private static class RewriteMapSubSetToMapWithElementAtRewriter
            extends RowExpressionRewriter<Void>
    {
        private final FunctionAndTypeManager functionAndTypeManager;
        private final FunctionResolution functionResolution;

        private RewriteMapSubSetToMapWithElementAtRewriter(FunctionAndTypeManager functionAndTypeManager)
        {
            this.functionAndTypeManager = functionAndTypeManager;
            this.functionResolution = new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver());
        }

        @Override
        public RowExpression rewriteCall(CallExpression callExpression, Void context, RowExpressionTreeRewriter<Void> treeRewriter)
        {
            if (isMapSubsetWithConstantArray(callExpression)) {
                Type mapValueType = ((MapType) callExpression.getArguments().get(0).getType()).getValueType();
                RowExpression mapExpression = callExpression.getArguments().get(0);
                ConstantExpression constantArray = (ConstantExpression) callExpression.getArguments().get(1);
                checkState(constantArray.getValue() instanceof Block && constantArray.getType() instanceof ArrayType);
                Block arrayValue = (Block) constantArray.getValue();
                Type arrayElementType = ((ArrayType) constantArray.getType()).getElementType();
                ImmutableList.Builder<RowExpression> arguments = ImmutableList.builder();
                for (int i = 0; i < arrayValue.getPositionCount(); ++i) {
                    ConstantExpression mapIndex = constant(readNativeValue(arrayElementType, arrayValue, i), arrayElementType);
                    arguments.add(call(functionAndTypeManager, "element_at", mapValueType, mapExpression, mapIndex));
                }
                CallExpression mapValueArray = call(functionAndTypeManager, "array_constructor", new ArrayType(mapValueType), arguments.build());
                MethodHandle keyEquals =
                        functionAndTypeManager.getJavaScalarFunctionImplementation(
                                functionAndTypeManager.resolveOperator(OperatorType.EQUAL, fromTypes(arrayElementType, arrayElementType))).getMethodHandle();
                MethodHandle keyHashcode =
                        functionAndTypeManager.getJavaScalarFunctionImplementation(
                                functionAndTypeManager.resolveOperator(OperatorType.HASH_CODE, fromTypes(arrayElementType))).getMethodHandle();
                RowExpression map = call(functionAndTypeManager, "MAP", new MapType(arrayElementType, mapValueType, keyEquals, keyHashcode), constantArray, mapValueArray);
                SpecialFormExpression isNullExpression = new SpecialFormExpression(callExpression.getSourceLocation(), SpecialFormExpression.Form.IS_NULL, BOOLEAN, new VariableReferenceExpression(callExpression.getSourceLocation(), "expr_0", mapValueType));
                CallExpression notNull = new CallExpression(callExpression.getSourceLocation(), "not", functionResolution.notFunction(), BOOLEAN, singletonList(isNullExpression));
                LambdaDefinitionExpression lambdaDefinitionExpression = new LambdaDefinitionExpression(callExpression.getSourceLocation(), ImmutableList.of(arrayElementType, mapValueType), ImmutableList.of("expr", "expr_0"), notNull);
                return call(functionAndTypeManager, "map_filter", map.getType(), map, lambdaDefinitionExpression);
            }
            return null;
        }

        private boolean isMapSubsetWithConstantArray(RowExpression rowExpression)
        {
            if (rowExpression instanceof CallExpression) {
                CallExpression callExpression = (CallExpression) rowExpression;
                if (functionResolution.isMapSubSetFunction(callExpression.getFunctionHandle())) {
                    return callExpression.getArguments().get(1) instanceof ConstantExpression;
                }
            }
            return false;
        }
    }
}
