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
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.expressions.RowExpressionRewriter;
import com.facebook.presto.expressions.RowExpressionTreeRewriter;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * Transforms:
 * <pre>
 * - Cardinality(Map_Values(map))
 *     - X
 * </pre>
 * Into:
 * <pre>
 * - Cardinality(map)
 *     - X
 * </pre>
 */
public class SimplifyCardinalityMapRewriter
{
    private static final Set<String> MAP_FUNCTIONS = ImmutableSet.of("map_values", "map_keys");

    private SimplifyCardinalityMapRewriter() {}

    public static RowExpression rewrite(RowExpression expression, FunctionAndTypeManager functionAndTypeManager, Session session)
    {
        return RowExpressionTreeRewriter.rewriteWith(new Visitor(functionAndTypeManager, session), expression);
    }

    private static class Visitor
            extends RowExpressionRewriter<Void>
    {
        private final FunctionAndTypeManager functionAndTypeManager;
        private final Session session;

        public Visitor(FunctionAndTypeManager functionAndTypeManager, Session session)
        {
            this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
            this.session = requireNonNull(session, "session is null");
        }

        @Override
        public RowExpression rewriteCall(CallExpression node, Void context, RowExpressionTreeRewriter<Void> treeRewriter)
        {
            ImmutableList.Builder<RowExpression> rewrittenArguments = ImmutableList.builder();

            if (node.getDisplayName().equals("cardinality")) {
                for (RowExpression argument : node.getArguments()) {
                    if (argument instanceof CallExpression) {
                        CallExpression callExpression = (CallExpression) argument;
                        if (MAP_FUNCTIONS.contains(callExpression.getDisplayName()) && callExpression.getArguments().size() == 1) {
                            rewrittenArguments.add(treeRewriter.rewrite(callExpression.getArguments().get(0), context));
                            continue;
                        }
                    }
                    rewrittenArguments.add(treeRewriter.rewrite(argument, context));
                }
                return newFunctionIfRewritten(node, rewrittenArguments.build());
            }
            for (RowExpression argument : node.getArguments()) {
                rewrittenArguments.add(treeRewriter.rewrite(argument, context));
            }
            return newFunctionIfRewritten(node, rewrittenArguments.build());
        }

        private RowExpression newFunctionIfRewritten(CallExpression node, List<RowExpression> rewrittenArguments)
        {
            if (!node.getArguments().equals(rewrittenArguments)) {
                List<Type> types = rewrittenArguments.stream()
                        .map(RowExpression::getType)
                        .collect(toImmutableList());
                // rewrite the FunctionHandle, as the input types may have changed
                // e.g. from ArrayCardinalityFunction for cardinality(map_keys(x)) to
                // MapCardinalityFunction for cardinality(x)
                FunctionHandle rewrittenFunctionHandle = functionAndTypeManager.resolveFunction(
                        Optional.of(session.getSessionFunctions()),
                        session.getTransactionId(),
                        QualifiedObjectName.valueOf(node.getFunctionHandle().getName()),
                        fromTypes(types));
                return new CallExpression(
                        node.getSourceLocation(),
                        node.getDisplayName(),
                        rewrittenFunctionHandle,
                        node.getType(),
                        rewrittenArguments);
            }
            return node;
        }
    }
}
