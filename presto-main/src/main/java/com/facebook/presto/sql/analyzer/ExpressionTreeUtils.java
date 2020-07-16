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
package com.facebook.presto.sql.analyzer;

import com.facebook.presto.metadata.FunctionManager;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DefaultExpressionTraversalVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NodeRef;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static com.facebook.presto.spi.function.FunctionKind.AGGREGATE;
import static com.google.common.base.Predicates.alwaysTrue;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public final class ExpressionTreeUtils
{
    private ExpressionTreeUtils() {}

    static List<FunctionCall> extractAggregateFunctions(Map<NodeRef<FunctionCall>, FunctionHandle> functionHandles, Iterable<? extends Node> nodes, FunctionManager functionManager)
    {
        return extractExpressions(nodes, FunctionCall.class, isAggregationPredicate(functionHandles, functionManager));
    }

    static List<FunctionCall> extractWindowFunctions(Iterable<? extends Node> nodes)
    {
        return extractExpressions(nodes, FunctionCall.class, ExpressionTreeUtils::isWindowFunction);
    }

    static List<FunctionCall> extractExternalFunctions(Map<NodeRef<FunctionCall>, FunctionHandle> functionHandles, Iterable<? extends Node> nodes, FunctionManager functionManager)
    {
        return extractExpressions(nodes, FunctionCall.class, isExternalFunctionPredicate(functionHandles, functionManager));
    }

    public static <T extends Expression> List<T> extractExpressions(
            Iterable<? extends Node> nodes,
            Class<T> clazz)
    {
        return extractExpressions(nodes, clazz, alwaysTrue());
    }

    private static Predicate<FunctionCall> isAggregationPredicate(Map<NodeRef<FunctionCall>, FunctionHandle> functionHandles, FunctionManager functionManager)
    {
        return functionCall -> (functionManager.getFunctionMetadata(functionHandles.get(NodeRef.of(functionCall))).getFunctionKind() == AGGREGATE || functionCall.getFilter().isPresent())
                && !functionCall.getWindow().isPresent()
                || functionCall.getOrderBy().isPresent();
    }

    private static boolean isWindowFunction(FunctionCall functionCall)
    {
        return functionCall.getWindow().isPresent();
    }

    private static Predicate<FunctionCall> isExternalFunctionPredicate(Map<NodeRef<FunctionCall>, FunctionHandle> functionHandles, FunctionManager functionManager)
    {
        return functionCall -> functionManager.getFunctionMetadata(functionHandles.get(NodeRef.of(functionCall))).getImplementationType().isExternal();
    }

    private static <T extends Expression> List<T> extractExpressions(
            Iterable<? extends Node> nodes,
            Class<T> clazz,
            Predicate<T> predicate)
    {
        requireNonNull(nodes, "nodes is null");
        requireNonNull(clazz, "clazz is null");
        requireNonNull(predicate, "predicate is null");

        return ImmutableList.copyOf(nodes).stream()
                .flatMap(node -> linearizeNodes(node).stream())
                .filter(clazz::isInstance)
                .map(clazz::cast)
                .filter(predicate)
                .collect(toImmutableList());
    }

    private static List<Node> linearizeNodes(Node node)
    {
        ImmutableList.Builder<Node> nodes = ImmutableList.builder();
        new DefaultExpressionTraversalVisitor<Node, Void>()
        {
            @Override
            public Node process(Node node, Void context)
            {
                Node result = super.process(node, context);
                nodes.add(node);
                return result;
            }
        }.process(node, null);
        return nodes.build();
    }

    public static boolean isEqualComparisonExpression(Expression expression)
    {
        return expression instanceof ComparisonExpression && ((ComparisonExpression) expression).getOperator() == ComparisonExpression.Operator.EQUAL;
    }
}
