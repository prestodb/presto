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
package io.prestosql.sql.analyzer;

import com.google.common.collect.ImmutableList;
import io.prestosql.metadata.FunctionRegistry;
import io.prestosql.sql.tree.DefaultExpressionTraversalVisitor;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.FunctionCall;
import io.prestosql.sql.tree.Node;

import java.util.List;
import java.util.function.Predicate;

import static com.google.common.base.Predicates.alwaysTrue;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public final class ExpressionTreeUtils
{
    private ExpressionTreeUtils() {}

    static List<FunctionCall> extractAggregateFunctions(Iterable<? extends Node> nodes, FunctionRegistry functionRegistry)
    {
        return extractExpressions(nodes, FunctionCall.class, isAggregationPredicate(functionRegistry));
    }

    static List<FunctionCall> extractWindowFunctions(Iterable<? extends Node> nodes)
    {
        return extractExpressions(nodes, FunctionCall.class, ExpressionTreeUtils::isWindowFunction);
    }

    public static <T extends Expression> List<T> extractExpressions(
            Iterable<? extends Node> nodes,
            Class<T> clazz)
    {
        return extractExpressions(nodes, clazz, alwaysTrue());
    }

    private static Predicate<FunctionCall> isAggregationPredicate(FunctionRegistry functionRegistry)
    {
        return ((functionCall) -> (functionRegistry.isAggregationFunction(functionCall.getName())
                || functionCall.getFilter().isPresent()) && !functionCall.getWindow().isPresent()
                || functionCall.getOrderBy().isPresent());
    }

    private static boolean isWindowFunction(FunctionCall functionCall)
    {
        return functionCall.getWindow().isPresent();
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
}
