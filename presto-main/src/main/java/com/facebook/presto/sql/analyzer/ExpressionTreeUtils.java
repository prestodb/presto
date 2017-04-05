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

import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.sql.planner.optimizations.Predicates;
import com.facebook.presto.sql.tree.DefaultExpressionTraversalVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.Node;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.function.Predicate;

import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static java.util.Objects.requireNonNull;

final class ExpressionTreeUtils
{
    private ExpressionTreeUtils() {}

    static Predicate<FunctionCall> getAggregateExtractorFunction(FunctionRegistry functionRegistry)
    {
        return ((functionCall) -> (functionRegistry.isAggregationFunction(functionCall.getName()) || functionCall.getFilter().isPresent()) && !functionCall.getWindow().isPresent());
    }

    static boolean isWindowFunction(FunctionCall functionCall)
    {
        return functionCall.getWindow().isPresent();
    }

    static <T extends Expression> List<T> extractExpressionsOfType(
            Iterable<? extends Node> nodes,
            Class<T> clazz)
    {
        return extractExpressionsOfTypeUsingPredicate(nodes, clazz, Predicates.alwaysTrue());
    }

    static <T extends Expression> List<T> extractExpressionsOfTypeUsingPredicate(
            Iterable<? extends Node> nodes,
            Class<T> clazz,
            Predicate<T> predicate)
    {
        requireNonNull(nodes, "nodes is null");
        requireNonNull(clazz, "clazz is null");
        requireNonNull(predicate, "predicate is null");

        ImmutableList.Builder<T> expressionsBuilder = ImmutableList.builder();
        for (Node node : nodes) {
            List<? extends Node> flattenedNodes = linearizeNodes(node);
            expressionsBuilder.addAll(flattenedNodes.stream()
                    .filter(clazz::isInstance)
                    .map(clazz::cast)
                    .filter(predicate)
                    .collect(toImmutableList()));
        }

        return expressionsBuilder.build();
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
