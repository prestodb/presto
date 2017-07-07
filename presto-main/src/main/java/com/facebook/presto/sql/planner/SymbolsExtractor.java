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

import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.SymbolReference;
import com.facebook.presto.sql.util.AstUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.Set;
import java.util.stream.Stream;

import static com.facebook.presto.sql.planner.ExpressionExtractor.extractExpressions;
import static com.facebook.presto.sql.planner.ExpressionExtractor.extractExpressionsNonRecursive;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Streams.stream;
import static java.util.Objects.requireNonNull;

public final class SymbolsExtractor
{
    private SymbolsExtractor() {}

    public static Set<Symbol> extractUnique(PlanNode node)
    {
        ImmutableSet.Builder<Symbol> uniqueSymbols = ImmutableSet.builder();
        extractExpressions(node).forEach(expression -> uniqueSymbols.addAll(extractUnique(expression)));

        return uniqueSymbols.build();
    }

    public static Set<Symbol> extractUniqueNonRecursive(PlanNode node)
    {
        ImmutableSet.Builder<Symbol> uniqueSymbols = ImmutableSet.builder();
        extractExpressionsNonRecursive(node).forEach(expression -> uniqueSymbols.addAll(extractUnique(expression)));

        return uniqueSymbols.build();
    }

    public static Set<Symbol> extractUnique(PlanNode node, Lookup lookup)
    {
        ImmutableSet.Builder<Symbol> uniqueSymbols = ImmutableSet.builder();
        extractExpressions(node, lookup).forEach(expression -> uniqueSymbols.addAll(extractUnique(expression)));

        return uniqueSymbols.build();
    }

    public static Set<Symbol> extractUnique(Expression expression)
    {
        return extractUnique(ImmutableList.of(expression));
    }

    public static Set<Symbol> extractUnique(Iterable<? extends Expression> expressions)
    {
        return stream(expressions)
                .flatMap(SymbolsExtractor::extractAll)
                .collect(toImmutableSet());
    }

    public static Stream<Symbol> extractAll(Expression expression)
    {
        return AstUtils.preOrder(expression)
                .filter(SymbolReference.class::isInstance)
                .map(SymbolReference.class::cast)
                .map(Symbol::from);
    }

    // to extract qualified name with prefix
    public static Set<QualifiedName> extractNames(Expression expression, Set<NodeRef<Expression>> columnReferences)
    {
        requireNonNull(expression, "expression is null");
        requireNonNull(columnReferences, "columnReferences is null");

        return AstUtils.preOrder(expression)
                .filter(node -> columnReferences.contains(NodeRef.of(node)))
                .map(Expression.class::cast)
                .map(QualifiedName::from)
                .collect(toImmutableSet());
    }
}
