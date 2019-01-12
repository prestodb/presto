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
package io.prestosql.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.prestosql.sql.planner.iterative.Lookup;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.tree.DefaultExpressionTraversalVisitor;
import io.prestosql.sql.tree.DefaultTraversalVisitor;
import io.prestosql.sql.tree.DereferenceExpression;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.Identifier;
import io.prestosql.sql.tree.NodeRef;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.SymbolReference;

import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.prestosql.sql.planner.ExpressionExtractor.extractExpressions;
import static io.prestosql.sql.planner.ExpressionExtractor.extractExpressionsNonRecursive;
import static io.prestosql.sql.planner.iterative.Lookup.noLookup;
import static io.prestosql.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
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
        return ImmutableSet.copyOf(extractAll(expression));
    }

    public static Set<Symbol> extractUnique(Iterable<? extends Expression> expressions)
    {
        ImmutableSet.Builder<Symbol> unique = ImmutableSet.builder();
        for (Expression expression : expressions) {
            unique.addAll(extractAll(expression));
        }
        return unique.build();
    }

    public static List<Symbol> extractAll(Expression expression)
    {
        ImmutableList.Builder<Symbol> builder = ImmutableList.builder();
        new SymbolBuilderVisitor().process(expression, builder);
        return builder.build();
    }

    // to extract qualified name with prefix
    public static Set<QualifiedName> extractNames(Expression expression, Set<NodeRef<Expression>> columnReferences)
    {
        ImmutableSet.Builder<QualifiedName> builder = ImmutableSet.builder();
        new QualifiedNameBuilderVisitor(columnReferences).process(expression, builder);
        return builder.build();
    }

    public static Set<Symbol> extractOutputSymbols(PlanNode planNode)
    {
        return extractOutputSymbols(planNode, noLookup());
    }

    public static Set<Symbol> extractOutputSymbols(PlanNode planNode, Lookup lookup)
    {
        return searchFrom(planNode, lookup)
                .findAll()
                .stream()
                .flatMap(node -> node.getOutputSymbols().stream())
                .collect(toImmutableSet());
    }

    private static class SymbolBuilderVisitor
            extends DefaultExpressionTraversalVisitor<Void, ImmutableList.Builder<Symbol>>
    {
        @Override
        protected Void visitSymbolReference(SymbolReference node, ImmutableList.Builder<Symbol> builder)
        {
            builder.add(Symbol.from(node));
            return null;
        }
    }

    private static class QualifiedNameBuilderVisitor
            extends DefaultTraversalVisitor<Void, ImmutableSet.Builder<QualifiedName>>
    {
        private final Set<NodeRef<Expression>> columnReferences;

        private QualifiedNameBuilderVisitor(Set<NodeRef<Expression>> columnReferences)
        {
            this.columnReferences = requireNonNull(columnReferences, "columnReferences is null");
        }

        @Override
        protected Void visitDereferenceExpression(DereferenceExpression node, ImmutableSet.Builder<QualifiedName> builder)
        {
            if (columnReferences.contains(NodeRef.<Expression>of(node))) {
                builder.add(DereferenceExpression.getQualifiedName(node));
            }
            else {
                process(node.getBase(), builder);
            }
            return null;
        }

        @Override
        protected Void visitIdentifier(Identifier node, ImmutableSet.Builder<QualifiedName> builder)
        {
            builder.add(QualifiedName.of(node.getValue()));
            return null;
        }
    }
}
