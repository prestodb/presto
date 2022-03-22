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

import com.facebook.presto.expressions.DefaultRowExpressionTraversalVisitor;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.tree.DefaultExpressionTraversalVisitor;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Set;

import static com.facebook.presto.sql.planner.ExpressionExtractor.extractExpressions;
import static com.facebook.presto.sql.planner.ExpressionExtractor.extractExpressionsNonRecursive;
import static com.facebook.presto.sql.planner.iterative.Lookup.noLookup;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToExpression;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.isExpression;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

public final class VariablesExtractor
{
    private VariablesExtractor() {}

    public static Set<VariableReferenceExpression> extractUnique(PlanNode node, TypeProvider types)
    {
        ImmutableSet.Builder<VariableReferenceExpression> unique = ImmutableSet.builder();
        extractExpressions(node).forEach(expression -> unique.addAll(extractUniqueVariableInternal(expression, types)));

        return unique.build();
    }

    public static Set<VariableReferenceExpression> extractUniqueNonRecursive(PlanNode node, TypeProvider types)
    {
        ImmutableSet.Builder<VariableReferenceExpression> uniqueVariables = ImmutableSet.builder();
        extractExpressionsNonRecursive(node).forEach(expression -> uniqueVariables.addAll(extractUniqueVariableInternal(expression, types)));

        return uniqueVariables.build();
    }

    public static Set<VariableReferenceExpression> extractUnique(PlanNode node, Lookup lookup, TypeProvider types)
    {
        ImmutableSet.Builder<VariableReferenceExpression> unique = ImmutableSet.builder();
        extractExpressions(node, lookup).forEach(expression -> unique.addAll(extractUniqueVariableInternal(expression, types)));
        return unique.build();
    }

    public static Set<VariableReferenceExpression> extractUnique(Expression expression, TypeProvider types)
    {
        return ImmutableSet.copyOf(extractAll(expression, types));
    }

    public static Set<VariableReferenceExpression> extractUnique(RowExpression expression)
    {
        return ImmutableSet.copyOf(extractAll(expression));
    }

    public static Set<VariableReferenceExpression> extractUnique(Iterable<? extends RowExpression> expressions)
    {
        ImmutableSet.Builder<VariableReferenceExpression> unique = ImmutableSet.builder();
        for (RowExpression expression : expressions) {
            unique.addAll(extractAll(expression));
        }
        return unique.build();
    }

    @Deprecated
    public static Set<VariableReferenceExpression> extractUnique(Iterable<? extends Expression> expressions, TypeProvider types)
    {
        ImmutableSet.Builder<VariableReferenceExpression> unique = ImmutableSet.builder();
        for (Expression expression : expressions) {
            unique.addAll(extractAll(expression, types));
        }
        return unique.build();
    }

    public static List<Symbol> extractAllSymbols(Expression expression)
    {
        ImmutableList.Builder<Symbol> builder = ImmutableList.builder();
        new SymbolBuilderVisitor().process(expression, builder);
        return builder.build();
    }

    public static List<VariableReferenceExpression> extractAll(Expression expression, TypeProvider types)
    {
        ImmutableList.Builder<VariableReferenceExpression> builder = ImmutableList.builder();
        new VariableFromExpressionBuilderVisitor(types).process(expression, builder);
        return builder.build();
    }

    public static List<VariableReferenceExpression> extractAll(RowExpression expression)
    {
        ImmutableList.Builder<VariableReferenceExpression> builder = ImmutableList.builder();
        expression.accept(new VariableBuilderVisitor(), builder);
        return builder.build();
    }

    // to extract qualified name with prefix
    public static Set<QualifiedName> extractNames(Expression expression, Set<NodeRef<Expression>> columnReferences)
    {
        ImmutableSet.Builder<QualifiedName> builder = ImmutableSet.builder();
        new QualifiedNameBuilderVisitor(columnReferences).process(expression, builder);
        return builder.build();
    }

    public static Set<VariableReferenceExpression> extractOutputVariables(PlanNode planNode)
    {
        return extractOutputVariables(planNode, noLookup());
    }

    public static Set<VariableReferenceExpression> extractOutputVariables(PlanNode planNode, Lookup lookup)
    {
        return searchFrom(planNode, lookup)
                .findAll()
                .stream()
                .flatMap(node -> node.getOutputVariables().stream())
                .collect(toImmutableSet());
    }

    private static Set<VariableReferenceExpression> extractUniqueVariableInternal(RowExpression expression, TypeProvider types)
    {
        if (isExpression(expression)) {
            return extractUnique(castToExpression(expression), types);
        }
        return extractUnique(expression);
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

    private static class VariableFromExpressionBuilderVisitor
            extends DefaultExpressionTraversalVisitor<Void, ImmutableList.Builder<VariableReferenceExpression>>
    {
        private final TypeProvider types;

        protected VariableFromExpressionBuilderVisitor(TypeProvider types)
        {
            this.types = types;
        }

        @Override
        protected Void visitSymbolReference(SymbolReference node, ImmutableList.Builder<VariableReferenceExpression> builder)
        {
            builder.add(new VariableReferenceExpression(node.getName(), types.get(node)));
            return null;
        }
    }

    private static class VariableBuilderVisitor
            extends DefaultRowExpressionTraversalVisitor<ImmutableList.Builder<VariableReferenceExpression>>
    {
        @Override
        public Void visitVariableReference(VariableReferenceExpression variable, ImmutableList.Builder<VariableReferenceExpression> builder)
        {
            builder.add(variable);
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
