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

import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FieldReference;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.SymbolReference;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

/**
 * Currently this class handles only simple expressions like:
 * <p>
 * A.a < B.x
 * <p>
 * It could be extended to handle any expressions like:
 * <p>
 * A.a * sin(A.b) / log(B.x) < cos(B.z)
 * <p>
 * by transforming it to:
 * <p>
 * f(A.a, A.b) < g(B.x, B.z)
 * <p>
 * Where f(...) and g(...) would be some functions/expressions. That
 * would allow us to perform binary search on arbitrary complex expressions
 * by sorting position links according to the result of f(...) function.
 */
public final class SortExpressionExtractor
{
    private SortExpressionExtractor() {}

    public static Optional<Expression> extractSortExpression(Set<Symbol> buildSymbols, Expression filter)
    {
        if (!DeterminismEvaluator.isDeterministic(filter)) {
            return Optional.empty();
        }

        if (filter instanceof ComparisonExpression) {
            ComparisonExpression comparison = (ComparisonExpression) filter;
            switch (comparison.getType()) {
                case GREATER_THAN:
                case GREATER_THAN_OR_EQUAL:
                case LESS_THAN:
                case LESS_THAN_OR_EQUAL:
                    Optional<SymbolReference> sortChannel = asBuildSymbolReference(buildSymbols, comparison.getRight());
                    boolean hasBuildReferencesOnOtherSide = hasBuildSymbolReference(buildSymbols, comparison.getLeft());
                    if (!sortChannel.isPresent()) {
                        sortChannel = asBuildSymbolReference(buildSymbols, comparison.getLeft());
                        hasBuildReferencesOnOtherSide = hasBuildSymbolReference(buildSymbols, comparison.getRight());
                    }
                    if (sortChannel.isPresent() && !hasBuildReferencesOnOtherSide) {
                        return sortChannel.map(symbolReference -> (Expression) symbolReference);
                    }
                    return Optional.empty();
                default:
                    return Optional.empty();
            }
        }

        return Optional.empty();
    }

    private static Optional<SymbolReference> asBuildSymbolReference(Set<Symbol> buildLayout, Expression expression)
    {
        if (expression instanceof SymbolReference) {
            SymbolReference symbolReference = (SymbolReference) expression;
            if (buildLayout.contains(new Symbol(symbolReference.getName()))) {
                return Optional.of(symbolReference);
            }
        }
        return Optional.empty();
    }

    private static boolean hasBuildSymbolReference(Set<Symbol> buildSymbols, Expression expression)
    {
        return new BuildSymbolReferenceFinder(buildSymbols).process(expression);
    }

    private static class BuildSymbolReferenceFinder
            extends AstVisitor<Boolean, Void>
    {
        private final Set<String> buildSymbols;

        public BuildSymbolReferenceFinder(Set<Symbol> buildSymbols)
        {
            this.buildSymbols = requireNonNull(buildSymbols, "buildSymbols is null").stream()
                    .map(Symbol::getName)
                    .collect(toImmutableSet());
        }

        @Override
        protected Boolean visitNode(Node node, Void context)
        {
            for (Node child : node.getChildren()) {
                if (process(child, context)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        protected Boolean visitSymbolReference(SymbolReference symbolReference, Void context)
        {
            return buildSymbols.contains(symbolReference.getName());
        }
    }

    public static class SortExpression
    {
        private final int channel;

        public SortExpression(int channel)
        {
            this.channel = channel;
        }

        public int getChannel()
        {
            return channel;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            SortExpression other = (SortExpression) obj;
            return Objects.equals(this.channel, other.channel);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(channel);
        }

        public String toString()
        {
            return toStringHelper(this)
                    .add("channel", channel)
                    .toString();
        }

        public static SortExpression fromExpression(Expression expression)
        {
            checkState(expression instanceof FieldReference, "Unsupported expression type [%s]", expression);
            return new SortExpression(((FieldReference) expression).getFieldIndex());
        }
    }
}
