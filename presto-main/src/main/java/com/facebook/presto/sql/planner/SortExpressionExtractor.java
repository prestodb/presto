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
import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * Currently this class handles only simple expressions like:
 *
 * A.a < B.x.
 *
 * It could be extended to handle any expressions like:
 *
 * A.a * sin(A.b) / log(B.x) < cos(B.z)
 *
 * by transforming it to:
 *
 * f(A.a, A.b) < g(B.x, B.z)
 *
 * Where f(...) and g(...) would be some functions/expressions. That
 * would allow us to perform binary search on arbitrary complex expressions
 * by sorting position links according to the result of f(...) function.
 */
public final class SortExpressionExtractor
{
    private SortExpressionExtractor() {}

    public static Optional<SortExpression> extractSortExpression(Map<Symbol, Integer> buildLayout, Expression filter)
    {
        Set<Integer> buildFields = ImmutableSet.copyOf(buildLayout.values());
        if (filter instanceof ComparisonExpression) {
            ComparisonExpression comparison = (ComparisonExpression) filter;
            switch (comparison.getType()) {
                case GREATER_THAN:
                case GREATER_THAN_OR_EQUAL:
                case LESS_THAN:
                case LESS_THAN_OR_EQUAL:
                    Optional<Integer> sortChannel = asBuildFieldReference(buildFields, comparison.getRight());
                    boolean hasBuildReferencesOnOtherSide = hasBuildFieldReference(buildFields, comparison.getLeft());
                    if (!sortChannel.isPresent()) {
                        sortChannel = asBuildFieldReference(buildFields, comparison.getLeft());
                        hasBuildReferencesOnOtherSide = hasBuildFieldReference(buildFields, comparison.getRight());
                    }
                    if (sortChannel.isPresent() && !hasBuildReferencesOnOtherSide) {
                        return Optional.of(new SortExpression(sortChannel.get()));
                    }
                    return Optional.empty();
                default:
                    return Optional.empty();
            }
        }

        return Optional.empty();
    }

    private static Optional<Integer> asBuildFieldReference(Set<Integer> buildLayout, Expression expression)
    {
        if (expression instanceof FieldReference) {
            FieldReference field = (FieldReference) expression;
            if (buildLayout.contains(field.getFieldIndex())) {
                return Optional.of(field.getFieldIndex());
            }
        }
        return Optional.empty();
    }

    private static boolean hasBuildFieldReference(Set<Integer> buildLayout, Expression expression)
    {
        return new BuildFieldReferenceFinder(buildLayout).process(expression);
    }

    private static class BuildFieldReferenceFinder
            extends AstVisitor<Boolean, Void>
    {
        private final Set<Integer> buildLayout;

        public BuildFieldReferenceFinder(Set<Integer> buildLayout)
        {
            this.buildLayout = ImmutableSet.copyOf(requireNonNull(buildLayout, "buildLayout is null"));
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
        protected Boolean visitFieldReference(FieldReference fieldReference, Void context)
        {
            return buildLayout.contains(fieldReference.getFieldIndex());
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
    }
}
