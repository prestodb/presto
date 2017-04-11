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

import com.facebook.presto.sql.tree.DefaultExpressionTraversalVisitor;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.LambdaExpression;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Extract expressions that are references to a given scope.
 */
class ScopeReferenceExtractor
{
    private ScopeReferenceExtractor() {}

    public static boolean hasReferencesToScope(Node node, Analysis analysis, Scope scope)
    {
        return !getReferencesToScope(node, analysis, scope).isEmpty();
    }

    public static List<Expression> getReferencesToScope(Node node, Analysis analysis, Scope scope)
    {
        ImmutableList.Builder<Expression> builder = ImmutableList.builder();
        new Visitor(analysis, scope).process(node, builder);
        return builder.build();
    }

    private static class Visitor
            extends DefaultExpressionTraversalVisitor<Void, ImmutableList.Builder<Expression>>
    {
        private final Analysis analysis;
        private final Scope scope;

        private Visitor(Analysis analysis, Scope scope)
        {
            this.analysis = requireNonNull(analysis, "analysis is null");
            this.scope = requireNonNull(scope, "scope is null");
        }

        @Override
        protected Void visitIdentifier(Identifier node, ImmutableList.Builder<Expression> context)
        {
            if (!analysis.getLambdaArgumentReferences().containsKey(node) && isReferenceToScope(node, QualifiedName.of(node.getName()))) {
                context.add(node);
            }
            return null;
        }

        @Override
        protected Void visitLambdaExpression(LambdaExpression node, ImmutableList.Builder<Expression> context)
        {
            return process(node.getBody(), context);
        }

        @Override
        protected Void visitDereferenceExpression(DereferenceExpression node, ImmutableList.Builder<Expression> context)
        {
            if (analysis.getColumnReferences().contains(node) && isReferenceToScope(node, DereferenceExpression.getQualifiedName(node))) {
                context.add(node);
                return null;
            }

            return super.visitDereferenceExpression(node, context);
        }

        private boolean isReferenceToScope(Expression node, QualifiedName qualifiedName)
        {
            Optional<ResolvedField> resolvedField = scope.tryResolveField(node, qualifiedName);
            return resolvedField.isPresent() && resolvedField.get().getScope().equals(scope);
        }
    }
}
