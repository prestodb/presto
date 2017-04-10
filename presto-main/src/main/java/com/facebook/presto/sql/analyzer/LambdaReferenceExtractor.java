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
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.Node;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Extract expressions that are references to a lambda argument.
 */
public class LambdaReferenceExtractor
{
    private LambdaReferenceExtractor() {}

    public static boolean hasReferencesToLambdaArgument(Node node, Analysis analysis)
    {
        return !getReferencesToLambdaArgument(node, analysis).isEmpty();
    }

    public static List<Expression> getReferencesToLambdaArgument(Node node, Analysis analysis)
    {
        ImmutableList.Builder<Expression> builder = ImmutableList.builder();
        new Visitor(analysis).process(node, builder);
        return builder.build();
    }

    private static class Visitor
            extends DefaultExpressionTraversalVisitor<Void, ImmutableList.Builder<Expression>>
    {
        private final Analysis analysis;

        private Visitor(Analysis analysis)
        {
            this.analysis = requireNonNull(analysis, "analysis is null");
        }

        @Override
        protected Void visitIdentifier(Identifier node, ImmutableList.Builder<Expression> context)
        {
            if (analysis.getLambdaArgumentReferences().containsKey(node)) {
                context.add(node);
            }
            return null;
        }
    }
}
