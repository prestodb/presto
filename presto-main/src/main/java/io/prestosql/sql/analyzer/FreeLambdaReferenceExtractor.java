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
import com.google.common.collect.ImmutableSet;
import io.prestosql.sql.tree.DefaultExpressionTraversalVisitor;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.Identifier;
import io.prestosql.sql.tree.LambdaArgumentDeclaration;
import io.prestosql.sql.tree.LambdaExpression;
import io.prestosql.sql.tree.Node;
import io.prestosql.sql.tree.NodeRef;

import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

/**
 * Extract expressions that are free (unbound) references to a lambda argument.
 */
public class FreeLambdaReferenceExtractor
{
    private FreeLambdaReferenceExtractor() {}

    public static boolean hasFreeReferencesToLambdaArgument(Node node, Analysis analysis)
    {
        return !getFreeReferencesToLambdaArgument(node, analysis).isEmpty();
    }

    public static List<Expression> getFreeReferencesToLambdaArgument(Node node, Analysis analysis)
    {
        Visitor visitor = new Visitor(analysis);
        visitor.process(node, ImmutableSet.of());
        return visitor.getFreeReferencesToLambdaArgument();
    }

    private static class Visitor
            extends DefaultExpressionTraversalVisitor<Void, Set<String>>
    {
        private final Analysis analysis;
        private final ImmutableList.Builder<Expression> freeReferencesToLambdaArgument = ImmutableList.builder();

        private Visitor(Analysis analysis)
        {
            this.analysis = requireNonNull(analysis, "analysis is null");
        }

        List<Expression> getFreeReferencesToLambdaArgument()
        {
            return freeReferencesToLambdaArgument.build();
        }

        @Override
        protected Void visitIdentifier(Identifier node, Set<String> lambdaArgumentNames)
        {
            if (analysis.getLambdaArgumentReferences().containsKey(NodeRef.of(node)) && !lambdaArgumentNames.contains(node.getValue())) {
                freeReferencesToLambdaArgument.add(node);
            }
            return null;
        }

        @Override
        protected Void visitLambdaExpression(LambdaExpression node, Set<String> lambdaArgumentNames)
        {
            return process(node.getBody(), ImmutableSet.<String>builder()
                    .addAll(lambdaArgumentNames)
                    .addAll(node.getArguments().stream()
                            .map(LambdaArgumentDeclaration::getName)
                            .map(Identifier::getValue)
                            .collect(toImmutableSet()))
                    .build());
        }
    }
}
