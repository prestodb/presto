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

package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.tree.BindExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.LambdaArgumentDeclaration;
import com.facebook.presto.sql.tree.LambdaExpression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static com.facebook.presto.sql.planner.ExpressionVariableInliner.inlineVariables;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

public class LambdaCaptureDesugaringRewriter
{
    public static Expression rewrite(Expression expression, PlanVariableAllocator variableAllocator)
    {
        return ExpressionTreeRewriter.rewriteWith(new Visitor(variableAllocator), expression, new Context());
    }

    private LambdaCaptureDesugaringRewriter() {}

    private static class Visitor
            extends ExpressionRewriter<Context>
    {
        private final PlanVariableAllocator variableAllocator;

        public Visitor(PlanVariableAllocator variableAllocator)
        {
            this.variableAllocator = requireNonNull(variableAllocator, "variableAllocator is null");
        }

        @Override
        public Expression rewriteLambdaExpression(LambdaExpression node, Context context, ExpressionTreeRewriter<Context> treeRewriter)
        {
            // Use linked hash set to guarantee deterministic iteration order
            LinkedHashSet<VariableReferenceExpression> referencedVariables = new LinkedHashSet<>();
            Expression rewrittenBody = treeRewriter.rewrite(node.getBody(), context.withReferencedVariables(referencedVariables));

            List<String> lambdaArgumentNames = node.getArguments().stream()
                    .map(LambdaArgumentDeclaration::getName)
                    .map(Identifier::getValue)
                    .collect(toImmutableList());

            // referenced variables - lambda arguments = capture variables
            Set<VariableReferenceExpression> captureVariables = referencedVariables.stream().filter(variable -> !lambdaArgumentNames.contains(variable.getName())).collect(toImmutableSet());

            // x -> f(x, captureVariable)    will be rewritten into
            // "$internal$bind"(captureVariable, (extraVariable, x) -> f(x, extraVariable))

            ImmutableMap.Builder<VariableReferenceExpression, VariableReferenceExpression> captureVariableToExtraVariable = ImmutableMap.builder();
            ImmutableList.Builder<LambdaArgumentDeclaration> newLambdaArguments = ImmutableList.builder();
            for (VariableReferenceExpression captureVariable : captureVariables) {
                VariableReferenceExpression extraVariable = variableAllocator.newVariable(captureVariable);
                captureVariableToExtraVariable.put(captureVariable, extraVariable);
                newLambdaArguments.add(new LambdaArgumentDeclaration(new Identifier(extraVariable.getName())));
            }
            newLambdaArguments.addAll(node.getArguments());

            ImmutableMap<VariableReferenceExpression, VariableReferenceExpression> variablesMap = captureVariableToExtraVariable.build();
            Function<VariableReferenceExpression, Expression> variableMapping = variable -> new SymbolReference(variablesMap.getOrDefault(variable, variable).getName());
            Expression rewrittenExpression = new LambdaExpression(newLambdaArguments.build(), inlineVariables(variableMapping, rewrittenBody, variableAllocator.getTypes()));

            if (captureVariables.size() != 0) {
                List<Expression> capturedValues = captureVariables.stream()
                        .map(variable -> new SymbolReference(variable.getName()))
                        .collect(toImmutableList());
                rewrittenExpression = new BindExpression(capturedValues, rewrittenExpression);
            }

            context.getReferencedVariables().addAll(captureVariables);
            return rewrittenExpression;
        }

        @Override
        public Expression rewriteSymbolReference(SymbolReference node, Context context, ExpressionTreeRewriter<Context> treeRewriter)
        {
            context.getReferencedVariables().add(variableAllocator.toVariableReference(node));
            return null;
        }
    }

    private static class Context
    {
        // Use linked hash set to guarantee deterministic iteration order
        final LinkedHashSet<VariableReferenceExpression> referencedVariables;

        public Context()
        {
            this(new LinkedHashSet<>());
        }

        private Context(LinkedHashSet<VariableReferenceExpression> referencedVariables)
        {
            this.referencedVariables = referencedVariables;
        }

        public LinkedHashSet<VariableReferenceExpression> getReferencedVariables()
        {
            return referencedVariables;
        }

        public Context withReferencedVariables(LinkedHashSet<VariableReferenceExpression> variables)
        {
            return new Context(variables);
        }
    }
}
