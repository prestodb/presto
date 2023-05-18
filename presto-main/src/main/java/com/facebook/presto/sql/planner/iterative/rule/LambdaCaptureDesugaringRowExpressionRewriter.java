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

import com.facebook.presto.common.type.Type;
import com.facebook.presto.expressions.RowExpressionRewriter;
import com.facebook.presto.expressions.RowExpressionTreeRewriter;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.BIND;
import static com.facebook.presto.sql.planner.RowExpressionVariableInliner.inlineVariables;
import static com.facebook.presto.sql.relational.Expressions.specialForm;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

public class LambdaCaptureDesugaringRowExpressionRewriter
{
    public static RowExpression rewrite(RowExpression expression, VariableAllocator variableAllocator)
    {
        return RowExpressionTreeRewriter.rewriteWith(new Visitor(variableAllocator), expression, new Context());
    }

    private LambdaCaptureDesugaringRowExpressionRewriter() {}

    private static class Visitor
            extends RowExpressionRewriter<Context>
    {
        private final VariableAllocator variableAllocator;

        public Visitor(VariableAllocator variableAllocator)
        {
            this.variableAllocator = requireNonNull(variableAllocator, "variableAllocator is null");
        }

        @Override
        public RowExpression rewriteLambda(LambdaDefinitionExpression node, Context context, RowExpressionTreeRewriter<Context> treeRewriter)
        {
            // Use linked hash set to guarantee deterministic iteration order
            LinkedHashSet<VariableReferenceExpression> referencedVariables = new LinkedHashSet<>();
            RowExpression rewrittenBody = treeRewriter.rewrite(node.getBody(), context.withReferencedVariables(referencedVariables));

            List<String> lambdaArgumentNames = node.getArguments();

            // referenced variables - lambda arguments = capture variables
            Set<VariableReferenceExpression> captureVariables = referencedVariables.stream().filter(variable -> !lambdaArgumentNames.contains(variable.getName())).collect(toImmutableSet());

            // x -> f(x, captureVariable)    will be rewritten into
            // "$internal$bind"(captureVariable, (extraVariable, x) -> f(x, extraVariable))

            ImmutableMap.Builder<VariableReferenceExpression, VariableReferenceExpression> captureVariableToExtraVariable = ImmutableMap.builder();
            ImmutableList.Builder<String> newLambdaArguments = ImmutableList.builder();
            ImmutableList.Builder<Type> newLambdaArgumentsType = ImmutableList.builder();
            for (VariableReferenceExpression captureVariable : captureVariables) {
                VariableReferenceExpression extraVariable = variableAllocator.newVariable(captureVariable);
                captureVariableToExtraVariable.put(captureVariable, extraVariable);
                newLambdaArguments.add(extraVariable.getName());
                newLambdaArgumentsType.add(extraVariable.getType());
            }
            newLambdaArguments.addAll(node.getArguments());
            newLambdaArgumentsType.addAll(node.getArgumentTypes());

            ImmutableMap<VariableReferenceExpression, VariableReferenceExpression> variablesMap = captureVariableToExtraVariable.build();
            Function<VariableReferenceExpression, RowExpression> variableMapping = variable -> variablesMap.getOrDefault(variable, variable);
            RowExpression rewrittenExpression = new LambdaDefinitionExpression(
                    node.getSourceLocation(),
                    newLambdaArgumentsType.build(),
                    newLambdaArguments.build(),
                    inlineVariables(variableMapping, rewrittenBody));

            if (!captureVariables.isEmpty()) {
                rewrittenExpression = specialForm(BIND, node.getType(), Stream.concat(captureVariables.stream(), Stream.of(rewrittenExpression)).collect(toImmutableList()));
            }

            context.getReferencedVariables().addAll(captureVariables);
            return rewrittenExpression;
        }

        @Override
        public RowExpression rewriteVariableReference(VariableReferenceExpression node, Context context, RowExpressionTreeRewriter<Context> treeRewriter)
        {
            context.getReferencedVariables().add(node);
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
