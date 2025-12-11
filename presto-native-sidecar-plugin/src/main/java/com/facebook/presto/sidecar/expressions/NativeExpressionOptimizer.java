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
package com.facebook.presto.sidecar.expressions;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SourceLocation;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.ExpressionOptimizer;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.facebook.presto.spi.relation.ExpressionOptimizer.Level.EVALUATED;
import static com.facebook.presto.spi.relation.ExpressionOptimizer.Level.OPTIMIZED;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.COALESCE;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Collections.newSetFromMap;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

public class NativeExpressionOptimizer
        implements ExpressionOptimizer
{
    private final FunctionMetadataManager functionMetadataManager;
    private final StandardFunctionResolution resolution;
    private final NativeSidecarExpressionInterpreter rowExpressionInterpreterService;

    @Inject
    public NativeExpressionOptimizer(
            NativeSidecarExpressionInterpreter rowExpressionInterpreterService,
            FunctionMetadataManager functionMetadataManager,
            StandardFunctionResolution resolution)
    {
        this.rowExpressionInterpreterService = requireNonNull(rowExpressionInterpreterService, "rowExpressionInterpreterService is null");
        this.functionMetadataManager = requireNonNull(functionMetadataManager, "functionMetadataManager is null");
        this.resolution = requireNonNull(resolution, "resolution is null");
    }

    @Override
    public RowExpression optimize(RowExpression expression, Level level, ConnectorSession session, Function<VariableReferenceExpression, Object> variableResolver)
    {
        // Collect expressions to optimize
        CollectingVisitor collectingVisitor = new CollectingVisitor(functionMetadataManager, level, resolution);
        expression.accept(collectingVisitor, variableResolver);
        List<RowExpression> expressionsToOptimize = collectingVisitor.getExpressionsToOptimize();

        // Create a map of original expressions and expressions with variables resolved to constants or row expressions.
        Map<RowExpression, RowExpression> expressions = expressionsToOptimize.stream()
                .collect(toMap(
                        Function.identity(),
                        rowExpression -> rowExpression.accept(
                                new ReplacingVisitor(variable -> {
                                    // Apply resolver
                                    Object replacement = variableResolver.apply(variable);
                                    // Preserve original variable if resolver returns null
                                    return replacement != null
                                            ? toRowExpression(variable.getSourceLocation(), replacement, variable.getType())
                                            : variable;
                                }),
                                null),
                        (a, b) -> a));
        if (expressions.isEmpty()) {
            return expression;
        }

        // Constants can be trivially replaced without invoking the interpreter.  Move them into a separate map.
        Map<RowExpression, RowExpression> constants = new HashMap<>();
        Iterator<Map.Entry<RowExpression, RowExpression>> entries = expressions.entrySet().iterator();
        while (entries.hasNext()) {
            Map.Entry<RowExpression, RowExpression> entry = entries.next();
            if (entry.getValue() instanceof ConstantExpression) {
                constants.put(entry.getKey(), entry.getValue());
                entries.remove();
            }
        }

        // Optimize the expressions using the sidecar interpreter
        Map<RowExpression, RowExpression> replacements = new HashMap<>();
        if (!expressions.isEmpty()) {
            // The native endpoint only supports optimizer levels OPTIMIZED or EVALUATED.
            // In the sidecar, SERIALIZABLE is effectively the same as OPTIMIZED,
            // so if SERIALIZABLE is requested, we use OPTIMIZED instead.
            replacements.putAll(
                    rowExpressionInterpreterService.optimizeBatch(
                            session,
                            expressions,
                            level.ordinal() < OPTIMIZED.ordinal() ? OPTIMIZED : level));
        }

        // Add back in the constants
        replacements.putAll(constants);

        // Replace all the expressions in the original expression with the optimized expressions
        return toRowExpression(expression.getSourceLocation(), expression.accept(new ReplacingVisitor(replacements), null), expression.getType());
    }

    /**
     * This visitor collects expressions that can be optimized by the sidecar interpreter.
     */
    private static class CollectingVisitor
            implements RowExpressionVisitor<Void, Object>
    {
        private final FunctionMetadataManager functionMetadataManager;
        private final Level optimizationLevel;
        private final StandardFunctionResolution resolution;
        private final Set<RowExpression> expressionsToOptimize = newSetFromMap(new IdentityHashMap<>());
        private final Set<RowExpression> hasOptimizedChildren = newSetFromMap(new IdentityHashMap<>());

        public CollectingVisitor(FunctionMetadataManager functionMetadataManager, Level optimizationLevel, StandardFunctionResolution resolution)
        {
            this.functionMetadataManager = requireNonNull(functionMetadataManager, "functionMetadataManager is null");
            this.optimizationLevel = requireNonNull(optimizationLevel, "optimizationLevel is null");
            this.resolution = requireNonNull(resolution, "resolution is null");
        }

        @Override
        public Void visitExpression(RowExpression node, Object context)
        {
            visitNode(node, false);
            return null;
        }

        @Override
        public Void visitConstant(ConstantExpression node, Object context)
        {
            visitNode(node, true);
            return null;
        }

        @Override
        public Void visitVariableReference(VariableReferenceExpression node, Object context)
        {
            Object value = null;
            if (context instanceof Function) {
                value = ((Function<VariableReferenceExpression, Object>) context).apply(node);
            }
            // If context is null or not a function, value stays null
            if (value == null || value instanceof RowExpression) {
                visitNode(node, false);
                return null;
            }
            visitNode(node, true);
            return null;
        }

        @Override
        public Void visitCall(CallExpression node, Object context)
        {
            // If the optimization level is not EVALUATED, then we cannot optimize non-deterministic functions
            boolean isDeterministic = functionMetadataManager.getFunctionMetadata(node.getFunctionHandle()).isDeterministic();
            boolean canBeEvaluated = (optimizationLevel.ordinal() < EVALUATED.ordinal() && isDeterministic) ||
                    optimizationLevel.ordinal() == EVALUATED.ordinal();

            // All arguments must be optimizable in order to evaluate the function
            for (RowExpression child : node.getArguments()) {
                child.accept(this, context);
            }

            boolean allConstantFoldable = node.getArguments().stream()
                    .allMatch(this::canBeOptimized);

            if (canBeEvaluated && allConstantFoldable) {
                visitNode(node, true);
                return null;
            }

            // If it's a cast and the type is already the same, then it's constant foldable
            if (resolution.isCastFunction(node.getFunctionHandle())
                    && node.getArguments().size() == 1
                    && node.getType().equals(node.getArguments().get(0).getType())) {
                visitNode(node, true);
                return null;
            }
            visitNode(node, false);
            return null;
        }

        @Override
        public Void visitSpecialForm(SpecialFormExpression node, Object context)
        {
            // Most special form expressions short circuit, meaning that they potentially don't evaluate all arguments.  For example, the AND expression
            // will stop evaluating arguments as soon as it finds a false argument.  Because a sub-expression could be simplified into a constant, and this
            // constant could cause the expression to short circuit, if there is at least one argument which is optimizable, then the entire expression should
            // be sent to the sidecar to be optimized.
            for (RowExpression child : node.getArguments()) {
                child.accept(this, context);
            }

            boolean anyArgumentsOptimizable = node.getArguments().stream()
                    .anyMatch(this::canBeOptimized);

            // If any arguments are constant foldable, then the whole expression is constant foldable
            if (anyArgumentsOptimizable) {
                visitNode(node, true);
                return null;
            }

            // If the special form is COALESCE, then we can optimize it if there are any duplicate arguments
            if (node.getForm() == COALESCE) {
                ImmutableSet.Builder<RowExpression> uniqueArgs = ImmutableSet.builder();
                int optimizableCount = 0;
                // Check if there's any duplicate arguments, these can be de-duplicated
                for (RowExpression argument : node.getArguments()) {
                    // The duplicate argument must either be a leaf (variable reference) or constant foldable
                    if (canBeOptimized(argument)) {
                        uniqueArgs.add(argument);
                        optimizableCount++;
                    }
                }

                // There is a duplicate when the number of optimizable args > number of unique ones
                // If there were any duplicates, or if there's no arguments (cancel out), or if there's only one argument (just return it),
                // then it's also constant foldable
                boolean canBeOptimized = uniqueArgs.build().size() < optimizableCount || node.getArguments().size() <= 1;
                if (canBeOptimized) {
                    visitNode(node, true);
                    return null;
                }
            }
            visitNode(node, false);
            return null;
        }

        @Override
        public Void visitLambda(LambdaDefinitionExpression node, Object context)
        {
            node.getBody().accept(this, (Function<VariableReferenceExpression, Object>) variable -> variable);
            if (canBeOptimized(node.getBody())) {
                visitNode(node, true);
                return null;
            }
            visitNode(node, false);
            return null;
        }

        public boolean canBeOptimized(RowExpression rowExpression)
        {
            return expressionsToOptimize.contains(rowExpression);
        }

        private void visitNode(RowExpression node, boolean canBeOptimized)
        {
            requireNonNull(node, "node is null");
            // If the present node can be optimized, then we send the whole expression.  Because an expression may consist of many
            // sub-expressions, we need to ensure that we don't send the sub-expression along with its parent expression.  For example,
            // if we have the expression (a + b) + c, and we can optimize a + b, then we don't want to send a + b to the sidecar, because
            // it will be optimized twice.  Instead, we want to send (a + b) + c to the sidecar, and then remove a + b from the list of
            // expressions to optimize.
            // We need to traverse the entire subtree of possible expressions to optimize because some special form expressions may
            // short circuit, and we need to ensure that we don't send the sub-expression to the sidecar if the parent expression is
            // constant foldable.  For example, consider the expression false AND (true OR a).  Although the expression true OR a is
            // constant foldable, the parent expression is also constant foldable, and we don't want to send both the parent expression
            // and the sub-expression to the sidecar because the entire expression can be constant folded in one pass.
            if (canBeOptimized) {
                ArrayDeque<RowExpression> queue = new ArrayDeque<>(node.getChildren());
                while (!queue.isEmpty()) {
                    RowExpression expression = queue.poll();
                    if (hasOptimizedChildren.remove(expression)) {
                        expressionsToOptimize.remove(expression);
                        queue.addAll(expression.getChildren());
                    }
                }
                expressionsToOptimize.add(node);
                hasOptimizedChildren.add(node);
            }
            else if (node.getChildren().stream().anyMatch(hasOptimizedChildren::contains)) {
                hasOptimizedChildren.add(node);
            }
        }

        public List<RowExpression> getExpressionsToOptimize()
        {
            return ImmutableList.copyOf(expressionsToOptimize);
        }
    }

    /**
     * This visitor replaces expressions with their optimized versions.
     */
    private static class ReplacingVisitor
            implements RowExpressionVisitor<RowExpression, Void>
    {
        private final Function<RowExpression, RowExpression> resolver;

        public ReplacingVisitor(Map<RowExpression, RowExpression> replacements)
        {
            requireNonNull(replacements, "replacements is null");
            this.resolver = i -> replacements.getOrDefault(i, i);
        }

        public ReplacingVisitor(Function<VariableReferenceExpression, RowExpression> variableResolver)
        {
            requireNonNull(variableResolver, "variableResolver is null");
            this.resolver = i -> i instanceof VariableReferenceExpression ? variableResolver.apply((VariableReferenceExpression) i) : i;
        }

        private boolean canBeReplaced(RowExpression rowExpression)
        {
            return resolver.apply(rowExpression) != rowExpression;
        }

        @Override
        public RowExpression visitExpression(RowExpression originalExpression, Void context)
        {
            return resolver.apply(originalExpression);
        }

        @Override
        public RowExpression visitLambda(LambdaDefinitionExpression lambda, Void context)
        {
            if (canBeReplaced(lambda.getBody())) {
                return new LambdaDefinitionExpression(
                        lambda.getSourceLocation(),
                        lambda.getArgumentTypes(),
                        lambda.getArguments(),
                        toRowExpression(lambda.getSourceLocation(), resolver.apply(lambda.getBody()), lambda.getBody().getType()));
            }
            return lambda;
        }

        @Override
        public RowExpression visitCall(CallExpression call, Void context)
        {
            if (canBeReplaced(call)) {
                return resolver.apply(call);
            }
            List<RowExpression> updatedArguments = call.getArguments().stream()
                    .map(argument -> toRowExpression(argument.getSourceLocation(), argument.accept(this, context), argument.getType()))
                    .collect(toImmutableList());
            return new CallExpression(call.getSourceLocation(), call.getDisplayName(), call.getFunctionHandle(), call.getType(), updatedArguments);
        }

        @Override
        public RowExpression visitSpecialForm(SpecialFormExpression specialForm, Void context)
        {
            if (canBeReplaced(specialForm)) {
                return resolver.apply(specialForm);
            }
            List<RowExpression> updatedArguments = specialForm.getArguments().stream()
                    .map(argument -> toRowExpression(argument.getSourceLocation(), argument.accept(this, context), argument.getType()))
                    .collect(toImmutableList());
            return new SpecialFormExpression(specialForm.getSourceLocation(), specialForm.getForm(), specialForm.getType(), updatedArguments);
        }
    }

    private static RowExpression toRowExpression(Optional<SourceLocation> sourceLocation, Object object, Type type)
    {
        requireNonNull(type, "type is null");

        if (object instanceof RowExpression) {
            return (RowExpression) object;
        }

        // If it's not a RowExpression, we assume it's a literal value.
        return new ConstantExpression(sourceLocation, object, type);
    }
}
