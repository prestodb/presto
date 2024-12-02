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
package com.facebook.presto.session.sql.expressions;

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

import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.facebook.presto.spi.relation.ExpressionOptimizer.Level.EVALUATED;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Collections.newSetFromMap;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

public class NativeExpressionOptimizer
        implements ExpressionOptimizer
{
    private static final Function<VariableReferenceExpression, Object> NO_RESOLUTION = i -> i;
    private final FunctionMetadataManager functionMetadataManager;
    private final StandardFunctionResolution resolution;
    private final NativeSidecarExpressionInterpreter rowExpressionInterpreterService;

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
    public RowExpression optimize(RowExpression expression, Level level, ConnectorSession session)
    {
        return toRowExpression(
                expression.getSourceLocation(),
                optimize(expression, level, session, NO_RESOLUTION),
                expression.getType());
    }

    @Override
    public Object optimize(RowExpression expression, Level level, ConnectorSession session, Function<VariableReferenceExpression, Object> variableResolver)
    {
        // Collect expressions to optimize
        CollectingVisitor collectingVisitor = new CollectingVisitor(functionMetadataManager, level);
        expression.accept(collectingVisitor, variableResolver);
        List<RowExpression> expressionsToOptimize = collectingVisitor.getExpressionsToOptimize();
        Map<RowExpression, RowExpression> expressions = expressionsToOptimize.stream()
                .collect(toMap(
                        Function.identity(),
                        rowExpression -> toRowExpression(
                                rowExpression.getSourceLocation(),
                                rowExpression.accept(new ReplacingVisitor(variableResolver), null),
                                rowExpression.getType()),
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
        Map<RowExpression, Object> replacements = new HashMap<>();
        if (!expressions.isEmpty()) {
            replacements.putAll(rowExpressionInterpreterService.optimizeBatch(session, expressions, level));
        }

        // Add back in the constants
        replacements.putAll(constants);

        // Replace all the expressions in the original expression with the optimized expressions
        return expression.accept(new ReplacingVisitor(replacements), null);
    }

    private static class CollectingVisitor
            implements RowExpressionVisitor<Void, Object>
    {
        private final FunctionMetadataManager functionMetadataManager;
        private final Level optimizationLevel;
        private final Set<RowExpression> expressionsToOptimize = newSetFromMap(new IdentityHashMap<>());

        public CollectingVisitor(FunctionMetadataManager functionMetadataManager, Level optimizationLevel)
        {
            this.functionMetadataManager = requireNonNull(functionMetadataManager, "functionMetadataManager is null");
            this.optimizationLevel = requireNonNull(optimizationLevel, "optimizationLevel is null");
        }

        @Override
        public Void visitExpression(RowExpression node, Object context)
        {
            return null;
        }

        @Override
        public Void visitConstant(ConstantExpression node, Object context)
        {
            addRowExpressionToOptimize(node);
            return null;
        }

        @Override
        public Void visitVariableReference(VariableReferenceExpression node, Object context)
        {
            Object value = ((Function<VariableReferenceExpression, Object>) context).apply(node);
            if (value == null || value instanceof RowExpression) {
                return null;
            }
            addRowExpressionToOptimize(node);
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
            boolean allConstantFoldable = node.getArguments().stream()
                    .peek(argument -> argument.accept(this, context))
                    .allMatch(this::canBeOptimized);
            if (canBeEvaluated && allConstantFoldable) {
                addRowExpressionToOptimize(node);
            }
            return null;
        }

        @Override
        public Void visitSpecialForm(SpecialFormExpression node, Object context)
        {
            // Most special form expressions short circuit, meaning that they potentially don't evaluate all arguments.  For example, the AND expression
            // will stop evaluating arguments as soon as it finds a false argument.  Because a sub-expression could be simplified into a constant, and this
            // constant could cause the expression to short circuit, if there is at least one argument which is optimizable, then the entire expression should
            // be sent to the sidecar to be optimized.
            boolean anyArgumentsOptimizable = node.getArguments().stream()
                    .peek(argument -> argument.accept(this, context))
                    .anyMatch(this::canBeOptimized);

            // If all arguments are constant foldable, then the whole expression is constant foldable
            if (anyArgumentsOptimizable) {
                addRowExpressionToOptimize(node);
            }
            return null;
        }

        @Override
        public Void visitLambda(LambdaDefinitionExpression node, Object context)
        {
            node.getBody().accept(this, (Function<VariableReferenceExpression, Object>) variable -> variable);
            if (canBeOptimized(node.getBody())) {
                addRowExpressionToOptimize(node.getBody());
            }
            return null;
        }

        public boolean canBeOptimized(RowExpression rowExpression)
        {
            return expressionsToOptimize.contains(rowExpression);
        }

        private void removeChildren(RowExpression resolvedRowExpression)
        {
            for (RowExpression rowExpression : resolvedRowExpression.getChildren()) {
                expressionsToOptimize.remove(rowExpression);
            }
        }

        private void addRowExpressionToOptimize(RowExpression original)
        {
            requireNonNull(original, "original is null");
            removeChildren(original);
            expressionsToOptimize.add(original);
        }

        public List<RowExpression> getExpressionsToOptimize()
        {
            return ImmutableList.copyOf(expressionsToOptimize);
        }
    }

    private static class ReplacingVisitor
            implements RowExpressionVisitor<Object, Void>
    {
        private final Function<RowExpression, Object> resolver;

        public ReplacingVisitor(Map<RowExpression, Object> replacements)
        {
            requireNonNull(replacements, "replacements is null");
            this.resolver = i -> replacements.getOrDefault(i, i);
        }

        public ReplacingVisitor(Function<VariableReferenceExpression, Object> variableResolver)
        {
            requireNonNull(variableResolver, "variableResolver is null");
            this.resolver = i -> i instanceof VariableReferenceExpression ? variableResolver.apply((VariableReferenceExpression) i) : i;
        }

        private boolean canBeReplaced(RowExpression rowExpression)
        {
            return resolver.apply(rowExpression) != rowExpression;
        }

        @Override
        public Object visitExpression(RowExpression originalExpression, Void context)
        {
            return resolver.apply(originalExpression);
        }

        @Override
        public Object visitLambda(LambdaDefinitionExpression lambda, Void context)
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
        public Object visitCall(CallExpression call, Void context)
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
        public Object visitSpecialForm(SpecialFormExpression specialForm, Void context)
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

        return new ConstantExpression(sourceLocation, object, type);
    }
}
