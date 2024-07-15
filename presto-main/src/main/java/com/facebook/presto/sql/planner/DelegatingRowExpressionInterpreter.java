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

import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.IntermediateFormExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.sql.planner.RowExpressionInterpreterService;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.common.type.StandardTypes.ARRAY;
import static com.facebook.presto.common.type.StandardTypes.MAP;
import static com.facebook.presto.common.type.StandardTypes.ROW;
import static com.facebook.presto.spi.relation.ExpressionOptimizer.Level;
import static com.facebook.presto.spi.relation.ExpressionOptimizer.Level.EVALUATED;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.SWITCH;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.WHEN;
import static com.facebook.presto.sql.planner.LiteralEncoder.toRowExpression;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

public class DelegatingRowExpressionInterpreter
{
    private final RowExpression expression;
    private final Level optimizationLevel;

    private final ConnectorSession session;

    private final CollectingVisitor collectingVisitor;
    private final ReplacingVisitor replacingVisitor;
    private final RowExpressionInterpreterService rowExpressionInterpreterService;

    public static Object evaluateConstantRowExpression(RowExpression expression, Metadata metadata, ConnectorSession session, RowExpressionInterpreterService rowExpressionInterpreterService)
    {
        // evaluate the expression
        Object result = new DelegatingRowExpressionInterpreter(expression, metadata, session, EVALUATED, rowExpressionInterpreterService).evaluate();
        verify(!(result instanceof RowExpression), "RowExpression interpreter returned an unresolved expression");
        return result;
    }

    public static DelegatingRowExpressionInterpreter rowExpressionInterpreter(RowExpression expression, Metadata metadata, ConnectorSession session, RowExpressionInterpreterService rowExpressionInterpreterService)
    {
        return new DelegatingRowExpressionInterpreter(expression, metadata, session, EVALUATED, rowExpressionInterpreterService);
    }

    public DelegatingRowExpressionInterpreter(RowExpression expression, Metadata metadata, ConnectorSession session, Level optimizationLevel, RowExpressionInterpreterService rowExpressionInterpreterService)
    {
        this.expression = requireNonNull(expression, "expression is null");
        this.session = requireNonNull(session, "session is null");
        this.optimizationLevel = requireNonNull(optimizationLevel, "optimizationLevel is null");
        requireNonNull(metadata, "metadata is null");
        FunctionResolution resolution = new FunctionResolution(metadata.getFunctionAndTypeManager().getFunctionAndTypeResolver());
        this.collectingVisitor = new CollectingVisitor(metadata.getFunctionAndTypeManager(), resolution, rowExpressionInterpreterService, optimizationLevel);
        this.replacingVisitor = new ReplacingVisitor();
        this.rowExpressionInterpreterService = requireNonNull(rowExpressionInterpreterService, "rowExpressionInterpreterService is null");
    }

    public Type getType()
    {
        return expression.getType();
    }

    public Object evaluate()
    {
        checkState(optimizationLevel.ordinal() >= EVALUATED.ordinal(), "evaluate() not allowed for optimizer");
        return expression.accept(collectingVisitor, null);
    }

    public Object optimize()
    {
        checkState(optimizationLevel.ordinal() < EVALUATED.ordinal(), "optimize() not allowed for interpreter");
        return optimize(null);
    }

    /**
     * Replace symbol with constants
     */
    public Object optimize(VariableResolver inputs)
    {
        checkState(optimizationLevel.ordinal() <= EVALUATED.ordinal(), "optimize(SymbolResolver) not allowed for interpreter");
        ResolvedRowExpression resolvedExpression = expression.accept(collectingVisitor, inputs);
        collectingVisitor.addRowExpressionToOptimize(resolvedExpression);
        Map<RowExpression, RowExpression> expressions = collectingVisitor.getExpressionsToOptimize();
        if (!expressions.isEmpty()) {
            Map<RowExpression, Object> replacements = rowExpressionInterpreterService.optimizeBatch(session, expressions, optimizationLevel);
            return expression.accept(replacingVisitor, new ReplacingState(replacements));
        }
        return expression;
    }

    private static class ReplacingState
    {
        private final Map<RowExpression, Object> replacements;

        public ReplacingState(Map<RowExpression, Object> replacements)
        {
            this.replacements = requireNonNull(replacements, "replacements is null");
        }

        public Map<RowExpression, Object> getReplacements()
        {
            return replacements;
        }
    }

    private static final boolean LEAF = true;
    private static final boolean NON_LEAF = false;
    private static final boolean CAN_BE_OPTIMIZED = true;
    private static final boolean CANNOT_BE_OPTIMIZED = false;

    private static class ResolvedRowExpression
    {
        private final boolean canBeOptimized;
        private final boolean anyChildrenCanBeOptimized;
        private final RowExpression originalExpression;
        private final RowExpression resolvedExpression;
        private final Set<ResolvedRowExpression> children;

        private ResolvedRowExpression(boolean canBeOptimized, RowExpression originalExpression, RowExpression resolvedExpression, Set<ResolvedRowExpression> children)
        {
            this.canBeOptimized = canBeOptimized;
            this.anyChildrenCanBeOptimized = canBeOptimized || children.stream().anyMatch(ResolvedRowExpression::anyChildrenCanBeOptimized);
            this.originalExpression = requireNonNull(originalExpression, "originalExpression is null");
            this.resolvedExpression = requireNonNull(resolvedExpression, "resolvedExpression is null");
            this.children = children instanceof HashSet ? children : new HashSet<>(children);
        }

        public ResolvedRowExpression(boolean canBeOptimized, RowExpression originalExpression, RowExpression resolvedExpression, ResolvedRowExpression... children)
        {
            this(canBeOptimized, originalExpression, resolvedExpression, new HashSet<>(Arrays.asList(children)));
        }

        public ResolvedRowExpression(boolean canBeOptimized, RowExpression originalExpression, List<ResolvedRowExpression> children)
        {
            this(
                    canBeOptimized,
                    originalExpression,
                    originalExpression.accept(new ResolvingVisitor(children), null),
                    new HashSet<>(children));
        }

        public ResolvedRowExpression(boolean canBeOptimized, RowExpression originalExpression, ResolvedRowExpression... children)
        {
            this(canBeOptimized, originalExpression, Arrays.asList(children));
        }

        public boolean canBeOptimized()
        {
            return canBeOptimized;
        }

        public boolean anyChildrenCanBeOptimized()
        {
            return anyChildrenCanBeOptimized;
        }

        public RowExpression getOriginalExpression()
        {
            return originalExpression;
        }

        public RowExpression getResolvedExpression()
        {
            return resolvedExpression;
        }

        public Set<ResolvedRowExpression> getChildren()
        {
            return children;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ResolvedRowExpression that = (ResolvedRowExpression) o;
            return canBeOptimized == that.canBeOptimized && anyChildrenCanBeOptimized == that.anyChildrenCanBeOptimized && Objects.equals(resolvedExpression, that.resolvedExpression) && Objects.equals(children, that.children);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(canBeOptimized, anyChildrenCanBeOptimized, resolvedExpression, children);
        }

        @Override
        public String toString()
        {
            return "ResolvedRowExpression{" +
                    ", canBeOptimized=" + canBeOptimized +
                    ", anyChildrenCanBeOptimized=" + anyChildrenCanBeOptimized +
                    ", originalExpression=" + originalExpression +
                    ", resolvedExpression=" + resolvedExpression +
                    ", children=" + children +
                    '}';
        }
    }

    private static class ResolvingVisitor
            implements RowExpressionVisitor<RowExpression, Void>
    {
        private final List<ResolvedRowExpression> resolvedChildren;

        public ResolvingVisitor(List<ResolvedRowExpression> resolvedChildren)
        {
            this.resolvedChildren = requireNonNull(resolvedChildren, "resolvedChildren is null");
        }

        @Override
        public RowExpression visitExpression(RowExpression node, Void context)
        {
            return node;
        }

        @Override
        public RowExpression visitCall(CallExpression node, Void context)
        {
            return new CallExpression(
                    node.getSourceLocation(),
                    node.getDisplayName(),
                    node.getFunctionHandle(),
                    node.getType(),
                    resolvedChildren.stream().map(ResolvedRowExpression::getResolvedExpression).collect(toImmutableList()));
        }

        @Override
        public RowExpression visitSpecialForm(SpecialFormExpression node, Void context)
        {
            return new SpecialFormExpression(
                    node.getSourceLocation(),
                    node.getForm(),
                    node.getType(),
                    resolvedChildren.stream().map(ResolvedRowExpression::getResolvedExpression).collect(toImmutableList()));
        }

        @Override
        public RowExpression visitLambda(LambdaDefinitionExpression node, Void context)
        {
            return new LambdaDefinitionExpression(
                    node.getSourceLocation(),
                    node.getArgumentTypes(),
                    node.getArguments(),
                    resolvedChildren.get(0).getResolvedExpression());
        }
    }

    private static class CollectingVisitor
            implements RowExpressionVisitor<ResolvedRowExpression, Object>
    {
        private final FunctionAndTypeManager functionAndTypeManager;
        private final FunctionResolution resolution;
        private final RowExpressionInterpreterService rowExpressionInterpreterService;
        private final Level optimizationLevel;

        public CollectingVisitor(FunctionAndTypeManager functionAndTypeManager, FunctionResolution resolution, RowExpressionInterpreterService rowExpressionInterpreterService, Level optimizationLevel)
        {
            this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
            this.resolution = requireNonNull(resolution, "resolution is null");
            this.rowExpressionInterpreterService = requireNonNull(rowExpressionInterpreterService, "rowExpressionInterpreterService is null");
            this.optimizationLevel = requireNonNull(optimizationLevel, "optimizationLevel is null");
        }
        private final Set<ResolvedRowExpression> expressionsToOptimize = new HashSet<>();

        @Override
        public ResolvedRowExpression visitInputReference(InputReferenceExpression node, Object context)
        {
            return new ResolvedRowExpression(CANNOT_BE_OPTIMIZED, node);
        }

        @Override
        public ResolvedRowExpression visitConstant(ConstantExpression node, Object context)
        {
            return new ResolvedRowExpression(CAN_BE_OPTIMIZED, node);
        }

        @Override
        public ResolvedRowExpression visitVariableReference(VariableReferenceExpression node, Object context)
        {
            if (context instanceof VariableResolver) {
                Object value = ((VariableResolver) context).getValue(node);
                if (value != null) {
                    if (value instanceof RowExpression) {
                        return new ResolvedRowExpression(CANNOT_BE_OPTIMIZED, (RowExpression) value);
                    }
                    return new ResolvedRowExpression(CAN_BE_OPTIMIZED, node, new ConstantExpression(node.getSourceLocation(), value, node.getType()));
                }
            }
            return new ResolvedRowExpression(CANNOT_BE_OPTIMIZED, node);
        }

        @Override
        public ResolvedRowExpression visitCall(CallExpression node, Object context)
        {
            List<RowExpression> arguments = node.getArguments();
            List<ResolvedRowExpression> resolvedArguments = new ArrayList<>();
            for (RowExpression argument : arguments) {
                ResolvedRowExpression returned = argument.accept(this, context);
                resolvedArguments.add(returned);
            }

            FunctionMetadata functionMetadata = functionAndTypeManager.getFunctionMetadata(node.getFunctionHandle());
            if (node.getArguments().isEmpty()) {
                return new ResolvedRowExpression(functionMetadata.isDeterministic() ? CAN_BE_OPTIMIZED : CANNOT_BE_OPTIMIZED, node);
            }

            boolean anyConstantFoldable = false;
            boolean allConstantFoldable = true;

            for (ResolvedRowExpression returned : resolvedArguments) {
                boolean constantFoldable = returned.canBeOptimized();
                anyConstantFoldable = anyConstantFoldable || constantFoldable;
                allConstantFoldable = allConstantFoldable && constantFoldable;
            }

            FunctionHandle functionHandle = node.getFunctionHandle();

            if (resolution.isCastFunction(functionHandle)) {
                // If the function is a cast function, we can optimize it if the argument is constant foldable
                // e.g. CAST(1 AS BIGINT) is 1
                ResolvedRowExpression resolved = node.getArguments().get(0).accept(this, context);
                // If the interpreter supports JSON cast optimization, mark it as constant foldable
                if (rowExpressionInterpreterService.supportsJsonToMapCastOptimization()) {
                    TypeSignature returnType = functionAndTypeManager.getFunctionMetadata(node.getFunctionHandle()).getReturnType();
                    if (returnType.getBase().equals(MAP) || returnType.getBase().equals(ARRAY) || returnType.getBase().equals(ROW)) {
                        ResolvedRowExpression resolvedCall = new ResolvedRowExpression(
                                CAN_BE_OPTIMIZED,
                                node,
                                resolved);
                        addRowExpressionToOptimize(resolvedCall);
                        return resolvedCall;
                    }
                }
                boolean canBeOptimized = resolved.canBeOptimized() || node.getArguments().get(0).getType().equals(node.getType());
                ResolvedRowExpression rowExpression = new ResolvedRowExpression(
                        // If the destination type and the source type are the same, the cast is a no-op, mark it as constant foldable
                        canBeOptimized,
                        node,
                        resolved);
                addRowExpressionToOptimize(rowExpression);
                return rowExpression;
            }

            boolean canBeOptimized = anyConstantFoldable && optimizationLevel.ordinal() < EVALUATED.ordinal() && functionMetadata.isDeterministic();
            ResolvedRowExpression resolvedRowExpression = new ResolvedRowExpression(
                    canBeOptimized,
                    // If this node can be optimized, then the children shouldn't be
                    node,
                    resolvedArguments);
            addRowExpressionToOptimize(resolvedRowExpression);
            return resolvedRowExpression;
        }

        @Override
        public ResolvedRowExpression visitSpecialForm(SpecialFormExpression node, Object context)
        {
            if (node.getForm() == SWITCH) {
                return handleSwitchExpression(node, context);
            }

            ImmutableList.Builder<ResolvedRowExpression> resolvedArgumentsBuilder = ImmutableList.builder();
            boolean anyArgumentsConstantFoldable = false;
            boolean allArgumentsConstantFoldable = true;

            for (RowExpression argument : node.getArguments()) {
                ResolvedRowExpression returned = argument.accept(this, context);
                resolvedArgumentsBuilder.add(returned);

                boolean canBeOptimized = returned.canBeOptimized();
                anyArgumentsConstantFoldable = anyArgumentsConstantFoldable || canBeOptimized;
                allArgumentsConstantFoldable = allArgumentsConstantFoldable && canBeOptimized;
            }
            List<ResolvedRowExpression> resolvedArguments = resolvedArgumentsBuilder.build();

            switch (node.getForm()) {
                case IF: {
                    ResolvedRowExpression returned = node.getArguments().get(0).accept(this, context);
                    // If the first argument is constant foldable, the whole expression is constant foldable
                    boolean canBeOptimized = returned.canBeOptimized();
                    ResolvedRowExpression rowExpression = new ResolvedRowExpression(
                            canBeOptimized,
                            node,
                            resolvedArguments);
                    addRowExpressionToOptimize(rowExpression);
                    return rowExpression;
                }
                case COALESCE: {
                    ImmutableSet.Builder<ResolvedRowExpression> builder = ImmutableSet.builder();
                    // Check if there's any duplicate arguments, these can be de-duplicated
                    for (ResolvedRowExpression argument : resolvedArguments) {
                        ResolvedRowExpression returned = argument.getResolvedExpression().accept(this, context);
                        // The duplicate argument must either be a leaf (variable reference) or constant foldable
                        if (returned.canBeOptimized()) {
                            builder.add(argument);
                        }
                    }
                    // If there were any duplicates, or if there's no arguments (cancel out), or if there's only one argument (just return it),
                    // then it's also constant foldable
                    boolean canBeOptimized = builder.build().size() <= resolvedArguments.size() || resolvedArguments.size() <= 1;
                    ResolvedRowExpression rowExpression = new ResolvedRowExpression(
                            canBeOptimized,
                            node,
                            resolvedArguments);
                    addRowExpressionToOptimize(rowExpression);
                    return rowExpression;
                }
                default:
                    ResolvedRowExpression rowExpression = new ResolvedRowExpression(
                            anyArgumentsConstantFoldable,
                            node,
                            resolvedArguments);
                    addRowExpressionToOptimize(rowExpression);
                    return rowExpression;
            }
        }

        /**
         * Switch statements require special handling, because when statements require special handling ({@link RowExpressionInterpreter} can't handle them).
         * This method will resolve the expression and all when clauses, and if any part of the switch expression is constant foldable, the entire switch
         * expression will be sent to the delegated expression optimizer.
         */
        private ResolvedRowExpression handleSwitchExpression(SpecialFormExpression node, Object context)
        {
            // First argument is the expression, follow by N when clauses, and an optional else clause
            RowExpression expression = node.getArguments().get(0);
            Optional<RowExpression> elseClause = Optional.empty();

            // Collect all when clauses
            List<RowExpression> whenClauses = buildWhenClauses(node);
            // Determine if the final clause is an else clause or a when clause
            RowExpression finalClause = node.getArguments().get(node.getArguments().size() - 1);
            if (finalClause instanceof SpecialFormExpression && ((SpecialFormExpression) finalClause).getForm() == WHEN) {
                whenClauses = ImmutableList.<RowExpression>builder().addAll(whenClauses).add(finalClause).build();
            }
            else {
                elseClause = Optional.of(finalClause);
            }

            boolean canBeOptimized;
            ResolvedRowExpression resolvedExpression;
            List<ResolvedRowExpression> resolvedWhenClauses = new ArrayList<>();
            Optional<ResolvedRowExpression> resolvedElseClause = Optional.empty();

            // First determine if the expression is constant foldable
            resolvedExpression = expression.accept(this, context);
            canBeOptimized = resolvedExpression.canBeOptimized();
            addRowExpressionToOptimize(resolvedExpression);

            // Next determine if all when clauses are constant foldable
            for (RowExpression whenClause : whenClauses) {
                SpecialFormExpression whenClauseSpecialForm = (SpecialFormExpression) whenClause;
                List<RowExpression> whenClauseArguments = whenClauseSpecialForm.getArguments();
                checkArgument(whenClauseArguments.size() == 2, "WHEN clause must have 2 arguments, got [%s]", whenClauseArguments);
                ResolvedRowExpression resolvedArgument = whenClauseArguments.get(0).accept(this, context);
                canBeOptimized = canBeOptimized || resolvedArgument.canBeOptimized();

                ResolvedRowExpression thenClause = whenClauseArguments.get(1).accept(this, context);

                // Create a rewritten when clause that's resolved all variables
                ResolvedRowExpression resolvedWhenClause = new ResolvedRowExpression(
                        resolvedArgument.canBeOptimized() || thenClause.canBeOptimized(),
                        whenClauseSpecialForm,
                        resolvedArgument,
                        thenClause);
                addRowExpressionToOptimize(resolvedWhenClause);
                resolvedWhenClauses.add(resolvedWhenClause);
            }

            // Resolve the else clause if it exists
            if (elseClause.isPresent()) {
                ResolvedRowExpression elseExpression = elseClause.get().accept(this, context);
                resolvedElseClause = Optional.of(elseExpression);
                canBeOptimized = canBeOptimized || elseExpression.canBeOptimized();
                addRowExpressionToOptimize(elseExpression);
            }

            ImmutableList.Builder<ResolvedRowExpression> resolvedArguments = ImmutableList.<ResolvedRowExpression>builder().add(resolvedExpression).addAll(resolvedWhenClauses);
            resolvedElseClause.ifPresent(resolvedArguments::add);

            ResolvedRowExpression rowExpression = new ResolvedRowExpression(
                    canBeOptimized,
                    node,
                    resolvedArguments.build());
            // If any part of the entire switch expression is constant foldable, send the whole thing over
            addRowExpressionToOptimize(rowExpression);
            // Otherwise it's not constant foldable.
            return rowExpression;
        }

        private List<RowExpression> buildWhenClauses(SpecialFormExpression node)
        {
            ImmutableList.Builder<RowExpression> whenClausesBuilder = ImmutableList.builder();
            for (int i = 1; i < node.getArguments().size() - 1; i++) {
                whenClausesBuilder.add(node.getArguments().get(i));
            }
            return whenClausesBuilder.build();
        }

        @Override
        public ResolvedRowExpression visitLambda(LambdaDefinitionExpression node, Object context)
        {
            ResolvedRowExpression resolvedBody = node.getBody().accept(this, null);
            return new ResolvedRowExpression(
                    resolvedBody.canBeOptimized(),
                    node,
                    resolvedBody);
        }

        @Override
        public ResolvedRowExpression visitIntermediateFormExpression(IntermediateFormExpression intermediateFormExpression, Object context)
        {
            return new ResolvedRowExpression(CANNOT_BE_OPTIMIZED, intermediateFormExpression);
        }

        private void removeChildren(ResolvedRowExpression resolvedRowExpression)
        {
            resolvedRowExpression.getChildren().forEach(this::removeChildren);
            resolvedRowExpression.getChildren().clear();
            expressionsToOptimize.remove(resolvedRowExpression);
        }

        private void addRowExpressionToOptimize(ResolvedRowExpression resolvedRowExpression)
        {
            if (resolvedRowExpression.canBeOptimized()) {
                removeChildren(resolvedRowExpression);
                expressionsToOptimize.add(resolvedRowExpression);
            }
        }

        public Map<RowExpression, RowExpression> getExpressionsToOptimize()
        {
            return expressionsToOptimize.stream().collect(toImmutableMap(
                    ResolvedRowExpression::getOriginalExpression,
                    ResolvedRowExpression::getResolvedExpression));
        }
    }

    private class ReplacingVisitor
            implements RowExpressionVisitor<Object, ReplacingState>
    {
        @Override
        public Object visitExpression(RowExpression originalExpression, ReplacingState context)
        {
            return context.getReplacements().getOrDefault(originalExpression, originalExpression);
        }

        @Override
        public Object visitLambda(LambdaDefinitionExpression lambda, ReplacingState context)
        {
            if (context.getReplacements().containsKey(lambda.getBody())) {
                return new LambdaDefinitionExpression(
                        lambda.getSourceLocation(),
                        lambda.getArgumentTypes(),
                        lambda.getArguments(),
                        toRowExpression(context.getReplacements().get(lambda.getBody()), lambda.getBody().getType()));
            }
            return lambda;
        }

        @Override
        public Object visitCall(CallExpression call, ReplacingState context)
        {
            if (context.getReplacements().containsKey(call)) {
                return context.getReplacements().get(call);
            }
            List<RowExpression> updatedArguments = call.getArguments().stream()
                    .map(argument -> toRowExpression(argument.accept(this, context), argument.getType()))
                    .collect(toImmutableList());
            return new CallExpression(call.getSourceLocation(), call.getDisplayName(), call.getFunctionHandle(), call.getType(), updatedArguments);
        }

        @Override
        public Object visitSpecialForm(SpecialFormExpression specialForm, ReplacingState context)
        {
            if (context.getReplacements().containsKey(specialForm)) {
                return context.getReplacements().get(specialForm);
            }
            List<RowExpression> updatedArguments = specialForm.getArguments().stream()
                    .map(argument -> toRowExpression(argument.accept(this, context), argument.getType()))
                    .collect(toImmutableList());
            return new SpecialFormExpression(specialForm.getSourceLocation(), specialForm.getForm(), specialForm.getType(), updatedArguments);
        }
    }
}
