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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
import static java.util.Objects.requireNonNull;

public class DelegatingRowExpressionInterpreter
{
    private static final long MAX_SERIALIZABLE_OBJECT_SIZE = 1000;
    private final RowExpression expression;
    private final Level optimizationLevel;

    private final ConnectorSession session;

    private final CollectingVisitor collectingVisitor;
    private final ReplacingVisitor replacingVisitor;
    private final RowExpressionInterpreterService rowExpressionInterpreterService;
    private final FunctionAndTypeManager functionAndTypeManager;
    private final FunctionResolution resolution;

    public static Object evaluateConstantRowExpression(RowExpression expression, Metadata metadata, ConnectorSession session, RowExpressionInterpreterService doConstantFolding)
    {
        // evaluate the expression
        Object result = new DelegatingRowExpressionInterpreter(expression, metadata, session, EVALUATED, doConstantFolding).evaluate();
        verify(!(result instanceof RowExpression), "RowExpression interpreter returned an unresolved expression");
        return result;
    }

    public static DelegatingRowExpressionInterpreter rowExpressionInterpreter(RowExpression expression, Metadata metadata, ConnectorSession session, RowExpressionInterpreterService rowExpressionInterpreterService)
    {
        return new DelegatingRowExpressionInterpreter(expression, metadata, session, EVALUATED, rowExpressionInterpreterService);
    }

    public DelegatingRowExpressionInterpreter(RowExpression expression, Metadata metadata, ConnectorSession session, Level optimizationLevel, RowExpressionInterpreterService doConstantFolding)
    {
        this.expression = requireNonNull(expression, "expression is null");
        this.session = requireNonNull(session, "session is null");
        this.functionAndTypeManager = metadata.getFunctionAndTypeManager();
        this.optimizationLevel = optimizationLevel;

        this.collectingVisitor = new CollectingVisitor();
        this.replacingVisitor = new ReplacingVisitor();
        this.rowExpressionInterpreterService = doConstantFolding;
        this.resolution = new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver());
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
        CollectingState context = new CollectingState(inputs);
        RowExpression resolvedExpression = expression.accept(collectingVisitor, context);
        Map<RowExpression, RowExpression> expressions = context.getExpressionsToOptimize();
        if (context.isConstantFoldable()) {
            expressions.put(expression, resolvedExpression);
        }
        if (!expressions.isEmpty()) {
            Map<RowExpression, Object> replacements = rowExpressionInterpreterService.optimizeBatch(session, expressions, optimizationLevel);
            return expression.accept(replacingVisitor, new ReplacingState(replacements));
        }
        return expression;
    }

    private static class CollectingState
    {
        private final VariableResolver variableResolver;
        private boolean isConstantFoldable;
        private boolean isLeaf;

        private final Map<RowExpression, RowExpression> expressionsToOptimize = new HashMap<>();

        private CollectingState(VariableResolver variableResolver)
        {
            this.variableResolver = variableResolver;
        }

        public boolean isConstantFoldable()
        {
            return isConstantFoldable;
        }

        public void setConstantFoldable(boolean constantFoldable)
        {
            isConstantFoldable = constantFoldable;
        }

        public VariableResolver getVariableResolver()
        {
            return variableResolver;
        }

        public boolean isLeaf()
        {
            return isLeaf;
        }

        public void setLeaf(boolean leaf)
        {
            isLeaf = leaf;
        }

        public void addRowExpressionToOptimize(RowExpression originalRowExpression, RowExpression resolvedRowExpression)
        {
            expressionsToOptimize.put(originalRowExpression, resolvedRowExpression);
        }

        public Map<RowExpression, RowExpression> getExpressionsToOptimize()
        {
            return expressionsToOptimize;
        }
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

    private class CollectingVisitor
            implements RowExpressionVisitor<RowExpression, CollectingState>
    {
        @Override
        public RowExpression visitInputReference(InputReferenceExpression node, CollectingState context)
        {
            context.setConstantFoldable(false);
            context.setLeaf(true);
            return node;
        }

        @Override
        public RowExpression visitConstant(ConstantExpression node, CollectingState context)
        {
            context.setConstantFoldable(true);
            context.setLeaf(true);
            return node;
        }

        @Override
        public RowExpression visitVariableReference(VariableReferenceExpression node, CollectingState context)
        {
            context.setLeaf(true);
            if (context.getVariableResolver() != null) {
                Object value = context.getVariableResolver().getValue(node);
                if (value != null) {
                    if (value instanceof RowExpression) {
                        return (RowExpression) value;
                    }
                    context.setConstantFoldable(true);
                    return new ConstantExpression(node.getSourceLocation(), value, node.getType());
                }
            }
            context.setConstantFoldable(false);
            return node;
        }

        @Override
        public RowExpression visitCall(CallExpression node, CollectingState context)
        {
            FunctionHandle functionHandle = node.getFunctionHandle();

            if (resolution.isCastFunction(functionHandle)) {
                // If the function is a cast function, we can optimize it if the argument is constant foldable
                // e.g. CAST(1 AS BIGINT) is 1
                RowExpression resolved = node.getArguments().get(0).accept(this, context);
                context.setLeaf(false);
                // If the interpreter supports JSON cast optimization, mark it as constant foldable
                if (rowExpressionInterpreterService.supportsJsonToMapCastOptimization()) {
                    TypeSignature returnType = functionAndTypeManager.getFunctionMetadata(node.getFunctionHandle()).getReturnType();
                    if (returnType.getBase().equals(MAP) || returnType.getBase().equals(ARRAY) || returnType.getBase().equals(ROW)) {
                        context.setConstantFoldable(true);
                        return new CallExpression(node.getSourceLocation(), node.getDisplayName(), node.getFunctionHandle(), node.getType(), ImmutableList.of(resolved));
                    }
                }
                // If the destination type and the source type are the same, the cast is a no-op, mark it as constant foldable
                if (!context.isConstantFoldable() && node.getArguments().get(0).getType().equals(node.getType())) {
                    context.setConstantFoldable(true);
                }
                return new CallExpression(node.getSourceLocation(), node.getDisplayName(), node.getFunctionHandle(), node.getType(), ImmutableList.of(resolved));
            }

            FunctionMetadata functionMetadata = functionAndTypeManager.getFunctionMetadata(node.getFunctionHandle());
            if (functionMetadata.isDeterministic() && node.getArguments().isEmpty()) {
                context.setConstantFoldable(true);
                context.setLeaf(true);
                return node;
            }

            List<RowExpression> arguments = node.getArguments();
            List<RowExpression> build = visitRowExpressionArguments(context, arguments);
            return new CallExpression(node.getSourceLocation(), node.getDisplayName(), node.getFunctionHandle(), node.getType(), build);
        }

        @Override
        public RowExpression visitSpecialForm(SpecialFormExpression node, CollectingState context)
        {
            if (node.getForm() == SWITCH) {
                return handleSwitchExpression(node, context);
            }

            List<RowExpression> resolvedExpressions = visitRowExpressionArguments(context, node.getArguments());
            context.setLeaf(false);
            switch (node.getForm()) {
                case OR:
                case AND: {
                    boolean constantFoldable = false;
                    // If any of the arguments are constant foldable, the entire expression is constant foldable
                    // e.g. false AND x is always false, true AND x is x, COALESCE(x, y) is x if x is not null, COALESCE(x, y) is y if x is null
                    for (RowExpression argument : node.getArguments()) {
                        argument.accept(this, context);
                        constantFoldable = constantFoldable || context.isConstantFoldable();
                    }
                    context.setConstantFoldable(constantFoldable);
                    context.setLeaf(false);
                    return new SpecialFormExpression(node.getSourceLocation(), node.getForm(), node.getType(), resolvedExpressions);
                }
                case COALESCE: {
                    boolean constantFoldable = context.isConstantFoldable();
                    ImmutableSet.Builder<RowExpression> builder = ImmutableSet.builder();
                    // Check if there's any duplicate arguments, these can be de-duplicated
                    for (RowExpression argument : resolvedExpressions) {
                        argument.accept(this, context);
                        // The duplicate argument must either be a leaf (variable reference) or constant foldable
                        if (context.isConstantFoldable()) {
                            builder.add(argument);
                        }
                    }
                    context.setConstantFoldable(constantFoldable);
                    // If there were any duplicates, or if there's no arguments (cancel out), or if there's only one argument (just return it),
                    // then it's also constant foldable
                    if (builder.build().size() <= resolvedExpressions.size() || resolvedExpressions.size() <= 1) {
                        context.setConstantFoldable(true);
                    }
                    return new SpecialFormExpression(node.getSourceLocation(), node.getForm(), node.getType(), resolvedExpressions);
                }
                case IF: {
                    node.getArguments().get(0).accept(this, context);
                    // If the first argument is constant foldable, the whole expression is constant foldable
                    return new SpecialFormExpression(node.getSourceLocation(), node.getForm(), node.getType(), resolvedExpressions);
                }
                default:
                    return new SpecialFormExpression(node.getSourceLocation(), node.getForm(), node.getType(), resolvedExpressions);
            }
        }

        /**
         * Switch statements require special handling, because when statements require special handling ({@link RowExpressionInterpreter} can't handle them).
         * This method will resolve the expression and all when clauses, and if any part of the switch expression is constant foldable, the entire switch
         * expression will be sent to the delegated expression optimizer.
         */
        private SpecialFormExpression handleSwitchExpression(SpecialFormExpression node, CollectingState context)
        {
            // First argument is the expression, follow by N when clauses, and an optional else clause
            RowExpression expression = node.getArguments().get(0);
            List<RowExpression> whenClauses;
            Optional<RowExpression> elseClause = Optional.empty();

            // Collect all when clauses
            ImmutableList.Builder<RowExpression> whenClausesBuilder = ImmutableList.builder();
            for (int i = 1; i < node.getArguments().size() - 1; i++) {
                whenClausesBuilder.add(node.getArguments().get(i));
            }
            // Determine if the final clause is an else clause or a when clause
            RowExpression finalClause = node.getArguments().get(node.getArguments().size() - 1);
            if (finalClause instanceof SpecialFormExpression && ((SpecialFormExpression) finalClause).getForm() == WHEN) {
                whenClausesBuilder.add(finalClause);
            }
            else {
                elseClause = Optional.of(finalClause);
            }
            whenClauses = whenClausesBuilder.build();

            boolean isConstantFoldable = false;
            RowExpression resolvedExpression;
            List<RowExpression> resolvedWhenClauses = new ArrayList<>();
            Optional<RowExpression> resolvedElseClause = Optional.empty();

            // First determine if the expression is constant foldable
            resolvedExpression = expression.accept(this, context);
            isConstantFoldable = context.isConstantFoldable();

            // Next determine if all when clauses are constant foldable
            for (RowExpression whenClause : whenClauses) {
                SpecialFormExpression whenClauseSpecialForm = (SpecialFormExpression) whenClause;
                List<RowExpression> whenClauseArguments = whenClauseSpecialForm.getArguments();
                checkArgument(whenClauseArguments.size() == 2, "WHEN clause must have 2 arguments, got [%s]", whenClauseArguments);
                RowExpression resolvedArgument = whenClauseArguments.get(0).accept(this, context);
                isConstantFoldable = isConstantFoldable || context.isConstantFoldable();

                // Create a rewritten when clause that's resolved all variables
                resolvedWhenClauses.add(
                        new SpecialFormExpression(
                                whenClauseSpecialForm.getSourceLocation(),
                                whenClauseSpecialForm.getForm(),
                                whenClauseSpecialForm.getType(),
                                ImmutableList.of(resolvedArgument, whenClauseArguments.get(1).accept(this, context))));
            }

            // Resolve the else clause if it exists
            if (elseClause.isPresent()) {
                resolvedElseClause = Optional.of(elseClause.get().accept(this, context));
                isConstantFoldable = isConstantFoldable || context.isConstantFoldable();
            }

            // If any part of the entire switch expression is constant foldable, send the whole thing over
            if (isConstantFoldable) {
                ImmutableList.Builder<RowExpression> resolvedArguments = ImmutableList.<RowExpression>builder().add(resolvedExpression).addAll(resolvedWhenClauses);
                resolvedElseClause.ifPresent(resolvedArguments::add);
                context.addRowExpressionToOptimize(
                        node,
                        new SpecialFormExpression(
                                node.getSourceLocation(),
                                node.getForm(),
                                node.getType(),
                                resolvedArguments.build()));
            }
            // Otherwise it's not constant foldable.
            context.setConstantFoldable(false);
            context.setLeaf(false);
            return node;
        }

        private List<RowExpression> visitRowExpressionArguments(CollectingState context, List<RowExpression> arguments)
        {
            requireNonNull(arguments, "arguments is null");
            if (arguments.isEmpty()) {
                return arguments;
            }

            Map<RowExpression, RowExpression> constantFoldableExpressions = new HashMap<>();
            List<RowExpression> resolvedArguments = new ArrayList<>();
            boolean anyConstantFoldable = false;
            boolean allConstantFoldable = true;

            for (RowExpression argument : arguments) {
                RowExpression resolved = argument.accept(this, context);
                resolvedArguments.add(resolved);

                boolean constantFoldable = context.isConstantFoldable();
                anyConstantFoldable = anyConstantFoldable || constantFoldable;
                allConstantFoldable = allConstantFoldable && constantFoldable;

                if (constantFoldable && !context.isLeaf()) {
                    constantFoldableExpressions.put(argument, resolved);
                }
            }

            context.setConstantFoldable(anyConstantFoldable);
            context.setLeaf(false);

            // if all arguments are constant foldable, then we should move up the tree to send a larger expression for evaluation
            if (anyConstantFoldable && !allConstantFoldable) {
                constantFoldableExpressions.forEach(context::addRowExpressionToOptimize);
            }
            return resolvedArguments;
        }

        @Override
        public RowExpression visitLambda(LambdaDefinitionExpression node, CollectingState context)
        {
            CollectingState lambdaContext = new CollectingState(null);
            node.getBody().accept(this, lambdaContext);
            lambdaContext.getExpressionsToOptimize().forEach(context::addRowExpressionToOptimize);
            context.setLeaf(false);
            return node;
        }

        @Override
        public RowExpression visitIntermediateFormExpression(IntermediateFormExpression intermediateFormExpression, CollectingState context)
        {
            context.setLeaf(false);
            context.setConstantFoldable(false);
            return null;
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
