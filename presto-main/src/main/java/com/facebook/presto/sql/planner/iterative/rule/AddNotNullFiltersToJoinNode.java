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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.expressions.DefaultRowExpressionTraversalVisitor;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.IntermediateFormExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.analyzer.FeaturesConfig.JoinNotNullInferenceStrategy;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.getNotNullInferenceStrategy;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.expressions.LogicalRowExpressions.and;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.AND;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IS_NULL;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinNotNullInferenceStrategy.NONE;
import static com.facebook.presto.sql.planner.plan.Patterns.join;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Sets.intersection;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Stream.concat;

public class AddNotNullFiltersToJoinNode
        implements Rule<JoinNode>
{
    private static final Pattern<JoinNode> PATTERN = join();
    private final FunctionAndTypeManager functionAndTypeManager;
    private final Logger logger = Logger.get(AddNotNullFiltersToJoinNode.class);
    private final FunctionResolution functionResolution;

    public AddNotNullFiltersToJoinNode(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
        this.functionResolution = new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver());
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return getNotNullInferenceStrategy(session) != NONE;
    }

    @Override
    public Pattern<JoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(JoinNode joinNode, Captures captures, Context context)
    {
        Collection<VariableReferenceExpression> inferredNotNullVariables;
        JoinNotNullInferenceStrategy notNullInferenceStrategy = getNotNullInferenceStrategy(context.getSession());

        switch (joinNode.getType()) {
            case LEFT:
                // NOT NULL can be inferred for the right-side variables
                inferredNotNullVariables = extractNotNullVariables(joinNode.getCriteria(), joinNode.getFilter(), joinNode.getRight().getOutputVariables(), notNullInferenceStrategy);
                break;
            case RIGHT:
                // NOT NULL can be inferred for the left-side variables
                inferredNotNullVariables = extractNotNullVariables(joinNode.getCriteria(), joinNode.getFilter(), joinNode.getLeft().getOutputVariables(), notNullInferenceStrategy);
                break;
            case INNER:
                // NOT NULL can be inferred for variables from both sides of the join
                inferredNotNullVariables = extractNotNullVariables(joinNode.getCriteria(), joinNode.getFilter(), concat(joinNode.getLeft().getOutputVariables().stream(),
                        joinNode.getRight().getOutputVariables().stream()).collect(toImmutableList()), notNullInferenceStrategy);
                break;
            case FULL:
            default:
                // NOT NULL cannot be inferred
                return Result.empty();
        }

        if (inferredNotNullVariables.isEmpty()) {
            return Result.empty();
        }

        Set<VariableReferenceExpression> existingNotNullVariables = getExistingNotNullVariables(joinNode.getFilter());
        logger.debug("NotNull filters :: Existing : %s, Inferred :%s", existingNotNullVariables, inferredNotNullVariables);

        if (existingNotNullVariables.containsAll(inferredNotNullVariables)) {
            // No new NOT NULL variables were inferred
            return Result.empty();
        }

        RowExpression updatedJoinFilter = and(joinNode.getFilter().orElse(TRUE_CONSTANT), buildNotNullRowExpression(inferredNotNullVariables));
        return Result.ofPlanNode(
                new JoinNode(joinNode.getSourceLocation(),
                        context.getIdAllocator().getNextId(),
                        joinNode.getType(),
                        joinNode.getLeft(),
                        joinNode.getRight(),
                        joinNode.getCriteria(),
                        joinNode.getOutputVariables(),
                        Optional.ofNullable(updatedJoinFilter),
                        joinNode.getLeftHashVariable(),
                        joinNode.getRightHashVariable(),
                        joinNode.getDistributionType(),
                        joinNode.getDynamicFilters()));
    }

    private Collection<VariableReferenceExpression> extractNotNullVariables(List<JoinNode.EquiJoinClause> joinCriteria, Optional<RowExpression> joinFilter,
            List<VariableReferenceExpression> candidates, JoinNotNullInferenceStrategy notNullInferenceStrategy)
    {
        RowExpression combinedFilter = TRUE_CONSTANT;

        for (JoinNode.EquiJoinClause criteria : joinCriteria) {
            combinedFilter = and(combinedFilter, criteria.getLeft());
            combinedFilter = and(combinedFilter, criteria.getRight());
        }

        combinedFilter = and(combinedFilter, joinFilter.orElse(TRUE_CONSTANT));

        return intersection(ImmutableSet.copyOf(candidates), inferNotNullVariables(combinedFilter, notNullInferenceStrategy));
    }

    @VisibleForTesting
    Set<VariableReferenceExpression> getExistingNotNullVariables(Optional<RowExpression> joinFilter)
    {
        if (!joinFilter.isPresent()) {
            return ImmutableSet.of();
        }

        ImmutableSet.Builder<VariableReferenceExpression> builder = ImmutableSet.builder();

        DefaultRowExpressionTraversalVisitor<ImmutableSet.Builder<VariableReferenceExpression>> isNotNullExtractingVisitor =
                new DefaultRowExpressionTraversalVisitor<ImmutableSet.Builder<VariableReferenceExpression>>()
                {
                    @Override
                    public Void visitCall(CallExpression call, ImmutableSet.Builder<VariableReferenceExpression> context)
                    {
                        // Match a 'not(IS_NULL(VariableReferenceExpression))' call *exactly*
                        if (functionResolution.isNotFunction(call.getFunctionHandle()) &&
                                call.getArguments().size() == 1 &&
                                call.getArguments().get(0) instanceof SpecialFormExpression &&
                                ((SpecialFormExpression) call.getArguments().get(0)).getForm() == IS_NULL &&
                                ((SpecialFormExpression) call.getArguments().get(0)).getArguments().size() == 1 &&
                                ((SpecialFormExpression) call.getArguments().get(0)).getArguments().get(0) instanceof VariableReferenceExpression) {
                            context.add((VariableReferenceExpression) ((SpecialFormExpression) call.getArguments().get(0)).getArguments().get(0));
                        }
                        return null;
                    }

                    @Override
                    public Void visitIntermediateFormExpression(IntermediateFormExpression expression, ImmutableSet.Builder<VariableReferenceExpression> context)
                    {
                        return null;
                    }

                    @Override
                    public Void visitSpecialForm(SpecialFormExpression specialForm, ImmutableSet.Builder<VariableReferenceExpression> context)
                    {
                        if (specialForm.getForm() == AND) {
                            return super.visitSpecialForm(specialForm, context);
                        }
                        return null;
                    }
                };

        joinFilter.get().accept(isNotNullExtractingVisitor, builder);
        return builder.build();
    }

    private ImmutableSet<VariableReferenceExpression> inferNotNullVariables(RowExpression expression, JoinNotNullInferenceStrategy notNullInferenceStrategy)
    {
        ImmutableSet.Builder<VariableReferenceExpression> builder = ImmutableSet.builder();
        expression.accept(new ExtractInferredNotNullVariablesVisitor(functionAndTypeManager, notNullInferenceStrategy), builder);
        return builder.build();
    }

    private RowExpression buildNotNullRowExpression(Collection<VariableReferenceExpression> expressions)
    {
        List<CallExpression> isNotNullExpressions = expressions.stream().map(x -> new CallExpression(
                        x.getSourceLocation(),
                        "not",
                        functionResolution.notFunction(),
                        BOOLEAN,
                        singletonList(new SpecialFormExpression(x.getSourceLocation(), IS_NULL, BOOLEAN, x))))
                .collect(toImmutableList());

        return and(isNotNullExpressions);
    }

    @VisibleForTesting
    public static class ExtractInferredNotNullVariablesVisitor
            extends DefaultRowExpressionTraversalVisitor<ImmutableSet.Builder<VariableReferenceExpression>>
    {
        private final FunctionAndTypeManager functionAndTypeManager;
        private final JoinNotNullInferenceStrategy notNullInferenceStrategy;

        public ExtractInferredNotNullVariablesVisitor(FunctionAndTypeManager functionAndTypeManager, JoinNotNullInferenceStrategy notNullInferenceStrategy)
        {
            this.functionAndTypeManager = functionAndTypeManager;
            this.notNullInferenceStrategy = notNullInferenceStrategy;
        }

        @Override
        public Void visitCall(CallExpression call, ImmutableSet.Builder<VariableReferenceExpression> context)
        {
            final FunctionHandle functionHandle = call.getFunctionHandle();
            final FunctionMetadata functionMetadata = functionAndTypeManager.getFunctionMetadata(functionHandle);

            switch (notNullInferenceStrategy) {
                case INFER_FROM_STANDARD_OPERATORS:
                    if (!functionMetadata.getOperatorType().isPresent() || functionMetadata.getOperatorType().get().isCalledOnNullInput()) {
                        // We can't map this CallExpression to an OperatorType OR
                        // this OperatorType can be called on NULL inputs, so we can't make NOT NULL inferences on it's arguments
                        return null;
                    }
                    break;
                case USE_FUNCTION_METADATA:
                    if (functionMetadata.isCalledOnNullInput()) {
                        // Since this function can operate on NULL inputs and return a valid value, we can't make NOT NULL inference on it's arguments
                        return null;
                    }
                    break;
                default:
                    return null;
            }

            return super.visitCall(call, context);
        }

        @Override
        public Void visitSpecialForm(SpecialFormExpression specialForm, ImmutableSet.Builder<VariableReferenceExpression> context)
        {
            SpecialFormExpression.Form form = specialForm.getForm();
            if (form == AND) {
                // All arguments of an AND expression must be NOT NULL for the expression to be true
                // Hence, we can proceed with extracting candidates for NOT NULL inference on it's arguments
                return super.visitSpecialForm(specialForm, context);
            }
            // For all other SpecialForms e.g. OR, COALESCE, IS_NULL, CASE, DEREFERENCE we abstain from making NOT NULL inferences
            return null;
        }

        @Override
        public Void visitIntermediateFormExpression(IntermediateFormExpression expression, ImmutableSet.Builder<VariableReferenceExpression> context)
        {
            // TODO : For now, we are not traversing any IntermediateFormExpression's. For some cases, such as InSubqueryExpression
            // we may be able to do some null inference-ing
            return null;
        }

        @Override
        public Void visitVariableReference(VariableReferenceExpression variableReferenceExpression, ImmutableSet.Builder<VariableReferenceExpression> context)
        {
            context.add(variableReferenceExpression);
            return super.visitVariableReference(variableReferenceExpression, context);
        }
    }
}
