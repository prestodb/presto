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

import com.facebook.presto.Session;
import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.constraints.NotNullConstraint;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.shouldPushAggregationThroughJoin;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.spi.plan.AggregationNode.singleGroupingSet;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.COALESCE;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.planner.optimizations.AggregationNodeUtils.count;
import static com.facebook.presto.sql.planner.optimizations.DistinctOutputQueryUtil.isDistinct;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.identityAssignments;
import static com.facebook.presto.sql.planner.plan.Patterns.aggregation;
import static com.facebook.presto.sql.planner.plan.Patterns.join;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * Pre-aggregates count(inner_column) below an outer join, then sums the per-key
 * counts above the join. Unlike PushAggregationThroughOuterJoin, this rule does
 * not require the outer side to be distinct: duplicate outer rows still
 * contribute duplicate pre-aggregated counts to the final sum.
 */
public class PreAggregateCountThroughOuterJoin
        implements Rule<AggregationNode>
{
    private static final Capture<JoinNode> JOIN = newCapture();
    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .with(source().matching(join().capturedAs(JOIN)));

    private final FunctionAndTypeManager functionAndTypeManager;
    private final FunctionResolution functionResolution;

    public PreAggregateCountThroughOuterJoin(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
        this.functionResolution = new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver());
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return shouldPushAggregationThroughJoin(session);
    }

    @Override
    public Result apply(AggregationNode aggregation, Captures captures, Context context)
    {
        JoinNode join = captures.get(JOIN);
        if (!isSupported(aggregation, join)) {
            return Result.empty();
        }

        JoinSides sides = new JoinSides(join);
        if (!ImmutableSet.copyOf(sides.getOuter().getOutputVariables()).containsAll(aggregation.getGroupingKeys())) {
            return Result.empty();
        }
        if (ImmutableSet.copyOf(aggregation.getGroupingKeys()).equals(ImmutableSet.copyOf(sides.getOuter().getOutputVariables()))
                && isDistinct(context.getLookup().resolve(sides.getOuter()), context.getLookup()::resolve)) {
            return Result.empty();
        }

        ImmutableMap.Builder<VariableReferenceExpression, AggregationNode.Aggregation> preAggregations = ImmutableMap.builder();
        ImmutableMap.Builder<VariableReferenceExpression, VariableReferenceExpression> originalToPreAggregation = ImmutableMap.builder();
        for (Map.Entry<VariableReferenceExpression, AggregationNode.Aggregation> entry : aggregation.getAggregations().entrySet()) {
            VariableReferenceExpression preAggregationVariable = context.getVariableAllocator().newVariable(entry.getKey().getSourceLocation(), "pre_count", entry.getKey().getType());
            preAggregations.put(preAggregationVariable, rewriteCountArgumentIfNonNull(entry.getValue(), sides.getInner(), context));
            originalToPreAggregation.put(entry.getKey(), preAggregationVariable);
        }

        AggregationNode innerAggregation = new AggregationNode(
                aggregation.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                sides.getInner(),
                preAggregations.build(),
                singleGroupingSet(sides.getInnerJoinKeys()),
                ImmutableList.of(),
                aggregation.getStep(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        JoinNode rewrittenJoin = new JoinNode(
                join.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                join.getType(),
                join.getType() == JoinType.LEFT ? join.getLeft() : innerAggregation,
                join.getType() == JoinType.LEFT ? innerAggregation : join.getRight(),
                join.getCriteria(),
                join.getType() == JoinType.LEFT ?
                        ImmutableList.<VariableReferenceExpression>builder()
                                .addAll(sides.getOuter().getOutputVariables())
                                .addAll(innerAggregation.getAggregations().keySet())
                                .build() :
                        ImmutableList.<VariableReferenceExpression>builder()
                                .addAll(innerAggregation.getAggregations().keySet())
                                .addAll(sides.getOuter().getOutputVariables())
                                .build(),
                Optional.empty(),
                join.getLeftHashVariable(),
                join.getRightHashVariable(),
                join.getDistributionType(),
                join.getDynamicFilters());

        ImmutableMap<VariableReferenceExpression, VariableReferenceExpression> preAggregationVariables = originalToPreAggregation.build();
        ImmutableMap.Builder<VariableReferenceExpression, AggregationNode.Aggregation> finalAggregations = ImmutableMap.builder();
        ImmutableMap.Builder<VariableReferenceExpression, VariableReferenceExpression> originalToFinalAggregation = ImmutableMap.builder();
        for (Map.Entry<VariableReferenceExpression, VariableReferenceExpression> entry : preAggregationVariables.entrySet()) {
            VariableReferenceExpression finalAggregationVariable = context.getVariableAllocator().newVariable(entry.getKey().getSourceLocation(), "count_sum", entry.getKey().getType());
            finalAggregations.put(finalAggregationVariable, new AggregationNode.Aggregation(
                    new CallExpression(
                            entry.getKey().getSourceLocation(),
                            "sum",
                            functionAndTypeManager.lookupFunction("sum", fromTypes(entry.getValue().getType())),
                            entry.getKey().getType(),
                            ImmutableList.of(entry.getValue())),
                    Optional.empty(),
                    Optional.empty(),
                    false,
                    Optional.empty()));
            originalToFinalAggregation.put(entry.getKey(), finalAggregationVariable);
        }

        AggregationNode finalAggregation = new AggregationNode(
                aggregation.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                rewrittenJoin,
                finalAggregations.build(),
                aggregation.getGroupingSets(),
                aggregation.getPreGroupedVariables(),
                aggregation.getStep(),
                Optional.empty(),
                aggregation.getGroupIdVariable(),
                aggregation.getAggregationId());

        Assignments.Builder assignments = Assignments.builder();
        assignments.putAll(identityAssignments(aggregation.getGroupingKeys()));
        originalToFinalAggregation.build().forEach((original, finalVariable) ->
                assignments.put(original, new SpecialFormExpression(
                        original.getSourceLocation(),
                        COALESCE,
                        original.getType(),
                        finalVariable,
                        new ConstantExpression(original.getSourceLocation(), 0L, BIGINT))));

        return Result.ofPlanNode(new ProjectNode(
                context.getIdAllocator().getNextId(),
                finalAggregation,
                assignments.build()));
    }

    private AggregationNode.Aggregation rewriteCountArgumentIfNonNull(AggregationNode.Aggregation aggregation, PlanNode source, Context context)
    {
        if (aggregation.getArguments().size() != 1) {
            return aggregation;
        }

        RowExpression argument = aggregation.getArguments().get(0);
        if (!(argument instanceof VariableReferenceExpression) || !isKnownNonNull(source, (VariableReferenceExpression) argument, context)) {
            return aggregation;
        }

        return count(functionAndTypeManager);
    }

    private boolean isKnownNonNull(PlanNode node, VariableReferenceExpression variable, Context context)
    {
        PlanNode resolved = context.getLookup().resolve(node);
        if (resolved instanceof FilterNode) {
            return isKnownNonNull(((FilterNode) resolved).getSource(), variable, context);
        }

        if (resolved instanceof ProjectNode) {
            ProjectNode project = (ProjectNode) resolved;
            RowExpression assignment = project.getAssignments().get(variable);
            return assignment instanceof VariableReferenceExpression
                    && isKnownNonNull(project.getSource(), (VariableReferenceExpression) assignment, context);
        }

        if (resolved instanceof TableScanNode) {
            TableScanNode tableScan = (TableScanNode) resolved;
            return Optional.ofNullable(tableScan.getAssignments().get(variable))
                    .map(column -> tableScan.getTableConstraints().stream()
                            .filter(constraint -> constraint instanceof NotNullConstraint && (constraint.isEnabled() || constraint.isRely()))
                            .anyMatch(constraint -> constraint.getColumns().contains(column)))
                    .orElse(false);
        }

        return false;
    }

    private boolean isSupported(AggregationNode aggregation, JoinNode join)
    {
        if (!(join.getType() == JoinType.LEFT || join.getType() == JoinType.RIGHT)
                || join.getFilter().isPresent()
                || join.getCriteria().isEmpty()
                || join.getLeftHashVariable().isPresent()
                || join.getRightHashVariable().isPresent()
                || aggregation.getAggregations().isEmpty()
                || aggregation.getStep() != AggregationNode.Step.SINGLE
                || aggregation.getGroupingSetCount() != 1
                || aggregation.getHashVariable().isPresent()
                || aggregation.getGroupIdVariable().isPresent()
                || aggregation.getAggregationId().isPresent()) {
            return false;
        }

        JoinSides sides = new JoinSides(join);
        if (join.getType() == JoinType.LEFT
                && !ImmutableSet.copyOf(sides.getInnerJoinKeys()).containsAll(join.getDynamicFilters().values())) {
            return false;
        }

        Set<VariableReferenceExpression> innerVariables = ImmutableSet.copyOf(sides.getInner().getOutputVariables());
        for (AggregationNode.Aggregation aggregate : aggregation.getAggregations().values()) {
            if (!functionResolution.isCountFunction(aggregate.getFunctionHandle())
                    || aggregate.getArguments().size() != 1
                    || !(aggregate.getArguments().get(0) instanceof VariableReferenceExpression)
                    || aggregate.isDistinct()
                    || aggregate.getFilter().isPresent()
                    || aggregate.getMask().isPresent()
                    || aggregate.getOrderBy().isPresent()) {
                return false;
            }
            if (!innerVariables.contains((VariableReferenceExpression) aggregate.getArguments().get(0))) {
                return false;
            }
        }
        return true;
    }

    private static class JoinSides
    {
        private final JoinNode join;

        private JoinSides(JoinNode join)
        {
            this.join = join;
        }

        public PlanNode getOuter()
        {
            return join.getType() == JoinType.LEFT ? join.getLeft() : join.getRight();
        }

        public PlanNode getInner()
        {
            return join.getType() == JoinType.LEFT ? join.getRight() : join.getLeft();
        }

        public List<VariableReferenceExpression> getInnerJoinKeys()
        {
            return join.getCriteria().stream()
                    .map(join.getType() == JoinType.LEFT ? EquiJoinClause::getRight : EquiJoinClause::getLeft)
                    .distinct()
                    .collect(toImmutableList());
        }
    }
}
