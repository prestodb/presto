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
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.AggregationNode.Aggregation;
import com.facebook.presto.spi.plan.Ordering;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.UnionNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.ExpressionOptimizer;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.RowExpressionVariableInliner;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.facebook.presto.sql.relational.RowExpressionOptimizer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import io.airlift.slice.Slice;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.isPushAggregationThroughDisjointUnion;
import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.spi.plan.AggregationNode.singleGroupingSet;
import static com.facebook.presto.sql.planner.optimizations.SetOperationNodeUtils.fromListMultimap;
import static com.facebook.presto.sql.planner.plan.Patterns.aggregation;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.facebook.presto.sql.planner.plan.Patterns.union;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * Pushes a {@code GROUP BY} aggregation completely below a {@code UNION ALL} when at least one
 * grouping key has constant values that are disjoint across union branches. Because every group
 * is then fully contained in exactly one branch, the top-level aggregation can be eliminated
 * entirely; each branch computes its own SINGLE-step aggregation and the results are re-unioned.
 *
 * <p>Example:
 * <pre>
 * SELECT count(*), x FROM (SELECT 1 x UNION ALL SELECT 2 x) GROUP BY x
 * </pre>
 * becomes
 * <pre>
 * (SELECT count(*), 1 x FROM (SELECT 1 x) GROUP BY 1)
 * UNION ALL
 * (SELECT count(*), 2 x FROM (SELECT 2 x) GROUP BY 1)
 * </pre>
 *
 * <p>Unlike {@code PushPartialAggregationThroughExchange}, which always retains a FINAL aggregation
 * on top, this rewrite drops the parent aggregation completely. Disjointness on at least one
 * grouping key guarantees no group spans more than one branch, so the rewrite is correct for any
 * aggregation function (including DISTINCT, FILTER, MASK, ORDER BY).
 */
public class PushAggregationThroughDisjointUnion
        implements Rule<AggregationNode>
{
    private static final Capture<UnionNode> CHILD = newCapture();

    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .with(source().matching(union().capturedAs(CHILD)));

    private final RowExpressionOptimizer rowExpressionOptimizer;
    private final RowExpressionDeterminismEvaluator determinismEvaluator;

    public PushAggregationThroughDisjointUnion(FunctionAndTypeManager functionAndTypeManager)
    {
        requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
        this.rowExpressionOptimizer = new RowExpressionOptimizer(functionAndTypeManager);
        this.determinismEvaluator = new RowExpressionDeterminismEvaluator(functionAndTypeManager);
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isPushAggregationThroughDisjointUnion(session);
    }

    @Override
    public Result apply(AggregationNode aggregation, Captures captures, Context context)
    {
        if (aggregation.getStep() != AggregationNode.Step.SINGLE
                || aggregation.getGroupingSetCount() != 1
                || aggregation.hasEmptyGroupingSet()
                || aggregation.getGroupingKeys().isEmpty()
                || aggregation.getGroupIdVariable().isPresent()
                || aggregation.getHashVariable().isPresent()
                || aggregation.getAggregations().isEmpty()) {
            return Result.empty();
        }

        UnionNode unionNode = captures.get(CHILD);
        if (unionNode.getSources().size() < 2) {
            return Result.empty();
        }

        if (!allAggregationsDeterministic(aggregation)) {
            return Result.empty();
        }

        if (!hasDisjointGroupingKey(aggregation, unionNode, context)) {
            return Result.empty();
        }

        // Build per-branch SINGLE aggregations and stitch them under a new UnionNode.
        ImmutableList.Builder<PlanNode> branchSources = ImmutableList.builder();
        ImmutableListMultimap.Builder<VariableReferenceExpression, VariableReferenceExpression> outputMappings = ImmutableListMultimap.builder();

        // Per-branch translated grouping keys, in original order.
        for (int i = 0; i < unionNode.getSources().size(); i++) {
            Map<VariableReferenceExpression, VariableReferenceExpression> branchMap = unionNode.sourceVariableMap(i);

            // Translate grouping keys (must all be present as union outputs).
            ImmutableList.Builder<VariableReferenceExpression> branchGroupingKeys = ImmutableList.builder();
            for (VariableReferenceExpression key : aggregation.getGroupingKeys()) {
                VariableReferenceExpression branchKey = branchMap.get(key);
                if (branchKey == null) {
                    return Result.empty();
                }
                branchGroupingKeys.add(branchKey);
            }

            // Translate each aggregation, allocating a fresh output variable.
            Map<VariableReferenceExpression, Aggregation> branchAggregations = new LinkedHashMap<>();
            Map<VariableReferenceExpression, VariableReferenceExpression> aggOutputMap = new LinkedHashMap<>();
            for (Map.Entry<VariableReferenceExpression, Aggregation> entry : aggregation.getAggregations().entrySet()) {
                VariableReferenceExpression originalOutput = entry.getKey();
                Aggregation original = entry.getValue();

                VariableReferenceExpression branchOutput = context.getVariableAllocator().newVariable(originalOutput);
                branchAggregations.put(branchOutput, translateAggregation(original, branchMap));
                aggOutputMap.put(originalOutput, branchOutput);
            }

            AggregationNode branchAgg = new AggregationNode(
                    aggregation.getSourceLocation(),
                    context.getIdAllocator().getNextId(),
                    unionNode.getSources().get(i),
                    branchAggregations,
                    singleGroupingSet(branchGroupingKeys.build()),
                    ImmutableList.of(),
                    AggregationNode.Step.SINGLE,
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty());
            branchSources.add(branchAgg);

            // Record per-branch output variables in the same order as the original aggregation's outputs.
            for (VariableReferenceExpression outputVar : aggregation.getOutputVariables()) {
                VariableReferenceExpression branchOutputVar = aggOutputMap.containsKey(outputVar)
                        ? aggOutputMap.get(outputVar)
                        : branchMap.get(outputVar); // grouping key
                outputMappings.put(outputVar, branchOutputVar);
            }
        }

        return Result.ofPlanNode(new UnionNode(
                aggregation.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                branchSources.build(),
                ImmutableList.copyOf(aggregation.getOutputVariables()),
                fromListMultimap(outputMappings.build())));
    }

    private boolean allAggregationsDeterministic(AggregationNode aggregation)
    {
        for (Aggregation agg : aggregation.getAggregations().values()) {
            for (RowExpression argument : agg.getArguments()) {
                if (!determinismEvaluator.isDeterministic(argument)) {
                    return false;
                }
            }
            if (agg.getFilter().isPresent() && !determinismEvaluator.isDeterministic(agg.getFilter().get())) {
                return false;
            }
        }
        return true;
    }

    private Aggregation translateAggregation(Aggregation original, Map<VariableReferenceExpression, VariableReferenceExpression> branchMap)
    {
        CallExpression originalCall = original.getCall();
        List<RowExpression> translatedArgs = originalCall.getArguments().stream()
                .map(arg -> RowExpressionVariableInliner.inlineVariables(branchMap, arg))
                .collect(ImmutableList.toImmutableList());
        CallExpression translatedCall = new CallExpression(
                originalCall.getSourceLocation(),
                originalCall.getDisplayName(),
                originalCall.getFunctionHandle(),
                originalCall.getType(),
                translatedArgs);
        Optional<RowExpression> translatedFilter = original.getFilter()
                .map(filter -> RowExpressionVariableInliner.inlineVariables(branchMap, filter));
        Optional<VariableReferenceExpression> translatedMask = original.getMask().map(m -> {
            VariableReferenceExpression mapped = branchMap.get(m);
            checkState(mapped != null, "Mask variable %s not found in branch mapping", m);
            return mapped;
        });
        Optional<OrderingScheme> translatedOrder = original.getOrderBy().map(scheme -> {
            ImmutableList.Builder<Ordering> orderings = ImmutableList.builder();
            for (Ordering ordering : scheme.getOrderBy()) {
                VariableReferenceExpression mapped = branchMap.get(ordering.getVariable());
                orderings.add(new Ordering(mapped == null ? ordering.getVariable() : mapped, ordering.getSortOrder()));
            }
            return new OrderingScheme(orderings.build());
        });
        return new Aggregation(translatedCall, translatedFilter, translatedOrder, original.isDistinct(), translatedMask);
    }

    /**
     * Returns true iff at least one grouping key has a single constant value within every branch
     * and those per-branch constants are pairwise distinct (NULL collides with NULL).
     */
    private boolean hasDisjointGroupingKey(AggregationNode aggregation, UnionNode unionNode, Context context)
    {
        ConnectorSession connectorSession = context.getSession().toConnectorSession();
        Lookup lookup = context.getLookup();
        int branchCount = unionNode.getSources().size();

        for (VariableReferenceExpression groupingKey : aggregation.getGroupingKeys()) {
            List<VariableReferenceExpression> perBranchInputs = unionNode.getVariableMapping().get(groupingKey);
            if (perBranchInputs == null || perBranchInputs.size() != branchCount) {
                continue;
            }

            boolean allBranchesConstant = true;
            Object[] branchValues = new Object[branchCount];
            for (int i = 0; i < branchCount; i++) {
                Optional<ConstantExpression> constant = resolveBranchConstant(unionNode.getSources().get(i), perBranchInputs.get(i), lookup, connectorSession);
                if (!constant.isPresent()) {
                    allBranchesConstant = false;
                    break;
                }
                branchValues[i] = constant.get().getValue();
            }
            if (!allBranchesConstant) {
                continue;
            }
            if (allPairwiseDistinct(branchValues)) {
                return true;
            }
        }
        return false;
    }

    private static boolean allPairwiseDistinct(Object[] values)
    {
        for (int i = 0; i < values.length; i++) {
            for (int j = i + 1; j < values.length; j++) {
                if (Objects.equals(values[i], values[j])) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Resolves the per-branch input variable to a constant if possible. Walks through cascading
     * Project nodes; handles single-row Values; handles multi-row Values where every row has the
     * same value at the column position.
     */
    private Optional<ConstantExpression> resolveBranchConstant(PlanNode branchSource, VariableReferenceExpression branchVariable, Lookup lookup, ConnectorSession connectorSession)
    {
        VariableReferenceExpression current = branchVariable;
        PlanNode node = lookup.resolve(branchSource);
        // Walk cascading projects.
        while (node instanceof ProjectNode) {
            ProjectNode project = (ProjectNode) node;
            RowExpression assigned = project.getAssignments().get(current);
            if (assigned == null) {
                return Optional.empty();
            }
            Optional<ConstantExpression> constant = tryAsConstant(assigned, connectorSession);
            if (constant.isPresent()) {
                return constant;
            }
            if (assigned instanceof VariableReferenceExpression) {
                current = (VariableReferenceExpression) assigned;
                node = lookup.resolve(project.getSource());
                continue;
            }
            return Optional.empty();
        }

        if (node instanceof ValuesNode) {
            ValuesNode values = (ValuesNode) node;
            int index = values.getOutputVariables().indexOf(current);
            if (index < 0) {
                return Optional.empty();
            }
            List<List<RowExpression>> rows = values.getRows();
            if (rows.isEmpty()) {
                return Optional.empty();
            }
            Optional<ConstantExpression> first = tryAsConstant(rows.get(0).get(index), connectorSession);
            if (!first.isPresent()) {
                return Optional.empty();
            }
            for (int r = 1; r < rows.size(); r++) {
                Optional<ConstantExpression> next = tryAsConstant(rows.get(r).get(index), connectorSession);
                if (!next.isPresent() || !Objects.equals(next.get().getValue(), first.get().getValue())) {
                    return Optional.empty();
                }
            }
            return first;
        }

        return Optional.empty();
    }

    private Optional<ConstantExpression> tryAsConstant(RowExpression expression, ConnectorSession connectorSession)
    {
        if (expression instanceof ConstantExpression) {
            return acceptIfSafeJavaType((ConstantExpression) expression);
        }
        RowExpression optimized = rowExpressionOptimizer.optimize(expression, ExpressionOptimizer.Level.OPTIMIZED, connectorSession);
        if (optimized instanceof ConstantExpression) {
            return acceptIfSafeJavaType((ConstantExpression) optimized);
        }
        return Optional.empty();
    }

    /**
     * Only accept constants whose Java representation is a primitive (boolean / long / double / etc.)
     * or a {@link Slice}. The disjointness check ({@link #allPairwiseDistinct}) and per-row equality
     * check ({@link #resolveBranchConstant}) compare values via {@link Objects#equals} on the raw
     * {@link ConstantExpression#getValue()}; that's only reliable for these simple types. Block-,
     * row- and array-shaped values would have unreliable equality semantics here.
     */
    private static Optional<ConstantExpression> acceptIfSafeJavaType(ConstantExpression constant)
    {
        Class<?> javaType = constant.getType().getJavaType();
        if (javaType.isPrimitive() || javaType == Slice.class) {
            return Optional.of(constant);
        }
        return Optional.empty();
    }
}
