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
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.function.AggregationFunctionImplementation;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.AggregationNode.Aggregation;
import com.facebook.presto.spi.plan.Partitioning;
import com.facebook.presto.spi.plan.PartitioningScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.GroupIdNode;
import com.google.common.collect.ImmutableList;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.isPreAggregateBeforeGroupingSets;
import static com.facebook.presto.operator.aggregation.AggregationUtils.isDecomposable;
import static com.facebook.presto.spi.plan.AggregationNode.Step.INTERMEDIATE;
import static com.facebook.presto.spi.plan.AggregationNode.Step.PARTIAL;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE_STREAMING;
import static com.facebook.presto.sql.planner.plan.Patterns.Aggregation.step;
import static com.facebook.presto.sql.planner.plan.Patterns.aggregation;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;

/**
 * Transforms
 * <pre>
 *   - Partial Aggregation
 *     - GroupId
 *       - Source
 * </pre>
 * to
 * <pre>
 *   - Intermediate Aggregation
 *     - GroupId
 *       - Intermediate Aggregation
 *         - RemoteExchange
 *           - Partial Aggregation
 *             - Source
 * </pre>
 * <p>
 * Rationale: GroupId increases the number of rows (one copy per grouping set), then partial
 * aggregation reduces them. By pre-aggregating at the finest granularity (union of all grouping
 * set columns) and shuffling by grouping keys before GroupId, we reduce the number of rows that
 * get multiplied. The original PARTIAL above GroupId is changed to INTERMEDIATE to merge the
 * pre-aggregated partial states within each grouping set.
 * <p>
 * Also handles the case where a ProjectNode (e.g., from hash generation) sits between
 * the Aggregation and GroupId.
 * <p>
 * Only applies to decomposable aggregation functions ({@code SUM}, {@code COUNT}, {@code MIN},
 * {@code MAX}) that support partial/intermediate/final splitting.
 */
public class PreAggregateBeforeGroupId
        implements Rule<AggregationNode>
{
    // Match Aggregation(PARTIAL) whose source is either GroupId directly
    // or a Project node (e.g., from hash generation) on top of GroupId.
    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .with(step().equalTo(PARTIAL));

    private final FunctionAndTypeManager functionAndTypeManager;

    public PreAggregateBeforeGroupId(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isPreAggregateBeforeGroupingSets(session);
    }

    @Override
    public Result apply(AggregationNode node, Captures captures, Context context)
    {
        // Find GroupIdNode: either the direct source or behind a ProjectNode.
        // Must resolve through Lookup since the Memo wraps sources in GroupReference.
        GroupIdNode groupIdNode = findGroupIdNode(node, context.getLookup());
        if (groupIdNode == null) {
            return Result.empty();
        }

        // Safety check: must be decomposable (no DISTINCT, no ORDER BY)
        if (!isDecomposable(node, functionAndTypeManager)) {
            return Result.empty();
        }

        // Skip checksum whose XOR-based intermediate merge produces different results
        // at different grouping granularities.
        for (Aggregation agg : node.getAggregations().values()) {
            String name = functionAndTypeManager.getFunctionMetadata(agg.getFunctionHandle()).getName().getObjectName();
            if (name.equals("checksum")) {
                return Result.empty();
            }
        }

        // Verify that the aggregation's grouping keys are consistent with GroupId output.
        if (!aggregationGroupingMatchesGroupId(node, groupIdNode)) {
            return Result.empty();
        }

        // Compute the union of all grouping set columns mapped back to source variables.
        Map<VariableReferenceExpression, VariableReferenceExpression> groupingColumns = groupIdNode.getGroupingColumns();
        Set<VariableReferenceExpression> allSourceGroupingKeys = new LinkedHashSet<>();
        for (List<VariableReferenceExpression> groupingSet : groupIdNode.getGroupingSets()) {
            for (VariableReferenceExpression outputVar : groupingSet) {
                VariableReferenceExpression sourceVar = groupingColumns.get(outputVar);
                if (sourceVar != null) {
                    allSourceGroupingKeys.add(sourceVar);
                }
            }
        }

        if (allSourceGroupingKeys.isEmpty()) {
            return Result.empty();
        }

        // Build variable mappings for the three aggregation levels:
        // 1. PARTIAL: raw values → partialVar (intermediate type)
        // 2. INTERMEDIATE below GroupId: partialVar → preGroupIdVar (intermediate type)
        // 3. INTERMEDIATE above GroupId: preGroupIdVar → originalOutputVar (intermediate type)
        // Use LinkedHashMap with pre-sizing to preserve deterministic iteration order
        // matching AggregationNode.getAggregations() and avoid type mismatches at native
        // execution boundaries (see issue #27492).
        int aggregationCount = node.getAggregations().size();
        Map<VariableReferenceExpression, VariableReferenceExpression> outputToPartialVarMap = new LinkedHashMap<>(aggregationCount);
        Map<VariableReferenceExpression, VariableReferenceExpression> outputToPreGroupIdVarMap = new LinkedHashMap<>(aggregationCount);
        Map<VariableReferenceExpression, Aggregation> newPartialAggregations = new LinkedHashMap<>(aggregationCount);

        for (Map.Entry<VariableReferenceExpression, Aggregation> entry : node.getAggregations().entrySet()) {
            Aggregation originalAggregation = entry.getValue();
            FunctionHandle functionHandle = originalAggregation.getFunctionHandle();
            String functionName = functionAndTypeManager.getFunctionMetadata(functionHandle).getName().getObjectName();
            AggregationFunctionImplementation function = functionAndTypeManager.getAggregateFunctionImplementation(functionHandle);

            // Variable for PARTIAL output
            VariableReferenceExpression partialVariable = context.getVariableAllocator().newVariable(
                    entry.getValue().getCall().getSourceLocation(),
                    functionName,
                    function.getIntermediateType());

            // Variable for INTERMEDIATE-below-GroupId output
            VariableReferenceExpression preGroupIdVariable = context.getVariableAllocator().newVariable(
                    entry.getValue().getCall().getSourceLocation(),
                    functionName,
                    function.getIntermediateType());

            outputToPartialVarMap.put(entry.getKey(), partialVariable);
            outputToPreGroupIdVarMap.put(entry.getKey(), preGroupIdVariable);

            // The new PARTIAL aggregation uses the original arguments (which are
            // GroupIdNode.aggregationArguments — source-side pass-through variables)
            newPartialAggregations.put(partialVariable, new Aggregation(
                    new CallExpression(
                            originalAggregation.getCall().getSourceLocation(),
                            functionName,
                            functionHandle,
                            function.getIntermediateType(),
                            originalAggregation.getArguments()),
                    originalAggregation.getFilter(),
                    originalAggregation.getOrderBy(),
                    originalAggregation.isDistinct(),
                    originalAggregation.getMask()));
        }

        // Step 1: Create new PARTIAL AggregationNode on Source
        ImmutableList<VariableReferenceExpression> groupingKeysList = ImmutableList.copyOf(allSourceGroupingKeys);
        PlanNode newPartialAggregation = new AggregationNode(
                node.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                groupIdNode.getSource(),
                newPartialAggregations,
                AggregationNode.singleGroupingSet(groupingKeysList),
                ImmutableList.of(),
                PARTIAL,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        // Step 2: Create Exchange (hash partitioned by grouping keys) to shuffle partial states
        PlanNode exchange = ExchangeNode.partitionedExchange(
                context.getIdAllocator().getNextId(),
                REMOTE_STREAMING,
                newPartialAggregation,
                new PartitioningScheme(
                        Partitioning.create(FIXED_HASH_DISTRIBUTION, groupingKeysList),
                        newPartialAggregation.getOutputVariables()));

        // Step 3: Create INTERMEDIATE AggregationNode below GroupId to merge partial states after shuffle
        Map<VariableReferenceExpression, Aggregation> preGroupIdIntermediateAggregations = new LinkedHashMap<>(aggregationCount);
        for (Map.Entry<VariableReferenceExpression, Aggregation> entry : node.getAggregations().entrySet()) {
            Aggregation originalAggregation = entry.getValue();
            FunctionHandle functionHandle = originalAggregation.getFunctionHandle();
            String functionName = functionAndTypeManager.getFunctionMetadata(functionHandle).getName().getObjectName();
            AggregationFunctionImplementation function = functionAndTypeManager.getAggregateFunctionImplementation(functionHandle);
            VariableReferenceExpression partialVariable = outputToPartialVarMap.get(entry.getKey());
            VariableReferenceExpression preGroupIdVariable = outputToPreGroupIdVarMap.get(entry.getKey());

            preGroupIdIntermediateAggregations.put(preGroupIdVariable, new Aggregation(
                    new CallExpression(
                            originalAggregation.getCall().getSourceLocation(),
                            functionName,
                            functionHandle,
                            function.getIntermediateType(),
                            ImmutableList.<RowExpression>builder()
                                    .add(partialVariable)
                                    .addAll(originalAggregation.getArguments()
                                            .stream()
                                            .filter(PreAggregateBeforeGroupId::isLambda)
                                            .collect(toImmutableList()))
                                    .build()),
                    Optional.empty(),
                    Optional.empty(),
                    false,
                    Optional.empty()));
        }

        PlanNode preGroupIdIntermediate = new AggregationNode(
                node.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                exchange,
                preGroupIdIntermediateAggregations,
                AggregationNode.singleGroupingSet(groupingKeysList),
                ImmutableList.of(),
                INTERMEDIATE,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        // Step 4: Create new GroupIdNode with INTERMEDIATE output as source
        ImmutableList<VariableReferenceExpression> newAggregationArguments = ImmutableList.copyOf(outputToPreGroupIdVarMap.values());

        GroupIdNode newGroupIdNode = new GroupIdNode(
                groupIdNode.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                Optional.empty(),
                preGroupIdIntermediate,
                groupIdNode.getGroupingSets(),
                groupIdNode.getGroupingColumns(),
                newAggregationArguments,
                groupIdNode.getGroupIdVariable());

        // Step 5: Change the original PARTIAL aggregation above GroupId to INTERMEDIATE.
        // It takes intermediate state (passed through GroupId) and produces intermediate
        // state for the existing FINAL above.
        Map<VariableReferenceExpression, Aggregation> aboveGroupIdIntermediateAggregations = new LinkedHashMap<>(aggregationCount);
        for (Map.Entry<VariableReferenceExpression, Aggregation> entry : node.getAggregations().entrySet()) {
            Aggregation originalAggregation = entry.getValue();
            FunctionHandle functionHandle = originalAggregation.getFunctionHandle();
            String functionName = functionAndTypeManager.getFunctionMetadata(functionHandle).getName().getObjectName();
            AggregationFunctionImplementation function = functionAndTypeManager.getAggregateFunctionImplementation(functionHandle);
            VariableReferenceExpression preGroupIdVariable = outputToPreGroupIdVarMap.get(entry.getKey());

            aboveGroupIdIntermediateAggregations.put(entry.getKey(), new Aggregation(
                    new CallExpression(
                            originalAggregation.getCall().getSourceLocation(),
                            functionName,
                            functionHandle,
                            function.getIntermediateType(),
                            ImmutableList.<RowExpression>builder()
                                    .add(preGroupIdVariable)
                                    .addAll(originalAggregation.getArguments()
                                            .stream()
                                            .filter(PreAggregateBeforeGroupId::isLambda)
                                            .collect(toImmutableList()))
                                    .build()),
                    Optional.empty(),
                    Optional.empty(),
                    false,
                    Optional.empty()));
        }

        PlanNode aboveGroupIdIntermediate = new AggregationNode(
                node.getSourceLocation(),
                node.getId(),
                newGroupIdNode,
                aboveGroupIdIntermediateAggregations,
                node.getGroupingSets(),
                ImmutableList.of(),
                INTERMEDIATE,
                node.getHashVariable(),
                node.getGroupIdVariable(),
                node.getAggregationId());

        return Result.ofPlanNode(aboveGroupIdIntermediate);
    }

    /**
     * Finds the GroupIdNode below the aggregation, looking through an optional
     * ProjectNode (e.g., inserted by hash generation). Uses Lookup to resolve
     * through GroupReference nodes in the Memo.
     */
    private static GroupIdNode findGroupIdNode(AggregationNode aggregation, Lookup lookup)
    {
        PlanNode source = lookup.resolve(aggregation.getSource());
        if (source instanceof GroupIdNode) {
            return (GroupIdNode) source;
        }
        if (source instanceof ProjectNode) {
            PlanNode projectSource = lookup.resolve(((ProjectNode) source).getSource());
            if (projectSource instanceof GroupIdNode) {
                return (GroupIdNode) projectSource;
            }
        }
        return null;
    }

    /**
     * Verifies that the aggregation's grouping keys are exactly the GroupIdNode's
     * grouping set columns plus the groupId variable.
     */
    private static boolean aggregationGroupingMatchesGroupId(AggregationNode aggregation, GroupIdNode groupId)
    {
        Set<VariableReferenceExpression> aggregationGroupingKeys = new HashSet<>(aggregation.getGroupingKeys());

        if (!aggregationGroupingKeys.contains(groupId.getGroupIdVariable())) {
            return false;
        }

        Set<VariableReferenceExpression> expectedKeys = groupId.getGroupingSets().stream()
                .flatMap(Collection::stream)
                .collect(toSet());
        expectedKeys.add(groupId.getGroupIdVariable());

        return aggregationGroupingKeys.equals(expectedKeys);
    }

    private static boolean isLambda(RowExpression rowExpression)
    {
        return rowExpression instanceof LambdaDefinitionExpression;
    }
}
