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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.DistinctLimitNode;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.IndexJoinNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.MarkDistinctNode;
import com.facebook.presto.spi.plan.MergeJoinNode;
import com.facebook.presto.spi.plan.PartitioningScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.SemiJoinNode;
import com.facebook.presto.spi.plan.SpatialJoinNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.UnionNode;
import com.facebook.presto.spi.plan.UnnestNode;
import com.facebook.presto.spi.plan.WindowNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.GroupIdNode;
import com.facebook.presto.sql.planner.plan.InternalPlanVisitor;
import com.facebook.presto.sql.planner.plan.LateralJoinNode;
import com.facebook.presto.sql.planner.plan.RowNumberNode;
import com.facebook.presto.sql.planner.plan.SequenceNode;
import com.facebook.presto.sql.planner.plan.TopNRowNumberNode;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.facebook.presto.SystemSessionProperties.skipHashGenerationForJoinWithTableScanInput;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.spi.plan.JoinType.LEFT;
import static com.facebook.presto.spi.plan.JoinType.RIGHT;
import static com.facebook.presto.spi.plan.ProjectNode.Locality.LOCAL;
import static com.facebook.presto.spi.plan.ProjectNode.Locality.REMOTE;
import static com.facebook.presto.sql.planner.PlannerUtils.HASH_CODE;
import static com.facebook.presto.sql.planner.PlannerUtils.INITIAL_HASH_VALUE;
import static com.facebook.presto.sql.planner.PlannerUtils.orNullHashCode;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static com.facebook.presto.sql.planner.optimizations.SetOperationNodeUtils.fromListMultimap;
import static com.facebook.presto.sql.planner.plan.ChildReplacer.replaceChildren;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Stream.concat;

public class HashGenerationOptimizer
        implements PlanOptimizer
{
    private final FunctionAndTypeManager functionAndTypeManager;
    private boolean isEnabledForTesting;

    public HashGenerationOptimizer(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionManager is null");
    }

    @Override
    public void setEnabledForTesting(boolean isSet)
    {
        isEnabledForTesting = isSet;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isEnabledForTesting || SystemSessionProperties.isOptimizeHashGenerationEnabled(session);
    }

    @Override
    public PlanOptimizerResult optimize(PlanNode plan, Session session, TypeProvider types, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(types, "types is null");
        requireNonNull(variableAllocator, "variableAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");
        if (isEnabled(session)) {
            PlanWithProperties result = new Rewriter(idAllocator, variableAllocator, functionAndTypeManager, session).accept(plan, new HashComputationSet());
            return PlanOptimizerResult.optimizerResult(result.getNode(), true);
        }
        return PlanOptimizerResult.optimizerResult(plan, false);
    }

    private static class Rewriter
            extends InternalPlanVisitor<PlanWithProperties, HashComputationSet>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final VariableAllocator variableAllocator;
        private final FunctionAndTypeManager functionAndTypeManager;
        private final Session session;

        private Rewriter(PlanNodeIdAllocator idAllocator, VariableAllocator variableAllocator, FunctionAndTypeManager functionAndTypeManager, Session session)
        {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.variableAllocator = requireNonNull(variableAllocator, "variableAllocator is null");
            this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionManager is null");
            this.session = requireNonNull(session, "session is null");
        }

        @Override
        public PlanWithProperties visitPlan(PlanNode node, HashComputationSet parentPreference)
        {
            return planSimpleNodeWithProperties(node, parentPreference);
        }

        @Override
        public PlanWithProperties visitEnforceSingleRow(EnforceSingleRowNode node, HashComputationSet parentPreference)
        {
            // this plan node can only have a single input variable, so do not add extra hash variables
            return planSimpleNodeWithProperties(node, new HashComputationSet(), true);
        }

        @Override
        public PlanWithProperties visitApply(ApplyNode node, HashComputationSet context)
        {
            // Apply node is not supported by execution, so do not rewrite it
            // that way query will fail in sanity checkers
            return new PlanWithProperties(node, ImmutableMap.of());
        }

        public PlanWithProperties visitSequence(SequenceNode node, HashComputationSet context)
        {
            List<PlanNode> cteProducers = node.getCteProducers().stream()
                    .map(c ->
                            planAndEnforce(c, new HashComputationSet(), true, new HashComputationSet()).getNode())
                    .collect(ImmutableList.toImmutableList());
            PlanWithProperties primarySource = plan(node.getPrimarySource(), context);
            return new PlanWithProperties(
                    replaceChildren(node, ImmutableList.<PlanNode>builder()
                            .addAll(cteProducers)
                            .add(primarySource.getNode())
                            .build()),
                    primarySource.getHashVariables());
        }

        @Override
        public PlanWithProperties visitLateralJoin(LateralJoinNode node, HashComputationSet context)
        {
            // Lateral join node is not supported by execution, so do not rewrite it
            // that way query will fail in sanity checkers
            return new PlanWithProperties(node, ImmutableMap.of());
        }

        @Override
        public PlanWithProperties visitAggregation(AggregationNode node, HashComputationSet parentPreference)
        {
            Optional<HashComputation> groupByHash = Optional.empty();
            List<VariableReferenceExpression> groupingKeys = node.getGroupingKeys();
            if (!node.isStreamable() && !node.isSegmentedAggregationEligible() && !canSkipHashGeneration(node.getGroupingKeys())) {
                // todo: for segmented aggregation, add optimizations for the fields that need to compute hash
                groupByHash = computeHash(groupingKeys, functionAndTypeManager);
            }

            // aggregation does not pass through preferred hash variables
            HashComputationSet requiredHashes = new HashComputationSet(groupByHash);
            PlanWithProperties child = planAndEnforce(node.getSource(), requiredHashes, false, requiredHashes);

            Optional<VariableReferenceExpression> hashVariable = groupByHash.map(child::getRequiredHashVariable);

            return new PlanWithProperties(
                    new AggregationNode(
                            node.getSourceLocation(),
                            node.getId(),
                            child.getNode(),
                            node.getAggregations(),
                            node.getGroupingSets(),
                            node.getPreGroupedVariables(),
                            node.getStep(),
                            hashVariable,
                            node.getGroupIdVariable(),
                            node.getAggregationId()),
                    hashVariable.isPresent() ? ImmutableMap.of(groupByHash.get(), hashVariable.get()) : ImmutableMap.of());
        }

        private boolean canSkipHashGeneration(List<VariableReferenceExpression> partitionVariables)
        {
            // HACK: bigint grouped aggregation has special operators that do not use precomputed hash, so we can skip hash generation
            return partitionVariables.isEmpty() || (partitionVariables.size() == 1 && Iterables.getOnlyElement(partitionVariables).getType().equals(BIGINT));
        }

        @Override
        public PlanWithProperties visitGroupId(GroupIdNode node, HashComputationSet parentPreference)
        {
            // remove any hash variables not exported by the source of this node
            return planSimpleNodeWithProperties(node, parentPreference.pruneVariables(node.getSource().getOutputVariables()));
        }

        @Override
        public PlanWithProperties visitDistinctLimit(DistinctLimitNode node, HashComputationSet parentPreference)
        {
            // skip hash variable generation for single bigint
            if (canSkipHashGeneration(node.getDistinctVariables())) {
                return planSimpleNodeWithProperties(node, parentPreference);
            }

            Optional<HashComputation> hashComputation = computeHash(node.getDistinctVariables(), functionAndTypeManager);
            PlanWithProperties child = planAndEnforce(
                    node.getSource(),
                    new HashComputationSet(hashComputation),
                    false,
                    parentPreference.withHashComputation(node, hashComputation));
            VariableReferenceExpression hashVariable = child.getRequiredHashVariable(hashComputation.get());

            // TODO: we need to reason about how pre-computed hashes from child relate to distinct variables. We should be able to include any precomputed hash
            // that's functionally dependent on the distinct field in the set of distinct fields of the new node to be able to propagate it downstream.
            // Currently, such precomputed hashes will be dropped by this operation.
            return new PlanWithProperties(
                    new DistinctLimitNode(node.getSourceLocation(), node.getId(), child.getNode(), node.getLimit(), node.isPartial(), node.getDistinctVariables(), Optional.of(hashVariable), node.getTimeoutMillis()),
                    ImmutableMap.of(hashComputation.get(), hashVariable));
        }

        @Override
        public PlanWithProperties visitMarkDistinct(MarkDistinctNode node, HashComputationSet parentPreference)
        {
            // skip hash variable generation for single bigint
            if (canSkipHashGeneration(node.getDistinctVariables())) {
                return planSimpleNodeWithProperties(node, parentPreference);
            }

            Optional<HashComputation> hashComputation = computeHash(node.getDistinctVariables(), functionAndTypeManager);
            PlanWithProperties child = planAndEnforce(
                    node.getSource(),
                    new HashComputationSet(hashComputation),
                    false,
                    parentPreference.withHashComputation(node, hashComputation));
            VariableReferenceExpression hashVariable = child.getRequiredHashVariable(hashComputation.get());

            return new PlanWithProperties(
                    new MarkDistinctNode(node.getSourceLocation(), node.getId(), child.getNode(), node.getMarkerVariable(), node.getDistinctVariables(), Optional.of(hashVariable)),
                    child.getHashVariables());
        }

        @Override
        public PlanWithProperties visitRowNumber(RowNumberNode node, HashComputationSet parentPreference)
        {
            if (node.getPartitionBy().isEmpty()) {
                return planSimpleNodeWithProperties(node, parentPreference);
            }

            Optional<HashComputation> hashComputation = computeHash(node.getPartitionBy(), functionAndTypeManager);
            PlanWithProperties child = planAndEnforce(
                    node.getSource(),
                    new HashComputationSet(hashComputation),
                    false,
                    parentPreference.withHashComputation(node, hashComputation));
            VariableReferenceExpression hashVariable = child.getRequiredHashVariable(hashComputation.get());

            return new PlanWithProperties(
                    new RowNumberNode(
                            node.getSourceLocation(),
                            node.getId(),
                            child.getNode(),
                            node.getPartitionBy(),
                            node.getRowNumberVariable(),
                            node.getMaxRowCountPerPartition(),
                            node.isPartial(),
                            Optional.of(hashVariable)),
                    child.getHashVariables());
        }

        @Override
        public PlanWithProperties visitTopNRowNumber(TopNRowNumberNode node, HashComputationSet parentPreference)
        {
            if (node.getPartitionBy().isEmpty()) {
                return planSimpleNodeWithProperties(node, parentPreference);
            }

            Optional<HashComputation> hashComputation = computeHash(node.getPartitionBy(), functionAndTypeManager);
            PlanWithProperties child = planAndEnforce(
                    node.getSource(),
                    new HashComputationSet(hashComputation),
                    false,
                    parentPreference.withHashComputation(node, hashComputation));
            VariableReferenceExpression hashVariable = child.getRequiredHashVariable(hashComputation.get());

            return new PlanWithProperties(
                    new TopNRowNumberNode(
                            node.getSourceLocation(),
                            node.getId(),
                            child.getNode(),
                            node.getSpecification(),
                            node.getRankingFunction(),
                            node.getRowNumberVariable(),
                            node.getMaxRowCountPerPartition(),
                            node.isPartial(),
                            Optional.of(hashVariable)),
                    child.getHashVariables());
        }

        private boolean skipHashComputeForJoinInput(PlanNode node, Optional<HashComputation> hashComputation, HashComputationSet parentPreference)
        {
            return node instanceof TableScanNode && hashComputation.isPresent() && hashComputation.get().isSingleBigIntVariable() && !parentPreference.getHashes().contains(hashComputation.get());
        }

        @Override
        public PlanWithProperties visitJoin(JoinNode node, HashComputationSet parentPreference)
        {
            List<EquiJoinClause> clauses = node.getCriteria();
            if (clauses.isEmpty()) {
                // join does not pass through preferred hash variables since they take more memory and since
                // the join node filters, may take more compute
                PlanWithProperties left = planAndEnforce(node.getLeft(), new HashComputationSet(), true, new HashComputationSet());
                PlanWithProperties right = planAndEnforce(node.getRight(), new HashComputationSet(), true, new HashComputationSet());
                checkState(left.getHashVariables().isEmpty() && right.getHashVariables().isEmpty());
                return new PlanWithProperties(
                        replaceChildren(node, ImmutableList.of(left.getNode(), right.getNode())),
                        ImmutableMap.of());
            }

            // join does not pass through preferred hash variables since they take more memory and since
            // the join node filters, may take more compute
            Optional<HashComputation> leftHashComputation = computeHash(Lists.transform(clauses, EquiJoinClause::getLeft), functionAndTypeManager);
            if (skipHashGenerationForJoinWithTableScanInput(session) && skipHashComputeForJoinInput(node.getLeft(), leftHashComputation, parentPreference)) {
                leftHashComputation = Optional.empty();
            }
            PlanWithProperties left = planAndEnforce(node.getLeft(), new HashComputationSet(leftHashComputation), true, new HashComputationSet(leftHashComputation));
            Optional<VariableReferenceExpression> leftHashVariable = leftHashComputation.isPresent() ? Optional.of(left.getRequiredHashVariable(leftHashComputation.get())) : Optional.empty();

            Optional<HashComputation> rightHashComputation = computeHash(Lists.transform(clauses, EquiJoinClause::getRight), functionAndTypeManager);
            if (skipHashGenerationForJoinWithTableScanInput(session) && skipHashComputeForJoinInput(node.getRight(), rightHashComputation, parentPreference)) {
                rightHashComputation = Optional.empty();
            }
            // drop undesired hash variables from build to save memory
            PlanWithProperties right = planAndEnforce(node.getRight(), new HashComputationSet(rightHashComputation), true, new HashComputationSet(rightHashComputation));
            Optional<VariableReferenceExpression> rightHashVariable = rightHashComputation.isPresent() ? Optional.of(right.getRequiredHashVariable(rightHashComputation.get())) : Optional.empty();

            // build map of all hash variables
            // NOTE: Full outer join doesn't use hash variables
            Map<HashComputation, VariableReferenceExpression> allHashVariables = new HashMap<>();
            if (node.getType() == INNER || node.getType() == LEFT) {
                allHashVariables.putAll(left.getHashVariables());
            }
            if (node.getType() == INNER || node.getType() == RIGHT) {
                allHashVariables.putAll(right.getHashVariables());
            }

            return buildJoinNodeWithPreferredHashes(node, left, right, allHashVariables, parentPreference, leftHashVariable, rightHashVariable);
        }

        private PlanWithProperties buildJoinNodeWithPreferredHashes(
                JoinNode node,
                PlanWithProperties left,
                PlanWithProperties right,
                Map<HashComputation, VariableReferenceExpression> allHashVariables,
                HashComputationSet parentPreference,
                Optional<VariableReferenceExpression> leftHashVariable,
                Optional<VariableReferenceExpression> rightHashVariable)
        {
            // retain only hash variables preferred by parent nodes
            Map<HashComputation, VariableReferenceExpression> hashVariablesWithParentPreferences =
                    allHashVariables.entrySet()
                            .stream()
                            .filter(entry -> parentPreference.getHashes().contains(entry.getKey()))
                            .collect(toImmutableMap(Entry::getKey, Entry::getValue));

            List<VariableReferenceExpression> outputVariables = concat(left.getNode().getOutputVariables().stream(), right.getNode().getOutputVariables().stream())
                    .filter(variable -> node.getOutputVariables().contains(variable) ||
                            hashVariablesWithParentPreferences.values().contains(variable))
                    .collect(toImmutableList());

            return new PlanWithProperties(
                    new JoinNode(
                            node.getSourceLocation(),
                            node.getId(),
                            node.getType(),
                            left.getNode(),
                            right.getNode(),
                            node.getCriteria(),
                            outputVariables,
                            node.getFilter(),
                            leftHashVariable,
                            rightHashVariable,
                            node.getDistributionType(),
                            node.getDynamicFilters()),
                    hashVariablesWithParentPreferences);
        }

        @Override
        public PlanWithProperties visitSemiJoin(SemiJoinNode node, HashComputationSet parentPreference)
        {
            Optional<HashComputation> sourceHashComputation = computeHash(ImmutableList.of(node.getSourceJoinVariable()), functionAndTypeManager);
            PlanWithProperties source = planAndEnforce(
                    node.getSource(),
                    new HashComputationSet(sourceHashComputation),
                    true,
                    new HashComputationSet(sourceHashComputation));
            VariableReferenceExpression sourceHashVariable = source.getRequiredHashVariable(sourceHashComputation.get());

            Optional<HashComputation> filterHashComputation = computeHash(ImmutableList.of(node.getFilteringSourceJoinVariable()), functionAndTypeManager);
            HashComputationSet requiredHashes = new HashComputationSet(filterHashComputation);
            PlanWithProperties filteringSource = planAndEnforce(node.getFilteringSource(), requiredHashes, true, requiredHashes);
            VariableReferenceExpression filteringSourceHashVariable = filteringSource.getRequiredHashVariable(filterHashComputation.get());

            return new PlanWithProperties(
                    new SemiJoinNode(
                            node.getSourceLocation(),
                            node.getId(),
                            source.getNode(),
                            filteringSource.getNode(),
                            node.getSourceJoinVariable(),
                            node.getFilteringSourceJoinVariable(),
                            node.getSemiJoinOutput(),
                            Optional.of(sourceHashVariable),
                            Optional.of(filteringSourceHashVariable),
                            node.getDistributionType(),
                            node.getDynamicFilters()),
                    source.getHashVariables());
        }

        @Override
        public PlanWithProperties visitSpatialJoin(SpatialJoinNode node, HashComputationSet parentPreference)
        {
            PlanWithProperties left = planAndEnforce(node.getLeft(), new HashComputationSet(), true, new HashComputationSet());
            PlanWithProperties right = planAndEnforce(node.getRight(), new HashComputationSet(), true, new HashComputationSet());
            verify(left.getHashVariables().isEmpty(), "probe side of the spatial join should not include hash variables");
            verify(right.getHashVariables().isEmpty(), "build side of the spatial join should not include hash variables");
            return new PlanWithProperties(
                    replaceChildren(node, ImmutableList.of(left.getNode(), right.getNode())),
                    ImmutableMap.of());
        }

        @Override
        public PlanWithProperties visitIndexJoin(IndexJoinNode node, HashComputationSet parentPreference)
        {
            List<IndexJoinNode.EquiJoinClause> clauses = node.getCriteria();

            // join does not pass through preferred hash variables since they take more memory and since
            // the join node filters, may take more compute
            Optional<HashComputation> probeHashComputation = computeHash(Lists.transform(clauses, IndexJoinNode.EquiJoinClause::getProbe), functionAndTypeManager);
            PlanWithProperties probe = planAndEnforce(
                    node.getProbeSource(),
                    new HashComputationSet(probeHashComputation),
                    true,
                    new HashComputationSet(probeHashComputation));
            VariableReferenceExpression probeHashVariable = probe.getRequiredHashVariable(probeHashComputation.get());

            Optional<HashComputation> indexHashComputation = computeHash(Lists.transform(clauses, IndexJoinNode.EquiJoinClause::getIndex), functionAndTypeManager);
            HashComputationSet requiredHashes = new HashComputationSet(indexHashComputation);
            PlanWithProperties index = planAndEnforce(node.getIndexSource(), requiredHashes, true, requiredHashes);
            VariableReferenceExpression indexHashVariable = index.getRequiredHashVariable(indexHashComputation.get());

            // build map of all hash variables
            Map<HashComputation, VariableReferenceExpression> allHashVariables = new HashMap<>();
            if (node.getType() == JoinType.INNER) {
                allHashVariables.putAll(probe.getHashVariables());
            }
            allHashVariables.putAll(index.getHashVariables());

            return new PlanWithProperties(
                    new IndexJoinNode(
                            node.getSourceLocation(),
                            node.getId(),
                            node.getType(),
                            probe.getNode(),
                            index.getNode(),
                            node.getCriteria(),
                            node.getFilter(),
                            Optional.of(probeHashVariable),
                            Optional.of(indexHashVariable),
                            node.getLookupVariables()),
                    allHashVariables);
        }

        @Override
        public PlanWithProperties visitMergeJoin(MergeJoinNode node, HashComputationSet parentPreference)
        {
            PlanWithProperties left = planAndEnforce(node.getLeft(), new HashComputationSet(), true, new HashComputationSet());
            PlanWithProperties right = planAndEnforce(node.getRight(), new HashComputationSet(), true, new HashComputationSet());
            verify(left.getHashVariables().isEmpty(), "left side of the merge join should not include hash variables");
            verify(right.getHashVariables().isEmpty(), "right side of the merge join should not include hash variables");
            return new PlanWithProperties(
                    replaceChildren(node, ImmutableList.of(left.getNode(), right.getNode())),
                    ImmutableMap.of());
        }

        @Override
        public PlanWithProperties visitWindow(WindowNode node, HashComputationSet parentPreference)
        {
            if (node.getPartitionBy().isEmpty()) {
                return planSimpleNodeWithProperties(node, parentPreference, true);
            }

            Optional<HashComputation> hashComputation = computeHash(node.getPartitionBy(), functionAndTypeManager);
            PlanWithProperties child = planAndEnforce(
                    node.getSource(),
                    new HashComputationSet(hashComputation),
                    true,
                    parentPreference.withHashComputation(node, hashComputation));

            VariableReferenceExpression hashSymbol = child.getRequiredHashVariable(hashComputation.get());

            return new PlanWithProperties(
                    new WindowNode(
                            node.getSourceLocation(),
                            node.getId(),
                            child.getNode(),
                            node.getSpecification(),
                            node.getWindowFunctions(),
                            Optional.of(hashSymbol),
                            node.getPrePartitionedInputs(),
                            node.getPreSortedOrderPrefix()),
                    child.getHashVariables());
        }

        @Override
        public PlanWithProperties visitExchange(ExchangeNode node, HashComputationSet parentPreference)
        {
            // remove any hash variables not exported by this node
            HashComputationSet preference = parentPreference.pruneVariables(node.getOutputVariables());

            // Currently, precomputed hash values are only supported for system hash distributions without constants
            Optional<HashComputation> partitionVariables = Optional.empty();
            PartitioningScheme partitioningScheme = node.getPartitioningScheme();
            if (partitioningScheme.getPartitioning().getHandle().equals(FIXED_HASH_DISTRIBUTION) &&
                    partitioningScheme.getPartitioning().getArguments().stream().allMatch(VariableReferenceExpression.class::isInstance)) {
                // add precomputed hash for exchange
                partitionVariables = computeHash(
                        partitioningScheme.getPartitioning().getArguments().stream()
                                .map(VariableReferenceExpression.class::cast)
                                .collect(toImmutableList()),
                        functionAndTypeManager);
                preference = preference.withHashComputation(partitionVariables);
            }

            // establish fixed ordering for hash variables
            List<HashComputation> hashVariableOrder = ImmutableList.copyOf(preference.getHashes());
            Map<HashComputation, VariableReferenceExpression> newHashVariables = new HashMap<>();
            for (HashComputation preferredHashVariable : hashVariableOrder) {
                newHashVariables.put(preferredHashVariable, variableAllocator.newHashVariable());
            }

            // rewrite partition function to include new variables (and precomputed hash
            partitioningScheme = new PartitioningScheme(
                    partitioningScheme.getPartitioning(),
                    ImmutableList.<VariableReferenceExpression>builder()
                            .addAll(partitioningScheme.getOutputLayout())
                            .addAll(hashVariableOrder.stream()
                                    .map(newHashVariables::get)
                                    .collect(toImmutableList()))
                            .build(),
                    partitionVariables.map(newHashVariables::get),
                    partitioningScheme.isReplicateNullsAndAny(),
                    partitioningScheme.isScaleWriters(),
                    partitioningScheme.getEncoding(),
                    partitioningScheme.getBucketToPartition());

            // add hash variables to sources
            ImmutableList.Builder<List<VariableReferenceExpression>> newInputs = ImmutableList.builder();
            ImmutableList.Builder<PlanNode> newSources = ImmutableList.builder();
            for (int sourceId = 0; sourceId < node.getSources().size(); sourceId++) {
                PlanNode source = node.getSources().get(sourceId);
                List<VariableReferenceExpression> inputVariables = node.getInputs().get(sourceId);

                Map<VariableReferenceExpression, VariableReferenceExpression> outputToInputMap = new HashMap<>();
                for (int variableId = 0; variableId < inputVariables.size(); variableId++) {
                    outputToInputMap.put(node.getOutputVariables().get(variableId), inputVariables.get(variableId));
                }
                Function<VariableReferenceExpression, Optional<VariableReferenceExpression>> outputToInputTranslator = variable -> Optional.of(outputToInputMap.get(variable));

                HashComputationSet sourceContext = preference.translate(outputToInputTranslator);
                PlanWithProperties child = planAndEnforce(source, sourceContext, true, sourceContext);
                newSources.add(child.getNode());

                // add hash variables to inputs in the required order
                ImmutableList.Builder<VariableReferenceExpression> newInputVariables = ImmutableList.builder();
                newInputVariables.addAll(inputVariables);
                for (HashComputation preferredHashSymbol : hashVariableOrder) {
                    HashComputation hashComputation = preferredHashSymbol.translate(outputToInputTranslator).get();
                    newInputVariables.add(child.getRequiredHashVariable(hashComputation));
                }

                newInputs.add(newInputVariables.build());
            }

            return new PlanWithProperties(
                    new ExchangeNode(
                            node.getSourceLocation(),
                            node.getId(),
                            node.getType(),
                            node.getScope(),
                            partitioningScheme,
                            newSources.build(),
                            newInputs.build(),
                            node.isEnsureSourceOrdering(),
                            node.getOrderingScheme()),
                    newHashVariables);
        }

        @Override
        public PlanWithProperties visitUnion(UnionNode node, HashComputationSet parentPreference)
        {
            // remove any hash variables not exported by this node
            HashComputationSet preference = parentPreference.pruneVariables(node.getOutputVariables());

            // create new hash variables
            Map<HashComputation, VariableReferenceExpression> newHashVariables = new HashMap<>();
            for (HashComputation preferredHashSymbol : preference.getHashes()) {
                newHashVariables.put(preferredHashSymbol, variableAllocator.newHashVariable());
            }

            // add hash variables to sources
            ImmutableListMultimap.Builder<VariableReferenceExpression, VariableReferenceExpression> newVariableMapping = ImmutableListMultimap.builder();
            node.getVariableMapping().forEach(newVariableMapping::putAll);
            ImmutableList.Builder<PlanNode> newSources = ImmutableList.builder();
            for (int sourceId = 0; sourceId < node.getSources().size(); sourceId++) {
                // translate preference to input variables
                Map<VariableReferenceExpression, VariableReferenceExpression> outputToInputMap = new HashMap<>();
                for (VariableReferenceExpression outputVariables : node.getOutputVariables()) {
                    outputToInputMap.put(outputVariables, node.getVariableMapping().get(outputVariables).get(sourceId));
                }
                Function<VariableReferenceExpression, Optional<VariableReferenceExpression>> outputToInputTranslator = variable -> Optional.of(outputToInputMap.get(variable));

                HashComputationSet sourcePreference = preference.translate(outputToInputTranslator);
                PlanWithProperties child = planAndEnforce(node.getSources().get(sourceId), sourcePreference, true, sourcePreference);
                newSources.add(child.getNode());

                // add hash variables to inputs
                for (Entry<HashComputation, VariableReferenceExpression> entry : newHashVariables.entrySet()) {
                    HashComputation hashComputation = entry.getKey().translate(outputToInputTranslator).get();
                    newVariableMapping.put(entry.getValue(), child.getRequiredHashVariable(hashComputation));
                }
            }

            ListMultimap<VariableReferenceExpression, VariableReferenceExpression> outputsToInputs = newVariableMapping.build();
            return new PlanWithProperties(
                    new UnionNode(
                            node.getSourceLocation(),
                            node.getId(),
                            newSources.build(),
                            ImmutableList.copyOf(outputsToInputs.keySet()),
                            fromListMultimap(outputsToInputs)),
                    newHashVariables);
        }

        @Override
        public PlanWithProperties visitProject(ProjectNode node, HashComputationSet parentPreference)
        {
            Map<VariableReferenceExpression, VariableReferenceExpression> outputToInputMapping = computeIdentityTranslations(node.getAssignments().getMap());
            HashComputationSet sourceContext = parentPreference.translate(variable -> Optional.ofNullable(outputToInputMapping.get(variable)));
            PlanWithProperties child = plan(node.getSource(), sourceContext);

            // create a new project node with all assignments from the original node
            Assignments.Builder newAssignments = Assignments.builder();
            newAssignments.putAll(node.getAssignments());

            // and all hash variables that could be translated to the source variables
            Map<VariableReferenceExpression, RowExpression> hashAssignments = new HashMap<>();
            Map<HashComputation, VariableReferenceExpression> allHashVariables = new HashMap<>();
            for (HashComputation hashComputation : sourceContext.getHashes()) {
                VariableReferenceExpression hashVariable = child.getHashVariables().get(hashComputation);
                RowExpression hashExpression;
                if (hashVariable == null) {
                    hashVariable = variableAllocator.newHashVariable();
                    hashExpression = hashComputation.getHashExpression();
                }
                else {
                    hashExpression = hashVariable;
                }
                hashAssignments.put(hashVariable, hashExpression);
                allHashVariables.put(hashComputation, hashVariable);
            }

            if (node.getLocality().equals(REMOTE) && !hashAssignments.isEmpty()) {
                // if the ProjectNode is remote, created a local projection with identity projection and hash
                Assignments.Builder localProjectionAssignments = Assignments.builder();
                child.getNode().getOutputVariables().forEach(variable -> localProjectionAssignments.put(variable, variable));
                localProjectionAssignments.putAll(hashAssignments);
                ProjectNode localProjectNode = new ProjectNode(child.getNode().getSourceLocation(), idAllocator.getNextId(), child.getNode(), localProjectionAssignments.build(), LOCAL);

                // add identity projection for hash variable to remote projection
                hashAssignments.keySet().forEach(variable -> newAssignments.put(variable, variable));
                return new PlanWithProperties(new ProjectNode(localProjectNode.getSourceLocation(), idAllocator.getNextId(), localProjectNode, newAssignments.build(), REMOTE), allHashVariables);
            }

            newAssignments.putAll(hashAssignments);
            return new PlanWithProperties(new ProjectNode(node.getSourceLocation(), node.getId(), child.getNode(), newAssignments.build(), node.getLocality()), allHashVariables);
        }

        @Override
        public PlanWithProperties visitUnnest(UnnestNode node, HashComputationSet parentPreference)
        {
            PlanWithProperties child = plan(node.getSource(), parentPreference.pruneVariables(node.getSource().getOutputVariables()));

            // only pass through hash variables requested by the parent
            Map<HashComputation, VariableReferenceExpression> hashVariables = new HashMap<>(child.getHashVariables());
            hashVariables.keySet().retainAll(parentPreference.getHashes());

            return new PlanWithProperties(
                    new UnnestNode(
                            node.getSourceLocation(),
                            node.getId(),
                            child.getNode(),
                            ImmutableList.<VariableReferenceExpression>builder()
                                    .addAll(node.getReplicateVariables())
                                    .addAll(hashVariables.values())
                                    .build(),
                            node.getUnnestVariables(),
                            node.getOrdinalityVariable()),
                    hashVariables);
        }

        private PlanWithProperties planSimpleNodeWithProperties(PlanNode node, HashComputationSet preferredHashes)
        {
            return planSimpleNodeWithProperties(node, preferredHashes, true);
        }

        private PlanWithProperties planSimpleNodeWithProperties(
                PlanNode node,
                HashComputationSet preferredHashes,
                boolean alwaysPruneExtraHashVariables)
        {
            if (node.getSources().isEmpty()) {
                return new PlanWithProperties(node, ImmutableMap.of());
            }

            // There is not requirement to produce hash variables and only preference for variables
            PlanWithProperties source = planAndEnforce(Iterables.getOnlyElement(node.getSources()), new HashComputationSet(), alwaysPruneExtraHashVariables, preferredHashes);
            PlanNode result = replaceChildren(node, ImmutableList.of(source.getNode()));

            // return only hash variables that are passed through the new node
            Map<HashComputation, VariableReferenceExpression> hashVariables = new HashMap<>(source.getHashVariables());
            hashVariables.values().retainAll(result.getOutputVariables());

            return new PlanWithProperties(result, hashVariables);
        }

        private PlanWithProperties planAndEnforce(
                PlanNode node,
                HashComputationSet requiredHashes,
                boolean pruneExtraHashVariables,
                HashComputationSet preferredHashes)
        {
            PlanWithProperties result = plan(node, preferredHashes);

            boolean preferenceSatisfied;
            if (pruneExtraHashVariables) {
                // Make sure that
                // (1) result has all required hashes
                // (2) any extra hashes are preferred hashes (e.g. no pruning is needed)
                Set<HashComputation> resultHashes = result.getHashVariables().keySet();
                Set<HashComputation> requiredAndPreferredHashes = ImmutableSet.<HashComputation>builder()
                        .addAll(requiredHashes.getHashes())
                        .addAll(preferredHashes.getHashes())
                        .build();
                preferenceSatisfied = resultHashes.containsAll(requiredHashes.getHashes()) &&
                        requiredAndPreferredHashes.containsAll(resultHashes);
            }
            else {
                preferenceSatisfied = result.getHashVariables().keySet().containsAll(requiredHashes.getHashes());
            }

            if (preferenceSatisfied) {
                return result;
            }

            return enforce(result, requiredHashes);
        }

        private PlanWithProperties enforce(PlanWithProperties planWithProperties, HashComputationSet requiredHashes)
        {
            Assignments.Builder assignments = Assignments.builder();

            Map<HashComputation, VariableReferenceExpression> outputHashVariables = new HashMap<>();

            // copy through all variables from child, except for hash variables not needed by the parent
            Map<VariableReferenceExpression, HashComputation> resultHashVariables = planWithProperties.getHashVariables().inverse();
            for (VariableReferenceExpression variable : planWithProperties.getNode().getOutputVariables()) {
                HashComputation partitionVariables = resultHashVariables.get(variable);
                if (partitionVariables == null || requiredHashes.getHashes().contains(partitionVariables)) {
                    assignments.put(variable, variable);

                    if (partitionVariables != null) {
                        outputHashVariables.put(partitionVariables, planWithProperties.getHashVariables().get(partitionVariables));
                    }
                }
            }

            // add new projections for hash variables needed by the parent
            for (HashComputation hashComputation : requiredHashes.getHashes()) {
                if (!planWithProperties.getHashVariables().containsKey(hashComputation)) {
                    RowExpression hashExpression = hashComputation.getHashExpression();
                    VariableReferenceExpression hashVariable = variableAllocator.newHashVariable();
                    assignments.put(hashVariable, hashExpression);
                    outputHashVariables.put(hashComputation, hashVariable);
                }
            }

            ProjectNode projectNode = new ProjectNode(planWithProperties.node.getSourceLocation(), idAllocator.getNextId(), planWithProperties.node.getStatsEquivalentPlanNode(), planWithProperties.getNode(), assignments.build(), LOCAL);
            return new PlanWithProperties(projectNode, outputHashVariables);
        }

        private PlanWithProperties plan(PlanNode node, HashComputationSet parentPreference)
        {
            PlanWithProperties result = accept(node, parentPreference);
            checkState(
                    result.getNode().getOutputVariables().containsAll(result.getHashVariables().values()),
                    "Node %s declares hash variables not in the output",
                    result.getNode().getClass().getSimpleName());
            return new PlanWithProperties(result.getNode().assignStatsEquivalentPlanNode(node.getStatsEquivalentPlanNode()), result.getHashVariables());
        }

        private PlanWithProperties accept(PlanNode node, HashComputationSet context)
        {
            PlanWithProperties result = node.accept(this, context);
            return new PlanWithProperties(
                    result.getNode().assignStatsEquivalentPlanNode(node.getStatsEquivalentPlanNode()),
                    result.getHashVariables());
        }
    }

    private static class HashComputationSet
    {
        private final Set<HashComputation> hashes;

        public HashComputationSet()
        {
            hashes = ImmutableSet.of();
        }

        public HashComputationSet(Optional<HashComputation> hash)
        {
            requireNonNull(hash, "hash is null");
            if (hash.isPresent()) {
                this.hashes = ImmutableSet.of(hash.get());
            }
            else {
                this.hashes = ImmutableSet.of();
            }
        }

        private HashComputationSet(Iterable<HashComputation> hashes)
        {
            requireNonNull(hashes, "hashes is null");
            this.hashes = ImmutableSet.copyOf(hashes);
        }

        public Set<HashComputation> getHashes()
        {
            return hashes;
        }

        public HashComputationSet pruneVariables(List<VariableReferenceExpression> variables)
        {
            Set<VariableReferenceExpression> uniqueVariables = ImmutableSet.copyOf(variables);
            return new HashComputationSet(hashes.stream()
                    .filter(hash -> hash.canComputeWith(uniqueVariables))
                    .collect(toImmutableSet()));
        }

        public HashComputationSet translate(Function<VariableReferenceExpression, Optional<VariableReferenceExpression>> translator)
        {
            Set<HashComputation> newHashes = hashes.stream()
                    .map(hash -> hash.translate(translator))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(toImmutableSet());

            return new HashComputationSet(newHashes);
        }

        public HashComputationSet withHashComputation(PlanNode node, Optional<HashComputation> hashComputation)
        {
            return pruneVariables(node.getOutputVariables()).withHashComputation(hashComputation);
        }

        public HashComputationSet withHashComputation(Optional<HashComputation> hashComputation)
        {
            if (!hashComputation.isPresent()) {
                return this;
            }
            return new HashComputationSet(ImmutableSet.<HashComputation>builder()
                    .addAll(hashes)
                    .add(hashComputation.get())
                    .build());
        }
    }

    private static Optional<HashComputation> computeHash(Iterable<VariableReferenceExpression> fields, FunctionAndTypeManager functionAndTypeManager)
    {
        requireNonNull(fields, "fields is null");
        List<VariableReferenceExpression> variables = ImmutableList.copyOf(fields);
        if (variables.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new HashComputation(fields, functionAndTypeManager));
    }

    private static class HashComputation
    {
        private final List<VariableReferenceExpression> fields;
        private final FunctionAndTypeManager functionAndTypeManager;

        private HashComputation(Iterable<VariableReferenceExpression> fields, FunctionAndTypeManager functionAndTypeManager)
        {
            requireNonNull(fields, "fields is null");
            this.fields = ImmutableList.copyOf(fields);
            this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionManager is null");
            checkArgument(!this.fields.isEmpty(), "fields can not be empty");
        }

        public Optional<HashComputation> translate(Function<VariableReferenceExpression, Optional<VariableReferenceExpression>> translator)
        {
            ImmutableList.Builder<VariableReferenceExpression> newVariables = ImmutableList.builder();
            for (VariableReferenceExpression field : fields) {
                Optional<VariableReferenceExpression> newVariable = translator.apply(field);
                if (!newVariable.isPresent()) {
                    return Optional.empty();
                }
                newVariables.add(newVariable.get());
            }
            return computeHash(newVariables.build(), functionAndTypeManager);
        }

        public boolean canComputeWith(Set<VariableReferenceExpression> availableFields)
        {
            return availableFields.containsAll(fields);
        }

        public boolean isSingleBigIntVariable()
        {
            return fields.size() == 1 && Iterables.getOnlyElement(fields).getType().equals(BIGINT);
        }

        private RowExpression getHashExpression()
        {
            RowExpression hashExpression = constant(INITIAL_HASH_VALUE, BIGINT);
            for (VariableReferenceExpression field : fields) {
                hashExpression = getHashFunctionCall(hashExpression, field);
            }
            return hashExpression;
        }

        private RowExpression getHashFunctionCall(RowExpression previousHashValue, VariableReferenceExpression variable)
        {
            CallExpression functionCall = call(functionAndTypeManager, HASH_CODE, BIGINT, variable);
            return call(functionAndTypeManager, "combine_hash", BIGINT, previousHashValue, orNullHashCode(functionCall));
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
            HashComputation that = (HashComputation) o;
            return Objects.equals(fields, that.fields);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(fields);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("fields", fields)
                    .toString();
        }
    }

    private static class PlanWithProperties
    {
        private final PlanNode node;
        private final BiMap<HashComputation, VariableReferenceExpression> hashVariables;

        public PlanWithProperties(PlanNode node, Map<HashComputation, VariableReferenceExpression> hashVariables)
        {
            this.node = requireNonNull(node, "node is null");
            this.hashVariables = ImmutableBiMap.copyOf(requireNonNull(hashVariables, "hashVariables is null"));
        }

        public PlanNode getNode()
        {
            return node;
        }

        public BiMap<HashComputation, VariableReferenceExpression> getHashVariables()
        {
            return hashVariables;
        }

        public VariableReferenceExpression getRequiredHashVariable(HashComputation hash)
        {
            VariableReferenceExpression hashVariable = hashVariables.get(hash);
            requireNonNull(hashVariable, () -> "No hash variable generated for " + hash);
            return hashVariable;
        }
    }

    private static Map<VariableReferenceExpression, VariableReferenceExpression> computeIdentityTranslations(Map<VariableReferenceExpression, RowExpression> assignments)
    {
        Map<VariableReferenceExpression, VariableReferenceExpression> outputToInput = new HashMap<>();
        for (Map.Entry<VariableReferenceExpression, RowExpression> assignment : assignments.entrySet()) {
            if (assignment.getValue() instanceof VariableReferenceExpression) {
                outputToInput.put(assignment.getKey(), (VariableReferenceExpression) assignment.getValue());
            }
        }
        return outputToInput;
    }
}
