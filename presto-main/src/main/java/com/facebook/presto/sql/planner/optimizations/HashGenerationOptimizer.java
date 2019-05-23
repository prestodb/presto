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
import com.facebook.presto.execution.warnings.WarningCollector;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.sql.planner.Partitioning.ArgumentBinding;
import com.facebook.presto.sql.planner.PartitioningScheme;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.DistinctLimitNode;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.GroupIdNode;
import com.facebook.presto.sql.planner.plan.IndexJoinNode;
import com.facebook.presto.sql.planner.plan.InternalPlanVisitor;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LateralJoinNode;
import com.facebook.presto.sql.planner.plan.MarkDistinctNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.RowNumberNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.SpatialJoinNode;
import com.facebook.presto.sql.planner.plan.TopNRowNumberNode;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.facebook.presto.sql.planner.plan.UnnestNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.tree.CoalesceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GenericLiteral;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.facebook.presto.metadata.OperatorSignatureUtils.mangleOperatorName;
import static com.facebook.presto.spi.function.FunctionKind.SCALAR;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static com.facebook.presto.sql.planner.optimizations.AddExchanges.toVariableReferences;
import static com.facebook.presto.sql.planner.plan.ChildReplacer.replaceChildren;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.LEFT;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.RIGHT;
import static com.facebook.presto.type.TypeUtils.NULL_HASH_CODE;
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
    public static final int INITIAL_HASH_VALUE = 0;
    private static final String HASH_CODE = mangleOperatorName("HASH_CODE");
    private static final Signature COMBINE_HASH = new Signature(
            "combine_hash",
            SCALAR,
            BIGINT.getTypeSignature(),
            ImmutableList.of(BIGINT.getTypeSignature(), BIGINT.getTypeSignature()));

    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(types, "types is null");
        requireNonNull(symbolAllocator, "symbolAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");
        if (SystemSessionProperties.isOptimizeHashGenerationEnabled(session)) {
            PlanWithProperties result = plan.accept(new Rewriter(idAllocator, symbolAllocator, types), new HashComputationSet());
            return result.getNode();
        }
        return plan;
    }

    private static class Rewriter
            extends InternalPlanVisitor<PlanWithProperties, HashComputationSet>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final SymbolAllocator symbolAllocator;
        private final TypeProvider types;

        private Rewriter(PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, TypeProvider types)
        {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
            this.types = requireNonNull(types, "types is null");
        }

        @Override
        protected PlanWithProperties visitPlan(PlanNode node, HashComputationSet parentPreference)
        {
            return planSimpleNodeWithProperties(node, parentPreference);
        }

        @Override
        public PlanWithProperties visitEnforceSingleRow(EnforceSingleRowNode node, HashComputationSet parentPreference)
        {
            // this plan node can only have a single input symbol, so do not add extra hash symbols
            return planSimpleNodeWithProperties(node, new HashComputationSet(), true);
        }

        @Override
        public PlanWithProperties visitApply(ApplyNode node, HashComputationSet context)
        {
            // Apply node is not supported by execution, so do not rewrite it
            // that way query will fail in sanity checkers
            return new PlanWithProperties(node, ImmutableMap.of());
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
            List<Symbol> groupingKeys = node.getGroupingKeys().stream().map(VariableReferenceExpression::getName).map(Symbol::new).collect(toImmutableList());
            if (!node.isStreamable() && !canSkipHashGeneration(node.getGroupingKeys())) {
                groupByHash = computeHash(groupingKeys);
            }

            // aggregation does not pass through preferred hash symbols
            HashComputationSet requiredHashes = new HashComputationSet(groupByHash);
            PlanWithProperties child = planAndEnforce(node.getSource(), requiredHashes, false, requiredHashes);

            Optional<VariableReferenceExpression> hashVariable = groupByHash.map(child::getRequiredHashVariable);

            return new PlanWithProperties(
                    new AggregationNode(
                            node.getId(),
                            child.getNode(),
                            node.getAggregations(),
                            node.getGroupingSets(),
                            node.getPreGroupedVariables(),
                            node.getStep(),
                            hashVariable,
                            node.getGroupIdVariable()),
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
            // remove any hash symbols not exported by the source of this node
            return planSimpleNodeWithProperties(node, parentPreference.pruneSymbols(node.getSource().getOutputSymbols()));
        }

        @Override
        public PlanWithProperties visitDistinctLimit(DistinctLimitNode node, HashComputationSet parentPreference)
        {
            // skip hash symbol generation for single bigint
            if (canSkipHashGeneration(node.getDistinctVariables())) {
                return planSimpleNodeWithProperties(node, parentPreference);
            }

            Optional<HashComputation> hashComputation = computeHash(node.getDistinctVariables().stream().map(VariableReferenceExpression::getName).map(Symbol::new).collect(toImmutableList()));
            PlanWithProperties child = planAndEnforce(
                    node.getSource(),
                    new HashComputationSet(hashComputation),
                    false,
                    parentPreference.withHashComputation(node, hashComputation));
            VariableReferenceExpression hashVariable = child.getRequiredHashVariable(hashComputation.get());

            // TODO: we need to reason about how pre-computed hashes from child relate to distinct symbols. We should be able to include any precomputed hash
            // that's functionally dependent on the distinct field in the set of distinct fields of the new node to be able to propagate it downstream.
            // Currently, such precomputed hashes will be dropped by this operation.
            return new PlanWithProperties(
                    new DistinctLimitNode(node.getId(), child.getNode(), node.getLimit(), node.isPartial(), node.getDistinctVariables(), Optional.of(hashVariable)),
                    ImmutableMap.of(hashComputation.get(), hashVariable));
        }

        @Override
        public PlanWithProperties visitMarkDistinct(MarkDistinctNode node, HashComputationSet parentPreference)
        {
            // skip hash symbol generation for single bigint
            if (canSkipHashGeneration(node.getDistinctVariables())) {
                return planSimpleNodeWithProperties(node, parentPreference);
            }

            Optional<HashComputation> hashComputation = computeHash(node.getDistinctVariables().stream().map(VariableReferenceExpression::getName).map(Symbol::new).collect(toImmutableList()));
            PlanWithProperties child = planAndEnforce(
                    node.getSource(),
                    new HashComputationSet(hashComputation),
                    false,
                    parentPreference.withHashComputation(node, hashComputation));
            VariableReferenceExpression hashVariable = child.getRequiredHashVariable(hashComputation.get());

            return new PlanWithProperties(
                    new MarkDistinctNode(node.getId(), child.getNode(), node.getMarkerVariable(), node.getDistinctVariables(), Optional.of(hashVariable)),
                    child.getHashVariables());
        }

        @Override
        public PlanWithProperties visitRowNumber(RowNumberNode node, HashComputationSet parentPreference)
        {
            if (node.getPartitionBy().isEmpty()) {
                return planSimpleNodeWithProperties(node, parentPreference);
            }

            Optional<HashComputation> hashComputation = computeHash(node.getPartitionBy().stream().map(VariableReferenceExpression::getName).map(Symbol::new).collect(toImmutableList()));
            PlanWithProperties child = planAndEnforce(
                    node.getSource(),
                    new HashComputationSet(hashComputation),
                    false,
                    parentPreference.withHashComputation(node, hashComputation));
            VariableReferenceExpression hashVariable = child.getRequiredHashVariable(hashComputation.get());

            return new PlanWithProperties(
                    new RowNumberNode(
                            node.getId(),
                            child.getNode(),
                            node.getPartitionBy(),
                            node.getRowNumberVariable(),
                            node.getMaxRowCountPerPartition(),
                            Optional.of(hashVariable)),
                    child.getHashVariables());
        }

        @Override
        public PlanWithProperties visitTopNRowNumber(TopNRowNumberNode node, HashComputationSet parentPreference)
        {
            if (node.getPartitionBy().isEmpty()) {
                return planSimpleNodeWithProperties(node, parentPreference);
            }

            Optional<HashComputation> hashComputation = computeHash(node.getPartitionBy().stream().map(VariableReferenceExpression::getName).map(Symbol::new).collect(toImmutableList()));
            PlanWithProperties child = planAndEnforce(
                    node.getSource(),
                    new HashComputationSet(hashComputation),
                    false,
                    parentPreference.withHashComputation(node, hashComputation));
            VariableReferenceExpression hashVariable = child.getRequiredHashVariable(hashComputation.get());

            return new PlanWithProperties(
                    new TopNRowNumberNode(
                            node.getId(),
                            child.getNode(),
                            node.getSpecification(),
                            node.getRowNumberVariable(),
                            node.getMaxRowCountPerPartition(),
                            node.isPartial(),
                            Optional.of(hashVariable)),
                    child.getHashVariables());
        }

        @Override
        public PlanWithProperties visitJoin(JoinNode node, HashComputationSet parentPreference)
        {
            List<JoinNode.EquiJoinClause> clauses = node.getCriteria();
            if (clauses.isEmpty()) {
                // join does not pass through preferred hash symbols since they take more memory and since
                // the join node filters, may take more compute
                PlanWithProperties left = planAndEnforce(node.getLeft(), new HashComputationSet(), true, new HashComputationSet());
                PlanWithProperties right = planAndEnforce(node.getRight(), new HashComputationSet(), true, new HashComputationSet());
                checkState(left.getHashVariables().isEmpty() && right.getHashVariables().isEmpty());
                return new PlanWithProperties(
                        replaceChildren(node, ImmutableList.of(left.getNode(), right.getNode())),
                        ImmutableMap.of());
            }

            // join does not pass through preferred hash symbols since they take more memory and since
            // the join node filters, may take more compute
            Optional<HashComputation> leftHashComputation = computeHash(Lists.transform(clauses, JoinNode.EquiJoinClause::getLeft).stream().map(VariableReferenceExpression::getName).map(Symbol::new).collect(toImmutableList()));
            PlanWithProperties left = planAndEnforce(node.getLeft(), new HashComputationSet(leftHashComputation), true, new HashComputationSet(leftHashComputation));
            VariableReferenceExpression leftHashVariable = left.getRequiredHashVariable(leftHashComputation.get());

            Optional<HashComputation> rightHashComputation = computeHash(Lists.transform(clauses, JoinNode.EquiJoinClause::getRight).stream().map(VariableReferenceExpression::getName).map(Symbol::new).collect(toImmutableList()));
            // drop undesired hash symbols from build to save memory
            PlanWithProperties right = planAndEnforce(node.getRight(), new HashComputationSet(rightHashComputation), true, new HashComputationSet(rightHashComputation));
            VariableReferenceExpression rightHashVariable = right.getRequiredHashVariable(rightHashComputation.get());

            // build map of all hash symbols
            // NOTE: Full outer join doesn't use hash symbols
            Map<HashComputation, VariableReferenceExpression> allHashVariables = new HashMap<>();
            if (node.getType() == INNER || node.getType() == LEFT) {
                allHashVariables.putAll(left.getHashVariables());
            }
            if (node.getType() == INNER || node.getType() == RIGHT) {
                allHashVariables.putAll(right.getHashVariables());
            }

            return buildJoinNodeWithPreferredHashes(node, left, right, allHashVariables, parentPreference, Optional.of(leftHashVariable), Optional.of(rightHashVariable));
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
            // retain only hash symbols preferred by parent nodes
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
                            node.getId(),
                            node.getType(),
                            left.getNode(),
                            right.getNode(),
                            node.getCriteria(),
                            outputVariables,
                            node.getFilter(),
                            leftHashVariable,
                            rightHashVariable,
                            node.getDistributionType()),
                    hashVariablesWithParentPreferences);
        }

        @Override
        public PlanWithProperties visitSemiJoin(SemiJoinNode node, HashComputationSet parentPreference)
        {
            Optional<HashComputation> sourceHashComputation = computeHash(ImmutableList.of(new Symbol(node.getSourceJoinVariable().getName())));
            PlanWithProperties source = planAndEnforce(
                    node.getSource(),
                    new HashComputationSet(sourceHashComputation),
                    true,
                    new HashComputationSet(sourceHashComputation));
            VariableReferenceExpression sourceHashVariable = source.getRequiredHashVariable(sourceHashComputation.get());

            Optional<HashComputation> filterHashComputation = computeHash(ImmutableList.of(new Symbol(node.getFilteringSourceJoinVariable().getName())));
            HashComputationSet requiredHashes = new HashComputationSet(filterHashComputation);
            PlanWithProperties filteringSource = planAndEnforce(node.getFilteringSource(), requiredHashes, true, requiredHashes);
            VariableReferenceExpression filteringSourceHashVariable = filteringSource.getRequiredHashVariable(filterHashComputation.get());

            return new PlanWithProperties(
                    new SemiJoinNode(
                            node.getId(),
                            source.getNode(),
                            filteringSource.getNode(),
                            node.getSourceJoinVariable(),
                            node.getFilteringSourceJoinVariable(),
                            node.getSemiJoinOutput(),
                            Optional.of(sourceHashVariable),
                            Optional.of(filteringSourceHashVariable),
                            node.getDistributionType()),
                    source.getHashVariables());
        }

        @Override
        public PlanWithProperties visitSpatialJoin(SpatialJoinNode node, HashComputationSet parentPreference)
        {
            PlanWithProperties left = planAndEnforce(node.getLeft(), new HashComputationSet(), true, new HashComputationSet());
            PlanWithProperties right = planAndEnforce(node.getRight(), new HashComputationSet(), true, new HashComputationSet());
            verify(left.getHashVariables().isEmpty(), "probe side of the spatial join should not include hash symbols");
            verify(right.getHashVariables().isEmpty(), "build side of the spatial join should not include hash symbols");
            return new PlanWithProperties(
                    replaceChildren(node, ImmutableList.of(left.getNode(), right.getNode())),
                    ImmutableMap.of());
        }

        @Override
        public PlanWithProperties visitIndexJoin(IndexJoinNode node, HashComputationSet parentPreference)
        {
            List<IndexJoinNode.EquiJoinClause> clauses = node.getCriteria();

            // join does not pass through preferred hash symbols since they take more memory and since
            // the join node filters, may take more compute
            Optional<HashComputation> probeHashComputation = computeHash(clauses.stream().map(equiJoin -> new Symbol(equiJoin.getProbe().getName())).collect(toImmutableList()));
            PlanWithProperties probe = planAndEnforce(
                    node.getProbeSource(),
                    new HashComputationSet(probeHashComputation),
                    true,
                    new HashComputationSet(probeHashComputation));
            VariableReferenceExpression probeHashVariable = probe.getRequiredHashVariable(probeHashComputation.get());

            Optional<HashComputation> indexHashComputation = computeHash(clauses.stream().map(equiJoin -> new Symbol(equiJoin.getIndex().getName())).collect(toImmutableList()));
            HashComputationSet requiredHashes = new HashComputationSet(indexHashComputation);
            PlanWithProperties index = planAndEnforce(node.getIndexSource(), requiredHashes, true, requiredHashes);
            VariableReferenceExpression indexHashVariable = index.getRequiredHashVariable(indexHashComputation.get());

            // build map of all hash symbols
            Map<HashComputation, VariableReferenceExpression> allHashVariables = new HashMap<>();
            if (node.getType() == IndexJoinNode.Type.INNER) {
                allHashVariables.putAll(probe.getHashVariables());
            }
            allHashVariables.putAll(index.getHashVariables());

            return new PlanWithProperties(
                    new IndexJoinNode(
                            node.getId(),
                            node.getType(),
                            probe.getNode(),
                            index.getNode(),
                            node.getCriteria(),
                            Optional.of(probeHashVariable),
                            Optional.of(indexHashVariable)),
                    allHashVariables);
        }

        @Override
        public PlanWithProperties visitWindow(WindowNode node, HashComputationSet parentPreference)
        {
            if (node.getPartitionBy().isEmpty()) {
                return planSimpleNodeWithProperties(node, parentPreference, true);
            }

            Optional<HashComputation> hashComputation = computeHash(node.getPartitionBy().stream().map(VariableReferenceExpression::getName).map(Symbol::new).collect(toImmutableList()));
            PlanWithProperties child = planAndEnforce(
                    node.getSource(),
                    new HashComputationSet(hashComputation),
                    true,
                    parentPreference.withHashComputation(node, hashComputation));

            VariableReferenceExpression hashSymbol = child.getRequiredHashVariable(hashComputation.get());

            return new PlanWithProperties(
                    new WindowNode(
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
            // remove any hash symbols not exported by this node
            HashComputationSet preference = parentPreference.pruneSymbols(node.getOutputSymbols());

            // Currently, precomputed hash values are only supported for system hash distributions without constants
            Optional<HashComputation> partitionVariables = Optional.empty();
            PartitioningScheme partitioningScheme = node.getPartitioningScheme();
            if (partitioningScheme.getPartitioning().getHandle().equals(FIXED_HASH_DISTRIBUTION) &&
                    partitioningScheme.getPartitioning().getArguments().stream().allMatch(ArgumentBinding::isVariable)) {
                // add precomputed hash for exchange
                partitionVariables = computeHash(partitioningScheme.getPartitioning().getArguments().stream()
                        .map(ArgumentBinding::getColumn)
                        .collect(toImmutableList()));
                preference = preference.withHashComputation(partitionVariables);
            }

            // establish fixed ordering for hash symbols
            List<HashComputation> hashVariableOrder = ImmutableList.copyOf(preference.getHashes());
            Map<HashComputation, VariableReferenceExpression> newHashVariables = new HashMap<>();
            for (HashComputation preferredHashVariable : hashVariableOrder) {
                newHashVariables.put(preferredHashVariable, symbolAllocator.newHashVariable());
            }

            // rewrite partition function to include new symbols (and precomputed hash
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
                    partitioningScheme.getBucketToPartition());

            // add hash symbols to sources
            ImmutableList.Builder<List<Symbol>> newInputs = ImmutableList.builder();
            ImmutableList.Builder<PlanNode> newSources = ImmutableList.builder();
            for (int sourceId = 0; sourceId < node.getSources().size(); sourceId++) {
                PlanNode source = node.getSources().get(sourceId);
                List<Symbol> inputSymbols = node.getInputs().get(sourceId).stream().map(variable -> new Symbol(variable.getName())).collect(toImmutableList());

                Map<Symbol, Symbol> outputToInputMap = new HashMap<>();
                for (int symbolId = 0; symbolId < inputSymbols.size(); symbolId++) {
                    outputToInputMap.put(node.getOutputSymbols().get(symbolId), inputSymbols.get(symbolId));
                }
                Function<Symbol, Optional<Symbol>> outputToInputTranslator = symbol -> Optional.of(outputToInputMap.get(symbol));

                HashComputationSet sourceContext = preference.translate(outputToInputTranslator);
                PlanWithProperties child = planAndEnforce(source, sourceContext, true, sourceContext);
                newSources.add(child.getNode());

                // add hash symbols to inputs in the required order
                ImmutableList.Builder<Symbol> newInputSymbols = ImmutableList.builder();
                newInputSymbols.addAll(inputSymbols);
                for (HashComputation preferredHashSymbol : hashVariableOrder) {
                    HashComputation hashComputation = preferredHashSymbol.translate(outputToInputTranslator).get();
                    newInputSymbols.add(new Symbol(child.getRequiredHashVariable(hashComputation).getName()));
                }

                newInputs.add(newInputSymbols.build());
            }

            return new PlanWithProperties(
                    new ExchangeNode(
                            node.getId(),
                            node.getType(),
                            node.getScope(),
                            partitioningScheme,
                            newSources.build(),
                            newInputs.build().stream().map(input -> toVariableReferences(input, symbolAllocator.getTypes())).collect(toImmutableList()),
                            node.getOrderingScheme()),
                    newHashVariables);
        }

        @Override
        public PlanWithProperties visitUnion(UnionNode node, HashComputationSet parentPreference)
        {
            // remove any hash symbols not exported by this node
            HashComputationSet preference = parentPreference.pruneSymbols(node.getOutputSymbols());

            // create new hash symbols
            Map<HashComputation, VariableReferenceExpression> newHashVariables = new HashMap<>();
            for (HashComputation preferredHashSymbol : preference.getHashes()) {
                newHashVariables.put(preferredHashSymbol, symbolAllocator.newHashVariable());
            }

            // add hash symbols to sources
            ImmutableListMultimap.Builder<VariableReferenceExpression, VariableReferenceExpression> newVariableMapping = ImmutableListMultimap.builder();
            newVariableMapping.putAll(node.getVariableMapping());
            ImmutableList.Builder<PlanNode> newSources = ImmutableList.builder();
            for (int sourceId = 0; sourceId < node.getSources().size(); sourceId++) {
                // translate preference to input symbols
                Map<VariableReferenceExpression, VariableReferenceExpression> outputToInputMap = new HashMap<>();
                for (VariableReferenceExpression outputVariables : node.getOutputVariables()) {
                    outputToInputMap.put(outputVariables, node.getVariableMapping().get(outputVariables).get(sourceId));
                }
                Map<Symbol, Symbol> outputToInputSymbolMap = outputToInputMap.entrySet().stream()
                        .collect(toImmutableMap(entry -> new Symbol(entry.getKey().getName()), entry -> new Symbol(entry.getValue().getName())));
                Function<Symbol, Optional<Symbol>> outputToInputTranslator = symbol -> Optional.of(outputToInputSymbolMap.get(symbol));

                HashComputationSet sourcePreference = preference.translate(outputToInputTranslator);
                PlanWithProperties child = planAndEnforce(node.getSources().get(sourceId), sourcePreference, true, sourcePreference);
                newSources.add(child.getNode());

                // add hash symbols to inputs
                for (Entry<HashComputation, VariableReferenceExpression> entry : newHashVariables.entrySet()) {
                    HashComputation hashComputation = entry.getKey().translate(outputToInputTranslator).get();
                    newVariableMapping.put(entry.getValue(), child.getRequiredHashVariable(hashComputation));
                }
            }

            return new PlanWithProperties(
                    new UnionNode(
                            node.getId(),
                            newSources.build(),
                            newVariableMapping.build()),
                    newHashVariables);
        }

        @Override
        public PlanWithProperties visitProject(ProjectNode node, HashComputationSet parentPreference)
        {
            Map<VariableReferenceExpression, VariableReferenceExpression> outputToInputMapping = computeIdentityTranslations(node.getAssignments().getMap());
            Map<Symbol, Symbol> outputToInputSymbolMapping = outputToInputMapping.entrySet().stream().collect(toImmutableMap(entry -> new Symbol(entry.getKey().getName()), entry -> new Symbol(entry.getValue().getName())));
            HashComputationSet sourceContext = parentPreference.translate(symbol -> Optional.ofNullable(outputToInputSymbolMapping.get(symbol)));
            PlanWithProperties child = plan(node.getSource(), sourceContext);

            // create a new project node with all assignments from the original node
            Assignments.Builder newAssignments = Assignments.builder();
            newAssignments.putAll(node.getAssignments());

            // and all hash symbols that could be translated to the source symbols
            Map<HashComputation, VariableReferenceExpression> allHashVariables = new HashMap<>();
            for (HashComputation hashComputation : sourceContext.getHashes()) {
                VariableReferenceExpression hashVariable = child.getHashVariables().get(hashComputation);
                Expression hashExpression;
                if (hashVariable == null) {
                    hashVariable = symbolAllocator.newHashVariable();
                    hashExpression = hashComputation.getHashExpression();
                }
                else {
                    hashExpression = (new Symbol(hashVariable.getName())).toSymbolReference();
                }
                newAssignments.put(hashVariable, hashExpression);
                allHashVariables.put(hashComputation, hashVariable);
            }

            return new PlanWithProperties(new ProjectNode(node.getId(), child.getNode(), newAssignments.build()), allHashVariables);
        }

        @Override
        public PlanWithProperties visitUnnest(UnnestNode node, HashComputationSet parentPreference)
        {
            PlanWithProperties child = plan(node.getSource(), parentPreference.pruneSymbols(node.getSource().getOutputSymbols()));

            // only pass through hash symbols requested by the parent
            Map<HashComputation, VariableReferenceExpression> hashVariables = new HashMap<>(child.getHashVariables());
            hashVariables.keySet().retainAll(parentPreference.getHashes());

            return new PlanWithProperties(
                    new UnnestNode(
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
                boolean alwaysPruneExtraHashSymbols)
        {
            if (node.getSources().isEmpty()) {
                return new PlanWithProperties(node, ImmutableMap.of());
            }

            // There is not requirement to produce hash symbols and only preference for symbols
            PlanWithProperties source = planAndEnforce(Iterables.getOnlyElement(node.getSources()), new HashComputationSet(), alwaysPruneExtraHashSymbols, preferredHashes);
            PlanNode result = replaceChildren(node, ImmutableList.of(source.getNode()));

            // return only hash symbols that are passed through the new node
            Map<HashComputation, VariableReferenceExpression> hashVariable = new HashMap<>(source.getHashVariables());
            List<String> resultOutputSymbolNames = result.getOutputSymbols().stream()
                    .map(Symbol::getName).collect(toImmutableList());
            hashVariable.values().removeIf(variable -> !resultOutputSymbolNames.contains(variable.getName()));

            return new PlanWithProperties(result, hashVariable);
        }

        private PlanWithProperties planAndEnforce(
                PlanNode node,
                HashComputationSet requiredHashes,
                boolean pruneExtraHashSymbols,
                HashComputationSet preferredHashes)
        {
            PlanWithProperties result = plan(node, preferredHashes);

            boolean preferenceSatisfied;
            if (pruneExtraHashSymbols) {
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

            // copy through all symbols from child, except for hash symbols not needed by the parent
            Map<Symbol, HashComputation> resultHashSymbols = planWithProperties.getHashVariables().inverse().entrySet().stream()
                    .collect(toImmutableMap(entry -> new Symbol(entry.getKey().getName()), Entry::getValue));
            for (Symbol symbol : planWithProperties.getNode().getOutputSymbols()) {
                HashComputation partitionSymbols = resultHashSymbols.get(symbol);
                if (partitionSymbols == null || requiredHashes.getHashes().contains(partitionSymbols)) {
                    assignments.put(symbolAllocator.toVariableReference(symbol), symbol.toSymbolReference());

                    if (partitionSymbols != null) {
                        outputHashVariables.put(partitionSymbols, planWithProperties.getHashVariables().get(partitionSymbols));
                    }
                }
            }

            // add new projections for hash symbols needed by the parent
            for (HashComputation hashComputation : requiredHashes.getHashes()) {
                if (!planWithProperties.getHashVariables().containsKey(hashComputation)) {
                    Expression hashExpression = hashComputation.getHashExpression();
                    VariableReferenceExpression hashVariable = symbolAllocator.newHashVariable();
                    assignments.put(hashVariable, hashExpression);
                    outputHashVariables.put(hashComputation, hashVariable);
                }
            }

            ProjectNode projectNode = new ProjectNode(idAllocator.getNextId(), planWithProperties.getNode(), assignments.build());
            return new PlanWithProperties(projectNode, outputHashVariables);
        }

        private PlanWithProperties plan(PlanNode node, HashComputationSet parentPreference)
        {
            PlanWithProperties result = node.accept(this, parentPreference);
            checkState(
                    result.getNode().getOutputSymbols().containsAll(result.getHashVariables().values().stream().map(variable -> new Symbol(variable.getName())).collect(toImmutableSet())),
                    "Node %s declares hash symbols not in the output",
                    result.getNode().getClass().getSimpleName());
            return result;
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

        public HashComputationSet pruneSymbols(List<Symbol> symbols)
        {
            Set<Symbol> uniqueSymbols = ImmutableSet.copyOf(symbols);
            return new HashComputationSet(hashes.stream()
                    .filter(hash -> hash.canComputeWith(uniqueSymbols))
                    .collect(toImmutableSet()));
        }

        public HashComputationSet translate(Function<Symbol, Optional<Symbol>> translator)
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
            return pruneSymbols(node.getOutputSymbols()).withHashComputation(hashComputation);
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

    public static Optional<HashComputation> computeHash(Iterable<Symbol> fields)
    {
        requireNonNull(fields, "fields is null");
        List<Symbol> symbols = ImmutableList.copyOf(fields);
        if (symbols.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new HashComputation(fields));
    }

    public static Optional<Expression> getHashExpression(List<Symbol> symbols)
    {
        if (symbols.isEmpty()) {
            return Optional.empty();
        }

        Expression result = new GenericLiteral(StandardTypes.BIGINT, String.valueOf(INITIAL_HASH_VALUE));
        for (Symbol symbol : symbols) {
            Expression hashField = new FunctionCall(
                    QualifiedName.of(HASH_CODE),
                    Optional.empty(),
                    false,
                    ImmutableList.of(new SymbolReference(symbol.getName())));

            hashField = new CoalesceExpression(hashField, new LongLiteral(String.valueOf(NULL_HASH_CODE)));

            result = new FunctionCall(QualifiedName.of("combine_hash"), ImmutableList.of(result, hashField));
        }
        return Optional.of(result);
    }

    private static class HashComputation
    {
        private final List<Symbol> fields;

        private HashComputation(Iterable<Symbol> fields)
        {
            requireNonNull(fields, "fields is null");
            this.fields = ImmutableList.copyOf(fields);
            checkArgument(!this.fields.isEmpty(), "fields can not be empty");
        }

        public List<Symbol> getFields()
        {
            return fields;
        }

        public Optional<HashComputation> translate(Function<Symbol, Optional<Symbol>> translator)
        {
            ImmutableList.Builder<Symbol> newSymbols = ImmutableList.builder();
            for (Symbol field : fields) {
                Optional<Symbol> newSymbol = translator.apply(field);
                if (!newSymbol.isPresent()) {
                    return Optional.empty();
                }
                newSymbols.add(newSymbol.get());
            }
            return computeHash(newSymbols.build());
        }

        public boolean canComputeWith(Set<Symbol> availableFields)
        {
            return availableFields.containsAll(fields);
        }

        private Expression getHashExpression()
        {
            Expression hashExpression = new GenericLiteral(StandardTypes.BIGINT, Integer.toString(INITIAL_HASH_VALUE));
            for (Symbol field : fields) {
                hashExpression = getHashFunctionCall(hashExpression, field);
            }
            return hashExpression;
        }

        private static Expression getHashFunctionCall(Expression previousHashValue, Symbol symbol)
        {
            FunctionCall functionCall = new FunctionCall(
                    QualifiedName.of(HASH_CODE),
                    Optional.empty(),
                    false,
                    ImmutableList.of(symbol.toSymbolReference()));
            List<Expression> arguments = ImmutableList.of(previousHashValue, orNullHashCode(functionCall));
            return new FunctionCall(QualifiedName.of("combine_hash"), arguments);
        }

        private static Expression orNullHashCode(Expression expression)
        {
            return new CoalesceExpression(expression, new LongLiteral(String.valueOf(NULL_HASH_CODE)));
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

    private static Map<VariableReferenceExpression, VariableReferenceExpression> computeIdentityTranslations(Map<VariableReferenceExpression, Expression> assignments)
    {
        Map<VariableReferenceExpression, VariableReferenceExpression> outputToInput = new HashMap<>();
        for (Map.Entry<VariableReferenceExpression, Expression> assignment : assignments.entrySet()) {
            if (assignment.getValue() instanceof SymbolReference) {
                outputToInput.put(assignment.getKey(), new VariableReferenceExpression(((SymbolReference) assignment.getValue()).getName(), assignment.getKey().getType()));
            }
        }
        return outputToInput;
    }
}
