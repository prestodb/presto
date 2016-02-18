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
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.PartitionFunctionBinding;
import com.facebook.presto.sql.planner.PartitionFunctionBinding.PartitionFunctionArgumentBinding;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.DistinctLimitNode;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.GroupIdNode;
import com.facebook.presto.sql.planner.plan.IndexJoinNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.MarkDistinctNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.RowNumberNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.TopNRowNumberNode;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.facebook.presto.sql.planner.plan.UnnestNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.tree.CoalesceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.Window;
import com.facebook.presto.type.TypeUtils;
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
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static com.facebook.presto.sql.planner.plan.ChildReplacer.replaceChildren;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.LEFT;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.RIGHT;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableSet;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class HashGenerationOptimizer
        extends PlanOptimizer
{
    public static final int INITIAL_HASH_VALUE = 0;
    private static final String HASH_CODE = FunctionRegistry.mangleOperatorName("HASH_CODE");

    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(types, "types is null");
        requireNonNull(symbolAllocator, "symbolAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");
        if (SystemSessionProperties.isOptimizeHashGenerationEnabled(session)) {
            PlanWithProperties result = plan.accept(new Rewriter(idAllocator, symbolAllocator, types), new HashSymbolPreference());
            return result.getNode();
        }
        return plan;
    }

    private static class HashSymbolPreference
    {
        private final Set<List<Symbol>> preferredHashSymbols;

        public HashSymbolPreference()
        {
            preferredHashSymbols = ImmutableSet.of();
        }

        public HashSymbolPreference(List<Symbol> preferredHashSymbol)
        {
            this.preferredHashSymbols = ImmutableSet.of(preferredHashSymbol);
        }

        private HashSymbolPreference(Set<List<Symbol>> preferredHashSymbols)
        {
            this.preferredHashSymbols = ImmutableSet.copyOf(preferredHashSymbols);
        }

        public Set<List<Symbol>> getPreferredHashSymbols()
        {
            return preferredHashSymbols;
        }

        public HashSymbolPreference pruneSymbols(List<Symbol> outputSymbols)
        {
            Set<Symbol> uniqueOutputSymbols = ImmutableSet.copyOf(outputSymbols);
            return new HashSymbolPreference(preferredHashSymbols.stream()
                    .filter(uniqueOutputSymbols::containsAll)
                    .collect(toImmutableSet()));
        }

        public HashSymbolPreference translate(Function<Symbol, Optional<Symbol>> translator)
        {
            ImmutableSet.Builder<List<Symbol>> newPreferredHashSymbols = ImmutableSet.builder();
            for (List<Symbol> symbols : preferredHashSymbols) {
                Optional<List<Symbol>> newSymbols = translate(symbols, translator);
                if (newSymbols.isPresent()) {
                    newPreferredHashSymbols.add(newSymbols.get());
                }
            }
            return new HashSymbolPreference(newPreferredHashSymbols.build());
        }

        private static Optional<List<Symbol>> translate(List<Symbol> symbols, Function<Symbol, Optional<Symbol>> translator)
        {
            ImmutableList.Builder<Symbol> newSymbols = ImmutableList.builder();
            for (Symbol symbol : symbols) {
                Optional<Symbol> newSymbol = translator.apply(symbol);
                if (!newSymbol.isPresent()) {
                    return Optional.empty();
                }
                newSymbols.add(newSymbol.get());
            }
            return Optional.of(newSymbols.build());
        }

        public HashSymbolPreference withHashSymbol(PlanNode node, List<Symbol> preferredHashSymbol)
        {
            return pruneSymbols(node.getOutputSymbols()).withHashSymbol(preferredHashSymbol);
        }

        public HashSymbolPreference withHashSymbol(List<Symbol> preferredHashSymbol)
        {
            return new HashSymbolPreference(ImmutableSet.<List<Symbol>>builder()
                    .addAll(preferredHashSymbols)
                    .add(preferredHashSymbol).build());
        }
    }

    private static class Rewriter
            extends PlanVisitor<HashSymbolPreference, PlanWithProperties>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final SymbolAllocator symbolAllocator;
        private final Map<Symbol, Type> types;

        private Rewriter(PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, Map<Symbol, Type> types)
        {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
            this.types = requireNonNull(types, "types is null");
        }

        @Override
        protected PlanWithProperties visitPlan(PlanNode node, HashSymbolPreference parentPreference)
        {
            return planSimpleNodeWithProperties(node, parentPreference);
        }

        @Override
        public PlanWithProperties visitEnforceSingleRow(EnforceSingleRowNode node, HashSymbolPreference parentPreference)
        {
            // this plan node can only have a single input symbol, so do not add extra hash symbols
            return planSimpleNodeWithProperties(node, new HashSymbolPreference(), true);
        }

        @Override
        public PlanWithProperties visitAggregation(AggregationNode node, HashSymbolPreference parentPreference)
        {
            List<Symbol> groupBy = ImmutableList.of();
            if (!canSkipHashGeneration(node.getGroupBy())) {
                groupBy = node.getGroupBy();
            }

            // aggregation does not pass through preferred hash symbols
            PlanWithProperties child = planAndEnforce(node.getSource(), groupBy.isEmpty() ? new HashSymbolPreference() : new HashSymbolPreference(groupBy));

            Optional<Symbol> hashSymbol = Optional.empty();
            if (!groupBy.isEmpty()) {
                hashSymbol = Optional.of(child.getRequiredHashSymbol(groupBy));
            }

            return new PlanWithProperties(
                    new AggregationNode(
                            idAllocator.getNextId(),
                            child.getNode(),
                            node.getGroupBy(),
                            node.getAggregations(),
                            node.getFunctions(),
                            node.getMasks(),
                            node.getStep(),
                            node.getSampleWeight(),
                            node.getConfidence(),
                            hashSymbol),
                    groupBy.isEmpty() ? ImmutableMap.of() : ImmutableMap.of(groupBy, hashSymbol.get()));
        }

        private boolean canSkipHashGeneration(List<Symbol> partitionSymbols)
        {
            // HACK: bigint grouped aggregation has special operators that do not use precomputed hash, so we can skip hash generation
            return partitionSymbols.size() == 1 && types.get(Iterables.getOnlyElement(partitionSymbols)).equals(BigintType.BIGINT);
        }

        @Override
        public PlanWithProperties visitGroupId(GroupIdNode node, HashSymbolPreference parentPreference)
        {
            // remove any hash symbols not exported by the source of this node
            return planSimpleNodeWithProperties(node, parentPreference.pruneSymbols(node.getSource().getOutputSymbols()));
        }

        @Override
        public PlanWithProperties visitDistinctLimit(DistinctLimitNode node, HashSymbolPreference parentPreference)
        {
            // skip hash symbol generation for single bigint
            if (!canSkipHashGeneration(node.getDistinctSymbols())) {
                return planSimpleNodeWithProperties(node, parentPreference);
            }

            PlanWithProperties child = planAndEnforce(node.getSource(), parentPreference.withHashSymbol(node, node.getDistinctSymbols()));
            Symbol hashSymbol = child.getRequiredHashSymbol(node.getOutputSymbols());

            return new PlanWithProperties(
                    new DistinctLimitNode(idAllocator.getNextId(), child.getNode(), node.getLimit(), Optional.of(hashSymbol)),
                    child.getHashSymbols());
        }

        @Override
        public PlanWithProperties visitMarkDistinct(MarkDistinctNode node, HashSymbolPreference parentPreference)
        {
            // skip hash symbol generation for single bigint
            if (!canSkipHashGeneration(node.getDistinctSymbols())) {
                return planSimpleNodeWithProperties(node, parentPreference);
            }

            PlanWithProperties child = planAndEnforce(node.getSource(), parentPreference.withHashSymbol(node, node.getDistinctSymbols()));
            Symbol hashSymbol = child.getRequiredHashSymbol(node.getDistinctSymbols());

            return new PlanWithProperties(
                    new MarkDistinctNode(idAllocator.getNextId(), child.getNode(), node.getMarkerSymbol(), node.getDistinctSymbols(), Optional.of(hashSymbol)),
                    child.getHashSymbols());
        }

        @Override
        public PlanWithProperties visitRowNumber(RowNumberNode node, HashSymbolPreference parentPreference)
        {
            if (node.getPartitionBy().isEmpty()) {
                return planSimpleNodeWithProperties(node, parentPreference);
            }

            PlanWithProperties child = planAndEnforce(node.getSource(), parentPreference.withHashSymbol(node, node.getPartitionBy()));
            Symbol hashSymbol = child.getRequiredHashSymbol(node.getPartitionBy());

            return new PlanWithProperties(
                    new RowNumberNode(
                            idAllocator.getNextId(),
                            child.getNode(),
                            node.getPartitionBy(),
                            node.getRowNumberSymbol(),
                            node.getMaxRowCountPerPartition(),
                            Optional.of(hashSymbol)),
                    child.getHashSymbols());
        }

        @Override
        public PlanWithProperties visitTopNRowNumber(TopNRowNumberNode node, HashSymbolPreference parentPreference)
        {
            if (node.getPartitionBy().isEmpty()) {
                return planSimpleNodeWithProperties(node, parentPreference);
            }

            PlanWithProperties child = planAndEnforce(node.getSource(), parentPreference.withHashSymbol(node, node.getPartitionBy()));
            Symbol hashSymbol = child.getRequiredHashSymbol(node.getPartitionBy());

            return new PlanWithProperties(
                    new TopNRowNumberNode(
                            idAllocator.getNextId(),
                            child.getNode(),
                            node.getPartitionBy(),
                            node.getOrderBy(),
                            node.getOrderings(),
                            node.getRowNumberSymbol(),
                            node.getMaxRowCountPerPartition(),
                            node.isPartial(),
                            Optional.of(hashSymbol)),
                    child.getHashSymbols());
        }

        @Override
        public PlanWithProperties visitJoin(JoinNode node, HashSymbolPreference parentPreference)
        {
            List<JoinNode.EquiJoinClause> clauses = node.getCriteria();
            if (clauses.isEmpty()) {
                PlanWithProperties left = planAndEnforce(node.getLeft(), new HashSymbolPreference());
                // drop undesired hash symbols from build to save memory
                PlanWithProperties right = planAndEnforce(node.getRight(), new HashSymbolPreference(), true);

                Map<List<Symbol>, Symbol> allHashSymbols = new HashMap<>();
                allHashSymbols.putAll(right.getHashSymbols());
                allHashSymbols.putAll(left.getHashSymbols());

                return new PlanWithProperties(
                        new JoinNode(
                                idAllocator.getNextId(),
                                node.getType(),
                                left.getNode(),
                                right.getNode(),
                                node.getCriteria(),
                                Optional.empty(),
                                Optional.empty()),
                        allHashSymbols);
            }

            // join does not pass through preferred hash symbols since they take more memory and since
            // the join node filters, may take more compute
            List<Symbol> leftSymbols = Lists.transform(clauses, JoinNode.EquiJoinClause::getLeft);
            PlanWithProperties left = planAndEnforce(node.getLeft(), new HashSymbolPreference(leftSymbols));
            Symbol leftHashSymbol = left.getRequiredHashSymbol(leftSymbols);

            List<Symbol> rightSymbols = Lists.transform(clauses, JoinNode.EquiJoinClause::getRight);
            // drop undesired hash symbols from build to save memory
            PlanWithProperties right = planAndEnforce(node.getRight(), new HashSymbolPreference(rightSymbols), true);
            Symbol rightHashSymbol = right.getRequiredHashSymbol(rightSymbols);

            // build map of all hash symbols
            // NOTE: Full outer join doesn't use hash symbols
            Map<List<Symbol>, Symbol> allHashSymbols = new HashMap<>();
            if (node.getType() == INNER || node.getType() == LEFT) {
                allHashSymbols.putAll(left.getHashSymbols());
            }
            if (node.getType() == INNER || node.getType() == RIGHT) {
                allHashSymbols.putAll(right.getHashSymbols());
            }

            return new PlanWithProperties(
                    new JoinNode(
                            idAllocator.getNextId(),
                            node.getType(),
                            left.getNode(),
                            right.getNode(),
                            node.getCriteria(),
                            Optional.of(leftHashSymbol),
                            Optional.of(rightHashSymbol)),
                    allHashSymbols);
        }

        @Override
        public PlanWithProperties visitSemiJoin(SemiJoinNode node, HashSymbolPreference parentPreference)
        {
            PlanWithProperties source = planAndEnforce(node.getSource(), new HashSymbolPreference(ImmutableList.of(node.getSourceJoinSymbol())));
            Symbol sourceHashSymbol = source.getRequiredHashSymbol(ImmutableList.of(node.getSourceJoinSymbol()));

            PlanWithProperties filteringSource = planAndEnforce(node.getFilteringSource(), new HashSymbolPreference(ImmutableList.of(node.getFilteringSourceJoinSymbol())), true);
            Symbol filteringSourceHashSymbol = filteringSource.getRequiredHashSymbol(ImmutableList.of(node.getFilteringSourceJoinSymbol()));

            return new PlanWithProperties(
                    new SemiJoinNode(
                            idAllocator.getNextId(),
                            source.getNode(),
                            filteringSource.getNode(),
                            node.getSourceJoinSymbol(),
                            node.getFilteringSourceJoinSymbol(),
                            node.getSemiJoinOutput(),
                            Optional.of(sourceHashSymbol),
                            Optional.of(filteringSourceHashSymbol)),
                    source.getHashSymbols());
        }

        @Override
        public PlanWithProperties visitIndexJoin(IndexJoinNode node, HashSymbolPreference parentPreference)
        {
            List<IndexJoinNode.EquiJoinClause> clauses = node.getCriteria();

            // join does not pass through preferred hash symbols since they take more memory and since
            // the join node filters, may take more compute
            List<Symbol> probeSymbols = Lists.transform(clauses, IndexJoinNode.EquiJoinClause::getProbe);
            PlanWithProperties probe = planAndEnforce(node.getProbeSource(), new HashSymbolPreference(probeSymbols));
            Symbol probeHashSymbol = probe.getRequiredHashSymbol(probeSymbols);

            List<Symbol> indexSymbols = Lists.transform(clauses, IndexJoinNode.EquiJoinClause::getIndex);
            PlanWithProperties index = planAndEnforce(node.getIndexSource(), new HashSymbolPreference(indexSymbols), true);
            Symbol indexHashSymbol = index.getRequiredHashSymbol(indexSymbols);

            // build map of all hash symbols
            Map<List<Symbol>, Symbol> allHashSymbols = new HashMap<>();
            if (node.getType() == IndexJoinNode.Type.INNER) {
                allHashSymbols.putAll(probe.getHashSymbols());
            }
            allHashSymbols.putAll(index.getHashSymbols());

            return new PlanWithProperties(
                    new IndexJoinNode(
                            idAllocator.getNextId(),
                            node.getType(),
                            probe.getNode(),
                            index.getNode(),
                            node.getCriteria(),
                            Optional.of(probeHashSymbol),
                            Optional.of(indexHashSymbol)),
                    allHashSymbols);
        }

        @Override
        public PlanWithProperties visitWindow(WindowNode node, HashSymbolPreference parentPreference)
        {
            if (node.getPartitionBy().isEmpty()) {
                return planSimpleNodeWithProperties(node, parentPreference, true);
            }

            PlanWithProperties child = planAndEnforce(node.getSource(), parentPreference.withHashSymbol(node, node.getPartitionBy()), true);
            Symbol hashSymbol = child.getRequiredHashSymbol(node.getPartitionBy());

            return new PlanWithProperties(
                    new WindowNode(
                            idAllocator.getNextId(),
                            child.getNode(),
                            node.getPartitionBy(),
                            node.getOrderBy(),
                            node.getOrderings(),
                            node.getFrame(),
                            node.getWindowFunctions(),
                            node.getSignatures(),
                            Optional.of(hashSymbol),
                            node.getPrePartitionedInputs(),
                            node.getPreSortedOrderPrefix()),
                    child.getHashSymbols());
        }

        @Override
        public PlanWithProperties visitExchange(ExchangeNode node, HashSymbolPreference parentPreference)
        {
            // remove any hash symbols not exported by this node
            HashSymbolPreference preference = parentPreference.pruneSymbols(node.getOutputSymbols());

            // Currently, precomputed hash values are only supported for system hash distributions without constants
            List<Symbol> partitionSymbols = ImmutableList.of();
            PartitionFunctionBinding partitionFunction = node.getPartitionFunction();
            if (partitionFunction.getPartitioningHandle().equals(FIXED_HASH_DISTRIBUTION) &&
                    partitionFunction.getPartitionFunctionArguments().stream().allMatch(PartitionFunctionArgumentBinding::isVariable)) {
                partitionSymbols = partitionFunction.getPartitionFunctionArguments().stream()
                        .map(PartitionFunctionArgumentBinding::getColumn)
                        .collect(toImmutableList());

                // add precomputed hash for exchange
                preference = preference.withHashSymbol(partitionSymbols);
            }

            // establish fixed ordering for hash symbols
            List<List<Symbol>> preferredHashSymbols = ImmutableList.copyOf(preference.getPreferredHashSymbols());
            Map<List<Symbol>, Symbol> newHashSymbols = new HashMap<>();
            for (List<Symbol> preferredHashSymbol : preferredHashSymbols) {
                newHashSymbols.put(preferredHashSymbol, symbolAllocator.newHashSymbol());
            }

            // rewrite partition function to include new symbols (and precomputed hash
            partitionFunction = new PartitionFunctionBinding(
                    partitionFunction.getPartitioningHandle(),
                    ImmutableList.<Symbol>builder()
                            .addAll(partitionFunction.getOutputLayout())
                            .addAll(preferredHashSymbols.stream()
                                    .map(newHashSymbols::get)
                                    .collect(toImmutableList()))
                            .build(),
                    partitionFunction.getPartitionFunctionArguments(),
                    Optional.ofNullable(newHashSymbols.get(partitionSymbols)),
                    partitionFunction.isReplicateNulls(),
                    partitionFunction.getBucketToPartition());

            // add hash symbols to sources
            ImmutableList.Builder<List<Symbol>> newInputs = ImmutableList.builder();
            ImmutableList.Builder<PlanNode> newSources = ImmutableList.builder();
            for (int sourceId = 0; sourceId < node.getSources().size(); sourceId++) {
                PlanNode source = node.getSources().get(sourceId);
                List<Symbol> inputSymbols = node.getInputs().get(sourceId);

                Map<Symbol, Symbol> outputToInputMap = new HashMap<>();
                for (int symbolId = 0; symbolId < inputSymbols.size(); symbolId++) {
                    outputToInputMap.put(node.getOutputSymbols().get(symbolId), inputSymbols.get(symbolId));
                }

                HashSymbolPreference sourceContext = preference.translate(symbol -> Optional.of(outputToInputMap.get(symbol)));
                PlanWithProperties child = planAndEnforce(source, sourceContext);
                newSources.add(child.getNode());

                // add hash symbols to inputs
                ImmutableList.Builder<Symbol> newInputSymbols = ImmutableList.<Symbol>builder();
                newInputSymbols.addAll(node.getInputs().get(sourceId));
                for (List<Symbol> preferredHashSymbol : preferredHashSymbols) {
                    List<Symbol> preferredHashInputSymbols = preferredHashSymbol.stream()
                            .map(outputToInputMap::get)
                            .collect(toImmutableList());
                    newInputSymbols.add(child.getRequiredHashSymbol(preferredHashInputSymbols));
                }

                newInputs.add(newInputSymbols.build());
            }

            return new PlanWithProperties(
                    new ExchangeNode(
                            idAllocator.getNextId(),
                            node.getType(),
                            partitionFunction,
                            newSources.build(),
                            newInputs.build()),
                    newHashSymbols);
        }

        @Override
        public PlanWithProperties visitUnion(UnionNode node, HashSymbolPreference parentPreference)
        {
            // remove any hash symbols not exported by this node
            HashSymbolPreference preference = parentPreference.pruneSymbols(node.getOutputSymbols());

            // create new hash symbols
            Map<List<Symbol>, Symbol> newHashSymbols = new HashMap<>();
            for (List<Symbol> preferredHashSymbol : preference.getPreferredHashSymbols()) {
                newHashSymbols.put(preferredHashSymbol, symbolAllocator.newHashSymbol());
            }

            // add hash symbols to sources
            ImmutableListMultimap.Builder<Symbol, Symbol> newSymbolMapping = ImmutableListMultimap.builder();
            newSymbolMapping.putAll(node.getSymbolMapping());
            ImmutableList.Builder<PlanNode> newSources = ImmutableList.builder();
            for (int sourceId = 0; sourceId < node.getSources().size(); sourceId++) {
                // translate preference to input symbols
                Map<Symbol, Symbol> outputToInputMap = new HashMap<>();
                for (Symbol outputSymbol : node.getOutputSymbols()) {
                    outputToInputMap.put(outputSymbol, node.getSymbolMapping().get(outputSymbol).get(sourceId));
                }
                HashSymbolPreference sourcePreference = preference.translate(symbol -> Optional.of(outputToInputMap.get(symbol)));

                PlanWithProperties child = planAndEnforce(node.getSources().get(sourceId), sourcePreference);
                newSources.add(child.getNode());

                // add hash symbols to inputs
                for (Entry<List<Symbol>, Symbol> entry : newHashSymbols.entrySet()) {
                    List<Symbol> preferredHashInputSymbols = entry.getKey().stream()
                            .map(outputToInputMap::get)
                            .collect(toImmutableList());
                    newSymbolMapping.put(entry.getValue(), child.getRequiredHashSymbol(preferredHashInputSymbols));
                }
            }

            return new PlanWithProperties(
                    new UnionNode(
                            idAllocator.getNextId(),
                            newSources.build(),
                            newSymbolMapping.build(),
                            ImmutableList.copyOf(newSymbolMapping.build().keySet())),
                    newHashSymbols);
        }

        @Override
        public PlanWithProperties visitProject(ProjectNode node, HashSymbolPreference parentPreference)
        {
            Map<Symbol, Symbol> outputToInputMapping = computeIdentityTranslations(node.getAssignments());
            HashSymbolPreference sourceContext = parentPreference.translate(symbol -> Optional.ofNullable(outputToInputMapping.get(symbol)));
            PlanWithProperties child = plan(node.getSource(), sourceContext);

            // create a new project node with all assignments from the original node
            Map<Symbol, Expression> newAssignments = new HashMap<>();
            newAssignments.putAll(node.getAssignments());

            // and all hash symbols that could be translated to the source symbols
            Map<List<Symbol>, Symbol> allHashSymbols = new HashMap<>();
            for (List<Symbol> preferredHashSymbol : sourceContext.getPreferredHashSymbols()) {
                Symbol hashSymbol = child.getHashSymbols().get(preferredHashSymbol);
                Expression hashExpression;
                if (hashSymbol == null) {
                    hashSymbol = symbolAllocator.newHashSymbol();
                    hashExpression = getHashExpression(preferredHashSymbol);
                }
                else {
                    hashExpression = new QualifiedNameReference(hashSymbol.toQualifiedName());
                }
                newAssignments.put(hashSymbol, hashExpression);
            }

            return new PlanWithProperties(new ProjectNode(idAllocator.getNextId(), child.getNode(), newAssignments), allHashSymbols);
        }

        @Override
        public PlanWithProperties visitUnnest(UnnestNode node, HashSymbolPreference parentPreference)
        {
            PlanWithProperties child = plan(node.getSource(), parentPreference.pruneSymbols(node.getSource().getOutputSymbols()));

            // only pass through hash symbols requested by the parent
            Map<List<Symbol>, Symbol> hashSymbols = new HashMap<>(child.getHashSymbols());
            hashSymbols.keySet().retainAll(parentPreference.getPreferredHashSymbols());

            return new PlanWithProperties(
                    new UnnestNode(
                            idAllocator.getNextId(),
                            child.getNode(),
                            ImmutableList.<Symbol>builder()
                                    .addAll(node.getReplicateSymbols())
                                    .addAll(hashSymbols.values())
                                    .build(),
                            node.getUnnestSymbols(),
                            node.getOrdinalitySymbol()),
                    hashSymbols);
        }

        private PlanWithProperties planSimpleNodeWithProperties(PlanNode node, HashSymbolPreference parentPreference)
        {
            return planSimpleNodeWithProperties(node, parentPreference, true);
        }

        private PlanWithProperties planSimpleNodeWithProperties(
                PlanNode node,
                HashSymbolPreference parentPreference,
                boolean alwaysPruneExtraHashSymbols)
        {
            if (node.getSources().isEmpty()) {
                return new PlanWithProperties(node, ImmutableMap.of());
            }

            PlanWithProperties source = planAndEnforce(Iterables.getOnlyElement(node.getSources()), parentPreference, alwaysPruneExtraHashSymbols);
            PlanNode result = replaceChildren(node, ImmutableList.of(source.getNode()));

            // return only hash symbols that are passed through the new node
            Map<List<Symbol>, Symbol> hashSymbols = new HashMap<>(source.getHashSymbols());
            hashSymbols.keySet().retainAll(result.getOutputSymbols());

            return new PlanWithProperties(result, hashSymbols);
        }

        private PlanWithProperties planAndEnforce(PlanNode node, HashSymbolPreference parentPreference)
        {
            return planAndEnforce(node, parentPreference, true);
        }

        private PlanWithProperties planAndEnforce(PlanNode node, HashSymbolPreference parentPreference, boolean alwaysPruneExtraHashSymbols)
        {
            PlanWithProperties result = plan(node, parentPreference);

            boolean preferenceSatisfied;
            if (alwaysPruneExtraHashSymbols) {
                preferenceSatisfied = result.getHashSymbols().keySet().equals(parentPreference.getPreferredHashSymbols());
            }
            else {
                preferenceSatisfied = result.getHashSymbols().keySet().containsAll(parentPreference.getPreferredHashSymbols());
            }

            if (preferenceSatisfied) {
                return result;
            }

            PlanNode source = result.getNode();
            ImmutableMap.Builder<Symbol, Expression> assignments = ImmutableMap.builder();

            Map<List<Symbol>, Symbol> outputHashSymbols = new HashMap<>();

            // copy through all symbols from child, except for hash symbols not needed by the parent
            Map<Symbol, List<Symbol>> resultHashSymbols = result.getHashSymbols().inverse();
            for (Symbol symbol : source.getOutputSymbols()) {
                List<Symbol> partitionSymbols = resultHashSymbols.get(symbol);
                if (partitionSymbols == null || parentPreference.getPreferredHashSymbols().contains(partitionSymbols)) {
                    assignments.put(symbol, new QualifiedNameReference(symbol.toQualifiedName()));

                    if (partitionSymbols != null) {
                        outputHashSymbols.put(partitionSymbols, symbol);
                    }
                }
            }

            // add new projections for hash symbols needed by the parent
            for (List<Symbol> partitionSymbols : parentPreference.getPreferredHashSymbols()) {
                if (!result.getHashSymbols().containsKey(partitionSymbols)) {
                    Expression hashExpression = getHashExpression(partitionSymbols);
                    Symbol hashSymbol = symbolAllocator.newHashSymbol();
                    assignments.put(hashSymbol, hashExpression);
                    outputHashSymbols.put(partitionSymbols, hashSymbol);
                }
            }

            return new PlanWithProperties(new ProjectNode(idAllocator.getNextId(), source, assignments.build()), outputHashSymbols);
        }

        private PlanWithProperties plan(PlanNode node, HashSymbolPreference parentPreference)
        {
            PlanWithProperties result = node.accept(this, parentPreference);
            checkState(
                    result.getNode().getOutputSymbols().containsAll(result.getHashSymbols().values()),
                    "Node %s declares hash symbols not in the output",
                    result.getNode().getClass().getSimpleName());
            return result;
        }
    }

    private static class PlanWithProperties
    {
        private final PlanNode node;
        private final BiMap<List<Symbol>, Symbol> hashSymbols;

        public PlanWithProperties(PlanNode node, Map<List<Symbol>, Symbol> hashSymbols)
        {
            this.node = requireNonNull(node, "node is null");
            this.hashSymbols = ImmutableBiMap.copyOf(requireNonNull(hashSymbols, "hashSymbols is null"));
        }

        public PlanNode getNode()
        {
            return node;
        }

        public BiMap<List<Symbol>, Symbol> getHashSymbols()
        {
            return hashSymbols;
        }

        public Symbol getRequiredHashSymbol(List<Symbol> symbols)
        {
            Symbol hashSymbol = hashSymbols.get(symbols);
            requireNonNull(hashSymbol, () -> "No hash symbol generated for " + symbols);
            return hashSymbol;
        }
    }

    private static Map<Symbol, Symbol> computeIdentityTranslations(Map<Symbol, Expression> assignments)
    {
        Map<Symbol, Symbol> outputToInput = new HashMap<>();
        for (Map.Entry<Symbol, Expression> assignment : assignments.entrySet()) {
            if (assignment.getValue() instanceof QualifiedNameReference) {
                outputToInput.put(assignment.getKey(), Symbol.fromQualifiedName(((QualifiedNameReference) assignment.getValue()).getName()));
            }
        }
        return outputToInput;
    }

    private static Expression getHashExpression(List<Symbol> partitioningSymbols)
    {
        Expression hashExpression = new LongLiteral(String.valueOf(INITIAL_HASH_VALUE));
        for (Symbol symbol : partitioningSymbols) {
            hashExpression = getHashFunctionCall(hashExpression, symbol);
        }
        return hashExpression;
    }

    private static Expression getHashFunctionCall(Expression previousHashValue, Symbol symbol)
    {
        FunctionCall functionCall = new FunctionCall(
                QualifiedName.of(HASH_CODE),
                Optional.<Window>empty(),
                false,
                ImmutableList.<Expression>of(new QualifiedNameReference(symbol.toQualifiedName())));
        List<Expression> arguments = ImmutableList.of(previousHashValue, orNullHashCode(functionCall));
        return new FunctionCall(QualifiedName.of("combine_hash"), arguments);
    }

    private static Expression orNullHashCode(Expression expression)
    {
        return new CoalesceExpression(expression, new LongLiteral(String.valueOf(TypeUtils.NULL_HASH_CODE)));
    }
}
