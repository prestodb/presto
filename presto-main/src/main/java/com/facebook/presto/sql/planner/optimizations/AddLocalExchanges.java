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
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.facebook.presto.spi.GroupingProperty;
import com.facebook.presto.spi.LocalProperty;
import com.facebook.presto.spi.SortingProperty;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.PartitionFunctionBinding;
import com.facebook.presto.sql.planner.PartitionFunctionBinding.PartitionFunctionArgumentBinding;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.optimizations.StreamPropertyDerivations.StreamProperties;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.AggregationNode.Step;
import com.facebook.presto.sql.planner.plan.DistinctLimitNode;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.ExplainAnalyzeNode;
import com.facebook.presto.sql.planner.plan.IndexJoinNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.MarkDistinctNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.RowNumberNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.TableFinishNode;
import com.facebook.presto.sql.planner.plan.TableWriterNode;
import com.facebook.presto.sql.planner.plan.TopNNode;
import com.facebook.presto.sql.planner.plan.TopNRowNumberNode;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.facebook.presto.SystemSessionProperties.getTaskConcurrency;
import static com.facebook.presto.SystemSessionProperties.getTaskWriterCount;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_RANDOM_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.optimizations.StreamPreferredProperties.any;
import static com.facebook.presto.sql.planner.optimizations.StreamPreferredProperties.defaultParallelism;
import static com.facebook.presto.sql.planner.optimizations.StreamPreferredProperties.exactlyPartitionedOn;
import static com.facebook.presto.sql.planner.optimizations.StreamPreferredProperties.fixedParallelism;
import static com.facebook.presto.sql.planner.optimizations.StreamPreferredProperties.singleStream;
import static com.facebook.presto.sql.planner.optimizations.StreamPropertyDerivations.StreamProperties.StreamDistribution.SINGLE;
import static com.facebook.presto.sql.planner.plan.ChildReplacer.replaceChildren;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.GATHER;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.gatheringExchange;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.partitionedExchange;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableSet;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class AddLocalExchanges
        implements PlanOptimizer
{
    private final Metadata metadata;
    private final SqlParser parser;

    public AddLocalExchanges(Metadata metadata, SqlParser parser)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.parser = requireNonNull(parser, "parser is null");
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        PlanWithProperties result = plan.accept(new Rewriter(symbolAllocator, idAllocator, session), any());
        return result.getNode();
    }

    private class Rewriter
            extends PlanVisitor<StreamPreferredProperties, PlanWithProperties>
    {
        private final SymbolAllocator symbolAllocator;
        private final PlanNodeIdAllocator idAllocator;
        private final Session session;

        public Rewriter(SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator, Session session)
        {
            this.symbolAllocator = symbolAllocator;
            this.idAllocator = idAllocator;
            this.session = session;
        }

        @Override
        protected PlanWithProperties visitPlan(PlanNode node, StreamPreferredProperties parentPreferences)
        {
            return planAndEnforceChildren(
                    node,
                    parentPreferences.withoutPreference().withDefaultParallelism(session),
                    parentPreferences.withDefaultParallelism(session));
        }

        @Override
        public PlanWithProperties visitOutput(OutputNode node, StreamPreferredProperties parentPreferences)
        {
            return planAndEnforceChildren(
                    node,
                    any().withOrderSensitivity(),
                    any().withOrderSensitivity());
        }

        @Override
        public PlanWithProperties visitExplainAnalyze(ExplainAnalyzeNode node, StreamPreferredProperties parentPreferences)
        {
            // Although explain analyze discards all output, we want to maintain the behavior
            // of a normal output node, so declare the node to be order sensitive
            return planAndEnforceChildren(
                    node,
                    singleStream().withOrderSensitivity(),
                    singleStream().withOrderSensitivity());
        }

        //
        // Nodes that always require a single stream
        //

        @Override
        public PlanWithProperties visitSort(SortNode node, StreamPreferredProperties parentPreferences)
        {
            // sort requires that all data be in one stream
            // this node changes the input organization completely, so we do not pass through parent preferences
            return planAndEnforceChildren(node, singleStream(), defaultParallelism(session));
        }

        @Override
        public PlanWithProperties visitTableFinish(TableFinishNode node, StreamPreferredProperties parentPreferences)
        {
            // table commit requires that all data be in one stream
            // this node changes the input organization completely, so we do not pass through parent preferences
            return planAndEnforceChildren(node, singleStream(), defaultParallelism(session));
        }

        @Override
        public PlanWithProperties visitTopN(TopNNode node, StreamPreferredProperties parentPreferences)
        {
            if (node.isPartial()) {
                return planAndEnforceChildren(
                        node,
                        parentPreferences.withoutPreference().withDefaultParallelism(session),
                        parentPreferences.withDefaultParallelism(session));
            }

            // final topN requires that all data be in one stream
            // also, a final changes the input organization completely, so we do not pass through parent preferences
            return planAndEnforceChildren(
                    node,
                    singleStream(),
                    defaultParallelism(session));
        }

        @Override
        public PlanWithProperties visitLimit(LimitNode node, StreamPreferredProperties parentPreferences)
        {
            if (node.isPartial()) {
                return planAndEnforceChildren(
                        node,
                        parentPreferences.withoutPreference().withDefaultParallelism(session),
                        parentPreferences.withDefaultParallelism(session));
            }

            // final limit requires that all data be in one stream
            // also, a final changes the input organization completely, so we do not pass through parent preferences
            return planAndEnforceChildren(
                    node,
                    singleStream(),
                    defaultParallelism(session));
        }

        @Override
        public PlanWithProperties visitDistinctLimit(DistinctLimitNode node, StreamPreferredProperties parentPreferences)
        {
            // final limit requires that all data be in one stream
            StreamPreferredProperties requiredProperties;
            StreamPreferredProperties preferredProperties;
            if (node.isPartial()) {
                requiredProperties = parentPreferences.withoutPreference().withDefaultParallelism(session);
                preferredProperties = parentPreferences.withDefaultParallelism(session);
            }
            else {
                // a final changes the input organization completely, so we do not pass through parent preferences
                requiredProperties = singleStream();
                preferredProperties = defaultParallelism(session);
            }

            return planAndEnforceChildren(node, requiredProperties, preferredProperties);
        }

        @Override
        public PlanWithProperties visitEnforceSingleRow(EnforceSingleRowNode node, StreamPreferredProperties parentPreferences)
        {
            return planAndEnforceChildren(node, singleStream(), defaultParallelism(session));
        }

        //
        // Nodes that require parallel streams to be partitioned
        //

        @Override
        public PlanWithProperties visitAggregation(AggregationNode node, StreamPreferredProperties parentPreferences)
        {
            StreamPreferredProperties requiredProperties;
            StreamPreferredProperties preferredChildProperties;

            if (node.getStep() == Step.FINAL || node.getStep() == Step.SINGLE) {
                // final aggregation requires that all data be partitioned
                requiredProperties = parentPreferences.withDefaultParallelism(session).withPartitioning(node.getGroupBy());
                preferredChildProperties = parentPreferences.withDefaultParallelism(session)
                        .withPartitioning(node.getGroupBy());
            }
            else {
                requiredProperties = parentPreferences.withoutPreference().withDefaultParallelism(session);
                preferredChildProperties = parentPreferences.withDefaultParallelism(session)
                        .constrainTo(node.getSource().getOutputSymbols());
            }

            // plan child with the preferred
            PlanWithProperties child = node.getSource().accept(this, preferredChildProperties);
            if (requiredProperties.isSatisfiedBy(child.getProperties())) {
                return rebaseAndDeriveProperties(node, ImmutableList.of(child));
            }

            // if aggregation is decomposable and not already parallel, push down a partial which will be executed in parallel
            FunctionRegistry functionRegistry = metadata.getFunctionRegistry();
            boolean decomposable = node.getFunctions()
                    .values().stream()
                    .map(functionRegistry::getAggregateFunctionImplementation)
                    .allMatch(InternalAggregationFunction::isDecomposable);
            if (decomposable && !requiredProperties.isParallelPreferred()) {
                return splitAggregation(node, child, source -> gatheringExchange(idAllocator.getNextId(), LOCAL, source));
            }

            return rebaseAndDeriveProperties(node, ImmutableList.of(enforce(child, requiredProperties)));
        }

        private PlanWithProperties splitAggregation(AggregationNode node, PlanWithProperties newChild, Function<PlanNode, PlanNode> exchanger)
        {
            // otherwise, add a partial and final with an exchange in between
            Map<Symbol, Symbol> masks = node.getMasks();

            Map<Symbol, FunctionCall> finalCalls = new HashMap<>();
            Map<Symbol, FunctionCall> intermediateCalls = new HashMap<>();
            Map<Symbol, Signature> intermediateFunctions = new HashMap<>();
            Map<Symbol, Symbol> intermediateMask = new HashMap<>();
            for (Map.Entry<Symbol, FunctionCall> entry : node.getAggregations().entrySet()) {
                Signature signature = node.getFunctions().get(entry.getKey());
                InternalAggregationFunction function = metadata.getFunctionRegistry().getAggregateFunctionImplementation(signature);

                Symbol intermediateSymbol = symbolAllocator.newSymbol(signature.getName(), function.getIntermediateType());
                intermediateCalls.put(intermediateSymbol, entry.getValue());
                intermediateFunctions.put(intermediateSymbol, signature);
                if (masks.containsKey(entry.getKey())) {
                    intermediateMask.put(intermediateSymbol, masks.get(entry.getKey()));
                }

                // rewrite final aggregation in terms of intermediate function
                finalCalls.put(entry.getKey(), new FunctionCall(QualifiedName.of(signature.getName()), ImmutableList.<Expression>of(new QualifiedNameReference(intermediateSymbol.toQualifiedName()))));
            }

            PlanWithProperties source = deriveProperties(
                    new AggregationNode(
                            idAllocator.getNextId(),
                            newChild.getNode(),
                            node.getGroupBy(),
                            intermediateCalls,
                            intermediateFunctions,
                            intermediateMask,
                            node.getGroupingSets(),
                            Step.PARTIAL,
                            node.getSampleWeight(),
                            node.getConfidence(),
                            node.getHashSymbol()),
                    newChild.getProperties());

            if (exchanger != null) {
                source = deriveProperties(exchanger.apply(source.getNode()), source.getProperties());
            }

            return deriveProperties(
                    new AggregationNode(
                            node.getId(),
                            source.getNode(),
                            node.getGroupBy(),
                            finalCalls,
                            node.getFunctions(),
                            ImmutableMap.of(),
                            node.getGroupingSets(),
                            Step.FINAL,
                            Optional.empty(),
                            node.getConfidence(),
                            node.getHashSymbol()),
                    source.getProperties());
        }

        @Override
        public PlanWithProperties visitWindow(WindowNode node, StreamPreferredProperties parentPreferences)
        {
            StreamPreferredProperties childRequirements = parentPreferences
                    .constrainTo(node.getSource().getOutputSymbols())
                    .withDefaultParallelism(session)
                    .withPartitioning(node.getPartitionBy());

            PlanWithProperties child = planAndEnforce(node.getSource(), childRequirements, childRequirements);

            List<LocalProperty<Symbol>> desiredProperties = new ArrayList<>();
            if (!node.getPartitionBy().isEmpty()) {
                desiredProperties.add(new GroupingProperty<>(node.getPartitionBy()));
            }
            for (Symbol symbol : node.getOrderBy()) {
                desiredProperties.add(new SortingProperty<>(symbol, node.getOrderings().get(symbol)));
            }
            Iterator<Optional<LocalProperty<Symbol>>> matchIterator = LocalProperties.match(child.getProperties().getLocalProperties(), desiredProperties).iterator();

            Set<Symbol> prePartitionedInputs = ImmutableSet.of();
            if (!node.getPartitionBy().isEmpty()) {
                Optional<LocalProperty<Symbol>> groupingRequirement = matchIterator.next();
                Set<Symbol> unPartitionedInputs = groupingRequirement.map(LocalProperty::getColumns).orElse(ImmutableSet.of());
                prePartitionedInputs = node.getPartitionBy().stream()
                        .filter(symbol -> !unPartitionedInputs.contains(symbol))
                        .collect(toImmutableSet());
            }

            int preSortedOrderPrefix = 0;
            if (prePartitionedInputs.equals(ImmutableSet.copyOf(node.getPartitionBy()))) {
                while (matchIterator.hasNext() && !matchIterator.next().isPresent()) {
                    preSortedOrderPrefix++;
                }
            }

            WindowNode result = new WindowNode(
                    node.getId(),
                    child.getNode(),
                    node.getPartitionBy(),
                    node.getOrderBy(),
                    node.getOrderings(),
                    node.getFrame(),
                    node.getWindowFunctions(),
                    node.getSignatures(),
                    node.getHashSymbol(),
                    prePartitionedInputs,
                    preSortedOrderPrefix);

            return deriveProperties(result, child.getProperties());
        }

        @Override
        public PlanWithProperties visitMarkDistinct(MarkDistinctNode node, StreamPreferredProperties parentPreferences)
        {
            // mark distinct requires that all data partitioned
            StreamPreferredProperties requiredProperties = parentPreferences.withDefaultParallelism(session).withPartitioning(node.getDistinctSymbols());
            return planAndEnforceChildren(node, requiredProperties, requiredProperties);
        }

        @Override
        public PlanWithProperties visitRowNumber(RowNumberNode node, StreamPreferredProperties parentPreferences)
        {
            // row number requires that all data be partitioned
            StreamPreferredProperties requiredProperties = parentPreferences.withDefaultParallelism(session).withPartitioning(node.getPartitionBy());
            return planAndEnforceChildren(node, requiredProperties, requiredProperties);
        }

        @Override
        public PlanWithProperties visitTopNRowNumber(TopNRowNumberNode node, StreamPreferredProperties parentPreferences)
        {
            StreamPreferredProperties requiredProperties = parentPreferences.withDefaultParallelism(session);

            // final topN row number requires that all data be partitioned
            if (!node.isPartial()) {
                requiredProperties = requiredProperties.withPartitioning(node.getPartitionBy());
            }

            return planAndEnforceChildren(node, requiredProperties, requiredProperties);
        }

        //
        // Table Writer
        //

        @Override
        public PlanWithProperties visitTableWriter(TableWriterNode node, StreamPreferredProperties parentPreferences)
        {
            StreamPreferredProperties requiredProperties;
            StreamPreferredProperties preferredProperties;
            if (getTaskWriterCount(session) > 1) {
                requiredProperties = fixedParallelism();
                preferredProperties = fixedParallelism();
            }
            else {
                requiredProperties = singleStream();
                preferredProperties = defaultParallelism(session);
            }
            return planAndEnforceChildren(node, requiredProperties, preferredProperties);
        }

        //
        // Exchanges
        //

        @Override
        public PlanWithProperties visitExchange(ExchangeNode node, StreamPreferredProperties parentPreferences)
        {
            checkArgument(node.getScope() != LOCAL, "AddLocalExchanges can not process a plan containing a local exchange");
            // this node changes the input organization completely, so we do not pass through parent preferences
            return planAndEnforceChildren(node, any(), defaultParallelism(session));
        }

        @Override
        public PlanWithProperties visitUnion(UnionNode node, StreamPreferredProperties preferredProperties)
        {
            // Union is replaced with an exchange which does not retain streaming properties from the children
            List<PlanWithProperties> sourcesWithProperties = node.getSources().stream()
                    .map(source -> source.accept(this, defaultParallelism(session)))
                    .collect(toImmutableList());

            List<PlanNode> sources = sourcesWithProperties.stream()
                    .map(PlanWithProperties::getNode)
                    .collect(toImmutableList());

            List<StreamProperties> inputProperties = sourcesWithProperties.stream()
                    .map(PlanWithProperties::getProperties)
                    .collect(toImmutableList());

            List<List<Symbol>> inputLayouts = new ArrayList<>(sources.size());
            for (int i = 0; i < sources.size(); i++) {
                inputLayouts.add(node.sourceOutputLayout(i));
            }

            if (preferredProperties.isSingleStreamPreferred()) {
                ExchangeNode exchangeNode = new ExchangeNode(
                        idAllocator.getNextId(),
                        GATHER,
                        LOCAL,
                        new PartitionFunctionBinding(SINGLE_DISTRIBUTION, node.getOutputSymbols(), ImmutableList.of()),
                        sources,
                        inputLayouts);
                return deriveProperties(exchangeNode, inputProperties);
            }

            Optional<List<Symbol>> preferredPartitionColumns = preferredProperties.getPartitioningColumns();
            if (preferredPartitionColumns.isPresent()) {
                ExchangeNode exchangeNode = new ExchangeNode(
                        idAllocator.getNextId(),
                        REPARTITION,
                        LOCAL,
                        new PartitionFunctionBinding(
                                FIXED_HASH_DISTRIBUTION,
                                node.getOutputSymbols(),
                                preferredPartitionColumns.get().stream()
                                        .map(PartitionFunctionArgumentBinding::new)
                                        .collect(toImmutableList()),
                                Optional.empty()),
                        sources,
                        inputLayouts);
                return deriveProperties(exchangeNode, inputProperties);
            }

            // multiple streams preferred
            ExchangeNode result = new ExchangeNode(
                    idAllocator.getNextId(),
                    REPARTITION,
                    LOCAL,
                    new PartitionFunctionBinding(FIXED_RANDOM_DISTRIBUTION, node.getOutputSymbols(), ImmutableList.of()),
                    sources,
                    inputLayouts);
            ExchangeNode exchangeNode = result;

            return deriveProperties(exchangeNode, inputProperties);
        }

        //
        // Joins
        //

        @Override
        public PlanWithProperties visitJoin(JoinNode node, StreamPreferredProperties parentPreferences)
        {
            PlanWithProperties probe = planAndEnforce(
                    node.getLeft(),
                    defaultParallelism(session),
                    parentPreferences.constrainTo(node.getLeft().getOutputSymbols()).withDefaultParallelism(session));

            // this build consumes the input completely, so we do not pass through parent preferences
            List<Symbol> buildHashSymbols = Lists.transform(node.getCriteria(), JoinNode.EquiJoinClause::getRight);
            StreamPreferredProperties buildPreference;
            if (getTaskConcurrency(session) > 1) {
                buildPreference = exactlyPartitionedOn(buildHashSymbols);
            }
            else {
                buildPreference = singleStream();
            }
            PlanWithProperties build = planAndEnforce(node.getRight(), buildPreference, buildPreference);

            return rebaseAndDeriveProperties(node, ImmutableList.of(probe, build));
        }

        @Override
        public PlanWithProperties visitSemiJoin(SemiJoinNode node, StreamPreferredProperties parentPreferences)
        {
            PlanWithProperties source = planAndEnforce(
                    node.getSource(),
                    defaultParallelism(session),
                    parentPreferences.constrainTo(node.getSource().getOutputSymbols()).withDefaultParallelism(session));

            // this filter source consumes the input completely, so we do not pass through parent preferences
            PlanWithProperties filteringSource = planAndEnforce(node.getFilteringSource(), singleStream(), defaultParallelism(session));

            return rebaseAndDeriveProperties(node, ImmutableList.of(source, filteringSource));
        }

        @Override
        public PlanWithProperties visitIndexJoin(IndexJoinNode node, StreamPreferredProperties parentPreferences)
        {
            PlanWithProperties probe = planAndEnforce(
                    node.getProbeSource(),
                    defaultParallelism(session),
                    parentPreferences.constrainTo(node.getProbeSource().getOutputSymbols()).withDefaultParallelism(session));

            // index source does not support local parallel and must produce a single stream
            StreamProperties indexStreamProperties = derivePropertiesRecursively(node.getIndexSource());
            checkArgument(indexStreamProperties.getDistribution() == SINGLE, "index source must be single stream");
            PlanWithProperties index = new PlanWithProperties(node.getIndexSource(), indexStreamProperties);

            return rebaseAndDeriveProperties(node, ImmutableList.of(probe, index));
        }

        //
        // Helpers
        //

        private PlanWithProperties planAndEnforceChildren(PlanNode node, StreamPreferredProperties requiredProperties, StreamPreferredProperties preferredProperties)
        {
            // plan and enforce each child, but strip any requirement not in terms of symbols produced from the child
            // Note: this assumes the child uses the same symbols as the parent
            List<PlanWithProperties> children = node.getSources().stream()
                    .map(source -> planAndEnforce(
                            source,
                            requiredProperties.constrainTo(source.getOutputSymbols()),
                            preferredProperties.constrainTo(source.getOutputSymbols())))
                    .collect(toImmutableList());

            return rebaseAndDeriveProperties(node, children);
        }

        private PlanWithProperties planAndEnforce(PlanNode node, StreamPreferredProperties requiredProperties, StreamPreferredProperties preferredProperties)
        {
            // verify properties are in terms of symbols produced by the node
            List<Symbol> outputSymbols = node.getOutputSymbols();
            checkArgument(requiredProperties.getPartitioningColumns().map(outputSymbols::containsAll).orElse(true));
            checkArgument(preferredProperties.getPartitioningColumns().map(outputSymbols::containsAll).orElse(true));

            // plan the node using the preferred properties
            PlanWithProperties result = node.accept(this, preferredProperties);

            // enforce the required properties
            result = enforce(result, requiredProperties);

            return result;
        }

        private PlanWithProperties enforce(PlanWithProperties planWithProperties, StreamPreferredProperties requiredProperties)
        {
            if (requiredProperties.isSatisfiedBy(planWithProperties.getProperties())) {
                return planWithProperties;
            }

            if (requiredProperties.isSingleStreamPreferred()) {
                ExchangeNode exchangeNode = gatheringExchange(idAllocator.getNextId(), LOCAL, planWithProperties.getNode());
                return deriveProperties(exchangeNode, planWithProperties.getProperties());
            }

            Optional<List<Symbol>> requiredPartitionColumns = requiredProperties.getPartitioningColumns();
            if (!requiredPartitionColumns.isPresent()) {
                // unpartitioned parallel streams required
                ExchangeNode exchangeNode = partitionedExchange(
                        idAllocator.getNextId(),
                        LOCAL,
                        planWithProperties.getNode(),
                        new PartitionFunctionBinding(FIXED_RANDOM_DISTRIBUTION, planWithProperties.getNode().getOutputSymbols(), ImmutableList.of()));

                return deriveProperties(exchangeNode, planWithProperties.getProperties());
            }

            if (requiredProperties.isParallelPreferred()) {
                // partitioned parallel streams required
                ExchangeNode exchangeNode = partitionedExchange(
                        idAllocator.getNextId(),
                        LOCAL,
                        planWithProperties.getNode(),
                        requiredPartitionColumns.get(),
                        Optional.empty());
                return deriveProperties(exchangeNode, planWithProperties.getProperties());
            }

            // no explicit parallel requirement, so gather to a single stream
            ExchangeNode exchangeNode = gatheringExchange(
                    idAllocator.getNextId(),
                    LOCAL,
                    planWithProperties.getNode());
            return deriveProperties(exchangeNode, planWithProperties.getProperties());
        }

        private PlanWithProperties rebaseAndDeriveProperties(PlanNode node, List<PlanWithProperties> children)
        {
            PlanNode result = replaceChildren(
                    node,
                    children.stream()
                            .map(PlanWithProperties::getNode)
                            .collect(toList()));

            List<StreamProperties> inputProperties = children.stream()
                    .map(PlanWithProperties::getProperties)
                    .collect(toImmutableList());

            return deriveProperties(result, inputProperties);
        }

        private PlanWithProperties deriveProperties(PlanNode result, StreamProperties inputProperties)
        {
            return new PlanWithProperties(result, StreamPropertyDerivations.deriveProperties(result, inputProperties, metadata, session, symbolAllocator.getTypes(), parser));
        }

        private PlanWithProperties deriveProperties(PlanNode result, List<StreamProperties> inputProperties)
        {
            return new PlanWithProperties(result, StreamPropertyDerivations.deriveProperties(result, inputProperties, metadata, session, symbolAllocator.getTypes(), parser));
        }

        private StreamProperties derivePropertiesRecursively(PlanNode node)
        {
            List<StreamProperties> inputProperties = node.getSources().stream()
                    .map(this::derivePropertiesRecursively)
                    .collect(toImmutableList());
            return StreamPropertyDerivations.deriveProperties(node, inputProperties, metadata, session, symbolAllocator.getTypes(), parser);
        }
    }

    private static class PlanWithProperties
    {
        private final PlanNode node;
        private final StreamProperties properties;

        public PlanWithProperties(PlanNode node, StreamProperties properties)
        {
            this.node = requireNonNull(node, "node is null");
            this.properties = requireNonNull(properties, "StreamProperties is null");
        }

        public PlanNode getNode()
        {
            return node;
        }

        public StreamProperties getProperties()
        {
            return properties;
        }
    }
}
