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
import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.CteConsumerNode;
import com.facebook.presto.spi.plan.CteProducerNode;
import com.facebook.presto.spi.plan.CteReferenceNode;
import com.facebook.presto.spi.plan.DataOrganizationSpecification;
import com.facebook.presto.spi.plan.DeleteNode;
import com.facebook.presto.spi.plan.DistinctLimitNode;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.ExceptNode;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.IndexJoinNode;
import com.facebook.presto.spi.plan.IndexSourceNode;
import com.facebook.presto.spi.plan.IntersectNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.MarkDistinctNode;
import com.facebook.presto.spi.plan.MetadataDeleteNode;
import com.facebook.presto.spi.plan.Ordering;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.OutputNode;
import com.facebook.presto.spi.plan.PartitioningScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.SemiJoinNode;
import com.facebook.presto.spi.plan.SetOperationNode;
import com.facebook.presto.spi.plan.SortNode;
import com.facebook.presto.spi.plan.SpatialJoinNode;
import com.facebook.presto.spi.plan.TableFinishNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.TableWriterNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.plan.UnionNode;
import com.facebook.presto.spi.plan.UnnestNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.plan.WindowNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.RowExpressionVariableInliner;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.AssignUniqueId;
import com.facebook.presto.sql.planner.plan.CallDistributedProcedureNode;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.ExplainAnalyzeNode;
import com.facebook.presto.sql.planner.plan.GroupIdNode;
import com.facebook.presto.sql.planner.plan.LateralJoinNode;
import com.facebook.presto.sql.planner.plan.MergeProcessorNode;
import com.facebook.presto.sql.planner.plan.MergeWriterNode;
import com.facebook.presto.sql.planner.plan.OffsetNode;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.facebook.presto.sql.planner.plan.RowNumberNode;
import com.facebook.presto.sql.planner.plan.SampleNode;
import com.facebook.presto.sql.planner.plan.SequenceNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.StatisticsWriterNode;
import com.facebook.presto.sql.planner.plan.TableFunctionNode;
import com.facebook.presto.sql.planner.plan.TableFunctionProcessorNode;
import com.facebook.presto.sql.planner.plan.TableWriterMergeNode;
import com.facebook.presto.sql.planner.plan.TopNRowNumberNode;
import com.facebook.presto.sql.planner.plan.UpdateNode;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.sql.analyzer.ExpressionTreeUtils.getNodeLocation;
import static com.facebook.presto.sql.planner.optimizations.ApplyNodeUtil.verifySubquerySupported;
import static com.facebook.presto.sql.planner.optimizations.PartitioningUtils.translateVariable;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.isNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

/**
 * Re-maps symbol references that are just aliases of each other (e.g., due to projections like {@code $0 := $1})
 * <p/>
 * E.g.,
 * <p/>
 * {@code Output[$0, $1] -> Project[$0 := $2, $1 := $3 * 100] -> Aggregate[$2, $3 := sum($4)] -> ...}
 * <p/>
 * gets rewritten as
 * <p/>
 * {@code Output[$2, $1] -> Project[$2, $1 := $3 * 100] -> Aggregate[$2, $3 := sum($4)] -> ...}
 */
public class UnaliasSymbolReferences
        implements PlanOptimizer
{
    private final FunctionAndTypeManager functionAndTypeManager;

    public UnaliasSymbolReferences(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionManager is null");
    }

    @Override
    public PlanOptimizerResult optimize(PlanNode plan, Session session, TypeProvider types, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(types, "types is null");
        requireNonNull(variableAllocator, "variableAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");
        PlanNode rewrittenPlan = SimplePlanRewriter.rewriteWith(new Rewriter(types, functionAndTypeManager, warningCollector), plan);
        return PlanOptimizerResult.optimizerResult(rewrittenPlan, !rewrittenPlan.equals(plan));
    }

    private static class Rewriter
            extends SimplePlanRewriter<Void>
    {
        private final Map<String, String> mapping = new HashMap<>();
        private final TypeProvider types;
        private final RowExpressionDeterminismEvaluator determinismEvaluator;

        private final FunctionAndTypeManager functionAndTypeManager;
        private final WarningCollector warningCollector;

        private Rewriter(TypeProvider types, FunctionAndTypeManager functionAndTypeManager, WarningCollector warningCollector)
        {
            this.types = types;
            this.functionAndTypeManager = functionAndTypeManager;
            this.determinismEvaluator = new RowExpressionDeterminismEvaluator(functionAndTypeManager);
            this.warningCollector = warningCollector;
        }

        public Map<String, String> getMapping()
        {
            return mapping;
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, RewriteContext<Void> context)
        {
            PlanNode source = context.rewrite(node.getSource());
            //TODO: use mapper in other methods
            SymbolMapper mapper = new SymbolMapper(mapping, types, warningCollector);
            return mapper.map(node, source);
        }

        @Override
        public PlanNode visitCteReference(CteReferenceNode node, RewriteContext<Void> context)
        {
            PlanNode source = context.rewrite(node.getSource());
            return new CteReferenceNode(node.getSourceLocation(), node.getId(), source, node.getCteId());
        }

        public PlanNode visitCteProducer(CteProducerNode node, RewriteContext<Void> context)
        {
            PlanNode source = context.rewrite(node.getSource());
            List<VariableReferenceExpression> canonical = Lists.transform(node.getOutputVariables(), this::canonicalize);
            return new CteProducerNode(node.getSourceLocation(), node.getId(), source, node.getCteId(), node.getRowCountVariable(), canonical);
        }

        public PlanNode visitCteConsumer(CteConsumerNode node, RewriteContext<Void> context)
        {
            // No rewrite on source by cte consumer
            return node;
        }

        public PlanNode visitSequence(SequenceNode node, RewriteContext<Void> context)
        {
            List<PlanNode> cteProducers = node.getCteProducers().stream().map(c ->
                            SimplePlanRewriter.rewriteWith(new Rewriter(types, functionAndTypeManager, warningCollector), c))
                    .collect(Collectors.toList());
            PlanNode primarySource = context.rewrite(node.getPrimarySource());
            return new SequenceNode(node.getSourceLocation(), node.getId(), cteProducers, primarySource, node.getCteDependencyGraph());
        }

        @Override
        public PlanNode visitGroupId(GroupIdNode node, RewriteContext<Void> context)
        {
            PlanNode source = context.rewrite(node.getSource());

            Map<VariableReferenceExpression, VariableReferenceExpression> newGroupingMappings = new HashMap<>();
            ImmutableList.Builder<List<VariableReferenceExpression>> newGroupingSets = ImmutableList.builder();

            for (List<VariableReferenceExpression> groupingSet : node.getGroupingSets()) {
                ImmutableList.Builder<VariableReferenceExpression> newGroupingSet = ImmutableList.builder();
                for (VariableReferenceExpression output : groupingSet) {
                    newGroupingMappings.putIfAbsent(canonicalize(output), canonicalize(node.getGroupingColumns().get(output)));
                    newGroupingSet.add(canonicalize(output));
                }
                newGroupingSets.add(newGroupingSet.build());
            }

            return new GroupIdNode(node.getSourceLocation(), node.getId(), source, newGroupingSets.build(), newGroupingMappings, canonicalizeAndDistinct(node.getAggregationArguments()), canonicalize(node.getGroupIdVariable()));
        }

        @Override
        public PlanNode visitExplainAnalyze(ExplainAnalyzeNode node, RewriteContext<Void> context)
        {
            PlanNode source = context.rewrite(node.getSource());
            return new ExplainAnalyzeNode(node.getSourceLocation(), node.getId(), source, canonicalize(node.getOutputVariable()), node.isVerbose(), node.getFormat());
        }

        @Override
        public PlanNode visitMarkDistinct(MarkDistinctNode node, RewriteContext<Void> context)
        {
            PlanNode source = context.rewrite(node.getSource());
            return new MarkDistinctNode(node.getSourceLocation(), node.getId(), source, canonicalize(node.getMarkerVariable()), canonicalizeAndDistinct(node.getDistinctVariables()), canonicalize(node.getHashVariable()));
        }

        @Override
        public PlanNode visitUnnest(UnnestNode node, RewriteContext<Void> context)
        {
            PlanNode source = context.rewrite(node.getSource());
            ImmutableMap.Builder<VariableReferenceExpression, List<VariableReferenceExpression>> builder = ImmutableMap.builder();
            for (Map.Entry<VariableReferenceExpression, List<VariableReferenceExpression>> entry : node.getUnnestVariables().entrySet()) {
                builder.put(canonicalize(entry.getKey()), entry.getValue());
            }
            return new UnnestNode(node.getSourceLocation(), node.getId(), source, canonicalizeAndDistinct(node.getReplicateVariables()), builder.build(), node.getOrdinalityVariable());
        }

        @Override
        public PlanNode visitWindow(WindowNode node, RewriteContext<Void> context)
        {
            PlanNode source = context.rewrite(node.getSource());

            ImmutableMap.Builder<VariableReferenceExpression, WindowNode.Function> functions = ImmutableMap.builder();
            for (Map.Entry<VariableReferenceExpression, WindowNode.Function> entry : node.getWindowFunctions().entrySet()) {
                VariableReferenceExpression variable = entry.getKey();

                // Be aware of the CallExpression handling.
                CallExpression callExpression = entry.getValue().getFunctionCall();
                List<RowExpression> rewrittenArguments = canonicalizeCallExpression(callExpression);
                WindowNode.Frame canonicalFrame = canonicalize(entry.getValue().getFrame());

                functions.put(
                        canonicalize(variable),
                        new WindowNode.Function(
                                call(
                                        callExpression.getDisplayName(),
                                        callExpression.getFunctionHandle(),
                                        callExpression.getType(),
                                        rewrittenArguments),
                                canonicalFrame,
                                entry.getValue().isIgnoreNulls()));
            }

            return new WindowNode(
                    node.getSourceLocation(),
                    node.getId(),
                    source,
                    canonicalizeAndDistinct(node.getSpecification()),
                    functions.build(),
                    canonicalize(node.getHashVariable()),
                    canonicalize(node.getPrePartitionedInputs()),
                    node.getPreSortedOrderPrefix());
        }

        private List<RowExpression> canonicalizeCallExpression(CallExpression callExpression)
        {
            // TODO: arguments will be pure RowExpression once we introduce subquery expression for RowExpression.
            return callExpression.getArguments()
                    .stream()
                    .map(this::canonicalize)
                    .collect(toImmutableList());
        }

        private WindowNode.Frame canonicalize(WindowNode.Frame frame)
        {
            return new WindowNode.Frame(
                    frame.getType(),
                    frame.getStartType(),
                    canonicalize(frame.getStartValue()),
                    canonicalize(frame.getSortKeyCoercedForFrameStartComparison()),
                    frame.getEndType(),
                    canonicalize(frame.getEndValue()),
                    canonicalize(frame.getSortKeyCoercedForFrameEndComparison()),
                    frame.getOriginalStartValue(),
                    frame.getOriginalEndValue());
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<Void> context)
        {
            return node;
        }

        @Override
        public PlanNode visitExchange(ExchangeNode node, RewriteContext<Void> context)
        {
            List<PlanNode> sources = node.getSources().stream()
                    .map(context::rewrite)
                    .collect(toImmutableList());

            mapExchangeNodeSymbols(node);

            List<List<VariableReferenceExpression>> inputs = new ArrayList<>();
            for (int i = 0; i < node.getInputs().size(); i++) {
                inputs.add(new ArrayList<>());
            }
            Set<VariableReferenceExpression> addedOutputs = new HashSet<>();
            ImmutableList.Builder<VariableReferenceExpression> outputs = ImmutableList.builder();
            for (int variableIndex = 0; variableIndex < node.getOutputVariables().size(); variableIndex++) {
                VariableReferenceExpression canonicalOutput = canonicalize(node.getOutputVariables().get(variableIndex));
                if (addedOutputs.add(canonicalOutput)) {
                    outputs.add(canonicalOutput);
                    for (int i = 0; i < node.getInputs().size(); i++) {
                        List<VariableReferenceExpression> input = node.getInputs().get(i);
                        inputs.get(i).add(canonicalize(input.get(variableIndex)));
                    }
                }
            }

            PartitioningScheme partitioningScheme = new PartitioningScheme(
                    translateVariable(node.getPartitioningScheme().getPartitioning(), this::canonicalize),
                    outputs.build(),
                    canonicalize(node.getPartitioningScheme().getHashColumn()),
                    node.getPartitioningScheme().isReplicateNullsAndAny(),
                    node.getPartitioningScheme().isScaleWriters(),
                    node.getPartitioningScheme().getEncoding(),
                    node.getPartitioningScheme().getBucketToPartition());

            Optional<OrderingScheme> orderingScheme = node.getOrderingScheme().map(this::canonicalizeAndDistinct);

            return new ExchangeNode(node.getSourceLocation(), node.getId(), node.getType(), node.getScope(), partitioningScheme, sources, inputs, node.isEnsureSourceOrdering(), orderingScheme);
        }

        private void mapExchangeNodeSymbols(ExchangeNode node)
        {
            if (node.getInputs().size() == 1) {
                mapExchangeNodeOutputToInputSymbols(node);
                return;
            }

            // Mapping from list [node.getInput(0).get(symbolIndex), node.getInput(1).get(symbolIndex), ...] to node.getOutputVariables(symbolIndex).
            // All symbols are canonical.
            Map<List<VariableReferenceExpression>, VariableReferenceExpression> inputsToOutputs = new HashMap<>();
            // Map each same list of input symbols [I1, I2, ..., In] to the same output symbol O
            for (int variableIndex = 0; variableIndex < node.getOutputVariables().size(); variableIndex++) {
                VariableReferenceExpression canonicalOutput = canonicalize(node.getOutputVariables().get(variableIndex));
                List<VariableReferenceExpression> canonicalInputs = canonicalizeExchangeNodeInputs(node, variableIndex);
                VariableReferenceExpression output = inputsToOutputs.get(canonicalInputs);

                if (output == null || canonicalOutput.equals(output)) {
                    inputsToOutputs.put(canonicalInputs, canonicalOutput);
                }
                else {
                    map(canonicalOutput, output);
                }
            }
        }

        private void mapExchangeNodeOutputToInputSymbols(ExchangeNode node)
        {
            checkState(node.getInputs().size() == 1);

            for (int variableIndex = 0; variableIndex < node.getOutputVariables().size(); variableIndex++) {
                VariableReferenceExpression canonicalOutput = canonicalize(node.getOutputVariables().get(variableIndex));
                VariableReferenceExpression canonicalInput = canonicalize(node.getInputs().get(0).get(variableIndex));

                if (!canonicalOutput.equals(canonicalInput)) {
                    map(canonicalOutput, canonicalInput);
                }
            }
        }

        private List<VariableReferenceExpression> canonicalizeExchangeNodeInputs(ExchangeNode node, int symbolIndex)
        {
            return node.getInputs().stream()
                    .map(input -> canonicalize(input.get(symbolIndex)))
                    .collect(toImmutableList());
        }

        @Override
        public PlanNode visitRemoteSource(RemoteSourceNode node, RewriteContext<Void> context)
        {
            return new RemoteSourceNode(
                    node.getSourceLocation(),
                    node.getId(),
                    node.getStatsEquivalentPlanNode(),
                    node.getSourceFragmentIds(),
                    canonicalizeAndDistinct(node.getOutputVariables()),
                    node.isEnsureSourceOrdering(),
                    node.getOrderingScheme().map(this::canonicalizeAndDistinct),
                    node.getExchangeType(),
                    node.getEncoding());
        }

        @Override
        public PlanNode visitOffset(OffsetNode node, RewriteContext<Void> context)
        {
            return context.defaultRewrite(node);
        }

        @Override
        public PlanNode visitLimit(LimitNode node, RewriteContext<Void> context)
        {
            return context.defaultRewrite(node);
        }

        @Override
        public PlanNode visitDistinctLimit(DistinctLimitNode node, RewriteContext<Void> context)
        {
            return new DistinctLimitNode(node.getSourceLocation(), node.getId(), context.rewrite(node.getSource()), node.getLimit(), node.isPartial(), canonicalizeAndDistinct(node.getDistinctVariables()), canonicalize(node.getHashVariable()), node.getTimeoutMillis());
        }

        @Override
        public PlanNode visitSample(SampleNode node, RewriteContext<Void> context)
        {
            return new SampleNode(node.getSourceLocation(), node.getId(), context.rewrite(node.getSource()), node.getSampleRatio(), node.getSampleType());
        }

        @Override
        public PlanNode visitValues(ValuesNode node, RewriteContext<Void> context)
        {
            List<List<RowExpression>> canonicalizedRows = node.getRows().stream()
                    .map(rowExpressions -> rowExpressions.stream()
                            .map(this::canonicalize)
                            .collect(toImmutableList()))
                    .collect(toImmutableList());
            List<VariableReferenceExpression> canonicalizedOutputVariables = canonicalizeAndDistinct(node.getOutputVariables());
            checkState(node.getOutputVariables().size() == canonicalizedOutputVariables.size(), "Values output symbols were pruned");
            return new ValuesNode(
                    node.getSourceLocation(),
                    node.getId(),
                    canonicalizedOutputVariables,
                    canonicalizedRows,
                    node.getValuesNodeLabel());
        }

        @Override
        public PlanNode visitDelete(DeleteNode node, RewriteContext<Void> context)
        {
            return new DeleteNode(node.getSourceLocation(), node.getId(), context.rewrite(node.getSource()), canonicalize(node.getRowId()), node.getOutputVariables(), node.getInputDistribution());
        }

        @Override
        public PlanNode visitUpdate(UpdateNode node, RewriteContext<Void> context)
        {
            return new UpdateNode(node.getSourceLocation(), node.getId(), node.getSource(), canonicalize(node.getRowId()), node.getColumnValueAndRowIdSymbols(), node.getOutputVariables());
        }

        @Override
        public PlanNode visitMergeWriter(MergeWriterNode node, RewriteContext<Void> context)
        {
            PlanNode source = context.rewrite(node.getSource());
            SymbolMapper mapper = new SymbolMapper(mapping, types, warningCollector);
            return mapper.map(node, source);
        }

        @Override
        public PlanNode visitMergeProcessor(MergeProcessorNode node, RewriteContext<Void> context)
        {
            PlanNode source = context.rewrite(node.getSource());
            SymbolMapper mapper = new SymbolMapper(mapping, types, warningCollector);
            return mapper.map(node, source);
        }

        @Override
        public PlanNode visitStatisticsWriterNode(StatisticsWriterNode node, RewriteContext<Void> context)
        {
            PlanNode source = context.rewrite(node.getSource());
            SymbolMapper mapper = new SymbolMapper(mapping, types, warningCollector);
            return mapper.map(node, source);
        }

        @Override
        public PlanNode visitTableFinish(TableFinishNode node, RewriteContext<Void> context)
        {
            PlanNode source = context.rewrite(node.getSource());
            SymbolMapper mapper = new SymbolMapper(mapping, types, warningCollector);
            return mapper.map(node, source);
        }

        @Override
        public PlanNode visitTableFunction(TableFunctionNode node, RewriteContext<Void> context)
        {
            Map<VariableReferenceExpression, VariableReferenceExpression> mappings =
                    Optional.ofNullable(context.get())
                            .map(c -> new HashMap<VariableReferenceExpression, VariableReferenceExpression>())
                            .orElseGet(HashMap::new);

            SymbolMapper mapper = new SymbolMapper(mappings, warningCollector);

            List<VariableReferenceExpression> newProperOutputs = node.getOutputVariables().stream()
                    .map(mapper::map)
                    .collect(toImmutableList());

            ImmutableList.Builder<PlanNode> newSources = ImmutableList.builder();
            ImmutableList.Builder<TableFunctionNode.TableArgumentProperties> newTableArgumentProperties = ImmutableList.builder();

            for (int i = 0; i < node.getSources().size(); i++) {
                PlanNode newSource = node.getSources().get(i).accept(this, context);
                newSources.add(newSource);

                SymbolMapper inputMapper = new SymbolMapper(new HashMap<>(), warningCollector);

                TableFunctionNode.TableArgumentProperties properties = node.getTableArgumentProperties().get(i);

                Optional<DataOrganizationSpecification> newSpecification = properties.getSpecification().map(inputMapper::mapAndDistinct);
                TableFunctionNode.PassThroughSpecification newPassThroughSpecification = new TableFunctionNode.PassThroughSpecification(
                        properties.getPassThroughSpecification().isDeclaredAsPassThrough(),
                        properties.getPassThroughSpecification().getColumns().stream()
                                .map(column -> new TableFunctionNode.PassThroughColumn(
                                        inputMapper.map(column.getOutputVariables()),
                                        column.isPartitioningColumn()))
                                .collect(toImmutableList()));
                newTableArgumentProperties.add(new TableFunctionNode.TableArgumentProperties(
                        properties.getArgumentName(),
                        properties.isRowSemantics(),
                        properties.isPruneWhenEmpty(),
                        newPassThroughSpecification,
                        properties.getRequiredColumns().stream()
                                .map(inputMapper::map)
                                .collect(toImmutableList()),
                        newSpecification));
            }

            return new TableFunctionNode(
                    node.getId(),
                    node.getName(),
                    node.getArguments(),
                    newProperOutputs,
                    newSources.build(),
                    newTableArgumentProperties.build(),
                    node.getCopartitioningLists(),
                    node.getHandle());
        }

        @Override
        public PlanNode visitTableFunctionProcessor(TableFunctionProcessorNode node, RewriteContext<Void> context)
        {
            if (!node.getSource().isPresent()) {
                Map<VariableReferenceExpression, VariableReferenceExpression> mappings =
                        Optional.ofNullable(context.get())
                                .map(c -> new HashMap<VariableReferenceExpression, VariableReferenceExpression>())
                                .orElseGet(HashMap::new);
                SymbolMapper mapper = new SymbolMapper(mappings, warningCollector);

                return new TableFunctionProcessorNode(
                        node.getId(),
                        node.getName(),
                        node.getProperOutputs().stream()
                                .map(mapper::map)
                                .collect(toImmutableList()),
                        Optional.empty(),
                        node.isPruneWhenEmpty(),
                        ImmutableList.of(),
                        ImmutableList.of(),
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableSet.of(),
                        0,
                        node.getHashSymbol().map(mapper::map),
                        node.getHandle());
            }

            PlanNode rewrittenSource = node.getSource().get().accept(this, context);
            Map<String, String> mappings = ((Rewriter) context.getNodeRewriter()).getMapping();
            SymbolMapper mapper = new SymbolMapper(mappings, types, warningCollector);

            return mapper.map(node, rewrittenSource);
        }

        @Override
        public PlanNode visitRowNumber(RowNumberNode node, RewriteContext<Void> context)
        {
            return new RowNumberNode(node.getSourceLocation(), node.getId(), context.rewrite(node.getSource()), canonicalizeAndDistinct(node.getPartitionBy()), canonicalize(node.getRowNumberVariable()), node.getMaxRowCountPerPartition(), node.isPartial(), canonicalize(node.getHashVariable()));
        }

        @Override
        public PlanNode visitTopNRowNumber(TopNRowNumberNode node, RewriteContext<Void> context)
        {
            return new TopNRowNumberNode(
                    node.getSourceLocation(),
                    node.getId(),
                    context.rewrite(node.getSource()),
                    canonicalizeAndDistinct(node.getSpecification()),
                    canonicalize(node.getRowNumberVariable()),
                    node.getMaxRowCountPerPartition(),
                    node.isPartial(),
                    canonicalize(node.getHashVariable()));
        }

        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<Void> context)
        {
            PlanNode source = context.rewrite(node.getSource());

            return new FilterNode(node.getSourceLocation(), node.getId(), source, canonicalize(node.getPredicate()));
        }

        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<Void> context)
        {
            PlanNode source = context.rewrite(node.getSource());
            return new ProjectNode(node.getSourceLocation(), node.getId(), source, canonicalize(node.getAssignments()), node.getLocality());
        }

        @Override
        public PlanNode visitOutput(OutputNode node, RewriteContext<Void> context)
        {
            PlanNode source = context.rewrite(node.getSource());

            List<VariableReferenceExpression> canonical = Lists.transform(node.getOutputVariables(), this::canonicalize);
            return new OutputNode(node.getSourceLocation(), node.getId(), source, node.getColumnNames(), canonical);
        }

        @Override
        public PlanNode visitEnforceSingleRow(EnforceSingleRowNode node, RewriteContext<Void> context)
        {
            PlanNode source = context.rewrite(node.getSource());

            return new EnforceSingleRowNode(node.getSourceLocation(), node.getId(), source);
        }

        @Override
        public PlanNode visitAssignUniqueId(AssignUniqueId node, RewriteContext<Void> context)
        {
            PlanNode source = context.rewrite(node.getSource());

            return new AssignUniqueId(node.getSourceLocation(), node.getId(), source, node.getIdVariable());
        }

        @Override
        public PlanNode visitApply(ApplyNode node, RewriteContext<Void> context)
        {
            PlanNode source = context.rewrite(node.getInput());
            PlanNode subquery = context.rewrite(node.getSubquery());
            List<VariableReferenceExpression> canonicalCorrelation = Lists.transform(node.getCorrelation(), this::canonicalize);

            Assignments assignments = canonicalize(node.getSubqueryAssignments());
            verifySubquerySupported(assignments);
            return new ApplyNode(node.getSourceLocation(), node.getId(), source, subquery, assignments, canonicalCorrelation, node.getOriginSubqueryError(), node.getMayParticipateInAntiJoin());
        }

        @Override
        public PlanNode visitLateralJoin(LateralJoinNode node, RewriteContext<Void> context)
        {
            PlanNode source = context.rewrite(node.getInput());
            PlanNode subquery = context.rewrite(node.getSubquery());
            List<VariableReferenceExpression> canonicalCorrelation = canonicalizeAndDistinct(node.getCorrelation());

            return new LateralJoinNode(node.getSourceLocation(), node.getId(), source, subquery, canonicalCorrelation, node.getType(), node.getOriginSubqueryError());
        }

        @Override
        public PlanNode visitTopN(TopNNode node, RewriteContext<Void> context)
        {
            PlanNode source = context.rewrite(node.getSource());

            SymbolMapper mapper = new SymbolMapper(mapping, types, warningCollector);
            return mapper.map(node, source, node.getId());
        }

        @Override
        public PlanNode visitSort(SortNode node, RewriteContext<Void> context)
        {
            PlanNode source = context.rewrite(node.getSource());

            return new SortNode(node.getSourceLocation(), node.getId(), source, canonicalizeAndDistinct(node.getOrderingScheme()), node.isPartial(), node.getPartitionBy());
        }

        @Override
        public PlanNode visitJoin(JoinNode node, RewriteContext<Void> context)
        {
            PlanNode left = context.rewrite(node.getLeft());
            PlanNode right = context.rewrite(node.getRight());

            List<EquiJoinClause> canonicalCriteria = canonicalizeJoinCriteria(node.getCriteria());
            Optional<RowExpression> canonicalFilter = node.getFilter().map(this::canonicalize);
            Optional<VariableReferenceExpression> canonicalLeftHashVariable = canonicalize(node.getLeftHashVariable());
            Optional<VariableReferenceExpression> canonicalRightHashVariable = canonicalize(node.getRightHashVariable());

            Map<String, VariableReferenceExpression> canonicalDynamicFilters = canonicalizeAndDistinct(node.getDynamicFilters());

            if (node.getType().equals(INNER)) {
                canonicalCriteria.stream()
                        .filter(clause -> clause.getLeft().getType().equals(clause.getRight().getType()) && clause.getLeft().getType().equalValuesAreIdentical())
                        .filter(clause -> node.getOutputVariables().contains(clause.getLeft()))
                        .forEach(clause -> map(clause.getRight(), clause.getLeft()));
            }

            return new JoinNode(
                    node.getSourceLocation(),
                    node.getId(),
                    node.getType(),
                    left,
                    right,
                    canonicalCriteria,
                    canonicalizeAndDistinct(node.getOutputVariables()),
                    canonicalFilter,
                    canonicalLeftHashVariable,
                    canonicalRightHashVariable,
                    node.getDistributionType(),
                    canonicalDynamicFilters);
        }

        @Override
        public PlanNode visitSemiJoin(SemiJoinNode node, RewriteContext<Void> context)
        {
            PlanNode source = context.rewrite(node.getSource());
            PlanNode filteringSource = context.rewrite(node.getFilteringSource());

            return new SemiJoinNode(
                    node.getSourceLocation(),
                    node.getId(),
                    source,
                    filteringSource,
                    canonicalize(node.getSourceJoinVariable()),
                    canonicalize(node.getFilteringSourceJoinVariable()),
                    canonicalize(node.getSemiJoinOutput()),
                    canonicalize(node.getSourceHashVariable()),
                    canonicalize(node.getFilteringSourceHashVariable()),
                    node.getDistributionType(),
                    node.getDynamicFilters());
        }

        @Override
        public PlanNode visitSpatialJoin(SpatialJoinNode node, RewriteContext<Void> context)
        {
            PlanNode left = context.rewrite(node.getLeft());
            PlanNode right = context.rewrite(node.getRight());

            return new SpatialJoinNode(
                    node.getSourceLocation(),
                    node.getId(),
                    node.getType(),
                    left,
                    right,
                    canonicalizeAndDistinct(node.getOutputVariables()),
                    canonicalize(node.getProbeGeometryVariable()),
                    canonicalize(node.getBuildGeometryVariable()),
                    canonicalize(node.getRadiusVariable()),
                    canonicalize(node.getFilter()),
                    canonicalize(node.getLeftPartitionVariable()),
                    canonicalize(node.getRightPartitionVariable()),
                    node.getKdbTree());
        }

        @Override
        public PlanNode visitIndexSource(IndexSourceNode node, RewriteContext<Void> context)
        {
            return new IndexSourceNode(node.getSourceLocation(), node.getId(), node.getIndexHandle(), node.getTableHandle(), canonicalize(node.getLookupVariables()), node.getOutputVariables(), node.getAssignments(), node.getCurrentConstraint());
        }

        @Override
        public PlanNode visitIndexJoin(IndexJoinNode node, RewriteContext<Void> context)
        {
            PlanNode probeSource = context.rewrite(node.getProbeSource());
            PlanNode indexSource = context.rewrite(node.getIndexSource());

            return new IndexJoinNode(
                    node.getSourceLocation(),
                    node.getId(),
                    node.getType(),
                    probeSource,
                    indexSource,
                    canonicalizeIndexJoinCriteria(node.getCriteria()),
                    node.getFilter().map(this::canonicalize),
                    canonicalize(node.getProbeHashVariable()),
                    canonicalize(node.getIndexHashVariable()),
                    node.getLookupVariables());
        }

        @Override
        public PlanNode visitUnion(UnionNode node, RewriteContext<Void> context)
        {
            return new UnionNode(node.getSourceLocation(), node.getId(), rewriteSources(node, context).build(), canonicalizeSetOperationOutputVariables(node.getOutputVariables()), canonicalizeSetOperationVariableMap(node.getVariableMapping()));
        }

        @Override
        public PlanNode visitIntersect(IntersectNode node, RewriteContext<Void> context)
        {
            return new IntersectNode(node.getSourceLocation(), node.getId(), rewriteSources(node, context).build(), canonicalizeSetOperationOutputVariables(node.getOutputVariables()), canonicalizeSetOperationVariableMap(node.getVariableMapping()));
        }

        @Override
        public PlanNode visitExcept(ExceptNode node, RewriteContext<Void> context)
        {
            return new ExceptNode(node.getSourceLocation(), node.getId(), rewriteSources(node, context).build(), canonicalizeSetOperationOutputVariables(node.getOutputVariables()), canonicalizeSetOperationVariableMap(node.getVariableMapping()));
        }

        private static ImmutableList.Builder<PlanNode> rewriteSources(SetOperationNode node, RewriteContext<Void> context)
        {
            ImmutableList.Builder<PlanNode> rewrittenSources = ImmutableList.builder();
            for (PlanNode source : node.getSources()) {
                rewrittenSources.add(context.rewrite(source));
            }
            return rewrittenSources;
        }

        @Override
        public PlanNode visitCallDistributedProcedure(CallDistributedProcedureNode node, RewriteContext<Void> context)
        {
            PlanNode source = context.rewrite(node.getSource());
            SymbolMapper mapper = new SymbolMapper(mapping, types, warningCollector);
            return mapper.map(node, source);
        }

        @Override
        public PlanNode visitTableWriter(TableWriterNode node, RewriteContext<Void> context)
        {
            PlanNode source = context.rewrite(node.getSource());
            SymbolMapper mapper = new SymbolMapper(mapping, types, warningCollector);
            return mapper.map(node, source);
        }

        @Override
        public PlanNode visitTableWriteMerge(TableWriterMergeNode node, RewriteContext<Void> context)
        {
            PlanNode source = context.rewrite(node.getSource());
            SymbolMapper mapper = new SymbolMapper(mapping, types, warningCollector);
            return mapper.map(node, source);
        }

        @Override
        public PlanNode visitMetadataDelete(MetadataDeleteNode node, RewriteContext<Void> context)
        {
            // MetadataDeleteNode has no symbols to unalias, so return unchanged
            return node;
        }

        @Override
        public PlanNode visitPlan(PlanNode node, RewriteContext<Void> context)
        {
            throw new UnsupportedOperationException("Unsupported plan node " + node.getClass().getSimpleName());
        }

        private void map(VariableReferenceExpression variable, VariableReferenceExpression canonical)
        {
            Preconditions.checkArgument(!variable.equals(canonical), "Can't map variable to itself: %s", variable);
            mapping.put(variable.getName(), canonical.getName());
        }

        private Assignments canonicalize(Assignments oldAssignments)
        {
            Map<RowExpression, VariableReferenceExpression> computedExpressions = new HashMap<>();
            Assignments.Builder assignments = Assignments.builder();
            for (Map.Entry<VariableReferenceExpression, RowExpression> entry : oldAssignments.getMap().entrySet()) {
                RowExpression expression = canonicalize(entry.getValue());
                if (expression instanceof VariableReferenceExpression) {
                    // Always map a trivial variable projection
                    VariableReferenceExpression variable = (VariableReferenceExpression) expression;
                    if (!variable.getName().equals(entry.getKey().getName())) {
                        map(entry.getKey(), variable);
                    }
                }
                else if (!isNull(expression) && determinismEvaluator.isDeterministic(expression)) {
                    // Try to map same deterministic expressions within a projection into the same symbol
                    // Omit NullLiterals since those have ambiguous types
                    VariableReferenceExpression computedVariable = computedExpressions.get(expression);
                    if (computedVariable == null) {
                        // If we haven't seen the expression before in this projection, record it
                        computedExpressions.put(expression, entry.getKey());
                    }
                    else {
                        // If we have seen the expression before and if it is deterministic
                        // then we can rewrite references to the current symbol in terms of the parallel computedVariable in the projection
                        map(entry.getKey(), computedVariable);
                    }
                }

                VariableReferenceExpression canonical = canonicalize(entry.getKey());
                assignments.put(canonical, expression);
            }
            return assignments.build();
        }

        private VariableReferenceExpression canonicalize(VariableReferenceExpression variable)
        {
            String canonical = variable.getName();
            while (mapping.containsKey(canonical)) {
                canonical = mapping.get(canonical);
            }
            return new VariableReferenceExpression(variable.getSourceLocation(), canonical, types.get(new SymbolReference(getNodeLocation(variable.getSourceLocation()), canonical)));
        }

        private Optional<VariableReferenceExpression> canonicalize(Optional<VariableReferenceExpression> variable)
        {
            if (variable.isPresent()) {
                return Optional.of(canonicalize(variable.get()));
            }
            return Optional.empty();
        }

        private RowExpression canonicalize(RowExpression value)
        {
            return RowExpressionVariableInliner.inlineVariables(this::canonicalize, value);
        }

        private List<VariableReferenceExpression> canonicalizeAndDistinct(List<VariableReferenceExpression> outputs)
        {
            Set<VariableReferenceExpression> added = new HashSet<>();
            ImmutableList.Builder<VariableReferenceExpression> builder = ImmutableList.builder();
            for (VariableReferenceExpression variable : outputs) {
                VariableReferenceExpression canonical = canonicalize(variable);
                if (added.add(canonical)) {
                    builder.add(canonical);
                }
            }
            return builder.build();
        }

        private Map<String, VariableReferenceExpression> canonicalizeAndDistinct(Map<String, VariableReferenceExpression> dynamicFilters)
        {
            Set<VariableReferenceExpression> added = new HashSet<>();
            ImmutableMap.Builder<String, VariableReferenceExpression> builder = ImmutableMap.builder();
            for (Map.Entry<String, VariableReferenceExpression> entry : dynamicFilters.entrySet()) {
                VariableReferenceExpression canonical = canonicalize(entry.getValue());
                if (added.add(canonical)) {
                    builder.put(entry.getKey(), canonical);
                }
            }
            return builder.build();
        }

        private DataOrganizationSpecification canonicalizeAndDistinct(DataOrganizationSpecification specification)
        {
            return new DataOrganizationSpecification(
                    canonicalizeAndDistinct(specification.getPartitionBy()),
                    specification.getOrderingScheme().map(this::canonicalizeAndDistinct));
        }

        private OrderingScheme canonicalizeAndDistinct(OrderingScheme orderingScheme)
        {
            Set<VariableReferenceExpression> added = new HashSet<>();
            ImmutableList.Builder<VariableReferenceExpression> variables = ImmutableList.builder();
            ImmutableMap.Builder<VariableReferenceExpression, SortOrder> orderings = ImmutableMap.builder();
            for (VariableReferenceExpression variable : orderingScheme.getOrderByVariables()) {
                VariableReferenceExpression canonical = canonicalize(variable);
                if (added.add(canonical)) {
                    variables.add(canonical);
                    orderings.put(canonical, orderingScheme.getOrdering(variable));
                }
            }

            ImmutableMap<VariableReferenceExpression, SortOrder> orderingsMap = orderings.build();
            return new OrderingScheme(variables.build().stream().map(variable -> new Ordering(variable, orderingsMap.get(variable))).collect(toImmutableList()));
        }

        private Set<VariableReferenceExpression> canonicalize(Set<VariableReferenceExpression> variables)
        {
            return variables.stream()
                    .map(this::canonicalize)
                    .collect(toImmutableSet());
        }

        private List<EquiJoinClause> canonicalizeJoinCriteria(List<EquiJoinClause> criteria)
        {
            ImmutableList.Builder<EquiJoinClause> builder = ImmutableList.builder();
            for (EquiJoinClause clause : criteria) {
                builder.add(new EquiJoinClause(canonicalize(clause.getLeft()), canonicalize(clause.getRight())));
            }

            return builder.build();
        }

        private List<IndexJoinNode.EquiJoinClause> canonicalizeIndexJoinCriteria(List<IndexJoinNode.EquiJoinClause> criteria)
        {
            ImmutableList.Builder<IndexJoinNode.EquiJoinClause> builder = ImmutableList.builder();
            for (IndexJoinNode.EquiJoinClause clause : criteria) {
                builder.add(new IndexJoinNode.EquiJoinClause(canonicalize(clause.getProbe()), canonicalize(clause.getIndex())));
            }

            return builder.build();
        }

        private Map<VariableReferenceExpression, List<VariableReferenceExpression>> canonicalizeSetOperationVariableMap(Map<VariableReferenceExpression, List<VariableReferenceExpression>> setOperationVariableMap)
        {
            LinkedHashMap<VariableReferenceExpression, List<VariableReferenceExpression>> result = new LinkedHashMap<>();
            Set<VariableReferenceExpression> addVariables = new HashSet<>();
            for (Map.Entry<VariableReferenceExpression, List<VariableReferenceExpression>> entry : setOperationVariableMap.entrySet()) {
                VariableReferenceExpression canonicalOutputVariable = canonicalize(entry.getKey());
                if (addVariables.add(canonicalOutputVariable)) {
                    result.put(canonicalOutputVariable, ImmutableList.copyOf(Iterables.transform(entry.getValue(), this::canonicalize)));
                }
            }
            return result;
        }

        private List<VariableReferenceExpression> canonicalizeSetOperationOutputVariables(List<VariableReferenceExpression> setOperationOutputVariables)
        {
            ImmutableList.Builder<VariableReferenceExpression> builder = ImmutableList.builder();
            for (VariableReferenceExpression variable : setOperationOutputVariables) {
                builder.add(canonicalize(variable));
            }
            return builder.build();
        }
    }
}
