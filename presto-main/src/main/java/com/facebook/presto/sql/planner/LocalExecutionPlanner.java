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

import com.facebook.presto.metadata.LocalStorageManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.operator.AggregationFunctionDefinition;
import com.facebook.presto.operator.AggregationOperator.AggregationOperatorFactory;
import com.facebook.presto.operator.DriverFactory;
import com.facebook.presto.operator.ExchangeClient;
import com.facebook.presto.operator.ExchangeOperator.ExchangeOperatorFactory;
import com.facebook.presto.operator.FilterAndProjectOperator.FilterAndProjectOperatorFactory;
import com.facebook.presto.operator.FilterFunction;
import com.facebook.presto.operator.FilterFunctions;
import com.facebook.presto.operator.HashAggregationOperator.HashAggregationOperatorFactory;
import com.facebook.presto.operator.HashBuilderOperator.HashBuilderOperatorFactory;
import com.facebook.presto.operator.HashBuilderOperator.HashSupplier;
import com.facebook.presto.operator.HashJoinOperator;
import com.facebook.presto.operator.HashJoinOperator.HashJoinOperatorFactory;
import com.facebook.presto.operator.HashSemiJoinOperator.HashSemiJoinOperatorFactory;
import com.facebook.presto.operator.InMemoryExchange;
import com.facebook.presto.operator.InMemoryExchangeSourceOperator.InMemoryExchangeSourceOperatorFactory;
import com.facebook.presto.operator.LimitOperator.LimitOperatorFactory;
import com.facebook.presto.operator.MarkDistinctOperator.MarkDistinctOperatorFactory;
import com.facebook.presto.operator.MaterializeSampleOperator;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.operator.OrderByOperator.OrderByOperatorFactory;
import com.facebook.presto.operator.OutputFactory;
import com.facebook.presto.operator.ProjectionFunction;
import com.facebook.presto.operator.ProjectionFunctions;
import com.facebook.presto.operator.RecordSinkManager;
import com.facebook.presto.operator.ScanFilterAndProjectOperator.ScanFilterAndProjectOperatorFactory;
import com.facebook.presto.operator.SetBuilderOperator.SetBuilderOperatorFactory;
import com.facebook.presto.operator.SetBuilderOperator.SetSupplier;
import com.facebook.presto.operator.SortOrder;
import com.facebook.presto.operator.SourceOperatorFactory;
import com.facebook.presto.operator.TableScanOperator.TableScanOperatorFactory;
import com.facebook.presto.operator.TopNOperator.TopNOperatorFactory;
import com.facebook.presto.operator.WindowOperator.WindowOperatorFactory;
import com.facebook.presto.operator.window.WindowFunction;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.RecordSink;
import com.facebook.presto.split.DataStreamProvider;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.sql.gen.ExpressionCompiler;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.DistinctLimitNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.MarkDistinctNode;
import com.facebook.presto.sql.planner.plan.MaterializeSampleNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SampleNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.SinkNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.TableCommitNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.TableWriterNode;
import com.facebook.presto.sql.planner.plan.TopNNode;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.Input;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.tuple.FieldOrderedTupleComparator;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleReadable;
import com.facebook.presto.util.IterableTransformer;
import com.facebook.presto.util.MoreFunctions;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Ints;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.validation.constraints.NotNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.operator.DistinctLimitOperator.DistinctLimitOperatorFactory;
import static com.facebook.presto.operator.TableCommitOperator.TableCommitOperatorFactory;
import static com.facebook.presto.operator.TableCommitOperator.TableCommitter;
import static com.facebook.presto.operator.TableWriterOperator.TableWriterOperatorFactory;
import static com.facebook.presto.sql.planner.plan.JoinNode.EquiJoinClause.leftGetter;
import static com.facebook.presto.sql.planner.plan.JoinNode.EquiJoinClause.rightGetter;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class LocalExecutionPlanner
{
    private static final Logger log = Logger.get(LocalExecutionPlanner.class);

    private final NodeInfo nodeInfo;
    private final Metadata metadata;

    private final DataStreamProvider dataStreamProvider;
    private final LocalStorageManager storageManager;
    private final RecordSinkManager recordSinkManager;
    private final Supplier<ExchangeClient> exchangeClientSupplier;
    private final ExpressionCompiler compiler;

    @Inject
    public LocalExecutionPlanner(NodeInfo nodeInfo,
            Metadata metadata,
            DataStreamProvider dataStreamProvider,
            LocalStorageManager storageManager,
            RecordSinkManager recordSinkManager,
            Supplier<ExchangeClient> exchangeClientSupplier,
            ExpressionCompiler compiler)
    {
        this.nodeInfo = checkNotNull(nodeInfo, "nodeInfo is null");
        this.dataStreamProvider = dataStreamProvider;
        this.exchangeClientSupplier = exchangeClientSupplier;
        this.metadata = checkNotNull(metadata, "metadata is null");
        this.storageManager = checkNotNull(storageManager, "storageManager is null");
        this.recordSinkManager = checkNotNull(recordSinkManager, "recordSinkManager is null");
        this.compiler = checkNotNull(compiler, "compiler is null");
    }

    public LocalExecutionPlan plan(Session session,
            PlanNode plan,
            Map<Symbol, Type> types,
            OutputFactory outputOperatorFactory)
    {
        LocalExecutionPlanContext context = new LocalExecutionPlanContext(session, types);

        PhysicalOperation physicalOperation = plan.accept(new Visitor(), context);
        DriverFactory driverFactory = new DriverFactory(
                context.isInputDriver(),
                true,
                ImmutableList.<OperatorFactory>builder()
                        .addAll(physicalOperation.getOperatorFactories())
                        .add(outputOperatorFactory.createOutputOperator(context.getNextOperatorId(), physicalOperation.getTupleInfos()))
                        .build());
        context.addDriverFactory(driverFactory);

        return new LocalExecutionPlan(context.getDriverFactories());
    }

    private static class LocalExecutionPlanContext
    {
        private final Session session;
        private final Map<Symbol, Type> types;

        private final List<DriverFactory> driverFactories;

        private int nextOperatorId;
        private boolean inputDriver = true;

        public LocalExecutionPlanContext(Session session, Map<Symbol, Type> types)
        {
            this(session, types, new ArrayList<DriverFactory>());
        }

        private LocalExecutionPlanContext(Session session, Map<Symbol, Type> types, List<DriverFactory> driverFactories)
        {
            this.session = session;
            this.types = types;
            this.driverFactories = driverFactories;
        }

        public void addDriverFactory(DriverFactory driverFactory)
        {
            driverFactories.add(checkNotNull(driverFactory, "driverFactory is null"));
        }

        private List<DriverFactory> getDriverFactories()
        {
            return ImmutableList.copyOf(driverFactories);
        }

        public Session getSession()
        {
            return session;
        }

        public Map<Symbol, Type> getTypes()
        {
            return types;
        }

        private int getNextOperatorId()
        {
            return nextOperatorId++;
        }

        private boolean isInputDriver()
        {
            return inputDriver;
        }

        private void setInputDriver(boolean inputDriver)
        {
            this.inputDriver = inputDriver;
        }

        public LocalExecutionPlanContext createSubContext()
        {
            return new LocalExecutionPlanContext(session, types, driverFactories);
        }
    }

    public static class LocalExecutionPlan
    {
        private final List<DriverFactory> driverFactories;

        public LocalExecutionPlan(List<DriverFactory> driverFactories)
        {
            this.driverFactories = ImmutableList.copyOf(checkNotNull(driverFactories, "driverFactories is null"));
        }

        public List<DriverFactory> getDriverFactories()
        {
            return driverFactories;
        }
    }

    private class Visitor
            extends PlanVisitor<LocalExecutionPlanContext, PhysicalOperation>
    {
        @Override
        public PhysicalOperation visitExchange(ExchangeNode node, LocalExecutionPlanContext context)
        {
            List<TupleInfo> tupleInfos = getSourceOperatorTupleInfos(node, context.getTypes());

            OperatorFactory operatorFactory = new ExchangeOperatorFactory(context.getNextOperatorId(), node.getId(), exchangeClientSupplier, tupleInfos);

            ImmutableMap.Builder<Symbol, Input> outputMappings = ImmutableMap.builder();
            int channel = 0;
            for (Symbol symbol : node.getOutputSymbols()) {
                outputMappings.put(symbol, new Input(channel));
                channel++;
            }

            return new PhysicalOperation(operatorFactory, outputMappings.build());
        }

        @Override
        public PhysicalOperation visitOutput(OutputNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            // see if we need to introduce a projection
            //   1. verify that there's one symbol per channel
            //   2. verify that symbols from "source" match the expected order of columns according to OutputNode
            Ordering<Input> comparator = inputOrdering();

            List<Symbol> sourceSymbols = IterableTransformer.on(source.getLayout().entrySet())
                    .orderBy(comparator.onResultOf(MoreFunctions.<Symbol, Input>valueGetter()))
                    .transform(MoreFunctions.<Symbol, Input>keyGetter())
                    .list();

            List<Symbol> resultSymbols = node.getOutputSymbols();
            if (resultSymbols.equals(sourceSymbols) && resultSymbols.size() == source.getTupleInfos().size()) {
                // no projection needed
                return source;
            }

            // otherwise, introduce a projection to match the expected output
            IdentityProjectionInfo mappings = computeIdentityMapping(resultSymbols, source.getLayout(), context.getTypes());

            OperatorFactory operatorFactory = new FilterAndProjectOperatorFactory(context.getNextOperatorId(), FilterFunctions.TRUE_FUNCTION, mappings.getProjections());
            return new PhysicalOperation(operatorFactory, mappings.getOutputLayout(), source);
        }

        @Override
        public PhysicalOperation visitWindow(WindowNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            List<Symbol> partitionBySymbols = node.getPartitionBy();
            List<Symbol> orderBySymbols = node.getOrderBy();

            // sort by PARTITION BY, then by ORDER BY
            int[] partitionChannels = new int[partitionBySymbols.size()];
            for (int i = 0; i < partitionChannels.length; i++) {
                Symbol symbol = partitionBySymbols.get(i);
                partitionChannels[i] = source.getLayout().get(symbol).getChannel();
            }

            int[] sortChannels = new int[orderBySymbols.size()];
            SortOrder[] sortOrder = new SortOrder[orderBySymbols.size()];
            for (int i = 0; i < sortChannels.length; i++) {
                Symbol symbol = orderBySymbols.get(i);
                sortChannels[i] = source.getLayout().get(symbol).getChannel();
                sortOrder[i] = node.getOrderings().get(symbol);
            }

            int[] outputChannels = new int[source.getTupleInfos().size()];
            for (int i = 0; i < outputChannels.length; i++) {
                outputChannels[i] = i;
            }

            ImmutableList.Builder<WindowFunction> windowFunctions = ImmutableList.builder();
            List<Symbol> windowFunctionOutputSymbols = new ArrayList<>();
            for (Map.Entry<Symbol, FunctionCall> entry : node.getWindowFunctions().entrySet()) {
                Symbol symbol = entry.getKey();
                Signature signature = node.getSignatures().get(symbol);
                windowFunctions.add(metadata.getFunction(signature).getWindowFunction().get());
                windowFunctionOutputSymbols.add(symbol);
            }

            // compute the layout of the output from the window operator
            ImmutableMap.Builder<Symbol, Input> outputMappings = ImmutableMap.builder();
            for (Symbol symbol : node.getSource().getOutputSymbols()) {
                outputMappings.put(symbol, source.getLayout().get(symbol));
            }

            // window functions go in remaining channels starting after the last channel from the source operator, one per channel
            int channel = source.getTupleInfos().size();
            for (Symbol symbol : windowFunctionOutputSymbols) {
                outputMappings.put(symbol, new Input(channel));
                channel++;
            }

            OperatorFactory operatorFactory = new WindowOperatorFactory(
                    context.getNextOperatorId(),
                    source.getTupleInfos(),
                    outputChannels,
                    windowFunctions.build(),
                    partitionChannels,
                    sortChannels,
                    sortOrder,
                    1_000_000);

            return new PhysicalOperation(operatorFactory, outputMappings.build(), source);
        }

        @Override
        public PhysicalOperation visitTopN(TopNNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            List<Symbol> orderBySymbols = node.getOrderBy();

            List<Integer> sortChannels = new ArrayList<>();
            List<SortOrder> sortOrders = new ArrayList<>();
            for (Symbol symbol : orderBySymbols) {
                sortChannels.add(source.getLayout().get(symbol).getChannel());
                sortOrders.add(node.getOrderings().get(symbol));
            }

            Ordering<TupleReadable[]> ordering = Ordering.from(new FieldOrderedTupleComparator(sortChannels, sortOrders));

            IdentityProjectionInfo mappings = computeIdentityMapping(node.getOutputSymbols(), source.getLayout(), context.getTypes());

            Optional<Integer> sampleWeightChannel = node.getSampleWeight().transform(source.channelGetter());

            OperatorFactory operator = new TopNOperatorFactory(
                    context.getNextOperatorId(),
                    (int) node.getCount(),
                    mappings.getProjections(),
                    ordering,
                    sampleWeightChannel,
                    node.isPartial());

            return new PhysicalOperation(operator, mappings.getOutputLayout(), source);
        }

        @Override
        public PhysicalOperation visitSort(SortNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            List<Symbol> orderBySymbols = node.getOrderBy();

            int[] orderByChannels = Ints.toArray(getChannelsForSymbols(orderBySymbols, source.getLayout()));

            SortOrder[] sortOrder = new SortOrder[orderBySymbols.size()];
            for (int i = 0; i < orderBySymbols.size(); i++) {
                Symbol symbol = orderBySymbols.get(i);
                sortOrder[i] = node.getOrderings().get(symbol);
            }

            int[] outputChannels = new int[source.getTupleInfos().size()];
            for (int i = 0; i < outputChannels.length; i++) {
                outputChannels[i] = i;
            }

            OperatorFactory operator = new OrderByOperatorFactory(
                    context.getNextOperatorId(),
                    source.getTupleInfos(),
                    outputChannels,
                    10_000,
                    orderByChannels,
                    sortOrder);

            return new PhysicalOperation(operator, source.getLayout(), source);
        }

        @Override
        public PhysicalOperation visitLimit(LimitNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            Optional<Integer> sampleWeightChannel = node.getSampleWeight().transform(source.channelGetter());

            OperatorFactory operatorFactory = new LimitOperatorFactory(context.getNextOperatorId(), source.getTupleInfos(), node.getCount(), sampleWeightChannel);
            return new PhysicalOperation(operatorFactory, source.getLayout(), source);
        }

        @Override
        public PhysicalOperation visitDistinctLimit(DistinctLimitNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);
            OperatorFactory operatorFactory = new DistinctLimitOperatorFactory(
                    context.getNextOperatorId(),
                    source.getTupleInfos(),
                    node.getLimit());
            return new PhysicalOperation(operatorFactory, source.getLayout(), source);
        }

        @Override
        public PhysicalOperation visitAggregation(AggregationNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            if (node.getGroupBy().isEmpty()) {
                return planGlobalAggregation(context.getNextOperatorId(), node, source);
            }

            return planGroupByAggregation(node, source, context);
        }

        @Override
        public PhysicalOperation visitMarkDistinct(MarkDistinctNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            List<Integer> channels = getChannelsForSymbols(node.getDistinctSymbols(), source.getLayout());

            // Source channels are always laid out first, followed by the boolean output symbol
            Map<Symbol, Input> outputMappings = ImmutableMap.<Symbol, Input>builder()
                    .putAll(source.getLayout())
                    .put(node.getMarkerSymbol(), new Input(source.getLayout().size())).build();

            Optional<Integer> sampleWeightChannel = node.getSampleWeightSymbol().transform(source.channelGetter());

            MarkDistinctOperatorFactory operator = new MarkDistinctOperatorFactory(context.getNextOperatorId(), source.getTupleInfos(), channels, sampleWeightChannel);
            return new PhysicalOperation(operator, outputMappings, source);
        }

        @Override
        public PhysicalOperation visitMaterializeSample(MaterializeSampleNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            int sampleWeightChannel = Iterables.getOnlyElement(getChannelsForSymbols(ImmutableList.of(node.getSampleWeightSymbol()), source.getLayout()));

            ImmutableMap.Builder<Symbol, Input> outputMappings = ImmutableMap.builder();
            for (Map.Entry<Symbol, Input> entry : source.getLayout().entrySet()) {
                int value = entry.getValue().getChannel();
                if (value == sampleWeightChannel) {
                    continue;
                }
                // Because we've removed the sample weight channel, all channels after it have been renumbered
                outputMappings.put(entry.getKey(), new Input(value > sampleWeightChannel ? value - 1 : value));
            }

            List<TupleInfo> tupleInfos = new ArrayList<>();
            tupleInfos.addAll(source.getTupleInfos());
            tupleInfos.remove(sampleWeightChannel);

            MaterializeSampleOperator.MaterializeSampleOperatorFactory operator = new MaterializeSampleOperator.MaterializeSampleOperatorFactory(context.getNextOperatorId(), tupleInfos, sampleWeightChannel);
            return new PhysicalOperation(operator, outputMappings.build(), source);
        }

        @Override
        public PhysicalOperation visitSample(SampleNode node, LocalExecutionPlanContext context)
        {
            // For system sample, the splits are already filtered out, so no specific action needs to be taken here
            if (node.getSampleType() == SampleNode.Type.SYSTEM) {
                return node.getSource().accept(this, context);
            }
            throw new UnsupportedOperationException("not yet implemented: " + node);
        }

        @Override
        public PhysicalOperation visitFilter(FilterNode node, LocalExecutionPlanContext context)
        {
            PlanNode sourceNode = node.getSource();

            Expression filterExpression = node.getPredicate();

            List<Expression> projectionExpressions = new ArrayList<>();
            for (int i = 0; i < node.getOutputSymbols().size(); i++) {
                Symbol symbol = node.getOutputSymbols().get(i);
                projectionExpressions.add(new QualifiedNameReference(symbol.toQualifiedName()));
            }

            List<Symbol> outputSymbols = node.getOutputSymbols();

            return visitScanFilterAndProject(context, sourceNode, filterExpression, projectionExpressions, outputSymbols);
        }

        @Override
        public PhysicalOperation visitProject(ProjectNode node, LocalExecutionPlanContext context)
        {
            PlanNode sourceNode;
            Expression filterExpression;
            if (node.getSource() instanceof FilterNode) {
                FilterNode filterNode = (FilterNode) node.getSource();
                sourceNode = filterNode.getSource();
                filterExpression = filterNode.getPredicate();
            }
            else {
                sourceNode = node.getSource();
                filterExpression = BooleanLiteral.TRUE_LITERAL;
            }

            List<Expression> projectionExpressions = node.getExpressions();

            List<Symbol> outputSymbols = node.getOutputSymbols();

            return visitScanFilterAndProject(context, sourceNode, filterExpression, projectionExpressions, outputSymbols);
        }

        private PhysicalOperation visitScanFilterAndProject(
                LocalExecutionPlanContext context,
                PlanNode sourceNode,
                Expression filterExpression,
                List<Expression> projectionExpressions,
                List<Symbol> outputSymbols)
        {
            // if source is a table scan we fold it directly into the filter and project
            // otherwise we plan it as a normal operator
            Map<Symbol, Input> sourceLayout;
            Map<Input, Type> sourceTypes;
            List<ColumnHandle> columns = null;
            PhysicalOperation source = null;
            if (sourceNode instanceof TableScanNode) {
                TableScanNode tableScanNode = (TableScanNode) sourceNode;

                // extract the column handles and input to type mapping
                sourceLayout = new LinkedHashMap<>();
                sourceTypes = new LinkedHashMap<>();
                columns = new ArrayList<>();
                int channel = 0;
                for (Symbol symbol : tableScanNode.getOutputSymbols()) {
                    columns.add(tableScanNode.getAssignments().get(symbol));

                    Input input = new Input(channel);
                    sourceLayout.put(symbol, input);

                    Type type = checkNotNull(context.getTypes().get(symbol), "No type for symbol %s", symbol);
                    sourceTypes.put(input, type);

                    channel++;
                }
            }
            else {
                // plan source
                source = sourceNode.accept(this, context);
                sourceLayout = source.getLayout();
                sourceTypes = getInputTypes(source.getLayout(), source.getTupleInfos());
            }

            // build output mapping
            ImmutableMap.Builder<Symbol, Input> outputMappingsBuilder = ImmutableMap.builder();
            for (int i = 0; i < outputSymbols.size(); i++) {
                Symbol symbol = outputSymbols.get(i);
                outputMappingsBuilder.put(symbol, new Input(i));
            }
            Map<Symbol, Input> outputMappings = outputMappingsBuilder.build();

            try {
                // compiler uses inputs instead of symbols, so rewrite the expressions first
                SymbolToInputRewriter symbolToInputRewriter = new SymbolToInputRewriter(sourceLayout);
                Expression rewrittenFilter = ExpressionTreeRewriter.rewriteWith(symbolToInputRewriter, filterExpression);

                List<Expression> rewrittenProjections = new ArrayList<>();
                List<Type> outputTypes = new ArrayList<>();
                for (int i = 0; i < projectionExpressions.size(); i++) {
                    Expression projection = projectionExpressions.get(i);
                    rewrittenProjections.add(ExpressionTreeRewriter.rewriteWith(symbolToInputRewriter, projection));
                    outputTypes.add(context.getTypes().get(outputSymbols.get(i)));
                }

                if (columns != null) {
                    SourceOperatorFactory operatorFactory = compiler.compileScanFilterAndProjectOperator(
                            context.getNextOperatorId(),
                            sourceNode.getId(),
                            dataStreamProvider,
                            columns,
                            rewrittenFilter,
                            rewrittenProjections,
                            sourceTypes,
                            outputTypes);

                    return new PhysicalOperation(operatorFactory, outputMappings);
                }
                else {
                    OperatorFactory operatorFactory = compiler.compileFilterAndProjectOperator(
                            context.getNextOperatorId(),
                            rewrittenFilter,
                            rewrittenProjections,
                            sourceTypes,
                            outputTypes);
                    return new PhysicalOperation(operatorFactory, outputMappings, source);
                }
            }
            catch (RuntimeException e) {
                // compilation failed, use interpreter
                log.error(e, "Compile failed for filter=%s projections=%s sourceTypes=%s error=%s", filterExpression, projectionExpressions, sourceTypes, e);
            }

            FilterFunction filterFunction;
            if (filterExpression != BooleanLiteral.TRUE_LITERAL) {
                filterFunction = new InterpretedFilterFunction(filterExpression, sourceLayout, metadata, context.getSession());
            }
            else {
                filterFunction = FilterFunctions.TRUE_FUNCTION;
            }

            List<ProjectionFunction> projectionFunctions = new ArrayList<>();
            for (int i = 0; i < projectionExpressions.size(); i++) {
                Symbol symbol = outputSymbols.get(i);
                Expression expression = projectionExpressions.get(i);

                ProjectionFunction function;
                if (expression instanceof QualifiedNameReference) {
                    // fast path when we know it's a direct symbol reference
                    Symbol reference = Symbol.fromQualifiedName(((QualifiedNameReference) expression).getName());
                    function = ProjectionFunctions.singleColumn(context.getTypes().get(reference).getRawType(), sourceLayout.get(reference));
                }
                else {
                    function = new InterpretedProjectionFunction(
                            context.getTypes().get(symbol),
                            expression,
                            sourceLayout,
                            metadata,
                            context.getSession()
                    );
                }
                projectionFunctions.add(function);
            }

            if (columns != null) {
                OperatorFactory operatorFactory = new ScanFilterAndProjectOperatorFactory(
                        context.getNextOperatorId(),
                        sourceNode.getId(),
                        dataStreamProvider,
                        columns,
                        filterFunction,
                        projectionFunctions);

                return new PhysicalOperation(operatorFactory, outputMappings);
            }
            else {
                OperatorFactory operatorFactory = new FilterAndProjectOperatorFactory(context.getNextOperatorId(), filterFunction, projectionFunctions);
                return new PhysicalOperation(operatorFactory, outputMappings, source);
            }
        }

        private Map<Input, Type> getInputTypes(Map<Symbol, Input> layout, List<TupleInfo> tupleInfos)
        {
            Builder<Input, Type> inputTypes = ImmutableMap.builder();
            for (Input input : ImmutableSet.copyOf(layout.values())) {
                TupleInfo.Type type = tupleInfos.get(input.getChannel()).getType();
                switch (type) {
                    case BOOLEAN:
                        inputTypes.put(input, Type.BOOLEAN);
                        break;
                    case FIXED_INT_64:
                        inputTypes.put(input, Type.BIGINT);
                        break;
                    case VARIABLE_BINARY:
                        inputTypes.put(input, Type.VARCHAR);
                        break;
                    case DOUBLE:
                        inputTypes.put(input, Type.DOUBLE);
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported type " + type);
                }
            }
            return inputTypes.build();
        }

        @Override
        public PhysicalOperation visitTableScan(TableScanNode node, LocalExecutionPlanContext context)
        {
            ImmutableMap.Builder<Symbol, Input> outputMappings = ImmutableMap.builder();
            List<ColumnHandle> columns = new ArrayList<>();

            int channel = 0;
            for (Symbol symbol : node.getOutputSymbols()) {
                columns.add(node.getAssignments().get(symbol));

                outputMappings.put(symbol, new Input(channel)); // one column per channel
                channel++;
            }

            List<TupleInfo> tupleInfos = getSourceOperatorTupleInfos(node, context.getTypes());
            OperatorFactory operatorFactory = new TableScanOperatorFactory(context.getNextOperatorId(), node.getId(), dataStreamProvider, tupleInfos, columns);
            return new PhysicalOperation(operatorFactory, outputMappings.build());
        }

        @Override
        public PhysicalOperation visitJoin(JoinNode node, LocalExecutionPlanContext context)
        {
            List<JoinNode.EquiJoinClause> clauses = node.getCriteria();

            List<Symbol> leftSymbols = Lists.transform(clauses, leftGetter());
            List<Symbol> rightSymbols = Lists.transform(clauses, rightGetter());

            switch (node.getType()) {
                case INNER:
                case LEFT:
                    return createJoinOperator(node, node.getLeft(), leftSymbols, node.getRight(), rightSymbols, context);
                case RIGHT:
                    return createJoinOperator(node, node.getRight(), rightSymbols, node.getLeft(), leftSymbols, context);
                default:
                    throw new UnsupportedOperationException("Unsupported join type: " + node.getType());
            }
        }

        private PhysicalOperation createJoinOperator(JoinNode node,
                PlanNode probeNode,
                List<Symbol> probeSymbols,
                PlanNode buildNode,
                List<Symbol> buildSymbols,
                LocalExecutionPlanContext context)
        {
            // Plan probe and introduce a projection to put all fields from the probe side into a single channel if necessary
            PhysicalOperation probeSource = probeNode.accept(this, context);
            List<Integer> probeChannels = ImmutableList.copyOf(getChannelsForSymbols(probeSymbols, probeSource.getLayout()));

            // do the same on the build side
            LocalExecutionPlanContext buildContext = context.createSubContext();
            PhysicalOperation buildSource = buildNode.accept(this, buildContext);
            List<Integer> buildChannels = ImmutableList.copyOf(getChannelsForSymbols(buildSymbols, buildSource.getLayout()));

            HashBuilderOperatorFactory hashBuilderOperatorFactory = new HashBuilderOperatorFactory(
                    buildContext.getNextOperatorId(),
                    buildSource.getTupleInfos(),
                    buildChannels,
                    100_000);
            HashSupplier hashSupplier = hashBuilderOperatorFactory.getHashSupplier();
            DriverFactory buildDriverFactory = new DriverFactory(
                    buildContext.isInputDriver(),
                    false,
                    ImmutableList.<OperatorFactory>builder()
                            .addAll(buildSource.getOperatorFactories())
                            .add(hashBuilderOperatorFactory)
                            .build());
            context.addDriverFactory(buildDriverFactory);

            ImmutableMap.Builder<Symbol, Input> outputMappings = ImmutableMap.builder();
            outputMappings.putAll(probeSource.getLayout());

            // inputs from build side of the join are laid out following the input from the probe side,
            // so adjust the channel ids but keep the field layouts intact
            int offset = probeSource.getTupleInfos().size();
            for (Map.Entry<Symbol, Input> entry : buildSource.getLayout().entrySet()) {
                Input input = entry.getValue();
                outputMappings.put(entry.getKey(), new Input(offset + input.getChannel()));
            }

            OperatorFactory operator = createJoinOperator(node.getType(), hashSupplier, probeSource.getTupleInfos(), probeChannels, context);
            return new PhysicalOperation(operator, outputMappings.build(), probeSource);
        }

        private HashJoinOperatorFactory createJoinOperator(
                JoinNode.Type type,
                HashSupplier hashSupplier,
                List<TupleInfo> probeTupleInfos,
                List<Integer> probeJoinChannels,
                LocalExecutionPlanContext context)
        {
            switch (type) {
                case INNER:
                    return HashJoinOperator.innerJoin(context.getNextOperatorId(), hashSupplier, probeTupleInfos, probeJoinChannels);
                case LEFT:
                case RIGHT:
                    return HashJoinOperator.outerJoin(context.getNextOperatorId(), hashSupplier, probeTupleInfos, probeJoinChannels);
                default:
                    throw new UnsupportedOperationException("Unsupported join type: " + type);
            }
        }

        @Override
        public PhysicalOperation visitSemiJoin(SemiJoinNode node, LocalExecutionPlanContext context)
        {
            // introduce a projection to put all fields from the probe side into a single channel if necessary
            PhysicalOperation probeSource = node.getSource().accept(this, context);

            // do the same on the build side
            LocalExecutionPlanContext buildContext = context.createSubContext();
            PhysicalOperation buildSource = node.getFilteringSource().accept(this, buildContext);

            int probeChannel = probeSource.getLayout().get(node.getSourceJoinSymbol()).getChannel();
            int buildChannel = buildSource.getLayout().get(node.getFilteringSourceJoinSymbol()).getChannel();

            SetBuilderOperatorFactory setBuilderOperatorFactory = new SetBuilderOperatorFactory(buildContext.getNextOperatorId(), buildSource.getTupleInfos(), buildChannel, 100_000);
            SetSupplier setProvider = setBuilderOperatorFactory.getSetProvider();
            DriverFactory buildDriverFactory = new DriverFactory(
                    buildContext.isInputDriver(),
                    false,
                    ImmutableList.<OperatorFactory>builder()
                            .addAll(buildSource.getOperatorFactories())
                            .add(setBuilderOperatorFactory)
                            .build());
            context.addDriverFactory(buildDriverFactory);

            // Source channels are always laid out first, followed by the boolean output symbol
            Map<Symbol, Input> outputMappings = ImmutableMap.<Symbol, Input>builder()
                    .putAll(probeSource.getLayout())
                    .put(node.getSemiJoinOutput(), new Input(probeSource.getLayout().size()))
                    .build();

            HashSemiJoinOperatorFactory operator = new HashSemiJoinOperatorFactory(context.getNextOperatorId(), setProvider, probeSource.getTupleInfos(), probeChannel);
            return new PhysicalOperation(operator, outputMappings, probeSource);
        }

        @Override
        public PhysicalOperation visitSink(SinkNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            // are the symbols of the source in the same order as the sink expects?
            boolean projectionMatchesOutput = IterableTransformer.on(source.getLayout().entrySet())
                    .orderBy(inputOrdering().onResultOf(MoreFunctions.<Symbol, Input>valueGetter()))
                    .transform(MoreFunctions.<Symbol, Input>keyGetter())
                    .list()
                    .equals(node.getOutputSymbols());

            if (!projectionMatchesOutput) {
                IdentityProjectionInfo mappings = computeIdentityMapping(node.getOutputSymbols(), source.getLayout(), context.getTypes());
                OperatorFactory operatorFactory = new FilterAndProjectOperatorFactory(context.getNextOperatorId(), FilterFunctions.TRUE_FUNCTION, mappings.getProjections());
                // NOTE: the generated output layout may not be completely accurate if the same field was projected as multiple inputs.
                // However, this should not affect the operation of the sink.
                return new PhysicalOperation(operatorFactory, mappings.getOutputLayout(), source);
            }

            return source;
        }

        @Override
        public PhysicalOperation visitTableWriter(TableWriterNode node, LocalExecutionPlanContext context)
        {
            // serialize writes by forcing data through a single writer
            PhysicalOperation exchange = createInMemoryExchange(node, context);
            PhysicalOperation source = node.getSource().accept(this, context);

            Optional<Integer> sampleWeightChannel = node.getSampleWeightSymbol().transform(source.channelGetter());

            // create the table writer
            RecordSink recordSink = recordSinkManager.getRecordSink(node.getTarget());
            List<TupleInfo.Type> outputTypes = new ArrayList<>(FluentIterable.from(source.getTupleInfos()).transform(new Function<TupleInfo, TupleInfo.Type>() {
                @Override
                public TupleInfo.Type apply(TupleInfo input)
                {
                    return input.getType();
                }
            }).toList());
            if (sampleWeightChannel.isPresent()) {
                outputTypes.remove((int) sampleWeightChannel.get());
            }
            OperatorFactory operatorFactory = new TableWriterOperatorFactory(context.getNextOperatorId(), recordSink, outputTypes, sampleWeightChannel);

            Map<Symbol, Input> layout = ImmutableMap.<Symbol, Input>builder()
                    .put(node.getOutputSymbols().get(0), new Input(0))
                    .put(node.getOutputSymbols().get(1), new Input(1))
                    .build();

            return new PhysicalOperation(operatorFactory, layout, exchange);
        }

        private PhysicalOperation createInMemoryExchange(TableWriterNode node, LocalExecutionPlanContext context)
        {
            LocalExecutionPlanContext subContext = context.createSubContext();
            PhysicalOperation source = node.getSource().accept(this, subContext);

            InMemoryExchange exchange = new InMemoryExchange(getSourceOperatorTupleInfos(node, context.getTypes()));

            // create exchange sink
            List<OperatorFactory> factories = ImmutableList.<OperatorFactory>builder()
                    .addAll(source.getOperatorFactories())
                    .add(exchange.createSinkFactory(subContext.getNextOperatorId()))
                    .build();

            // add sub-context to current context
            context.addDriverFactory(new DriverFactory(subContext.isInputDriver(), false, factories));

            exchange.noMoreSinkFactories();

            // the main driver is not an input: the source is the input for the plan
            context.setInputDriver(false);

            List<Symbol> layout = node.getOutputSymbols();
            ImmutableMap.Builder<Symbol, Input> outputMappings = ImmutableMap.builder();
            for (int i = 0; i < layout.size(); i++) {
                outputMappings.put(layout.get(i), new Input(i));
            }

            // add exchange source as first operator in the current context
            OperatorFactory factory = new InMemoryExchangeSourceOperatorFactory(context.getNextOperatorId(), exchange);
            return new PhysicalOperation(factory, outputMappings.build());
        }

        @Override
        public PhysicalOperation visitTableCommit(TableCommitNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            OperatorFactory operatorFactory = new TableCommitOperatorFactory(context.getNextOperatorId(), createTableCommitter(node, metadata));
            Map<Symbol, Input> layout = ImmutableMap.of(node.getOutputSymbols().get(0), new Input(0));

            return new PhysicalOperation(operatorFactory, layout, source);
        }

        @Override
        public PhysicalOperation visitUnion(UnionNode node, LocalExecutionPlanContext context)
        {
            List<TupleInfo> tupleInfos = getSourceOperatorTupleInfos(node, context.getTypes());
            InMemoryExchange inMemoryExchange = new InMemoryExchange(tupleInfos);

            for (int i = 0; i < node.getSources().size(); i++) {
                PlanNode subplan = node.getSources().get(i);
                List<Symbol> expectedLayout = node.sourceOutputLayout(i);

                LocalExecutionPlanContext subContext = context.createSubContext();
                PhysicalOperation source = subplan.accept(this, subContext);
                List<OperatorFactory> operatorFactories = new ArrayList<>(source.getOperatorFactories());

                boolean projectionMatchesOutput = IterableTransformer.on(source.getLayout().entrySet())
                        .orderBy(inputOrdering().onResultOf(MoreFunctions.<Symbol, Input>valueGetter()))
                        .transform(MoreFunctions.<Symbol, Input>keyGetter())
                        .list()
                        .equals(expectedLayout);

                if (!projectionMatchesOutput) {
                    IdentityProjectionInfo mappings = computeIdentityMapping(expectedLayout, source.getLayout(), context.getTypes());
                    operatorFactories.add(new FilterAndProjectOperatorFactory(subContext.getNextOperatorId(), FilterFunctions.TRUE_FUNCTION, mappings.getProjections()));
                }

                operatorFactories.add(inMemoryExchange.createSinkFactory(subContext.getNextOperatorId()));

                DriverFactory driverFactory = new DriverFactory(subContext.isInputDriver(), false, operatorFactories);
                context.addDriverFactory(driverFactory);
            }
            inMemoryExchange.noMoreSinkFactories();

            // the main driver is not an input... the union sources are the input for the plan
            context.setInputDriver(false);

            ImmutableMap.Builder<Symbol, Input> outputMappings = ImmutableMap.builder();
            int channel = 0;
            for (Symbol symbol : node.getOutputSymbols()) {
                outputMappings.put(symbol, new Input(channel)); // one symbol per channel
                channel++;
            }

            return new PhysicalOperation(new InMemoryExchangeSourceOperatorFactory(context.getNextOperatorId(), inMemoryExchange), outputMappings.build());
        }

        @Override
        protected PhysicalOperation visitPlan(PlanNode node, LocalExecutionPlanContext context)
        {
            throw new UnsupportedOperationException("not yet implemented");
        }

        private List<TupleInfo> getSourceOperatorTupleInfos(PlanNode node, Map<Symbol, Type> types)
        {
            return ImmutableList.copyOf(IterableTransformer.on(node.getOutputSymbols())
                    .transform(Functions.forMap(types))
                    .transform(Type.toRaw())
                    .transform(new Function<TupleInfo.Type, TupleInfo>()
                    {
                        @Override
                        public TupleInfo apply(TupleInfo.Type input)
                        {
                            return new TupleInfo(input);
                        }
                    })
                    .list());
        }

        private AggregationFunctionDefinition buildFunctionDefinition(PhysicalOperation source, Signature function, FunctionCall call, @Nullable Symbol mask, Optional<Symbol> sampleWeight, double confidence)
        {
            List<Input> arguments = new ArrayList<>();
            for (Expression argument : call.getArguments()) {
                Symbol argumentSymbol = Symbol.fromQualifiedName(((QualifiedNameReference) argument).getName());
                arguments.add(source.getLayout().get(argumentSymbol));
            }

            Optional<Input> maskInput = Optional.absent();

            if (mask != null) {
                maskInput = Optional.of(source.getLayout().get(mask));
            }

            Optional<Input> sampleWeightInput = Optional.absent();
            if (sampleWeight.isPresent()) {
                sampleWeightInput = Optional.of(source.getLayout().get(sampleWeight.get()));
            }

            return metadata.getFunction(function).bind(arguments, maskInput, sampleWeightInput, confidence);
        }

        private PhysicalOperation planGlobalAggregation(int operatorId, AggregationNode node, PhysicalOperation source)
        {
            int outputChannel = 0;
            ImmutableMap.Builder<Symbol, Input> outputMappings = ImmutableMap.builder();
            List<AggregationFunctionDefinition> functionDefinitions = new ArrayList<>();
            for (Map.Entry<Symbol, FunctionCall> entry : node.getAggregations().entrySet()) {
                Symbol symbol = entry.getKey();

                functionDefinitions.add(buildFunctionDefinition(source, node.getFunctions().get(symbol), entry.getValue(), node.getMasks().get(entry.getKey()), node.getSampleWeight(), node.getConfidence()));
                outputMappings.put(symbol, new Input(outputChannel)); // one aggregation per channel
                outputChannel++;
            }

            OperatorFactory operatorFactory = new AggregationOperatorFactory(operatorId, node.getStep(), functionDefinitions);
            return new PhysicalOperation(operatorFactory, outputMappings.build(), source);
        }

        private PhysicalOperation planGroupByAggregation(AggregationNode node, final PhysicalOperation source, LocalExecutionPlanContext context)
        {
            List<Symbol> groupBySymbols = node.getGroupBy();

            List<Symbol> aggregationOutputSymbols = new ArrayList<>();
            List<AggregationFunctionDefinition> functionDefinitions = new ArrayList<>();
            for (Map.Entry<Symbol, FunctionCall> entry : node.getAggregations().entrySet()) {
                Symbol symbol = entry.getKey();

                functionDefinitions.add(buildFunctionDefinition(source, node.getFunctions().get(symbol), entry.getValue(), node.getMasks().get(entry.getKey()), node.getSampleWeight(), node.getConfidence()));
                aggregationOutputSymbols.add(symbol);
            }

            ImmutableMap.Builder<Symbol, Input> outputMappings = ImmutableMap.builder();
            // add group-by key fields each in a separate channel
            int channel = 0;
            for (Symbol symbol : groupBySymbols) {
                outputMappings.put(symbol, new Input(channel));
                channel++;
            }

            // aggregations go in following channels
            for (Symbol symbol : aggregationOutputSymbols) {
                outputMappings.put(symbol, new Input(channel));
                channel++;
            }

            List<Integer> groupByChannels = ImmutableList.copyOf(getChannelsForSymbols(groupBySymbols, source.getLayout()));
            List<TupleInfo> groupByTupleInfos = ImmutableList.copyOf(Iterables.transform(groupByChannels, new Function<Integer, TupleInfo>()
            {
                public TupleInfo apply(Integer input)
                {
                    return source.getTupleInfos().get(input);
                }
            }));

            OperatorFactory operatorFactory = new HashAggregationOperatorFactory(
                    context.getNextOperatorId(),
                    groupByTupleInfos,
                    groupByChannels,
                    node.getStep(),
                    functionDefinitions,
                    10_000);

            return new PhysicalOperation(operatorFactory, outputMappings.build(), source);
        }
    }

    private static TableCommitter createTableCommitter(final TableCommitNode node, final Metadata metadata)
    {
        return new TableCommitter()
        {
            @Override
            public void commitTable(Collection<String> fragments)
            {
                metadata.commitCreateTable(node.getTarget(), fragments);
            }
        };
    }

    private static IdentityProjectionInfo computeIdentityMapping(List<Symbol> symbols, Map<Symbol, Input> inputLayout, Map<Symbol, Type> types)
    {
        Map<Symbol, Input> outputMappings = new HashMap<>();
        List<ProjectionFunction> projections = new ArrayList<>();

        int channel = 0;
        for (Symbol symbol : symbols) {
            ProjectionFunction function = ProjectionFunctions.singleColumn(types.get(symbol).getRawType(), inputLayout.get(symbol));
            projections.add(function);
            if (!outputMappings.containsKey(symbol)) {
                outputMappings.put(symbol, new Input(channel));
                channel++;
            }
        }

        return new IdentityProjectionInfo(ImmutableMap.copyOf(outputMappings), projections);
    }

    private static List<Integer> getChannelsForSymbols(List<Symbol> symbols, Map<Symbol, Input> layout)
    {
        ImmutableList.Builder<Integer> builder = ImmutableList.builder();
        for (Symbol symbol : symbols) {
            builder.add(layout.get(symbol).getChannel());
        }
        return builder.build();
    }

    private static class IdentityProjectionInfo
    {
        private final Map<Symbol, Input> layout;
        private final List<ProjectionFunction> projections;

        public IdentityProjectionInfo(Map<Symbol, Input> outputLayout, List<ProjectionFunction> projections)
        {
            this.layout = checkNotNull(outputLayout, "outputLayout is null");
            this.projections = checkNotNull(projections, "projections is null");
        }

        public Map<Symbol, Input> getOutputLayout()
        {
            return layout;
        }

        public List<ProjectionFunction> getProjections()
        {
            return projections;
        }
    }

    /**
     * Encapsulates an physical operator plus the mapping of logical symbols to channel/field
     */
    private static class PhysicalOperation
    {
        private final List<OperatorFactory> operatorFactories;
        private final Map<Symbol, Input> layout;
        private final List<TupleInfo> tupleInfos;

        public PhysicalOperation(OperatorFactory operatorFactory, Map<Symbol, Input> layout)
        {
            checkNotNull(operatorFactory, "operatorFactory is null");
            checkNotNull(layout, "layout is null");

            this.operatorFactories = ImmutableList.of(operatorFactory);
            this.layout = ImmutableMap.copyOf(layout);
            this.tupleInfos = operatorFactory.getTupleInfos();
        }

        public PhysicalOperation(OperatorFactory operatorFactory, Map<Symbol, Input> layout, PhysicalOperation source)
        {
            checkNotNull(operatorFactory, "operatorFactory is null");
            checkNotNull(layout, "layout is null");
            checkNotNull(source, "source is null");

            this.operatorFactories = ImmutableList.<OperatorFactory>builder().addAll(source.getOperatorFactories()).add(operatorFactory).build();
            this.layout = ImmutableMap.copyOf(layout);
            this.tupleInfos = operatorFactory.getTupleInfos();
        }

        public Function<Symbol, Integer> channelGetter()
        {
            return new Function<Symbol, Integer>() {
                @NotNull
                @Override
                public Integer apply(Symbol input)
                {
                    checkArgument(layout.containsKey(input));
                    return layout.get(input).getChannel();
                }
            };
        }

        public List<TupleInfo> getTupleInfos()
        {
            return tupleInfos;
        }

        public Map<Symbol, Input> getLayout()
        {
            return layout;
        }

        private List<OperatorFactory> getOperatorFactories()
        {
            return operatorFactories;
        }
    }

    private static Ordering<Input> inputOrdering()
    {
        return Ordering.from(new Comparator<Input>()
        {
            @Override
            public int compare(Input o1, Input o2)
            {
                return ComparisonChain.start()
                        .compare(o1.getChannel(), o2.getChannel())
                        .result();
            }
        });
    }
}
