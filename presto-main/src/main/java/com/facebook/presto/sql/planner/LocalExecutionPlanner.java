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

import com.facebook.presto.Session;
import com.facebook.presto.block.BlockUtils;
import com.facebook.presto.execution.TaskManagerConfig;
import com.facebook.presto.index.IndexManager;
import com.facebook.presto.metadata.ColumnHandle;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.operator.AggregationOperator.AggregationOperatorFactory;
import com.facebook.presto.operator.CursorProcessor;
import com.facebook.presto.operator.DriverFactory;
import com.facebook.presto.operator.ExchangeClient;
import com.facebook.presto.operator.ExchangeOperator.ExchangeOperatorFactory;
import com.facebook.presto.operator.FilterAndProjectOperator;
import com.facebook.presto.operator.FilterFunction;
import com.facebook.presto.operator.FilterFunctions;
import com.facebook.presto.operator.GenericCursorProcessor;
import com.facebook.presto.operator.GenericPageProcessor;
import com.facebook.presto.operator.HashAggregationOperator.HashAggregationOperatorFactory;
import com.facebook.presto.operator.HashBuilderOperator.HashBuilderOperatorFactory;
import com.facebook.presto.operator.HashSemiJoinOperator.HashSemiJoinOperatorFactory;
import com.facebook.presto.operator.InMemoryExchange;
import com.facebook.presto.operator.InMemoryExchangeSourceOperator.InMemoryExchangeSourceOperatorFactory;
import com.facebook.presto.operator.LimitOperator.LimitOperatorFactory;
import com.facebook.presto.operator.LookupJoinOperators;
import com.facebook.presto.operator.LookupSourceSupplier;
import com.facebook.presto.operator.MarkDistinctOperator.MarkDistinctOperatorFactory;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.operator.OrderByOperator.OrderByOperatorFactory;
import com.facebook.presto.operator.OutputFactory;
import com.facebook.presto.operator.PageProcessor;
import com.facebook.presto.operator.ProjectionFunction;
import com.facebook.presto.operator.ProjectionFunctions;
import com.facebook.presto.operator.RecordSinkManager;
import com.facebook.presto.operator.RowNumberLimitOperator;
import com.facebook.presto.operator.SampleOperator.SampleOperatorFactory;
import com.facebook.presto.operator.ScanFilterAndProjectOperator;
import com.facebook.presto.operator.SetBuilderOperator.SetBuilderOperatorFactory;
import com.facebook.presto.operator.SetBuilderOperator.SetSupplier;
import com.facebook.presto.operator.SourceOperatorFactory;
import com.facebook.presto.operator.TableScanOperator.TableScanOperatorFactory;
import com.facebook.presto.operator.TopNOperator.TopNOperatorFactory;
import com.facebook.presto.operator.TopNRowNumberOperator;
import com.facebook.presto.operator.ValuesOperator.ValuesOperatorFactory;
import com.facebook.presto.operator.WindowFunctionDefinition;
import com.facebook.presto.operator.WindowOperator.WindowOperatorFactory;
import com.facebook.presto.operator.aggregation.AccumulatorFactory;
import com.facebook.presto.operator.index.FieldSetFilteringRecordSet;
import com.facebook.presto.operator.index.IndexBuildDriverFactoryProvider;
import com.facebook.presto.operator.index.IndexJoinLookupStats;
import com.facebook.presto.operator.index.IndexLookupSourceSupplier;
import com.facebook.presto.operator.index.IndexSourceOperator;
import com.facebook.presto.spi.Index;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.RecordSink;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.split.MappedRecordSet;
import com.facebook.presto.split.PageSourceProvider;
import com.facebook.presto.sql.gen.ExpressionCompiler;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.optimizations.IndexJoinOptimizer;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.DistinctLimitNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.IndexJoinNode;
import com.facebook.presto.sql.planner.plan.IndexSourceNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.MarkDistinctNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.RowNumberLimitNode;
import com.facebook.presto.sql.planner.plan.SampleNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.SinkNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.TableCommitNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.TableWriterNode;
import com.facebook.presto.sql.planner.plan.TopNNode;
import com.facebook.presto.sql.planner.plan.TopNRowNumberNode;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.relational.RowExpression;
import com.facebook.presto.sql.relational.SqlToRowExpressionTranslator;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.util.IterableTransformer;
import com.facebook.presto.util.MoreFunctions;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import com.google.common.collect.SetMultimap;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.validation.constraints.NotNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.operator.DistinctLimitOperator.DistinctLimitOperatorFactory;
import static com.facebook.presto.operator.TableCommitOperator.TableCommitOperatorFactory;
import static com.facebook.presto.operator.TableCommitOperator.TableCommitter;
import static com.facebook.presto.operator.TableWriterOperator.TableWriterOperatorFactory;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypes;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypesFromInput;
import static com.facebook.presto.sql.planner.plan.IndexJoinNode.EquiJoinClause.indexGetter;
import static com.facebook.presto.sql.planner.plan.IndexJoinNode.EquiJoinClause.probeGetter;
import static com.facebook.presto.sql.planner.plan.JoinNode.EquiJoinClause.leftGetter;
import static com.facebook.presto.sql.planner.plan.JoinNode.EquiJoinClause.rightGetter;
import static com.facebook.presto.sql.planner.plan.TableWriterNode.CreateHandle;
import static com.facebook.presto.sql.planner.plan.TableWriterNode.InsertHandle;
import static com.facebook.presto.sql.planner.plan.TableWriterNode.WriterTarget;
import static com.google.common.base.Functions.forMap;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.concat;
import static java.util.Collections.singleton;

public class LocalExecutionPlanner
{
    private static final Logger log = Logger.get(LocalExecutionPlanner.class);

    private final Metadata metadata;
    private final SqlParser sqlParser;

    private final PageSourceProvider pageSourceProvider;
    private final IndexManager indexManager;
    private final RecordSinkManager recordSinkManager;
    private final Supplier<ExchangeClient> exchangeClientSupplier;
    private final ExpressionCompiler compiler;
    private final boolean interpreterEnabled;
    private final DataSize maxIndexMemorySize;
    private final IndexJoinLookupStats indexJoinLookupStats;

    @Inject
    public LocalExecutionPlanner(
            Metadata metadata,
            SqlParser sqlParser,
            PageSourceProvider pageSourceProvider,
            IndexManager indexManager,
            RecordSinkManager recordSinkManager,
            Supplier<ExchangeClient> exchangeClientSupplier,
            ExpressionCompiler compiler,
            IndexJoinLookupStats indexJoinLookupStats,
            CompilerConfig compilerConfig,
            TaskManagerConfig taskManagerConfig)
    {
        checkNotNull(compilerConfig, "compilerConfig is null");
        this.pageSourceProvider = checkNotNull(pageSourceProvider, "pageSourceProvider is null");
        this.indexManager = checkNotNull(indexManager, "indexManager is null");
        this.exchangeClientSupplier = exchangeClientSupplier;
        this.metadata = checkNotNull(metadata, "metadata is null");
        this.sqlParser = checkNotNull(sqlParser, "sqlParser is null");
        this.recordSinkManager = checkNotNull(recordSinkManager, "recordSinkManager is null");
        this.compiler = checkNotNull(compiler, "compiler is null");
        this.indexJoinLookupStats = checkNotNull(indexJoinLookupStats, "indexJoinLookupStats is null");
        this.maxIndexMemorySize = checkNotNull(taskManagerConfig, "taskManagerConfig is null").getMaxTaskIndexMemoryUsage();

        interpreterEnabled = compilerConfig.isInterpreterEnabled();
    }

    public LocalExecutionPlan plan(Session session,
            PlanNode plan,
            Map<Symbol, Type> types,
            OutputFactory outputOperatorFactory)
    {
        LocalExecutionPlanContext context = new LocalExecutionPlanContext(session, types);

        PhysicalOperation physicalOperation = plan.accept(new Visitor(session), context);
        DriverFactory driverFactory = new DriverFactory(
                context.isInputDriver(),
                true,
                ImmutableList.<OperatorFactory>builder()
                        .addAll(physicalOperation.getOperatorFactories())
                        .add(outputOperatorFactory.createOutputOperator(context.getNextOperatorId(), physicalOperation.getTypes()))
                        .build());
        context.addDriverFactory(driverFactory);

        return new LocalExecutionPlan(context.getDriverFactories());
    }

    private static class LocalExecutionPlanContext
    {
        private final Session session;
        private final Map<Symbol, Type> types;
        private final List<DriverFactory> driverFactories;
        private final Optional<IndexSourceContext> indexSourceContext;

        private int nextOperatorId;
        private boolean inputDriver = true;

        public LocalExecutionPlanContext(Session session, Map<Symbol, Type> types)
        {
            this(session, types, new ArrayList<DriverFactory>(), Optional.<IndexSourceContext>absent());
        }

        private LocalExecutionPlanContext(Session session, Map<Symbol, Type> types, List<DriverFactory> driverFactories, Optional<IndexSourceContext> indexSourceContext)
        {
            this.session = session;
            this.types = types;
            this.driverFactories = driverFactories;
            this.indexSourceContext = indexSourceContext;
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

        public Optional<IndexSourceContext> getIndexSourceContext()
        {
            return indexSourceContext;
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
            checkState(!indexSourceContext.isPresent(), "index build plan can not have sub-contexts");
            return new LocalExecutionPlanContext(session, types, driverFactories, indexSourceContext);
        }

        public LocalExecutionPlanContext createIndexSourceSubContext(IndexSourceContext indexSourceContext)
        {
            return new LocalExecutionPlanContext(session, types, driverFactories, Optional.of(indexSourceContext));
        }
    }

    private static class IndexSourceContext
    {
        private final SetMultimap<Symbol, Integer> indexLookupToProbeInput;

        public IndexSourceContext(SetMultimap<Symbol, Integer> indexLookupToProbeInput)
        {
            this.indexLookupToProbeInput = ImmutableSetMultimap.copyOf(checkNotNull(indexLookupToProbeInput, "indexLookupToProbeInput is null"));
        }

        private SetMultimap<Symbol, Integer> getIndexLookupToProbeInput()
        {
            return indexLookupToProbeInput;
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
        private final Session session;

        private Visitor(Session session)
        {
            this.session = session;
        }

        @Override
        public PhysicalOperation visitExchange(ExchangeNode node, LocalExecutionPlanContext context)
        {
            List<Type> types = getSourceOperatorTypes(node, context.getTypes());

            OperatorFactory operatorFactory = new ExchangeOperatorFactory(context.getNextOperatorId(), node.getId(), exchangeClientSupplier, types);

            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            int channel = 0;
            for (Symbol symbol : node.getOutputSymbols()) {
                outputMappings.put(symbol, channel);
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
            Ordering<Integer> comparator = Ordering.natural();

            List<Symbol> sourceSymbols = IterableTransformer.on(source.getLayout().entrySet())
                    .orderBy(comparator.onResultOf(MoreFunctions.<Symbol, Integer>valueGetter()))
                    .transform(MoreFunctions.<Symbol, Integer>keyGetter())
                    .list();

            List<Symbol> resultSymbols = node.getOutputSymbols();
            if (resultSymbols.equals(sourceSymbols) && resultSymbols.size() == source.getTypes().size()) {
                // no projection needed
                return source;
            }

            // otherwise, introduce a projection to match the expected output
            IdentityProjectionInfo mappings = computeIdentityMapping(resultSymbols, source.getLayout(), context.getTypes());

            OperatorFactory operatorFactory = new FilterAndProjectOperator.FilterAndProjectOperatorFactory(
                    context.getNextOperatorId(),
                    new GenericPageProcessor(FilterFunctions.TRUE_FUNCTION, mappings.getProjections()),
                    toTypes(mappings.getProjections()));

            return new PhysicalOperation(operatorFactory, mappings.getOutputLayout(), source);
        }

        @Override
        public PhysicalOperation visitRowNumberLimit(RowNumberLimitNode node, LocalExecutionPlanContext context)
        {
            final PhysicalOperation source = node.getSource().accept(this, context);

            List<Symbol> partitionBySymbols = node.getPartitionBy();
            List<Integer> partitionChannels = ImmutableList.copyOf(getChannelsForSymbols(partitionBySymbols, source.getLayout()));
            List<Type> partitionTypes = ImmutableList.copyOf(Iterables.transform(partitionChannels, new Function<Integer, Type>()
            {
                @Override
                public Type apply(Integer input)
                {
                    return source.getTypes().get(input);
                }
            }));

            ImmutableList.Builder<Integer> outputChannels = ImmutableList.builder();
            for (int i = 0; i < source.getTypes().size(); i++) {
                outputChannels.add(i);
            }

            // compute the layout of the output from the window operator
            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            outputMappings.putAll(source.getLayout());

            // row number function goes in the last channel
            int channel = source.getTypes().size();
            outputMappings.put(node.getRowNumberSymbol(), channel);

            OperatorFactory operatorFactory = new RowNumberLimitOperator.RowNumberLimitOperatorFactory(
                    context.getNextOperatorId(),
                    source.getTypes(),
                    outputChannels.build(),
                    partitionChannels,
                    partitionTypes,
                    1_000_000,
                    node.getMaxRowCountPerPartition());
            return new PhysicalOperation(operatorFactory, outputMappings.build(), source);
        }

        @Override
        public PhysicalOperation visitTopNRowNumber(final TopNRowNumberNode node, LocalExecutionPlanContext context)
        {
            final PhysicalOperation source = node.getSource().accept(this, context);

            List<Symbol> partitionBySymbols = node.getPartitionBy();
            List<Integer> partitionChannels = ImmutableList.copyOf(getChannelsForSymbols(partitionBySymbols, source.getLayout()));
            List<Type> partitionTypes = ImmutableList.copyOf(Iterables.transform(partitionChannels, new Function<Integer, Type>()
            {
                @Override
                public Type apply(Integer input)
                {
                    return source.getTypes().get(input);
                }
            }));

            List<Symbol> orderBySymbols = node.getOrderBy();
            List<Integer> sortChannels = ImmutableList.copyOf(getChannelsForSymbols(orderBySymbols, source.getLayout()));
            List<SortOrder> sortOrder = ImmutableList.copyOf(Iterables.transform(orderBySymbols, new Function<Symbol, SortOrder>()
            {
                @Override
                public SortOrder apply(Symbol input)
                {
                    return node.getOrderings().get(input);
                }
            }));

            ImmutableList.Builder<Integer> outputChannels = ImmutableList.builder();
            for (int i = 0; i < source.getTypes().size(); i++) {
                outputChannels.add(i);
            }

            // compute the layout of the output from the window operator
            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            outputMappings.putAll(source.getLayout());

            // row number function goes in the last channel
            int channel = source.getTypes().size();
            outputMappings.put(node.getRowNumberSymbol(), channel);

            OperatorFactory operatorFactory = new TopNRowNumberOperator.TopNRowNumberOperatorFactory(
                    context.getNextOperatorId(),
                    source.getTypes(),
                    outputChannels.build(),
                    partitionChannels,
                    partitionTypes,
                    sortChannels,
                    sortOrder,
                    node.getMaxRowCountPerPartition(),
                    1_000_000);

            return new PhysicalOperation(operatorFactory, outputMappings.build(), source);
        }

        @Override
        public PhysicalOperation visitWindow(final WindowNode node, LocalExecutionPlanContext context)
        {
            final PhysicalOperation source = node.getSource().accept(this, context);

            List<Symbol> partitionBySymbols = node.getPartitionBy();
            List<Symbol> orderBySymbols = node.getOrderBy();
            List<Integer> partitionChannels = ImmutableList.copyOf(getChannelsForSymbols(partitionBySymbols, source.getLayout()));

            List<Integer> sortChannels = ImmutableList.copyOf(getChannelsForSymbols(orderBySymbols, source.getLayout()));
            List<SortOrder> sortOrder = ImmutableList.copyOf(Iterables.transform(orderBySymbols, new Function<Symbol, SortOrder>()
            {
                @Override
                public SortOrder apply(Symbol input)
                {
                    return node.getOrderings().get(input);
                }
            }));

            ImmutableList.Builder<Integer> outputChannels = ImmutableList.builder();
            for (int i = 0; i < source.getTypes().size(); i++) {
                outputChannels.add(i);
            }

            ImmutableList.Builder<WindowFunctionDefinition> windowFunctionsBuilder = ImmutableList.builder();
            ImmutableList.Builder<Symbol> windowFunctionOutputSymbolsBuilder = ImmutableList.builder();
            for (Map.Entry<Symbol, FunctionCall> entry : node.getWindowFunctions().entrySet()) {
                ImmutableList.Builder<Integer> arguments = ImmutableList.builder();
                for (Expression argument : entry.getValue().getArguments()) {
                    Symbol argumentSymbol = Symbol.fromQualifiedName(((QualifiedNameReference) argument).getName());
                    arguments.add(source.getLayout().get(argumentSymbol));
                }
                Symbol symbol = entry.getKey();
                Signature signature = node.getSignatures().get(symbol);
                windowFunctionsBuilder.add(metadata.getExactFunction(signature).bindWindowFunction(arguments.build()));
                windowFunctionOutputSymbolsBuilder.add(symbol);
            }

            List<Symbol> windowFunctionOutputSymbols = windowFunctionOutputSymbolsBuilder.build();
            List<WindowFunctionDefinition> windowFunctions = windowFunctionsBuilder.build();

            // compute the layout of the output from the window operator
            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            for (Symbol symbol : node.getSource().getOutputSymbols()) {
                outputMappings.put(symbol, source.getLayout().get(symbol));
            }

            // window functions go in remaining channels starting after the last channel from the source operator, one per channel
            int channel = source.getTypes().size();
            for (Symbol symbol : windowFunctionOutputSymbols) {
                outputMappings.put(symbol, channel);
                channel++;
            }

            OperatorFactory operatorFactory = new WindowOperatorFactory(
                        context.getNextOperatorId(),
                        source.getTypes(),
                        outputChannels.build(),
                        windowFunctions,
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
                sortChannels.add(source.getLayout().get(symbol));
                sortOrders.add(node.getOrderings().get(symbol));
            }

            OperatorFactory operator = new TopNOperatorFactory(
                    context.getNextOperatorId(),
                    source.getTypes(),
                    (int) node.getCount(),
                    sortChannels,
                    sortOrders,
                    node.isPartial());

            return new PhysicalOperation(operator, source.getLayout(), source);
        }

        @Override
        public PhysicalOperation visitSort(SortNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            List<Symbol> orderBySymbols = node.getOrderBy();

            List<Integer> orderByChannels = getChannelsForSymbols(orderBySymbols, source.getLayout());

            ImmutableList.Builder<SortOrder> sortOrder = ImmutableList.builder();
            for (Symbol symbol : orderBySymbols) {
                sortOrder.add(node.getOrderings().get(symbol));
            }

            ImmutableList.Builder<Integer> outputChannels = ImmutableList.builder();
            for (int i = 0; i < source.getTypes().size(); i++) {
                outputChannels.add(i);
            }

            OperatorFactory operator = new OrderByOperatorFactory(
                    context.getNextOperatorId(),
                    source.getTypes(),
                    outputChannels.build(),
                    10_000,
                    orderByChannels,
                    sortOrder.build());

            return new PhysicalOperation(operator, source.getLayout(), source);
        }

        @Override
        public PhysicalOperation visitLimit(LimitNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            OperatorFactory operatorFactory = new LimitOperatorFactory(context.getNextOperatorId(), source.getTypes(), node.getCount());
            return new PhysicalOperation(operatorFactory, source.getLayout(), source);
        }

        @Override
        public PhysicalOperation visitDistinctLimit(DistinctLimitNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);
            OperatorFactory operatorFactory = new DistinctLimitOperatorFactory(
                    context.getNextOperatorId(),
                    source.getTypes(),
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
            Map<Symbol, Integer> outputMappings = ImmutableMap.<Symbol, Integer>builder()
                    .putAll(source.getLayout())
                    .put(node.getMarkerSymbol(), source.getLayout().size()).build();

            MarkDistinctOperatorFactory operator = new MarkDistinctOperatorFactory(context.getNextOperatorId(), source.getTypes(), channels);
            return new PhysicalOperation(operator, outputMappings, source);
        }

        @Override
        public PhysicalOperation visitSample(SampleNode node, LocalExecutionPlanContext context)
        {
            // For system sample, the splits are already filtered out, so no specific action needs to be taken here
            if (node.getSampleType() == SampleNode.Type.SYSTEM) {
                return node.getSource().accept(this, context);
            }

            if (node.getSampleType() == SampleNode.Type.POISSONIZED) {
                PhysicalOperation source = node.getSource().accept(this, context);
                OperatorFactory operatorFactory = new SampleOperatorFactory(context.getNextOperatorId(), node.getSampleRatio(), node.isRescaled(), source.getTypes());
                checkState(node.getSampleWeightSymbol().isPresent(), "sample weight symbol missing");
                Map<Symbol, Integer> layout = ImmutableMap.<Symbol, Integer>builder().putAll(source.getLayout()).put(node.getSampleWeightSymbol().get(), source.getTypes().size()).build();
                return new PhysicalOperation(operatorFactory, layout, source);
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
            Map<Symbol, Integer> sourceLayout;
            Map<Integer, Type> sourceTypes;
            List<ColumnHandle> columns = null;
            PhysicalOperation source = null;
            if (sourceNode instanceof TableScanNode) {
                TableScanNode tableScanNode = (TableScanNode) sourceNode;

                // extract the column handles and channel to type mapping
                sourceLayout = new LinkedHashMap<>();
                sourceTypes = new LinkedHashMap<>();
                columns = new ArrayList<>();
                int channel = 0;
                for (Symbol symbol : tableScanNode.getOutputSymbols()) {
                    columns.add(tableScanNode.getAssignments().get(symbol));

                    Integer input = channel;
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
                sourceTypes = getInputTypes(source.getLayout(), source.getTypes());
            }

            // build output mapping
            ImmutableMap.Builder<Symbol, Integer> outputMappingsBuilder = ImmutableMap.builder();
            for (int i = 0; i < outputSymbols.size(); i++) {
                Symbol symbol = outputSymbols.get(i);
                outputMappingsBuilder.put(symbol, i);
            }
            Map<Symbol, Integer> outputMappings = outputMappingsBuilder.build();

            try {
                // compiler uses inputs instead of symbols, so rewrite the expressions first
                SymbolToInputRewriter symbolToInputRewriter = new SymbolToInputRewriter(sourceLayout);
                Expression rewrittenFilter = ExpressionTreeRewriter.rewriteWith(symbolToInputRewriter, filterExpression);

                List<Expression> rewrittenProjections = new ArrayList<>();
                for (Expression projection : projectionExpressions) {
                    rewrittenProjections.add(ExpressionTreeRewriter.rewriteWith(symbolToInputRewriter, projection));
                }

                IdentityHashMap<Expression, Type> expressionTypes = getExpressionTypesFromInput(
                        context.getSession(),
                        metadata,
                        sqlParser,
                        sourceTypes,
                        concat(singleton(rewrittenFilter), rewrittenProjections));

                RowExpression traslatedFilter = SqlToRowExpressionTranslator.translate(rewrittenFilter, expressionTypes, metadata, session, true);
                List<RowExpression> translatedProjections = SqlToRowExpressionTranslator.translate(rewrittenProjections, expressionTypes, metadata, session, true);

                if (columns != null) {
                    CursorProcessor cursorProcessor = compiler.compileCursorProcessor(traslatedFilter, translatedProjections, sourceNode.getId());
                    PageProcessor pageProcessor = compiler.compilePageProcessor(traslatedFilter, translatedProjections);

                    SourceOperatorFactory operatorFactory = new ScanFilterAndProjectOperator.ScanFilterAndProjectOperatorFactory(
                            context.getNextOperatorId(),
                            sourceNode.getId(),
                            pageSourceProvider,
                            cursorProcessor,
                            pageProcessor,
                            columns,
                            Lists.transform(rewrittenProjections, forMap(expressionTypes)));

                    return new PhysicalOperation(operatorFactory, outputMappings);
                }
                else {
                    PageProcessor processor = compiler.compilePageProcessor(traslatedFilter, translatedProjections);

                    OperatorFactory operatorFactory = new FilterAndProjectOperator.FilterAndProjectOperatorFactory(
                            context.getNextOperatorId(),
                            processor,
                            Lists.transform(rewrittenProjections, forMap(expressionTypes)));

                    return new PhysicalOperation(operatorFactory, outputMappings, source);
                }
            }
            catch (RuntimeException e) {
                if (!interpreterEnabled) {
                    throw e;
                }

                // compilation failed, use interpreter
                log.error(e, "Compile failed for filter=%s projections=%s sourceTypes=%s error=%s", filterExpression, projectionExpressions, sourceTypes, e);
            }

            FilterFunction filterFunction;
            if (filterExpression != BooleanLiteral.TRUE_LITERAL) {
                filterFunction = new InterpretedFilterFunction(filterExpression, context.getTypes(), sourceLayout, metadata, sqlParser, context.getSession());
            }
            else {
                filterFunction = FilterFunctions.TRUE_FUNCTION;
            }

            List<ProjectionFunction> projectionFunctions = new ArrayList<>();
            for (Expression expression : projectionExpressions) {
                ProjectionFunction function;
                if (expression instanceof QualifiedNameReference) {
                    // fast path when we know it's a direct symbol reference
                    Symbol reference = Symbol.fromQualifiedName(((QualifiedNameReference) expression).getName());
                    function = ProjectionFunctions.singleColumn(context.getTypes().get(reference), sourceLayout.get(reference));
                }
                else {
                    function = new InterpretedProjectionFunction(
                            expression,
                            context.getTypes(),
                            sourceLayout,
                            metadata,
                            sqlParser,
                            context.getSession()
                    );
                }
                projectionFunctions.add(function);
            }

            if (columns != null) {
                OperatorFactory operatorFactory = new ScanFilterAndProjectOperator.ScanFilterAndProjectOperatorFactory(
                        context.getNextOperatorId(),
                        sourceNode.getId(),
                        pageSourceProvider,
                        new GenericCursorProcessor(filterFunction, projectionFunctions),
                        new GenericPageProcessor(filterFunction, projectionFunctions),
                        columns,
                        toTypes(projectionFunctions));

                return new PhysicalOperation(operatorFactory, outputMappings);
            }
            else {
                OperatorFactory operatorFactory = new FilterAndProjectOperator.FilterAndProjectOperatorFactory(
                        context.getNextOperatorId(),
                        new GenericPageProcessor(filterFunction, projectionFunctions),
                        toTypes(projectionFunctions));
                return new PhysicalOperation(operatorFactory, outputMappings, source);
            }
        }

        private Map<Integer, Type> getInputTypes(Map<Symbol, Integer> layout, List<Type> types)
        {
            Builder<Integer, Type> inputTypes = ImmutableMap.builder();
            for (Integer input : ImmutableSet.copyOf(layout.values())) {
                Type type = types.get(input);
                inputTypes.put(input, type);
            }
            return inputTypes.build();
        }

        @Override
        public PhysicalOperation visitTableScan(TableScanNode node, LocalExecutionPlanContext context)
        {
            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            List<ColumnHandle> columns = new ArrayList<>();

            int channel = 0;
            for (Symbol symbol : node.getOutputSymbols()) {
                columns.add(node.getAssignments().get(symbol));

                outputMappings.put(symbol, channel); // one column per channel
                channel++;
            }

            List<Type> types = getSourceOperatorTypes(node, context.getTypes());
            OperatorFactory operatorFactory = new TableScanOperatorFactory(context.getNextOperatorId(), node.getId(), pageSourceProvider, types, columns);
            return new PhysicalOperation(operatorFactory, outputMappings.build());
        }

        @Override
        public PhysicalOperation visitValues(ValuesNode node, LocalExecutionPlanContext context)
        {
            Map<Symbol, Integer> outputMappings = new LinkedHashMap<>();
            List<Type> outputTypes = new ArrayList<>();

            int channel = 0;
            for (Symbol symbol : node.getOutputSymbols()) {
                Integer input = channel;
                outputMappings.put(symbol, input);

                Type type = checkNotNull(context.getTypes().get(symbol), "No type for symbol %s", symbol);
                outputTypes.add(type);

                channel++;
            }

            PageBuilder pageBuilder = new PageBuilder(outputTypes);
            for (List<Expression> row : node.getRows()) {
                pageBuilder.declarePosition();
                IdentityHashMap<Expression, Type> expressionTypes = getExpressionTypes(
                        context.getSession(),
                        metadata,
                        sqlParser,
                        ImmutableMap.<Symbol, Type>of(),
                        ImmutableList.copyOf(row));
                for (int i = 0; i < row.size(); i++) {
                    // evaluate the literal value
                    Object result = ExpressionInterpreter.expressionInterpreter(row.get(i), metadata, context.getSession(), expressionTypes).evaluate(0);
                    BlockUtils.appendObject(outputTypes.get(i), pageBuilder.getBlockBuilder(i), result);
                }
            }

            OperatorFactory operatorFactory = new ValuesOperatorFactory(context.getNextOperatorId(), outputTypes, ImmutableList.of(pageBuilder.build()));
            return new PhysicalOperation(operatorFactory, outputMappings);
        }

        @Override
        public PhysicalOperation visitIndexSource(IndexSourceNode node, LocalExecutionPlanContext context)
        {
            checkState(context.getIndexSourceContext().isPresent(), "Must be in an index source context");
            IndexSourceContext indexSourceContext = context.getIndexSourceContext().get();

            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            int channel = 0;
            for (Symbol symbol : node.getOutputSymbols()) {
                outputMappings.put(symbol, channel);
                channel++;
            }

            SetMultimap<Symbol, Integer> indexLookupToProbeInput = indexSourceContext.getIndexLookupToProbeInput();
            checkState(indexLookupToProbeInput.keySet().equals(node.getLookupSymbols()));

            // Finalize the symbol lookup layout for the index source
            List<Symbol> lookupSymbolSchema = ImmutableList.copyOf(node.getLookupSymbols());

            // Identify how to remap the probe key Input to match the source index lookup layout
            ImmutableList.Builder<Integer> remappedProbeKeyChannelsBuilder = ImmutableList.builder();
            // Identify overlapping fields that can produce the same lookup symbol.
            // We will filter incoming keys to ensure that overlapping fields will have the same value.
            ImmutableList.Builder<Set<Integer>> overlappingFieldSetsBuilder = ImmutableList.builder();
            for (Symbol lookupSymbol : lookupSymbolSchema) {
                Set<Integer> potentialProbeInputs = indexLookupToProbeInput.get(lookupSymbol);
                checkState(!potentialProbeInputs.isEmpty(), "Must have at least one source from the probe input");
                if (potentialProbeInputs.size() > 1) {
                    overlappingFieldSetsBuilder.add(FluentIterable.from(potentialProbeInputs)
                            .toSet());
                }
                remappedProbeKeyChannelsBuilder.add(Iterables.getFirst(potentialProbeInputs, null));
            }
            final List<Set<Integer>> overlappingFieldSets = overlappingFieldSetsBuilder.build();
            final List<Integer> remappedProbeKeyChannels = remappedProbeKeyChannelsBuilder.build();
            Function<RecordSet, RecordSet> probeKeyNormalizer = new Function<RecordSet, RecordSet>()
            {
                @Override
                public RecordSet apply(RecordSet recordSet)
                {
                    if (!overlappingFieldSets.isEmpty()) {
                        recordSet = new FieldSetFilteringRecordSet(recordSet, overlappingFieldSets);
                    }
                    return new MappedRecordSet(recordSet, remappedProbeKeyChannels);
                }
            };

            // Declare the input and output schemas for the index and acquire the actual Index
            List<ColumnHandle> lookupSchema = Lists.transform(lookupSymbolSchema, forMap(node.getAssignments()));
            List<ColumnHandle> outputSchema = Lists.transform(node.getOutputSymbols(), forMap(node.getAssignments()));
            Index index = indexManager.getIndex(node.getIndexHandle(), lookupSchema, outputSchema);

            List<Type> types = getSourceOperatorTypes(node, context.getTypes());
            OperatorFactory operatorFactory = new IndexSourceOperator.IndexSourceOperatorFactory(context.getNextOperatorId(), node.getId(), index, types, probeKeyNormalizer);
            return new PhysicalOperation(operatorFactory, outputMappings.build());
        }

        /**
         * This method creates a mapping from each index source lookup symbol (directly applied to the index)
         * to the corresponding probe key Input
         */
        private SetMultimap<Symbol, Integer> mapIndexSourceLookupSymbolToProbeKeyInput(IndexJoinNode node, Map<Symbol, Integer> probeKeyLayout)
        {
            Set<Symbol> indexJoinSymbols = FluentIterable.from(node.getCriteria())
                    .transform(indexGetter())
                    .toSet();

            // Trace the index join symbols to the index source lookup symbols
            // Map: Index join symbol => Index source lookup symbol
            Map<Symbol, Symbol> indexKeyTrace = IndexJoinOptimizer.IndexKeyTracer.trace(node.getIndexSource(), indexJoinSymbols);

            // Map the index join symbols to the probe key Input
            Multimap<Symbol, Integer> indexToProbeKeyInput = HashMultimap.create();
            for (IndexJoinNode.EquiJoinClause clause : node.getCriteria()) {
                indexToProbeKeyInput.put(clause.getIndex(), probeKeyLayout.get(clause.getProbe()));
            }

            // Create the mapping from index source look up symbol to probe key Input
            ImmutableSetMultimap.Builder<Symbol, Integer> builder = ImmutableSetMultimap.builder();
            for (Map.Entry<Symbol, Symbol> entry : indexKeyTrace.entrySet()) {
                Symbol indexJoinSymbol = entry.getKey();
                Symbol indexLookupSymbol = entry.getValue();
                builder.putAll(indexLookupSymbol, indexToProbeKeyInput.get(indexJoinSymbol));
            }
            return builder.build();
        }

        @Override
        public PhysicalOperation visitIndexJoin(IndexJoinNode node, LocalExecutionPlanContext context)
        {
            List<IndexJoinNode.EquiJoinClause> clauses = node.getCriteria();

            List<Symbol> probeSymbols = Lists.transform(clauses, probeGetter());
            List<Symbol> indexSymbols = Lists.transform(clauses, indexGetter());

            // Plan probe side
            PhysicalOperation probeSource = node.getProbeSource().accept(this, context);
            List<Integer> probeChannels = getChannelsForSymbols(probeSymbols, probeSource.getLayout());

            // The probe key channels will be handed to the index according to probeSymbol order
            Map<Symbol, Integer> probeKeyLayout = new HashMap<>();
            for (int i = 0; i < probeSymbols.size(); i++) {
                // Duplicate symbols can appear and we only need to take take one of the Inputs
                probeKeyLayout.put(probeSymbols.get(i), i);
            }

            // Plan the index source side
            SetMultimap<Symbol, Integer> indexLookupToProbeInput = mapIndexSourceLookupSymbolToProbeKeyInput(node, probeKeyLayout);
            LocalExecutionPlanContext indexContext = context.createIndexSourceSubContext(new IndexSourceContext(indexLookupToProbeInput));
            PhysicalOperation indexSource = node.getIndexSource().accept(this, indexContext);
            List<Integer> indexChannels = getChannelsForSymbols(indexSymbols, indexSource.getLayout());

            IndexBuildDriverFactoryProvider indexBuildDriverFactoryProvider = new IndexBuildDriverFactoryProvider(
                    indexContext.getNextOperatorId(),
                    indexContext.isInputDriver(),
                    indexSource.getOperatorFactories());

            IndexLookupSourceSupplier indexLookupSourceSupplier = new IndexLookupSourceSupplier(
                    indexChannels,
                    indexSource.getTypes(),
                    indexBuildDriverFactoryProvider,
                    maxIndexMemorySize,
                    indexJoinLookupStats);

            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            outputMappings.putAll(probeSource.getLayout());

            // inputs from index side of the join are laid out following the input from the probe side,
            // so adjust the channel ids but keep the field layouts intact
            int offset = probeSource.getTypes().size();
            for (Map.Entry<Symbol, Integer> entry : indexSource.getLayout().entrySet()) {
                Integer input = entry.getValue();
                outputMappings.put(entry.getKey(), offset + input);
            }

            OperatorFactory lookupJoinOperatorFactory;
            switch (node.getType()) {
                case INNER:
                    lookupJoinOperatorFactory = LookupJoinOperators.innerJoin(context.getNextOperatorId(), indexLookupSourceSupplier, probeSource.getTypes(), probeChannels);
                    break;
                case SOURCE_OUTER:
                    lookupJoinOperatorFactory = LookupJoinOperators.outerJoin(context.getNextOperatorId(), indexLookupSourceSupplier, probeSource.getTypes(), probeChannels);
                    break;
                default:
                    throw new AssertionError("Unknown type: " + node.getType());
            }
            return new PhysicalOperation(lookupJoinOperatorFactory, outputMappings.build(), probeSource);
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
                    buildSource.getTypes(),
                    buildChannels,
                    100_000);
            LookupSourceSupplier lookupSourceSupplier = hashBuilderOperatorFactory.getLookupSourceSupplier();
            DriverFactory buildDriverFactory = new DriverFactory(
                    buildContext.isInputDriver(),
                    false,
                    ImmutableList.<OperatorFactory>builder()
                            .addAll(buildSource.getOperatorFactories())
                            .add(hashBuilderOperatorFactory)
                            .build());
            context.addDriverFactory(buildDriverFactory);

            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            outputMappings.putAll(probeSource.getLayout());

            // inputs from build side of the join are laid out following the input from the probe side,
            // so adjust the channel ids but keep the field layouts intact
            int offset = probeSource.getTypes().size();
            for (Map.Entry<Symbol, Integer> entry : buildSource.getLayout().entrySet()) {
                Integer input = entry.getValue();
                outputMappings.put(entry.getKey(), offset + input);
            }

            OperatorFactory operator = createJoinOperator(node.getType(), lookupSourceSupplier, probeSource.getTypes(), probeChannels, context);
            return new PhysicalOperation(operator, outputMappings.build(), probeSource);
        }

        private OperatorFactory createJoinOperator(
                JoinNode.Type type,
                LookupSourceSupplier lookupSourceSupplier,
                List<Type> probeTypes,
                List<Integer> probeJoinChannels,
                LocalExecutionPlanContext context)
        {
            switch (type) {
                case INNER:
                    return LookupJoinOperators.innerJoin(context.getNextOperatorId(), lookupSourceSupplier, probeTypes, probeJoinChannels);
                case LEFT:
                case RIGHT:
                    return LookupJoinOperators.outerJoin(context.getNextOperatorId(), lookupSourceSupplier, probeTypes, probeJoinChannels);
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

            int probeChannel = probeSource.getLayout().get(node.getSourceJoinSymbol());
            int buildChannel = buildSource.getLayout().get(node.getFilteringSourceJoinSymbol());

            SetBuilderOperatorFactory setBuilderOperatorFactory = new SetBuilderOperatorFactory(buildContext.getNextOperatorId(), buildSource.getTypes(), buildChannel, 100_000);
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
            Map<Symbol, Integer> outputMappings = ImmutableMap.<Symbol, Integer>builder()
                    .putAll(probeSource.getLayout())
                    .put(node.getSemiJoinOutput(), probeSource.getLayout().size())
                    .build();

            HashSemiJoinOperatorFactory operator = new HashSemiJoinOperatorFactory(context.getNextOperatorId(), setProvider, probeSource.getTypes(), probeChannel);
            return new PhysicalOperation(operator, outputMappings, probeSource);
        }

        @Override
        public PhysicalOperation visitSink(SinkNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            // are the symbols of the source in the same order as the sink expects?
            boolean projectionMatchesOutput = IterableTransformer.on(source.getLayout().entrySet())
                    .orderBy(Ordering.<Integer>natural().onResultOf(MoreFunctions.<Symbol, Integer>valueGetter()))
                    .transform(MoreFunctions.<Symbol, Integer>keyGetter())
                    .list()
                    .equals(node.getOutputSymbols());

            if (!projectionMatchesOutput) {
                IdentityProjectionInfo mappings = computeIdentityMapping(node.getOutputSymbols(), source.getLayout(), context.getTypes());
                OperatorFactory operatorFactory = new FilterAndProjectOperator.FilterAndProjectOperatorFactory(
                        context.getNextOperatorId(),
                        new GenericPageProcessor(FilterFunctions.TRUE_FUNCTION, mappings.getProjections()),
                        toTypes(mappings.getProjections()));
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
            PhysicalOperation exchange = createInMemoryExchange(node.getSource(), context);

            Optional<Integer> sampleWeightChannel = node.getSampleWeightSymbol().transform(exchange.channelGetter());

            // create the table writer
            RecordSink recordSink = getRecordSink(node);

            List<Type> types = IterableTransformer.on(node.getColumns())
                    .transform(forMap(context.getTypes()))
                    .list();

            List<Integer> inputChannels = IterableTransformer.on(node.getColumns())
                    .transform(exchange.channelGetter())
                    .list();

            OperatorFactory operatorFactory = new TableWriterOperatorFactory(context.getNextOperatorId(), recordSink, types, inputChannels, sampleWeightChannel);

            Map<Symbol, Integer> layout = ImmutableMap.<Symbol, Integer>builder()
                    .put(node.getOutputSymbols().get(0), 0)
                    .put(node.getOutputSymbols().get(1), 1)
                    .build();

            return new PhysicalOperation(operatorFactory, layout, exchange);
        }

        private PhysicalOperation createInMemoryExchange(PlanNode node, LocalExecutionPlanContext context)
        {
            LocalExecutionPlanContext subContext = context.createSubContext();
            PhysicalOperation source = node.accept(this, subContext);

            InMemoryExchange exchange = new InMemoryExchange(getSourceOperatorTypes(node, context.getTypes()));

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
            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            for (int i = 0; i < layout.size(); i++) {
                outputMappings.put(layout.get(i), i);
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
            Map<Symbol, Integer> layout = ImmutableMap.of(node.getOutputSymbols().get(0), 0);

            return new PhysicalOperation(operatorFactory, layout, source);
        }

        @Override
        public PhysicalOperation visitUnion(UnionNode node, LocalExecutionPlanContext context)
        {
            List<Type> types = getSourceOperatorTypes(node, context.getTypes());
            InMemoryExchange inMemoryExchange = new InMemoryExchange(types);

            for (int i = 0; i < node.getSources().size(); i++) {
                PlanNode subplan = node.getSources().get(i);
                List<Symbol> expectedLayout = node.sourceOutputLayout(i);

                LocalExecutionPlanContext subContext = context.createSubContext();
                PhysicalOperation source = subplan.accept(this, subContext);
                List<OperatorFactory> operatorFactories = new ArrayList<>(source.getOperatorFactories());

                boolean projectionMatchesOutput = IterableTransformer.on(source.getLayout().entrySet())
                        .orderBy(Ordering.<Integer>natural().onResultOf(MoreFunctions.<Symbol, Integer>valueGetter()))
                        .transform(MoreFunctions.<Symbol, Integer>keyGetter())
                        .list()
                        .equals(expectedLayout);

                if (!projectionMatchesOutput) {
                    IdentityProjectionInfo mappings = computeIdentityMapping(expectedLayout, source.getLayout(), context.getTypes());
                    operatorFactories.add(new FilterAndProjectOperator.FilterAndProjectOperatorFactory(
                            subContext.getNextOperatorId(),
                            new GenericPageProcessor(FilterFunctions.TRUE_FUNCTION, mappings.getProjections()),
                            toTypes(mappings.getProjections())));
                }

                operatorFactories.add(inMemoryExchange.createSinkFactory(subContext.getNextOperatorId()));

                DriverFactory driverFactory = new DriverFactory(subContext.isInputDriver(), false, operatorFactories);
                context.addDriverFactory(driverFactory);
            }
            inMemoryExchange.noMoreSinkFactories();

            // the main driver is not an input... the union sources are the input for the plan
            context.setInputDriver(false);

            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            int channel = 0;
            for (Symbol symbol : node.getOutputSymbols()) {
                outputMappings.put(symbol, channel); // one symbol per channel
                channel++;
            }

            return new PhysicalOperation(new InMemoryExchangeSourceOperatorFactory(context.getNextOperatorId(), inMemoryExchange), outputMappings.build());
        }

        @Override
        protected PhysicalOperation visitPlan(PlanNode node, LocalExecutionPlanContext context)
        {
            throw new UnsupportedOperationException("not yet implemented");
        }

        private List<Type> getSourceOperatorTypes(PlanNode node, Map<Symbol, Type> types)
        {
            return getSymbolTypes(node.getOutputSymbols(), types);
        }

        private List<Type> getSymbolTypes(List<Symbol> symbols, Map<Symbol, Type> types)
        {
            return ImmutableList.copyOf(IterableTransformer.on(symbols)
                    .transform(forMap(types))
                    .list());
        }

        private AccumulatorFactory buildAccumulatorFactory(PhysicalOperation source, Signature function, FunctionCall call, @Nullable Symbol mask, Optional<Symbol> sampleWeight, double confidence)
        {
            List<Integer> arguments = new ArrayList<>();
            for (Expression argument : call.getArguments()) {
                Symbol argumentSymbol = Symbol.fromQualifiedName(((QualifiedNameReference) argument).getName());
                arguments.add(source.getLayout().get(argumentSymbol));
            }

            Optional<Integer> maskChannel = Optional.absent();

            if (mask != null) {
                maskChannel = Optional.of(source.getLayout().get(mask));
            }

            Optional<Integer> sampleWeightChannel = Optional.absent();
            if (sampleWeight.isPresent()) {
                sampleWeightChannel = Optional.of(source.getLayout().get(sampleWeight.get()));
            }

            return metadata.getExactFunction(function).getAggregationFunction().bind(arguments, maskChannel, sampleWeightChannel, confidence);
        }

        private PhysicalOperation planGlobalAggregation(int operatorId, AggregationNode node, PhysicalOperation source)
        {
            int outputChannel = 0;
            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            List<AccumulatorFactory> accumulatorFactories = new ArrayList<>();
            for (Map.Entry<Symbol, FunctionCall> entry : node.getAggregations().entrySet()) {
                Symbol symbol = entry.getKey();

                accumulatorFactories.add(buildAccumulatorFactory(source, node.getFunctions().get(symbol), entry.getValue(), node.getMasks().get(entry.getKey()), node.getSampleWeight(), node.getConfidence()));
                outputMappings.put(symbol, outputChannel); // one aggregation per channel
                outputChannel++;
            }

            OperatorFactory operatorFactory = new AggregationOperatorFactory(operatorId, node.getStep(), accumulatorFactories);
            return new PhysicalOperation(operatorFactory, outputMappings.build(), source);
        }

        private PhysicalOperation planGroupByAggregation(AggregationNode node, final PhysicalOperation source, LocalExecutionPlanContext context)
        {
            List<Symbol> groupBySymbols = node.getGroupBy();

            List<Symbol> aggregationOutputSymbols = new ArrayList<>();
            List<AccumulatorFactory> accumulatorFactories = new ArrayList<>();
            for (Map.Entry<Symbol, FunctionCall> entry : node.getAggregations().entrySet()) {
                Symbol symbol = entry.getKey();

                accumulatorFactories.add(buildAccumulatorFactory(source, node.getFunctions().get(symbol), entry.getValue(), node.getMasks().get(entry.getKey()), node.getSampleWeight(), node.getConfidence()));
                aggregationOutputSymbols.add(symbol);
            }

            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            // add group-by key fields each in a separate channel
            int channel = 0;
            for (Symbol symbol : groupBySymbols) {
                outputMappings.put(symbol, channel);
                channel++;
            }

            // aggregations go in following channels
            for (Symbol symbol : aggregationOutputSymbols) {
                outputMappings.put(symbol, channel);
                channel++;
            }

            List<Integer> groupByChannels = ImmutableList.copyOf(getChannelsForSymbols(groupBySymbols, source.getLayout()));
            List<Type> groupByTypes = ImmutableList.copyOf(Iterables.transform(groupByChannels, new Function<Integer, Type>()
            {
                @Override
                public Type apply(Integer input)
                {
                    return source.getTypes().get(input);
                }
            }));

            OperatorFactory operatorFactory = new HashAggregationOperatorFactory(
                    context.getNextOperatorId(),
                    groupByTypes,
                    groupByChannels,
                    node.getStep(),
                    accumulatorFactories,
                    10_000);

            return new PhysicalOperation(operatorFactory, outputMappings.build(), source);
        }
    }

    private RecordSink getRecordSink(TableWriterNode node)
    {
        WriterTarget target = node.getTarget();
        if (target instanceof CreateHandle) {
            return recordSinkManager.getRecordSink(((CreateHandle) target).getHandle());
        }
        if (target instanceof InsertHandle) {
            return recordSinkManager.getRecordSink(((InsertHandle) target).getHandle());
        }
        throw new AssertionError("Unhandled target type: " + target.getClass().getName());
    }

    public static List<Type> toTypes(List<ProjectionFunction> projections)
    {
        ImmutableList.Builder<Type> builder = ImmutableList.builder();
        for (ProjectionFunction projection : projections) {
            builder.add(projection.getType());
        }
        return builder.build();
    }

    private static TableCommitter createTableCommitter(final TableCommitNode node, final Metadata metadata)
    {
        final WriterTarget target = node.getTarget();
        return new TableCommitter()
        {
            @Override
            public void commitTable(Collection<String> fragments)
            {
                if (target instanceof CreateHandle) {
                    metadata.commitCreateTable(((CreateHandle) target).getHandle(), fragments);
                }
                else if (target instanceof InsertHandle) {
                    metadata.commitInsert(((InsertHandle) target).getHandle(), fragments);
                }
                else {
                    throw new AssertionError("Unhandled target type: " + target.getClass().getName());
                }
            }
        };
    }

    private static IdentityProjectionInfo computeIdentityMapping(List<Symbol> symbols, Map<Symbol, Integer> inputLayout, Map<Symbol, Type> types)
    {
        Map<Symbol, Integer> outputMappings = new HashMap<>();
        List<ProjectionFunction> projections = new ArrayList<>();

        int channel = 0;
        for (Symbol symbol : symbols) {
            ProjectionFunction function = ProjectionFunctions.singleColumn(types.get(symbol), inputLayout.get(symbol));
            projections.add(function);
            if (!outputMappings.containsKey(symbol)) {
                outputMappings.put(symbol, channel);
                channel++;
            }
        }

        return new IdentityProjectionInfo(ImmutableMap.copyOf(outputMappings), projections);
    }

    private static List<Integer> getChannelsForSymbols(List<Symbol> symbols, Map<Symbol, Integer> layout)
    {
        ImmutableList.Builder<Integer> builder = ImmutableList.builder();
        for (Symbol symbol : symbols) {
            builder.add(layout.get(symbol));
        }
        return builder.build();
    }

    private static class IdentityProjectionInfo
    {
        private final Map<Symbol, Integer> layout;
        private final List<ProjectionFunction> projections;

        public IdentityProjectionInfo(Map<Symbol, Integer> outputLayout, List<ProjectionFunction> projections)
        {
            this.layout = checkNotNull(outputLayout, "outputLayout is null");
            this.projections = checkNotNull(projections, "projections is null");
        }

        public Map<Symbol, Integer> getOutputLayout()
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
        private final Map<Symbol, Integer> layout;
        private final List<Type> types;

        public PhysicalOperation(OperatorFactory operatorFactory, Map<Symbol, Integer> layout)
        {
            checkNotNull(operatorFactory, "operatorFactory is null");
            checkNotNull(layout, "layout is null");

            this.operatorFactories = ImmutableList.of(operatorFactory);
            this.layout = ImmutableMap.copyOf(layout);
            this.types = operatorFactory.getTypes();
        }

        public PhysicalOperation(OperatorFactory operatorFactory, Map<Symbol, Integer> layout, PhysicalOperation source)
        {
            checkNotNull(operatorFactory, "operatorFactory is null");
            checkNotNull(layout, "layout is null");
            checkNotNull(source, "source is null");

            this.operatorFactories = ImmutableList.<OperatorFactory>builder().addAll(source.getOperatorFactories()).add(operatorFactory).build();
            this.layout = ImmutableMap.copyOf(layout);
            this.types = operatorFactory.getTypes();
        }

        public Function<Symbol, Integer> channelGetter()
        {
            return new Function<Symbol, Integer>() {
                @NotNull
                @Override
                public Integer apply(Symbol input)
                {
                    checkArgument(layout.containsKey(input));
                    return layout.get(input);
                }
            };
        }

        public List<Type> getTypes()
        {
            return types;
        }

        public Map<Symbol, Integer> getLayout()
        {
            return layout;
        }

        private List<OperatorFactory> getOperatorFactories()
        {
            return operatorFactories;
        }
    }
}
