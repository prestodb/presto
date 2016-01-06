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
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.execution.QueryPerformanceFetcher;
import com.facebook.presto.execution.SharedBuffer;
import com.facebook.presto.execution.TaskManagerConfig;
import com.facebook.presto.index.IndexManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.operator.AggregationOperator.AggregationOperatorFactory;
import com.facebook.presto.operator.CursorProcessor;
import com.facebook.presto.operator.DeleteOperator.DeleteOperatorFactory;
import com.facebook.presto.operator.DriverFactory;
import com.facebook.presto.operator.EnforceSingleRowOperator;
import com.facebook.presto.operator.ExchangeClientSupplier;
import com.facebook.presto.operator.ExchangeOperator.ExchangeOperatorFactory;
import com.facebook.presto.operator.ExplainAnalyzeOperator.ExplainAnalyzeOperatorFactory;
import com.facebook.presto.operator.FilterAndProjectOperator;
import com.facebook.presto.operator.FilterFunction;
import com.facebook.presto.operator.FilterFunctions;
import com.facebook.presto.operator.GenericCursorProcessor;
import com.facebook.presto.operator.GenericPageProcessor;
import com.facebook.presto.operator.GroupIdOperator;
import com.facebook.presto.operator.HashAggregationOperator.HashAggregationOperatorFactory;
import com.facebook.presto.operator.HashBuilderOperator.HashBuilderOperatorFactory;
import com.facebook.presto.operator.HashPartitionMaskOperator.HashPartitionMaskOperatorFactory;
import com.facebook.presto.operator.HashSemiJoinOperator.HashSemiJoinOperatorFactory;
import com.facebook.presto.operator.InMemoryExchange;
import com.facebook.presto.operator.JoinOperatorFactory;
import com.facebook.presto.operator.LimitOperator.LimitOperatorFactory;
import com.facebook.presto.operator.LocalPlannerAware;
import com.facebook.presto.operator.LookupJoinOperators;
import com.facebook.presto.operator.LookupOuterOperator.OuterLookupSourceSupplier;
import com.facebook.presto.operator.LookupSourceSupplier;
import com.facebook.presto.operator.MarkDistinctOperator.MarkDistinctOperatorFactory;
import com.facebook.presto.operator.MetadataDeleteOperator.MetadataDeleteOperatorFactory;
import com.facebook.presto.operator.NestedLoopJoinPagesSupplier;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.operator.OrderByOperator.OrderByOperatorFactory;
import com.facebook.presto.operator.OutputFactory;
import com.facebook.presto.operator.PageProcessor;
import com.facebook.presto.operator.ParallelHashBuilder;
import com.facebook.presto.operator.PartitionFunction;
import com.facebook.presto.operator.PartitionedOutputOperator.PartitionedOutputFactory;
import com.facebook.presto.operator.ProjectionFunction;
import com.facebook.presto.operator.ProjectionFunctions;
import com.facebook.presto.operator.RowNumberOperator;
import com.facebook.presto.operator.SampleOperator.SampleOperatorFactory;
import com.facebook.presto.operator.ScanFilterAndProjectOperator;
import com.facebook.presto.operator.SetBuilderOperator.SetBuilderOperatorFactory;
import com.facebook.presto.operator.SetBuilderOperator.SetSupplier;
import com.facebook.presto.operator.SourceOperatorFactory;
import com.facebook.presto.operator.TableScanOperator.TableScanOperatorFactory;
import com.facebook.presto.operator.TaskOutputOperator.TaskOutputFactory;
import com.facebook.presto.operator.TopNOperator.TopNOperatorFactory;
import com.facebook.presto.operator.TopNRowNumberOperator;
import com.facebook.presto.operator.ValuesOperator.ValuesOperatorFactory;
import com.facebook.presto.operator.WindowFunctionDefinition;
import com.facebook.presto.operator.WindowOperator.WindowOperatorFactory;
import com.facebook.presto.operator.aggregation.AccumulatorFactory;
import com.facebook.presto.operator.index.DynamicTupleFilterFactory;
import com.facebook.presto.operator.index.FieldSetFilteringRecordSet;
import com.facebook.presto.operator.index.IndexBuildDriverFactoryProvider;
import com.facebook.presto.operator.index.IndexJoinLookupStats;
import com.facebook.presto.operator.index.IndexLookupSourceSupplier;
import com.facebook.presto.operator.index.IndexSourceOperator;
import com.facebook.presto.operator.window.FrameInfo;
import com.facebook.presto.operator.window.WindowFunctionSupplier;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorIndex;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.predicate.NullableValue;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.split.MappedRecordSet;
import com.facebook.presto.split.PageSinkManager;
import com.facebook.presto.split.PageSourceProvider;
import com.facebook.presto.sql.gen.ExpressionCompiler;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.PartitionFunctionBinding.PartitionFunctionArgumentBinding;
import com.facebook.presto.sql.planner.optimizations.IndexJoinOptimizer;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.AggregationNode.Step;
import com.facebook.presto.sql.planner.plan.DeleteNode;
import com.facebook.presto.sql.planner.plan.DistinctLimitNode;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.ExplainAnalyzeNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.GroupIdNode;
import com.facebook.presto.sql.planner.plan.IndexJoinNode;
import com.facebook.presto.sql.planner.plan.IndexSourceNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.MarkDistinctNode;
import com.facebook.presto.sql.planner.plan.MetadataDeleteNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.facebook.presto.sql.planner.plan.RowNumberNode;
import com.facebook.presto.sql.planner.plan.SampleNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.TableFinishNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.TableWriterNode;
import com.facebook.presto.sql.planner.plan.TableWriterNode.DeleteHandle;
import com.facebook.presto.sql.planner.plan.TopNNode;
import com.facebook.presto.sql.planner.plan.TopNRowNumberNode;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.facebook.presto.sql.planner.plan.UnnestNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.planner.plan.WindowNode.Frame;
import com.facebook.presto.sql.relational.ConstantExpression;
import com.facebook.presto.sql.relational.InputReferenceExpression;
import com.facebook.presto.sql.relational.RowExpression;
import com.facebook.presto.sql.relational.SqlToRowExpressionTranslator;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedNameReference;
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
import com.google.common.primitives.Ints;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.facebook.presto.SystemSessionProperties.getTaskAggregationConcurrency;
import static com.facebook.presto.SystemSessionProperties.getTaskHashBuildConcurrency;
import static com.facebook.presto.SystemSessionProperties.getTaskJoinConcurrency;
import static com.facebook.presto.SystemSessionProperties.getTaskWriterCount;
import static com.facebook.presto.metadata.FunctionKind.SCALAR;
import static com.facebook.presto.operator.DistinctLimitOperator.DistinctLimitOperatorFactory;
import static com.facebook.presto.operator.InMemoryExchangeSourceOperator.InMemoryExchangeSourceOperatorFactory.createBroadcastDistribution;
import static com.facebook.presto.operator.InMemoryExchangeSourceOperator.InMemoryExchangeSourceOperatorFactory.createRandomDistribution;
import static com.facebook.presto.operator.NestedLoopBuildOperator.NestedLoopBuildOperatorFactory;
import static com.facebook.presto.operator.NestedLoopJoinOperator.NestedLoopJoinOperatorFactory;
import static com.facebook.presto.operator.TableFinishOperator.TableFinishOperatorFactory;
import static com.facebook.presto.operator.TableFinishOperator.TableFinisher;
import static com.facebook.presto.operator.TableWriterOperator.TableWriterOperatorFactory;
import static com.facebook.presto.operator.UnnestOperator.UnnestOperatorFactory;
import static com.facebook.presto.operator.WindowFunctionDefinition.window;
import static com.facebook.presto.spi.StandardErrorCode.COMPILER_ERROR;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.TypeUtils.writeNativeValue;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypes;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypesFromInput;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.COORDINATOR_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_BROADCAST_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.FULL;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.RIGHT;
import static com.facebook.presto.sql.planner.plan.TableWriterNode.CreateHandle;
import static com.facebook.presto.sql.planner.plan.TableWriterNode.InsertHandle;
import static com.facebook.presto.sql.planner.plan.TableWriterNode.WriterTarget;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableSet;
import static com.google.common.base.Functions.forMap;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.Boolean.TRUE;
import static java.lang.String.format;
import static java.util.Collections.singleton;
import static java.util.Objects.requireNonNull;

public class LocalExecutionPlanner
{
    private static final Logger log = Logger.get(LocalExecutionPlanner.class);

    private final Metadata metadata;
    private final SqlParser sqlParser;

    private final Optional<QueryPerformanceFetcher> queryPerformanceFetcher;
    private final PageSourceProvider pageSourceProvider;
    private final IndexManager indexManager;
    private final NodePartitioningManager nodePartitioningManager;
    private final PageSinkManager pageSinkManager;
    private final ExchangeClientSupplier exchangeClientSupplier;
    private final ExpressionCompiler compiler;
    private final boolean interpreterEnabled;
    private final DataSize maxIndexMemorySize;
    private final IndexJoinLookupStats indexJoinLookupStats;
    private final DataSize maxPartialAggregationMemorySize;

    @Inject
    public LocalExecutionPlanner(
            Metadata metadata,
            SqlParser sqlParser,
            Optional<QueryPerformanceFetcher> queryPerformanceFetcher,
            PageSourceProvider pageSourceProvider,
            IndexManager indexManager,
            NodePartitioningManager nodePartitioningManager,
            PageSinkManager pageSinkManager,
            ExchangeClientSupplier exchangeClientSupplier,
            ExpressionCompiler compiler,
            IndexJoinLookupStats indexJoinLookupStats,
            CompilerConfig compilerConfig,
            TaskManagerConfig taskManagerConfig)
    {
        requireNonNull(compilerConfig, "compilerConfig is null");
        this.queryPerformanceFetcher = requireNonNull(queryPerformanceFetcher, "queryPerformanceFetcher is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        this.indexManager = requireNonNull(indexManager, "indexManager is null");
        this.nodePartitioningManager = requireNonNull(nodePartitioningManager, "nodePartitioningManager is null");
        this.exchangeClientSupplier = exchangeClientSupplier;
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.pageSinkManager = requireNonNull(pageSinkManager, "pageSinkManager is null");
        this.compiler = requireNonNull(compiler, "compiler is null");
        this.indexJoinLookupStats = requireNonNull(indexJoinLookupStats, "indexJoinLookupStats is null");
        this.maxIndexMemorySize = requireNonNull(taskManagerConfig, "taskManagerConfig is null").getMaxIndexMemoryUsage();
        this.maxPartialAggregationMemorySize = taskManagerConfig.getMaxPartialAggregationMemoryUsage();

        interpreterEnabled = compilerConfig.isInterpreterEnabled();
    }

    public LocalExecutionPlan plan(
            Session session,
            PlanNode plan,
            Map<Symbol, Type> types,
            PartitionFunctionBinding functionBinding,
            SharedBuffer sharedBuffer,
            boolean singleNode,
            boolean allowLocalParallel)
    {
        List<Symbol> outputLayout = functionBinding.getOutputLayout();
        if (functionBinding.getPartitioningHandle().equals(FIXED_BROADCAST_DISTRIBUTION) ||
                functionBinding.getPartitioningHandle().equals(SINGLE_DISTRIBUTION) ||
                functionBinding.getPartitioningHandle().equals(COORDINATOR_DISTRIBUTION)) {
            return plan(session, plan, outputLayout, types, new TaskOutputFactory(sharedBuffer), singleNode, allowLocalParallel);
        }

        // We can convert the symbols directly into channels, because the root must be a sink and therefore the layout is fixed
        List<Integer> partitionChannels;
        List<Optional<NullableValue>> partitionConstants;
        List<Type> partitionChannelTypes;
        if (functionBinding.getHashColumn().isPresent()) {
            partitionChannels = ImmutableList.of(outputLayout.indexOf(functionBinding.getHashColumn().get()));
            partitionConstants = ImmutableList.of(Optional.empty());
            partitionChannelTypes = ImmutableList.of(BIGINT);
        }
        else {
            partitionChannels = functionBinding.getPartitionFunctionArguments().stream()
                    .map(PartitionFunctionArgumentBinding::getColumn)
                    .map(outputLayout::indexOf)
                    .collect(toImmutableList());
            partitionConstants = functionBinding.getPartitionFunctionArguments().stream()
                    .map(argument -> {
                        if (argument.isConstant()) {
                            return Optional.of(argument.getConstant());
                        }
                        return Optional.<NullableValue>empty();
                    })
                    .collect(toImmutableList());
            partitionChannelTypes = functionBinding.getPartitionFunctionArguments().stream()
                    .map(argument -> {
                        if (argument.isConstant()) {
                            return argument.getConstant().getType();
                        }
                        return types.get(argument.getColumn());
                    })
                    .collect(toImmutableList());
        }

        PartitionFunction partitionFunction = nodePartitioningManager.getPartitionFunction(session, functionBinding, partitionChannelTypes);
        OptionalInt nullChannel = OptionalInt.empty();
        if (functionBinding.isReplicateNulls()) {
            checkArgument(functionBinding.getPartitionFunctionArguments().size() == 1);
            checkArgument(functionBinding.getPartitionFunctionArguments().get(0).isVariable());
            nullChannel = OptionalInt.of(outputLayout.indexOf(getOnlyElement(functionBinding.getPartitionFunctionArguments()).getColumn()));
        }

        return plan(
                session,
                plan,
                outputLayout,
                types,
                new PartitionedOutputFactory(partitionFunction, partitionChannels, partitionConstants, nullChannel, sharedBuffer),
                singleNode,
                allowLocalParallel);
    }

    public LocalExecutionPlan plan(Session session,
            PlanNode plan,
            List<Symbol> outputLayout,
            Map<Symbol, Type> types,
            OutputFactory outputOperatorFactory,
            boolean singleNode,
            boolean allowLocalParallel)
    {
        LocalExecutionPlanContext context = new LocalExecutionPlanContext(session, types, singleNode, allowLocalParallel);

        PhysicalOperation physicalOperation = enforceLayout(plan.getId(), outputLayout, context, plan.accept(new Visitor(session), context));

        DriverFactory driverFactory = new DriverFactory(
                context.isInputDriver(),
                true,
                ImmutableList.<OperatorFactory>builder()
                        .addAll(physicalOperation.getOperatorFactories())
                        .add(outputOperatorFactory.createOutputOperator(context.getNextOperatorId(), plan.getId(), physicalOperation.getTypes()))
                        .build(),
                context.getDriverInstanceCount());
        context.addDriverFactory(driverFactory);

        addLookupOuterDrivers(context);

        // notify operator factories that planning has completed
        context.getDriverFactories().stream()
                .map(DriverFactory::getOperatorFactories)
                .flatMap(List::stream)
                .filter(LocalPlannerAware.class::isInstance)
                .map(LocalPlannerAware.class::cast)
                .forEach(LocalPlannerAware::localPlannerComplete);

        return new LocalExecutionPlan(context.getDriverFactories());
    }

    private static void addLookupOuterDrivers(LocalExecutionPlanContext context)
    {
        // For an outer join on the lookup side (RIGHT or FULL) add an additional
        // driver to output the unused rows in the lookup source
        for (DriverFactory factory : context.getDriverFactories()) {
            List<OperatorFactory> operatorFactories = factory.getOperatorFactories();
            for (int i = 0; i < operatorFactories.size(); i++) {
                OperatorFactory operatorFactory = operatorFactories.get(i);
                if (!(operatorFactory instanceof JoinOperatorFactory)) {
                    continue;
                }

                JoinOperatorFactory lookupJoin = (JoinOperatorFactory) operatorFactory;
                Optional<OperatorFactory> outerOperatorFactory = lookupJoin.createOuterOperatorFactory();
                if (outerOperatorFactory.isPresent()) {
                    // Add a new driver to output the unmatched rows in an outer join.
                    // We duplicate all of the factories above the JoinOperator (the ones reading from the joins),
                    // and replace the JoinOperator with the OuterOperator (the one that produces unmatched rows).
                    ImmutableList.Builder<OperatorFactory> newOperators = ImmutableList.builder();
                    newOperators.add(outerOperatorFactory.get());
                    operatorFactories.subList(i + 1, operatorFactories.size()).stream()
                            .map(OperatorFactory::duplicate)
                            .forEach(newOperators::add);

                    context.addDriverFactory(new DriverFactory(false, factory.isOutputDriver(), newOperators.build()));
                }
            }
        }
    }

    private PhysicalOperation enforceLayout(PlanNodeId planNodeId, List<Symbol> outputLayout, LocalExecutionPlanContext context, PhysicalOperation physicalOperation)
    {
        // are the symbols of the source in the same order as the sink expects?
        boolean projectionMatchesOutput = physicalOperation.getLayout()
                .entrySet().stream()
                .sorted(Ordering.<Integer>natural().onResultOf(Map.Entry::getValue))
                .map(Map.Entry::getKey)
                .collect(toImmutableList())
                .equals(outputLayout);

        if (!projectionMatchesOutput) {
            IdentityProjectionInfo projectionInfo = computeIdentityProjectionInfo(outputLayout, physicalOperation.getLayout(), context.getTypes());
            List<RowExpression> projections = projectionInfo.getProjections();

            OperatorFactory operatorFactory = new FilterAndProjectOperator.FilterAndProjectOperatorFactory(
                    context.getNextOperatorId(),
                    planNodeId,
                    compiler.compilePageProcessor(trueExpression(), projections),
                    projections.stream()
                            .map(RowExpression::getType)
                            .collect(toImmutableList()));
            // NOTE: the generated output layout may not be completely accurate if the same field was projected as multiple inputs.
            // However, this should not affect the operation of the sink.
            physicalOperation = new PhysicalOperation(operatorFactory, projectionInfo.getOutputLayout(), physicalOperation);
        }

        return physicalOperation;
    }

    private static RowExpression trueExpression()
    {
        return new ConstantExpression(TRUE, BOOLEAN);
    }

    private static class LocalExecutionPlanContext
    {
        private final Session session;
        private final Map<Symbol, Type> types;
        private final boolean singleNode;
        private final boolean allowLocalParallel;
        private final List<DriverFactory> driverFactories;
        private final Optional<IndexSourceContext> indexSourceContext;

        private int nextOperatorId;
        private boolean inputDriver = true;
        private int driverInstanceCount = 1;

        public LocalExecutionPlanContext(Session session, Map<Symbol, Type> types, boolean singleNode, boolean allowLocalParallel)
        {
            this(session, types, singleNode, allowLocalParallel, new ArrayList<>(), Optional.empty());
        }

        private LocalExecutionPlanContext(
                Session session,
                Map<Symbol, Type> types,
                boolean singleNode,
                boolean allowLocalParallel,
                List<DriverFactory> driverFactories,
                Optional<IndexSourceContext> indexSourceContext)
        {
            this.session = session;
            this.types = types;
            this.singleNode = singleNode;
            this.allowLocalParallel = allowLocalParallel;
            this.driverFactories = driverFactories;
            this.indexSourceContext = indexSourceContext;
        }

        public void addDriverFactory(DriverFactory driverFactory)
        {
            driverFactories.add(requireNonNull(driverFactory, "driverFactory is null"));
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
            return new LocalExecutionPlanContext(session, types, singleNode, allowLocalParallel, driverFactories, indexSourceContext);
        }

        public LocalExecutionPlanContext createIndexSourceSubContext(IndexSourceContext indexSourceContext)
        {
            return new LocalExecutionPlanContext(session, types, true, false, driverFactories, Optional.of(indexSourceContext));
        }

        public boolean isSingleNode()
        {
            return singleNode;
        }

        public boolean isAllowLocalParallel()
        {
            return allowLocalParallel;
        }

        public int getDriverInstanceCount()
        {
            return driverInstanceCount;
        }

        public void setDriverInstanceCount(int driverInstanceCount)
        {
            checkArgument(driverInstanceCount > 0, "driverInstanceCount must be > 0");
            this.driverInstanceCount = driverInstanceCount;
        }
    }

    private static class IndexSourceContext
    {
        private final SetMultimap<Symbol, Integer> indexLookupToProbeInput;

        public IndexSourceContext(SetMultimap<Symbol, Integer> indexLookupToProbeInput)
        {
            this.indexLookupToProbeInput = ImmutableSetMultimap.copyOf(requireNonNull(indexLookupToProbeInput, "indexLookupToProbeInput is null"));
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
            this.driverFactories = ImmutableList.copyOf(requireNonNull(driverFactories, "driverFactories is null"));
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
        public PhysicalOperation visitRemoteSource(RemoteSourceNode node, LocalExecutionPlanContext context)
        {
            List<Type> types = getSourceOperatorTypes(node, context.getTypes());

            OperatorFactory operatorFactory = new ExchangeOperatorFactory(context.getNextOperatorId(), node.getId(), exchangeClientSupplier, types);

            return new PhysicalOperation(operatorFactory, makeLayout(node));
        }

        @Override
        public PhysicalOperation visitExplainAnalyze(ExplainAnalyzeNode node, LocalExecutionPlanContext context)
        {
            checkState(queryPerformanceFetcher.isPresent(), "ExplainAnalyze can only run on coordinator");
            PhysicalOperation source = node.getSource().accept(this, context);

            OperatorFactory operatorFactory = new ExplainAnalyzeOperatorFactory(context.getNextOperatorId(), node.getId(), queryPerformanceFetcher.get(), metadata);
            return new PhysicalOperation(operatorFactory, makeLayout(node), source);
        }

        @Override
        public PhysicalOperation visitOutput(OutputNode node, LocalExecutionPlanContext context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public PhysicalOperation visitRowNumber(RowNumberNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            List<Symbol> partitionBySymbols = node.getPartitionBy();
            List<Integer> partitionChannels = getChannelsForSymbols(partitionBySymbols, source.getLayout());

            List<Type> partitionTypes = partitionChannels.stream()
                    .map(channel -> source.getTypes().get(channel))
                    .collect(toImmutableList());

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

            Optional<Integer> hashChannel = node.getHashSymbol().map(channelGetter(source));
            OperatorFactory operatorFactory = new RowNumberOperator.RowNumberOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    source.getTypes(),
                    outputChannels.build(),
                    partitionChannels,
                    partitionTypes,
                    node.getMaxRowCountPerPartition(),
                    hashChannel,
                    10_000);
            return new PhysicalOperation(operatorFactory, outputMappings.build(), source);
        }

        @Override
        public PhysicalOperation visitTopNRowNumber(TopNRowNumberNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            List<Symbol> partitionBySymbols = node.getPartitionBy();
            List<Integer> partitionChannels = getChannelsForSymbols(partitionBySymbols, source.getLayout());
            List<Type> partitionTypes = partitionChannels.stream()
                    .map(channel -> source.getTypes().get(channel))
                    .collect(toImmutableList());

            List<Symbol> orderBySymbols = node.getOrderBy();
            List<Integer> sortChannels = getChannelsForSymbols(orderBySymbols, source.getLayout());
            List<SortOrder> sortOrder = orderBySymbols.stream()
                    .map(symbol -> node.getOrderings().get(symbol))
                    .collect(toImmutableList());

            ImmutableList.Builder<Integer> outputChannels = ImmutableList.builder();
            for (int i = 0; i < source.getTypes().size(); i++) {
                outputChannels.add(i);
            }

            // compute the layout of the output from the window operator
            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            outputMappings.putAll(source.getLayout());

            if (!node.isPartial() || !partitionChannels.isEmpty()) {
                // row number function goes in the last channel
                int channel = source.getTypes().size();
                outputMappings.put(node.getRowNumberSymbol(), channel);
            }

            Optional<Integer> hashChannel = node.getHashSymbol().map(channelGetter(source));
            OperatorFactory operatorFactory = new TopNRowNumberOperator.TopNRowNumberOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    source.getTypes(),
                    outputChannels.build(),
                    partitionChannels,
                    partitionTypes,
                    sortChannels,
                    sortOrder,
                    node.getMaxRowCountPerPartition(),
                    node.isPartial(),
                    hashChannel,
                    1000);

            return new PhysicalOperation(operatorFactory, makeLayout(node), source);
        }

        @Override
        public PhysicalOperation visitWindow(WindowNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            List<Symbol> partitionBySymbols = node.getPartitionBy();
            List<Symbol> orderBySymbols = node.getOrderBy();
            List<Integer> partitionChannels = ImmutableList.copyOf(getChannelsForSymbols(partitionBySymbols, source.getLayout()));
            List<Integer> preGroupedChannels = ImmutableList.copyOf(getChannelsForSymbols(ImmutableList.copyOf(node.getPrePartitionedInputs()), source.getLayout()));

            List<Integer> sortChannels = getChannelsForSymbols(orderBySymbols, source.getLayout());
            List<SortOrder> sortOrder = orderBySymbols.stream()
                    .map(symbol -> node.getOrderings().get(symbol))
                    .collect(toImmutableList());

            Optional<Integer> frameStartChannel = Optional.empty();
            Optional<Integer> frameEndChannel = Optional.empty();
            Frame frame = node.getFrame();
            if (frame.getStartValue().isPresent()) {
                frameStartChannel = Optional.of(source.getLayout().get(frame.getStartValue().get()));
            }
            if (frame.getEndValue().isPresent()) {
                frameEndChannel = Optional.of(source.getLayout().get(frame.getEndValue().get()));
            }

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
                WindowFunctionSupplier windowFunctionSupplier = metadata.getFunctionRegistry().getWindowFunctionImplementation(signature);
                Type type = metadata.getType(signature.getReturnType());
                windowFunctionsBuilder.add(window(windowFunctionSupplier, type, arguments.build()));
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
                    node.getId(),
                    source.getTypes(),
                    outputChannels.build(),
                    windowFunctions,
                    partitionChannels,
                    preGroupedChannels,
                    sortChannels,
                    sortOrder,
                    node.getPreSortedOrderPrefix(),
                    new FrameInfo(frame.getType(), frame.getStartType(), frameStartChannel, frame.getEndType(), frameEndChannel),
                    10_000);

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
                    node.getId(),
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
                    node.getId(),
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

            OperatorFactory operatorFactory = new LimitOperatorFactory(context.getNextOperatorId(), node.getId(), source.getTypes(), node.getCount());
            return new PhysicalOperation(operatorFactory, source.getLayout(), source);
        }

        @Override
        public PhysicalOperation visitDistinctLimit(DistinctLimitNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            Optional<Integer> hashChannel = node.getHashSymbol().map(channelGetter(source));
            List<Integer> distinctChannels = getChannelsForSymbols(node.getDistinctSymbols(), source.getLayout());

            OperatorFactory operatorFactory = new DistinctLimitOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    source.getTypes(),
                    distinctChannels,
                    node.getLimit(),
                    hashChannel);
            return new PhysicalOperation(operatorFactory, source.getLayout(), source);
        }

        @Override
        public PhysicalOperation visitGroupId(GroupIdNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            // add groupId to the layout
            int groupIdChannel = source.getLayout().values().stream()
                    .mapToInt(Integer::intValue)
                    .max()
                    .orElse(-1) + 1;

            Map<Symbol, Integer> newLayout = ImmutableMap.<Symbol, Integer>builder()
                    .putAll(source.getLayout())
                    .put(node.getGroupIdSymbol(), groupIdChannel)
                    .build();

            List<List<Integer>> groupingSetChannels = node.getGroupingSets().stream()
                    .map(groupingSet -> getChannelsForSymbols(groupingSet, source.getLayout()))
                    .collect(toImmutableList());

            OperatorFactory groupIdOperatorFactory = new GroupIdOperator.GroupIdOperatorFactory(context.getNextOperatorId(), node.getId(), source.getTypes(), groupingSetChannels);
            return new PhysicalOperation(groupIdOperatorFactory, newLayout, source);
        }

        @Override
        public PhysicalOperation visitAggregation(AggregationNode node, LocalExecutionPlanContext context)
        {
            if (node.getGroupBy().isEmpty()) {
                PhysicalOperation source = node.getSource().accept(this, context);
                return planGlobalAggregation(context.getNextOperatorId(), node, source);
            }

            if (needsLocalGather(node)) {
                LocalExecutionPlanContext intermediateContext = context.createSubContext();
                intermediateContext.setInputDriver(context.isInputDriver());

                PhysicalOperation source = node.getSource().accept(this, intermediateContext);
                InMemoryExchange exchange = new InMemoryExchange(source.getTypes());
                List<OperatorFactory> factories = ImmutableList.<OperatorFactory>builder()
                        .addAll(source.getOperatorFactories())
                        .add(exchange.createSinkFactory(intermediateContext.getNextOperatorId(), node.getId()))
                        .build();
                context.addDriverFactory(new DriverFactory(intermediateContext.isInputDriver(), false, factories));

                OperatorFactory exchangeSource = createRandomDistribution(context.getNextOperatorId(), node.getId(), exchange);
                source = new PhysicalOperation(exchangeSource, source.getLayout());
                return planGroupByAggregation(node, source, context.getNextOperatorId(), Optional.empty());
            }

            int aggregationConcurrency = getTaskAggregationConcurrency(session);
            if (node.getStep() == Step.PARTIAL || !context.isAllowLocalParallel() || context.getDriverInstanceCount() > 1 || aggregationConcurrency <= 1) {
                PhysicalOperation source = node.getSource().accept(this, context);
                return planGroupByAggregation(node, source, context.getNextOperatorId(), Optional.empty());
            }

            // create context for parallel operators
            LocalExecutionPlanContext parallelContext = context.createSubContext();
            parallelContext.setDriverInstanceCount(aggregationConcurrency);

            // create context for source operators
            LocalExecutionPlanContext sourceContext = parallelContext.createSubContext();
            parallelContext.setInputDriver(false);

            // plan aggregation source
            PhysicalOperation source = node.getSource().accept(this, sourceContext);

            // add a broadcast exchange which copies every page into all parallel workers
            InMemoryExchange exchange = new InMemoryExchange(source.getTypes(), aggregationConcurrency);

            // finish source operator
            List<OperatorFactory> factories = ImmutableList.<OperatorFactory>builder()
                    .addAll(source.getOperatorFactories())
                    .add(exchange.createSinkFactory(sourceContext.getNextOperatorId(), node.getId()))
                    .build();
            parallelContext.addDriverFactory(new DriverFactory(sourceContext.isInputDriver(), false, factories));

            // add broadcast exchange as first parallel operator
            OperatorFactory exchangeSource = createBroadcastDistribution(parallelContext.getNextOperatorId(), node.getId(), exchange);
            source = new PhysicalOperation(exchangeSource, source.getLayout());

            // mask each parallel driver to only see one partition of groups
            HashPartitionMaskOperatorFactory hashPartitionMask = new HashPartitionMaskOperatorFactory(
                    parallelContext.getNextOperatorId(),
                    node.getId(),
                    aggregationConcurrency,
                    exchangeSource.getTypes(),
                    getChannelsForSymbols(ImmutableList.copyOf(node.getMasks().values()), source.getLayout()),
                    getChannelsForSymbols(ImmutableList.copyOf(node.getGroupBy()), source.getLayout()),
                    node.getHashSymbol().map(channelGetter(source)));
            int defaultMaskChannel = hashPartitionMask.getDefaultMaskChannel();
            source = new PhysicalOperation(hashPartitionMask, source.getLayout(), source);

            // plan aggregation
            PhysicalOperation operation = planGroupByAggregation(node, source, parallelContext.getNextOperatorId(), Optional.of(defaultMaskChannel));

            // merge parallel tasks back into a single stream
            operation = addInMemoryExchange(context, node.getId(), operation, parallelContext);

            return operation;
        }

        private boolean needsLocalGather(AggregationNode node)
        {
            // intermediate is used for node local aggregations
            if (node.getStep() == Step.INTERMEDIATE) {
                return true;
            }

            // a final aggregation with a partial as a source needs is used to signal a plan that is
            // distributed on the grouped keys but not partitioned on the group keys.
            if (node.getStep() != Step.FINAL || node.getSources().size() != 1) {
                return false;
            }
            PlanNode source = node.getSources().get(0);
            return source instanceof AggregationNode && ((AggregationNode) source).getStep() == Step.PARTIAL;
        }

        @Override
        public PhysicalOperation visitMarkDistinct(MarkDistinctNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            List<Integer> channels = getChannelsForSymbols(node.getDistinctSymbols(), source.getLayout());
            Optional<Integer> hashChannel = node.getHashSymbol().map(channelGetter(source));
            MarkDistinctOperatorFactory operator = new MarkDistinctOperatorFactory(context.getNextOperatorId(), node.getId(), source.getTypes(), channels, hashChannel);
            return new PhysicalOperation(operator, makeLayout(node), source);
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
                OperatorFactory operatorFactory = new SampleOperatorFactory(context.getNextOperatorId(), node.getId(), node.getSampleRatio(), node.isRescaled(), source.getTypes());
                checkState(node.getSampleWeightSymbol().isPresent(), "sample weight symbol missing");
                return new PhysicalOperation(operatorFactory, makeLayout(node), source);
            }

            throw new UnsupportedOperationException("not yet implemented: " + node);
        }

        @Override
        public PhysicalOperation visitFilter(FilterNode node, LocalExecutionPlanContext context)
        {
            PlanNode sourceNode = node.getSource();

            Expression filterExpression = node.getPredicate();
            List<Symbol> outputSymbols = node.getOutputSymbols();

            Map<Symbol, Expression> projectionExpressions = outputSymbols.stream()
                    .collect(Collectors.toMap(x -> x, Symbol::toQualifiedNameReference));

            return visitScanFilterAndProject(context, node.getId(), sourceNode, filterExpression, projectionExpressions, outputSymbols);
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

            List<Symbol> outputSymbols = node.getOutputSymbols();

            return visitScanFilterAndProject(context, node.getId(), sourceNode, filterExpression, node.getAssignments(), outputSymbols);
        }

        // TODO: This should be refactored, so that there's an optimizer that merges scan-filter-project into a single PlanNode
        private PhysicalOperation visitScanFilterAndProject(
                LocalExecutionPlanContext context,
                PlanNodeId planNodeId,
                PlanNode sourceNode,
                Expression filterExpression,
                Map<Symbol, Expression> projectionExpressions,
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

                    Type type = requireNonNull(context.getTypes().get(symbol), format("No type for symbol %s", symbol));
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

            // compiler uses inputs instead of symbols, so rewrite the expressions first
            SymbolToInputRewriter symbolToInputRewriter = new SymbolToInputRewriter(sourceLayout);
            Expression rewrittenFilter = ExpressionTreeRewriter.rewriteWith(symbolToInputRewriter, filterExpression);

            List<Expression> rewrittenProjections = new ArrayList<>();
            for (Symbol symbol : outputSymbols) {
                rewrittenProjections.add(ExpressionTreeRewriter.rewriteWith(symbolToInputRewriter, projectionExpressions.get(symbol)));
            }

            IdentityHashMap<Expression, Type> expressionTypes = getExpressionTypesFromInput(
                    context.getSession(),
                    metadata,
                    sqlParser,
                    sourceTypes,
                    concat(singleton(rewrittenFilter), rewrittenProjections));

            RowExpression translatedFilter = toRowExpression(rewrittenFilter, expressionTypes);
            List<RowExpression> translatedProjections = rewrittenProjections.stream()
                    .map(expression -> toRowExpression(expression, expressionTypes))
                    .collect(toImmutableList());

            try {
                if (columns != null) {
                    CursorProcessor cursorProcessor = compiler.compileCursorProcessor(translatedFilter, translatedProjections, sourceNode.getId());
                    Supplier<PageProcessor> pageProcessor = compiler.compilePageProcessor(translatedFilter, translatedProjections);

                    SourceOperatorFactory operatorFactory = new ScanFilterAndProjectOperator.ScanFilterAndProjectOperatorFactory(
                            context.getNextOperatorId(),
                            planNodeId,
                            sourceNode.getId(),
                            pageSourceProvider,
                            cursorProcessor,
                            pageProcessor,
                            columns,
                            Lists.transform(rewrittenProjections, forMap(expressionTypes)));

                    return new PhysicalOperation(operatorFactory, outputMappings);
                }
                else {
                    Supplier<PageProcessor> processor = compiler.compilePageProcessor(translatedFilter, translatedProjections);

                    OperatorFactory operatorFactory = new FilterAndProjectOperator.FilterAndProjectOperatorFactory(
                            context.getNextOperatorId(),
                            planNodeId,
                            processor,
                            Lists.transform(rewrittenProjections, forMap(expressionTypes)));

                    return new PhysicalOperation(operatorFactory, outputMappings, source);
                }
            }
            catch (RuntimeException e) {
                if (!interpreterEnabled) {
                    throw new PrestoException(COMPILER_ERROR, "Compiler failed and interpreter is disabled", e);
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
            for (Symbol symbol : outputSymbols) {
                Expression expression = projectionExpressions.get(symbol);
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
                        planNodeId,
                        sourceNode.getId(),
                        pageSourceProvider,
                        new GenericCursorProcessor(filterFunction, projectionFunctions),
                        () -> new GenericPageProcessor(filterFunction, projectionFunctions),
                        columns,
                        toTypes(projectionFunctions));

                return new PhysicalOperation(operatorFactory, outputMappings);
            }
            else {
                OperatorFactory operatorFactory = new FilterAndProjectOperator.FilterAndProjectOperatorFactory(
                        context.getNextOperatorId(),
                        planNodeId,
                        () -> new GenericPageProcessor(filterFunction, projectionFunctions),
                        toTypes(projectionFunctions));
                return new PhysicalOperation(operatorFactory, outputMappings, source);
            }
        }

        private RowExpression toRowExpression(Expression expression, IdentityHashMap<Expression, Type> types)
        {
            return SqlToRowExpressionTranslator.translate(expression, SCALAR, types, metadata.getFunctionRegistry(), metadata.getTypeManager(), session, true);
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
            List<ColumnHandle> columns = new ArrayList<>();
            for (Symbol symbol : node.getOutputSymbols()) {
                columns.add(node.getAssignments().get(symbol));
            }

            List<Type> types = getSourceOperatorTypes(node, context.getTypes());
            OperatorFactory operatorFactory = new TableScanOperatorFactory(context.getNextOperatorId(), node.getId(), pageSourceProvider, types, columns);
            return new PhysicalOperation(operatorFactory, makeLayout(node));
        }

        @Override
        public PhysicalOperation visitValues(ValuesNode node, LocalExecutionPlanContext context)
        {
            List<Type> outputTypes = new ArrayList<>();

            for (Symbol symbol : node.getOutputSymbols()) {
                Type type = requireNonNull(context.getTypes().get(symbol), format("No type for symbol %s", symbol));
                outputTypes.add(type);
            }

            if (node.getRows().isEmpty()) {
                OperatorFactory operatorFactory = new ValuesOperatorFactory(context.getNextOperatorId(), node.getId(), outputTypes, ImmutableList.of());
                return new PhysicalOperation(operatorFactory, makeLayout(node));
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
                    writeNativeValue(outputTypes.get(i), pageBuilder.getBlockBuilder(i), result);
                }
            }

            OperatorFactory operatorFactory = new ValuesOperatorFactory(context.getNextOperatorId(), node.getId(), outputTypes, ImmutableList.of(pageBuilder.build()));
            return new PhysicalOperation(operatorFactory, makeLayout(node));
        }

        @Override
        public PhysicalOperation visitUnnest(UnnestNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            ImmutableList.Builder<Type> replicateTypes = ImmutableList.builder();
            for (Symbol symbol : node.getReplicateSymbols()) {
                replicateTypes.add(context.getTypes().get(symbol));
            }
            List<Symbol> unnestSymbols = ImmutableList.copyOf(node.getUnnestSymbols().keySet());
            ImmutableList.Builder<Type> unnestTypes = ImmutableList.builder();
            for (Symbol symbol : unnestSymbols) {
                unnestTypes.add(context.getTypes().get(symbol));
            }
            Optional<Symbol> ordinalitySymbol = node.getOrdinalitySymbol();
            Optional<Type> ordinalityType = ordinalitySymbol.map(context.getTypes()::get);
            ordinalityType.ifPresent(type -> checkState(type.equals(BIGINT), "Type of ordinalitySymbol must always be BIGINT."));

            List<Integer> replicateChannels = getChannelsForSymbols(node.getReplicateSymbols(), source.getLayout());
            List<Integer> unnestChannels = getChannelsForSymbols(unnestSymbols, source.getLayout());

            // Source channels are always laid out first, followed by the unnested symbols
            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            int channel = 0;
            for (Symbol symbol : node.getReplicateSymbols()) {
                outputMappings.put(symbol, channel);
                channel++;
            }
            for (Symbol symbol : unnestSymbols) {
                for (Symbol unnestedSymbol : node.getUnnestSymbols().get(symbol)) {
                    outputMappings.put(unnestedSymbol, channel);
                    channel++;
                }
            }
            if (ordinalitySymbol.isPresent()) {
                outputMappings.put(ordinalitySymbol.get(), channel);
                channel++;
            }
            OperatorFactory operatorFactory = new UnnestOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    replicateChannels,
                    replicateTypes.build(),
                    unnestChannels,
                    unnestTypes.build(),
                    ordinalityType.isPresent());
            return new PhysicalOperation(operatorFactory, outputMappings.build(), source);
        }

        private ImmutableMap<Symbol, Integer> makeLayout(PlanNode node)
        {
            Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            int channel = 0;
            for (Symbol symbol : node.getOutputSymbols()) {
                outputMappings.put(symbol, channel);
                channel++;
            }
            return outputMappings.build();
        }

        @Override
        public PhysicalOperation visitIndexSource(IndexSourceNode node, LocalExecutionPlanContext context)
        {
            checkState(context.getIndexSourceContext().isPresent(), "Must be in an index source context");
            IndexSourceContext indexSourceContext = context.getIndexSourceContext().get();

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
                    overlappingFieldSetsBuilder.add(potentialProbeInputs.stream().collect(toImmutableSet()));
                }
                remappedProbeKeyChannelsBuilder.add(Iterables.getFirst(potentialProbeInputs, null));
            }
            List<Set<Integer>> overlappingFieldSets = overlappingFieldSetsBuilder.build();
            List<Integer> remappedProbeKeyChannels = remappedProbeKeyChannelsBuilder.build();
            Function<RecordSet, RecordSet> probeKeyNormalizer = recordSet -> {
                if (!overlappingFieldSets.isEmpty()) {
                    recordSet = new FieldSetFilteringRecordSet(metadata.getFunctionRegistry(), recordSet, overlappingFieldSets);
                }
                return new MappedRecordSet(recordSet, remappedProbeKeyChannels);
            };

            // Declare the input and output schemas for the index and acquire the actual Index
            List<ColumnHandle> lookupSchema = Lists.transform(lookupSymbolSchema, forMap(node.getAssignments()));
            List<ColumnHandle> outputSchema = Lists.transform(node.getOutputSymbols(), forMap(node.getAssignments()));
            ConnectorIndex index = indexManager.getIndex(session, node.getIndexHandle(), lookupSchema, outputSchema);

            List<Type> types = getSourceOperatorTypes(node, context.getTypes());
            OperatorFactory operatorFactory = new IndexSourceOperator.IndexSourceOperatorFactory(context.getNextOperatorId(), node.getId(), index, types, probeKeyNormalizer);
            return new PhysicalOperation(operatorFactory, makeLayout(node));
        }

        /**
         * This method creates a mapping from each index source lookup symbol (directly applied to the index)
         * to the corresponding probe key Input
         */
        private SetMultimap<Symbol, Integer> mapIndexSourceLookupSymbolToProbeKeyInput(IndexJoinNode node, Map<Symbol, Integer> probeKeyLayout)
        {
            Set<Symbol> indexJoinSymbols = node.getCriteria().stream()
                    .map(IndexJoinNode.EquiJoinClause::getIndex)
                    .collect(toImmutableSet());

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

            List<Symbol> probeSymbols = Lists.transform(clauses, IndexJoinNode.EquiJoinClause::getProbe);
            List<Symbol> indexSymbols = Lists.transform(clauses, IndexJoinNode.EquiJoinClause::getIndex);

            // Plan probe side
            PhysicalOperation probeSource = node.getProbeSource().accept(this, context);
            List<Integer> probeChannels = getChannelsForSymbols(probeSymbols, probeSource.getLayout());
            Optional<Integer> probeHashChannel = node.getProbeHashSymbol().map(channelGetter(probeSource));

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
            List<Integer> indexOutputChannels = getChannelsForSymbols(indexSymbols, indexSource.getLayout());
            Optional<Integer> indexHashChannel = node.getIndexHashSymbol().map(channelGetter(indexSource));

            // Identify just the join keys/channels needed for lookup by the index source (does not have to use all of them).
            Set<Symbol> indexSymbolsNeededBySource = IndexJoinOptimizer.IndexKeyTracer.trace(node.getIndexSource(), ImmutableSet.copyOf(indexSymbols)).keySet();

            Set<Integer> lookupSourceInputChannels = node.getCriteria().stream()
                    .filter(equiJoinClause -> indexSymbolsNeededBySource.contains(equiJoinClause.getIndex()))
                    .map(IndexJoinNode.EquiJoinClause::getProbe)
                    .map(probeKeyLayout::get)
                    .collect(toImmutableSet());

            Optional<DynamicTupleFilterFactory> dynamicTupleFilterFactory = Optional.empty();
            if (lookupSourceInputChannels.size() < probeKeyLayout.values().size()) {
                int[] nonLookupInputChannels = Ints.toArray(node.getCriteria().stream()
                        .filter(equiJoinClause -> !indexSymbolsNeededBySource.contains(equiJoinClause.getIndex()))
                        .map(IndexJoinNode.EquiJoinClause::getProbe)
                        .map(probeKeyLayout::get)
                        .collect(toImmutableList()));
                int[] nonLookupOutputChannels = Ints.toArray(node.getCriteria().stream()
                        .filter(equiJoinClause -> !indexSymbolsNeededBySource.contains(equiJoinClause.getIndex()))
                        .map(IndexJoinNode.EquiJoinClause::getIndex)
                        .map(indexSource.getLayout()::get)
                        .collect(toImmutableList()));

                int filterOperatorId = indexContext.getNextOperatorId();
                dynamicTupleFilterFactory = Optional.of(new DynamicTupleFilterFactory(filterOperatorId, node.getId(), nonLookupInputChannels, nonLookupOutputChannels, indexSource.getTypes()));
            }

            IndexBuildDriverFactoryProvider indexBuildDriverFactoryProvider = new IndexBuildDriverFactoryProvider(
                    indexContext.getNextOperatorId(),
                    node.getId(),
                    indexContext.isInputDriver(),
                    indexSource.getOperatorFactories(),
                    dynamicTupleFilterFactory);

            IndexLookupSourceSupplier indexLookupSourceSupplier = new IndexLookupSourceSupplier(
                    lookupSourceInputChannels,
                    indexOutputChannels,
                    indexHashChannel,
                    indexSource.getTypes(),
                    indexBuildDriverFactoryProvider,
                    maxIndexMemorySize,
                    indexJoinLookupStats,
                    SystemSessionProperties.isShareIndexLoading(session));

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
                    lookupJoinOperatorFactory = LookupJoinOperators.innerJoin(context.getNextOperatorId(), node.getId(), indexLookupSourceSupplier, probeSource.getTypes(), probeChannels, probeHashChannel);
                    break;
                case SOURCE_OUTER:
                    lookupJoinOperatorFactory = LookupJoinOperators.probeOuterJoin(context.getNextOperatorId(), node.getId(), indexLookupSourceSupplier, probeSource.getTypes(), probeChannels, probeHashChannel);
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
            if (clauses.isEmpty()) {
                return createNestedLoopJoin(node, context);
            }

            List<Symbol> leftSymbols = Lists.transform(clauses, JoinNode.EquiJoinClause::getLeft);
            List<Symbol> rightSymbols = Lists.transform(clauses, JoinNode.EquiJoinClause::getRight);

            switch (node.getType()) {
                case INNER:
                case LEFT:
                case RIGHT:
                case FULL:
                    return createLookupJoin(node, node.getLeft(), leftSymbols, node.getLeftHashSymbol(), node.getRight(), rightSymbols, node.getRightHashSymbol(), context);
                default:
                    throw new UnsupportedOperationException("Unsupported join type: " + node.getType());
            }
        }

        private PhysicalOperation createNestedLoopJoin(JoinNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation probeSource = node.getLeft().accept(this, context);

            LocalExecutionPlanContext buildContext = context.createSubContext();
            PhysicalOperation buildSource = node.getRight().accept(this, buildContext);
            NestedLoopBuildOperatorFactory nestedLoopBuildOperatorFactory = new NestedLoopBuildOperatorFactory(
                    buildContext.getNextOperatorId(),
                    node.getId(),
                    buildSource.getTypes());

            context.addDriverFactory(new DriverFactory(
                    buildContext.isInputDriver(),
                    false,
                    ImmutableList.<OperatorFactory>builder()
                            .addAll(buildSource.getOperatorFactories())
                            .add(nestedLoopBuildOperatorFactory)
                            .build()));

            NestedLoopJoinPagesSupplier nestedLoopJoinPagesSupplier = nestedLoopBuildOperatorFactory.getNestedLoopJoinPagesSupplier();
            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            outputMappings.putAll(probeSource.getLayout());

            // inputs from build side of the join are laid out following the input from the probe side,
            // so adjust the channel ids but keep the field layouts intact
            int offset = probeSource.getTypes().size();
            for (Map.Entry<Symbol, Integer> entry : buildSource.getLayout().entrySet()) {
                outputMappings.put(entry.getKey(), offset + entry.getValue());
            }

            OperatorFactory operatorFactory = new NestedLoopJoinOperatorFactory(context.getNextOperatorId(), node.getId(), nestedLoopJoinPagesSupplier, probeSource.getTypes());
            PhysicalOperation operation = new PhysicalOperation(operatorFactory, outputMappings.build(), probeSource);
            return operation;
        }

        private PhysicalOperation createLookupJoin(JoinNode node,
                PlanNode probeNode,
                List<Symbol> probeSymbols,
                Optional<Symbol> probeHashSymbol,
                PlanNode buildNode,
                List<Symbol> buildSymbols,
                Optional<Symbol> buildHashSymbol,
                LocalExecutionPlanContext context)
        {
            // Plan build
            LookupSourceSupplier lookupSourceSupplier = createLookupJoinSource(node, buildNode, buildSymbols, buildHashSymbol, context);

            // Plan probe and introduce a projection to put all fields from the probe side into a single channel if necessary
            PhysicalOperation probeSource;
            LocalExecutionPlanContext parallelParentContext = null;
            int joinConcurrency = getTaskJoinConcurrency(session);
            if (context.isAllowLocalParallel() && context.getDriverInstanceCount() == 1 && joinConcurrency > 1) {
                parallelParentContext = context;
                context = context.createSubContext();
                probeSource = createInMemoryExchange(probeNode, context);
                context.setDriverInstanceCount(joinConcurrency);
            }
            else if (context.isSingleNode() && isBuildOuter(node)) {
                // if this is a build side outer running on a single node, we must
                // insert a gather exchange after the join to pull the joined rows
                // and outer rows into a single stream
                parallelParentContext = context;
                context = context.createSubContext();
                probeSource = probeNode.accept(this, context);
            }
            else {
                probeSource = probeNode.accept(this, context);
            }
            OperatorFactory operator = createLookupJoin(node, probeSource, probeSymbols, probeHashSymbol, lookupSourceSupplier, context);

            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            List<Symbol> outputSymbols = node.getOutputSymbols();
            for (int i = 0; i < outputSymbols.size(); i++) {
                Symbol symbol = outputSymbols.get(i);
                outputMappings.put(symbol, i);
            }
            PhysicalOperation operation = new PhysicalOperation(operator, outputMappings.build(), probeSource);

            // merge parallel joiners (and outer) back into a single stream
            if (parallelParentContext != null) {
                operation = addInMemoryExchange(parallelParentContext, node.getId(), operation, context);
            }

            return operation;
        }

        private boolean isBuildOuter(JoinNode node)
        {
            return node.getType() == RIGHT || node.getType() == FULL;
        }

        private LookupSourceSupplier createLookupJoinSource(
                JoinNode node,
                PlanNode buildNode,
                List<Symbol> buildSymbols,
                Optional<Symbol> buildHashSymbol,
                LocalExecutionPlanContext context)
        {
            LocalExecutionPlanContext buildContext = context.createSubContext();
            PhysicalOperation buildSource = buildNode.accept(this, buildContext);
            List<Integer> buildChannels = ImmutableList.copyOf(getChannelsForSymbols(buildSymbols, buildSource.getLayout()));
            Optional<Integer> buildHashChannel = buildHashSymbol.map(channelGetter(buildSource));

            LookupSourceSupplier lookupSourceSupplier;
            int hashBuildConcurrency = getTaskHashBuildConcurrency(session);
            if (isBuildOuter(node) || hashBuildConcurrency <= 1) {
                HashBuilderOperatorFactory hashBuilderOperatorFactory = new HashBuilderOperatorFactory(
                        buildContext.getNextOperatorId(),
                        node.getId(),
                        buildSource.getTypes(),
                        buildChannels,
                        buildHashChannel,
                        10_000);

                context.addDriverFactory(new DriverFactory(
                        buildContext.isInputDriver(),
                        false,
                        ImmutableList.<OperatorFactory>builder()
                                .addAll(buildSource.getOperatorFactories())
                                .add(hashBuilderOperatorFactory)
                                .build()));

                lookupSourceSupplier = hashBuilderOperatorFactory.getLookupSourceSupplier();
            }
            else {
                // round partitionCount down to the last power of 2
                int parallelBuildCount = Integer.highestOneBit(hashBuildConcurrency);

                ParallelHashBuilder parallelHashBuilder = new ParallelHashBuilder(
                        buildSource.getTypes(),
                        buildChannels,
                        buildHashChannel,
                        10_000,
                        parallelBuildCount);

                context.addDriverFactory(new DriverFactory(
                        buildContext.isInputDriver(),
                        false,
                        ImmutableList.<OperatorFactory>builder()
                                .addAll(buildSource.getOperatorFactories())
                                .add(parallelHashBuilder.getCollectOperatorFactory(buildContext.getNextOperatorId(), node.getId()))
                                .build()));

                context.addDriverFactory(new DriverFactory(
                        false,
                        false,
                        ImmutableList.of(parallelHashBuilder.getBuildOperatorFactory(node.getId())),
                        parallelBuildCount));

                lookupSourceSupplier = parallelHashBuilder.getLookupSourceSupplier();
            }

            if (node.getType() == RIGHT || node.getType() == FULL) {
                lookupSourceSupplier = new OuterLookupSourceSupplier(lookupSourceSupplier);
            }

            return lookupSourceSupplier;
        }

        private OperatorFactory createLookupJoin(
                JoinNode node,
                PhysicalOperation probeSource,
                List<Symbol> probeSymbols,
                Optional<Symbol> probeHashSymbol,
                LookupSourceSupplier lookupSourceSupplier,
                LocalExecutionPlanContext context)
        {
            List<Type> probeTypes = probeSource.getTypes();
            List<Integer> probeJoinChannels = ImmutableList.copyOf(getChannelsForSymbols(probeSymbols, probeSource.getLayout()));
            Optional<Integer> probeHashChannel = probeHashSymbol.map(channelGetter(probeSource));

            switch (node.getType()) {
                case INNER:
                    return LookupJoinOperators.innerJoin(context.getNextOperatorId(), node.getId(), lookupSourceSupplier, probeTypes, probeJoinChannels, probeHashChannel);
                case LEFT:
                    return LookupJoinOperators.probeOuterJoin(context.getNextOperatorId(), node.getId(), lookupSourceSupplier, probeTypes, probeJoinChannels, probeHashChannel);
                case RIGHT:
                    return LookupJoinOperators.lookupOuterJoin(context.getNextOperatorId(), node.getId(), lookupSourceSupplier, probeTypes, probeJoinChannels, probeHashChannel);
                case FULL:
                    return LookupJoinOperators.fullOuterJoin(context.getNextOperatorId(), node.getId(), lookupSourceSupplier, probeTypes, probeJoinChannels, probeHashChannel);
                default:
                    throw new UnsupportedOperationException("Unsupported join type: " + node.getType());
            }
        }

        @Override
        public PhysicalOperation visitSemiJoin(SemiJoinNode node, LocalExecutionPlanContext context)
        {
            // introduce a projection to put all fields from the probe side into a single channel if necessary
            PhysicalOperation probeSource;
            LocalExecutionPlanContext parallelParentContext = null;
            int joinConcurrency = getTaskJoinConcurrency(session);
            if (context.isAllowLocalParallel() && context.getDriverInstanceCount() == 1 && joinConcurrency > 1) {
                parallelParentContext = context;
                context = context.createSubContext();
                probeSource = createInMemoryExchange(node.getSource(), context);
                context.setDriverInstanceCount(joinConcurrency);
            }
            else {
                probeSource = node.getSource().accept(this, context);
            }

            // do the same on the build side
            LocalExecutionPlanContext buildContext = context.createSubContext();
            PhysicalOperation buildSource = node.getFilteringSource().accept(this, buildContext);

            int probeChannel = probeSource.getLayout().get(node.getSourceJoinSymbol());
            int buildChannel = buildSource.getLayout().get(node.getFilteringSourceJoinSymbol());

            Optional<Integer> buildHashChannel = node.getFilteringSourceHashSymbol().map(channelGetter(buildSource));

            SetBuilderOperatorFactory setBuilderOperatorFactory = new SetBuilderOperatorFactory(
                    buildContext.getNextOperatorId(),
                    node.getId(),
                    buildSource.getTypes().get(buildChannel),
                    buildChannel,
                    buildHashChannel,
                    10_000);
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

            HashSemiJoinOperatorFactory operator = new HashSemiJoinOperatorFactory(context.getNextOperatorId(), node.getId(), setProvider, probeSource.getTypes(), probeChannel);
            PhysicalOperation operation = new PhysicalOperation(operator, outputMappings, probeSource);

            // merge parallel joiners back into a single stream
            if (parallelParentContext != null) {
                operation = addInMemoryExchange(parallelParentContext, node.getId(), operation, context);
            }

            return operation;
        }

        @Override
        public PhysicalOperation visitTableWriter(TableWriterNode node, LocalExecutionPlanContext context)
        {
            // serialize writes by forcing data through a single writer
            PhysicalOperation exchange = createInMemoryExchange(node.getSource(), context);

            Optional<Integer> sampleWeightChannel = node.getSampleWeightSymbol().map(exchange::symbolToChannel);

            // Set table writer count
            if (node.getPartitionFunction().isPresent()) {
                context.setDriverInstanceCount(1);
            }
            else {
                context.setDriverInstanceCount(getTaskWriterCount(session));
            }

            List<Integer> inputChannels = node.getColumns().stream()
                    .map(exchange::symbolToChannel)
                    .collect(toImmutableList());

            OperatorFactory operatorFactory = new TableWriterOperatorFactory(context.getNextOperatorId(), node.getId(), pageSinkManager, node.getTarget(), inputChannels, sampleWeightChannel,
                    session);

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

            return addInMemoryExchange(context, node.getId(), source, subContext);
        }

        private PhysicalOperation addInMemoryExchange(LocalExecutionPlanContext context, PlanNodeId planNodeId, PhysicalOperation source, LocalExecutionPlanContext sourceContext)
        {
            InMemoryExchange exchange = new InMemoryExchange(source.getTypes());

            // create exchange sink
            List<OperatorFactory> factories = ImmutableList.<OperatorFactory>builder()
                    .addAll(source.getOperatorFactories())
                    .add(exchange.createSinkFactory(sourceContext.getNextOperatorId(), planNodeId))
                    .build();

            // add sub-context to current context
            context.addDriverFactory(new DriverFactory(sourceContext.isInputDriver(), false, factories, sourceContext.getDriverInstanceCount()));

            // the main driver is not an input: the source is the input for the plan
            context.setInputDriver(false);

            // add exchange source as first operator in the current context
            OperatorFactory factory = createRandomDistribution(context.getNextOperatorId(), planNodeId, exchange);
            return new PhysicalOperation(factory, source.getLayout());
        }

        @Override
        public PhysicalOperation visitTableFinish(TableFinishNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            OperatorFactory operatorFactory = new TableFinishOperatorFactory(context.getNextOperatorId(), node.getId(), createTableFinisher(session, node, metadata));
            Map<Symbol, Integer> layout = ImmutableMap.of(node.getOutputSymbols().get(0), 0);

            return new PhysicalOperation(operatorFactory, layout, source);
        }

        @Override
        public PhysicalOperation visitDelete(DeleteNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            OperatorFactory operatorFactory = new DeleteOperatorFactory(context.getNextOperatorId(), node.getId(), source.getLayout().get(node.getRowId()));

            Map<Symbol, Integer> layout = ImmutableMap.<Symbol, Integer>builder()
                    .put(node.getOutputSymbols().get(0), 0)
                    .put(node.getOutputSymbols().get(1), 1)
                    .build();

            return new PhysicalOperation(operatorFactory, layout, source);
        }

        @Override
        public PhysicalOperation visitMetadataDelete(MetadataDeleteNode node, LocalExecutionPlanContext context)
        {
            OperatorFactory operatorFactory = new MetadataDeleteOperatorFactory(context.getNextOperatorId(), node.getId(), node.getTableLayout(), metadata, session, node.getTarget().getHandle());

            return new PhysicalOperation(operatorFactory, makeLayout(node));
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

                boolean projectionMatchesOutput = source.getLayout()
                        .entrySet().stream()
                        .sorted(Ordering.<Integer>natural().onResultOf(Map.Entry::getValue))
                        .map(Map.Entry::getKey)
                        .collect(toImmutableList())
                        .equals(expectedLayout);

                if (!projectionMatchesOutput) {
                    IdentityProjectionInfo projectionInfo = computeIdentityProjectionInfo(expectedLayout, source.getLayout(), context.getTypes());
                    List<RowExpression> projections = projectionInfo.getProjections();
                    OperatorFactory operatorFactory = new FilterAndProjectOperator.FilterAndProjectOperatorFactory(
                            subContext.getNextOperatorId(),
                            node.getId(),
                            compiler.compilePageProcessor(trueExpression(), projections),
                            projections.stream()
                                    .map(RowExpression::getType)
                                    .collect(toImmutableList()));
                    operatorFactories.add(operatorFactory);
                }

                operatorFactories.add(inMemoryExchange.createSinkFactory(subContext.getNextOperatorId(), node.getId()));

                DriverFactory driverFactory = new DriverFactory(subContext.isInputDriver(), false, operatorFactories);
                context.addDriverFactory(driverFactory);
            }

            // the main driver is not an input... the union sources are the input for the plan
            context.setInputDriver(false);

            return new PhysicalOperation(createRandomDistribution(context.getNextOperatorId(), node.getId(), inMemoryExchange), makeLayout(node));
        }

        @Override
        public PhysicalOperation visitEnforceSingleRow(EnforceSingleRowNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            Symbol symbol = getOnlyElement(node.getOutputSymbols());
            Type type = requireNonNull(context.getTypes().get(symbol), format("No type for symbol %s", symbol));

            OperatorFactory operatorFactory = new EnforceSingleRowOperator.EnforceSingleRowOperatorFactory(context.getNextOperatorId(), node.getId(), type);
            return new PhysicalOperation(operatorFactory, makeLayout(node), source);
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
            return symbols.stream()
                    .map(types::get)
                    .collect(toImmutableList());
        }

        private AccumulatorFactory buildAccumulatorFactory(
                PhysicalOperation source,
                Signature function,
                FunctionCall call,
                @Nullable Symbol mask,
                Optional<Integer> defaultMaskChannel,
                Optional<Symbol> sampleWeight,
                double confidence)
        {
            List<Integer> arguments = new ArrayList<>();
            for (Expression argument : call.getArguments()) {
                Symbol argumentSymbol = Symbol.fromQualifiedName(((QualifiedNameReference) argument).getName());
                arguments.add(source.getLayout().get(argumentSymbol));
            }

            Optional<Integer> maskChannel = defaultMaskChannel;

            if (mask != null) {
                maskChannel = Optional.of(source.getLayout().get(mask));
            }

            Optional<Integer> sampleWeightChannel = Optional.empty();
            if (sampleWeight.isPresent()) {
                sampleWeightChannel = Optional.of(source.getLayout().get(sampleWeight.get()));
            }

            return metadata.getFunctionRegistry().getAggregateFunctionImplementation(function).bind(arguments, maskChannel, sampleWeightChannel, confidence);
        }

        private PhysicalOperation planGlobalAggregation(int operatorId, AggregationNode node, PhysicalOperation source)
        {
            int outputChannel = 0;
            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            List<AccumulatorFactory> accumulatorFactories = new ArrayList<>();
            for (Map.Entry<Symbol, FunctionCall> entry : node.getAggregations().entrySet()) {
                Symbol symbol = entry.getKey();

                accumulatorFactories.add(buildAccumulatorFactory(source,
                        node.getFunctions().get(symbol),
                        entry.getValue(),
                        node.getMasks().get(entry.getKey()),
                        Optional.<Integer>empty(),
                        node.getSampleWeight(),
                        node.getConfidence()));
                outputMappings.put(symbol, outputChannel); // one aggregation per channel
                outputChannel++;
            }

            OperatorFactory operatorFactory = new AggregationOperatorFactory(operatorId, node.getId(), node.getStep(), accumulatorFactories);
            return new PhysicalOperation(operatorFactory, outputMappings.build(), source);
        }

        private PhysicalOperation planGroupByAggregation(AggregationNode node, PhysicalOperation source, int operatorId, Optional<Integer> defaultMaskChannel)
        {
            List<Symbol> groupBySymbols = node.getGroupBy();

            List<Symbol> aggregationOutputSymbols = new ArrayList<>();
            List<AccumulatorFactory> accumulatorFactories = new ArrayList<>();
            for (Map.Entry<Symbol, FunctionCall> entry : node.getAggregations().entrySet()) {
                Symbol symbol = entry.getKey();

                accumulatorFactories.add(buildAccumulatorFactory(source, node.getFunctions().get(symbol), entry.getValue(), node.getMasks().get(entry.getKey()), defaultMaskChannel, node.getSampleWeight(), node.getConfidence()));
                aggregationOutputSymbols.add(symbol);
            }

            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            // add group-by key fields each in a separate channel
            int channel = 0;
            for (Symbol symbol : groupBySymbols) {
                outputMappings.put(symbol, channel);
                channel++;
            }

            // hashChannel follows the group by channels
            if (node.getHashSymbol().isPresent()) {
                outputMappings.put(node.getHashSymbol().get(), channel++);
            }

            // aggregations go in following channels
            for (Symbol symbol : aggregationOutputSymbols) {
                outputMappings.put(symbol, channel);
                channel++;
            }

            List<Integer> groupByChannels = getChannelsForSymbols(groupBySymbols, source.getLayout());
            List<Type> groupByTypes = groupByChannels.stream()
                    .map(entry -> source.getTypes().get(entry))
                    .collect(toImmutableList());

            Optional<Integer> hashChannel = node.getHashSymbol().map(channelGetter(source));

            OperatorFactory operatorFactory = new HashAggregationOperatorFactory(
                    operatorId,
                    node.getId(),
                    groupByTypes,
                    groupByChannels,
                    node.getStep(),
                    accumulatorFactories,
                    defaultMaskChannel,
                    hashChannel,
                    10_000,
                    maxPartialAggregationMemorySize);

            return new PhysicalOperation(operatorFactory, outputMappings.build(), source);
        }
    }

    public static List<Type> toTypes(List<ProjectionFunction> projections)
    {
        ImmutableList.Builder<Type> builder = ImmutableList.builder();
        for (ProjectionFunction projection : projections) {
            builder.add(projection.getType());
        }
        return builder.build();
    }

    private static TableFinisher createTableFinisher(Session session, TableFinishNode node, Metadata metadata)
    {
        WriterTarget target = node.getTarget();
        return new TableFinisher()
        {
            @Override
            public void finishTable(Collection<Slice> fragments)
            {
                if (target instanceof CreateHandle) {
                    metadata.finishCreateTable(session, ((CreateHandle) target).getHandle(), fragments);
                }
                else if (target instanceof InsertHandle) {
                    metadata.finishInsert(session, ((InsertHandle) target).getHandle(), fragments);
                }
                else if (target instanceof DeleteHandle) {
                    metadata.finishDelete(session, ((DeleteHandle) target).getHandle(), fragments);
                }
                else {
                    throw new AssertionError("Unhandled target type: " + target.getClass().getName());
                }
            }
        };
    }

    private static IdentityProjectionInfo computeIdentityProjectionInfo(List<Symbol> symbols, Map<Symbol, Integer> inputLayout, Map<Symbol, Type> types)
    {
        Map<Symbol, Integer> outputMappings = new HashMap<>();
        List<RowExpression> projections = new ArrayList<>();

        int channel = 0;
        for (Symbol symbol : symbols) {
            Type type = types.get(symbol);
            RowExpression expression = new InputReferenceExpression(inputLayout.get(symbol), type);
            projections.add(expression);
            if (!outputMappings.containsKey(symbol)) {
                outputMappings.put(symbol, channel);
                channel++;
            }
        }

        return new IdentityProjectionInfo(projections, outputMappings);
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
        private final List<RowExpression> projections;
        private final Map<Symbol, Integer> outputLayout;

        public IdentityProjectionInfo(List<RowExpression> projections, Map<Symbol, Integer> outputLayout)
        {
            this.projections = requireNonNull(projections, "projections is null");
            this.outputLayout = requireNonNull(outputLayout, "outputLayout is null");
        }

        public List<RowExpression> getProjections()
        {
            return projections;
        }

        public Map<Symbol, Integer> getOutputLayout()
        {
            return outputLayout;
        }
    }

    private static Function<Symbol, Integer> channelGetter(PhysicalOperation source)
    {
        return input -> {
            checkArgument(source.getLayout().containsKey(input));
            return source.getLayout().get(input);
        };
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
            requireNonNull(operatorFactory, "operatorFactory is null");
            requireNonNull(layout, "layout is null");

            this.operatorFactories = ImmutableList.of(operatorFactory);
            this.layout = ImmutableMap.copyOf(layout);
            this.types = operatorFactory.getTypes();
        }

        public PhysicalOperation(OperatorFactory operatorFactory, Map<Symbol, Integer> layout, PhysicalOperation source)
        {
            requireNonNull(operatorFactory, "operatorFactory is null");
            requireNonNull(layout, "layout is null");
            requireNonNull(source, "source is null");

            this.operatorFactories = ImmutableList.<OperatorFactory>builder().addAll(source.getOperatorFactories()).add(operatorFactory).build();
            this.layout = ImmutableMap.copyOf(layout);
            this.types = operatorFactory.getTypes();
        }

        public int symbolToChannel(Symbol input)
        {
            checkArgument(layout.containsKey(input));
            return layout.get(input);
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
