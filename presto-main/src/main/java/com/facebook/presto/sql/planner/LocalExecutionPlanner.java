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
import com.facebook.presto.execution.ExplainAnalyzeContext;
import com.facebook.presto.execution.StageId;
import com.facebook.presto.execution.TaskManagerConfig;
import com.facebook.presto.execution.buffer.OutputBuffer;
import com.facebook.presto.execution.buffer.PagesSerdeFactory;
import com.facebook.presto.index.IndexManager;
import com.facebook.presto.metadata.FunctionManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.AggregationOperator.AggregationOperatorFactory;
import com.facebook.presto.operator.AssignUniqueIdOperator;
import com.facebook.presto.operator.DeleteOperator.DeleteOperatorFactory;
import com.facebook.presto.operator.DevNullOperator.DevNullOperatorFactory;
import com.facebook.presto.operator.DriverFactory;
import com.facebook.presto.operator.EnforceSingleRowOperator;
import com.facebook.presto.operator.ExchangeOperator.ExchangeOperatorFactory;
import com.facebook.presto.operator.ExplainAnalyzeOperator.ExplainAnalyzeOperatorFactory;
import com.facebook.presto.operator.FilterAndProjectOperator;
import com.facebook.presto.operator.GroupIdOperator;
import com.facebook.presto.operator.HashAggregationOperator.HashAggregationOperatorFactory;
import com.facebook.presto.operator.HashBuilderOperator.HashBuilderOperatorFactory;
import com.facebook.presto.operator.HashSemiJoinOperator.HashSemiJoinOperatorFactory;
import com.facebook.presto.operator.JoinBridgeManager;
import com.facebook.presto.operator.JoinOperatorFactory;
import com.facebook.presto.operator.JoinOperatorFactory.OuterOperatorFactoryResult;
import com.facebook.presto.operator.LimitOperator.LimitOperatorFactory;
import com.facebook.presto.operator.LocalPlannerAware;
import com.facebook.presto.operator.LookupJoinOperators;
import com.facebook.presto.operator.LookupOuterOperator.LookupOuterOperatorFactory;
import com.facebook.presto.operator.LookupSourceFactory;
import com.facebook.presto.operator.MarkDistinctOperator.MarkDistinctOperatorFactory;
import com.facebook.presto.operator.MergeOperator.MergeOperatorFactory;
import com.facebook.presto.operator.MetadataDeleteOperator.MetadataDeleteOperatorFactory;
import com.facebook.presto.operator.NestedLoopJoinBridge;
import com.facebook.presto.operator.NestedLoopJoinPagesSupplier;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.operator.OrderByOperator.OrderByOperatorFactory;
import com.facebook.presto.operator.OutputFactory;
import com.facebook.presto.operator.PagesIndex;
import com.facebook.presto.operator.PagesSpatialIndexFactory;
import com.facebook.presto.operator.PartitionFunction;
import com.facebook.presto.operator.PartitionedLookupSourceFactory;
import com.facebook.presto.operator.PartitionedOutputOperator.PartitionedOutputFactory;
import com.facebook.presto.operator.PipelineExecutionStrategy;
import com.facebook.presto.operator.RowNumberOperator;
import com.facebook.presto.operator.ScanFilterAndProjectOperator.ScanFilterAndProjectOperatorFactory;
import com.facebook.presto.operator.SetBuilderOperator.SetBuilderOperatorFactory;
import com.facebook.presto.operator.SetBuilderOperator.SetSupplier;
import com.facebook.presto.operator.SourceOperatorFactory;
import com.facebook.presto.operator.SpatialIndexBuilderOperator.SpatialIndexBuilderOperatorFactory;
import com.facebook.presto.operator.SpatialIndexBuilderOperator.SpatialPredicate;
import com.facebook.presto.operator.SpatialJoinOperator.SpatialJoinOperatorFactory;
import com.facebook.presto.operator.StageExecutionDescriptor;
import com.facebook.presto.operator.StatisticsWriterOperator.StatisticsWriterOperatorFactory;
import com.facebook.presto.operator.StreamingAggregationOperator.StreamingAggregationOperatorFactory;
import com.facebook.presto.operator.TableScanOperator.TableScanOperatorFactory;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.operator.TaskExchangeClientManager;
import com.facebook.presto.operator.TaskOutputOperator.TaskOutputFactory;
import com.facebook.presto.operator.TopNOperator.TopNOperatorFactory;
import com.facebook.presto.operator.TopNRowNumberOperator;
import com.facebook.presto.operator.ValuesOperator.ValuesOperatorFactory;
import com.facebook.presto.operator.WindowFunctionDefinition;
import com.facebook.presto.operator.WindowOperator.WindowOperatorFactory;
import com.facebook.presto.operator.aggregation.AccumulatorFactory;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.facebook.presto.operator.aggregation.LambdaProvider;
import com.facebook.presto.operator.exchange.LocalExchange.LocalExchangeFactory;
import com.facebook.presto.operator.exchange.LocalExchangeSinkOperator.LocalExchangeSinkOperatorFactory;
import com.facebook.presto.operator.exchange.LocalExchangeSourceOperator.LocalExchangeSourceOperatorFactory;
import com.facebook.presto.operator.exchange.LocalMergeSourceOperator.LocalMergeSourceOperatorFactory;
import com.facebook.presto.operator.exchange.PageChannelSelector;
import com.facebook.presto.operator.index.DynamicTupleFilterFactory;
import com.facebook.presto.operator.index.FieldSetFilteringRecordSet;
import com.facebook.presto.operator.index.IndexBuildDriverFactoryProvider;
import com.facebook.presto.operator.index.IndexJoinLookupStats;
import com.facebook.presto.operator.index.IndexLookupSourceFactory;
import com.facebook.presto.operator.index.IndexSourceOperator;
import com.facebook.presto.operator.project.CursorProcessor;
import com.facebook.presto.operator.project.PageProcessor;
import com.facebook.presto.operator.window.FrameInfo;
import com.facebook.presto.operator.window.WindowFunctionSupplier;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorIndex;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.type.FunctionType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spiller.PartitioningSpillerFactory;
import com.facebook.presto.spiller.SingleStreamSpillerFactory;
import com.facebook.presto.spiller.SpillerFactory;
import com.facebook.presto.split.MappedRecordSet;
import com.facebook.presto.split.PageSinkManager;
import com.facebook.presto.split.PageSourceProvider;
import com.facebook.presto.sql.gen.ExpressionCompiler;
import com.facebook.presto.sql.gen.JoinCompiler;
import com.facebook.presto.sql.gen.JoinFilterFunctionCompiler;
import com.facebook.presto.sql.gen.JoinFilterFunctionCompiler.JoinFilterFunctionFactory;
import com.facebook.presto.sql.gen.OrderingCompiler;
import com.facebook.presto.sql.gen.PageFunctionCompiler;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.optimizations.IndexJoinOptimizer;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.AggregationNode.Aggregation;
import com.facebook.presto.sql.planner.plan.AggregationNode.Step;
import com.facebook.presto.sql.planner.plan.AssignUniqueId;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.DeleteNode;
import com.facebook.presto.sql.planner.plan.DistinctLimitNode;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
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
import com.facebook.presto.sql.planner.plan.SpatialJoinNode;
import com.facebook.presto.sql.planner.plan.StatisticAggregationsDescriptor;
import com.facebook.presto.sql.planner.plan.StatisticsWriterNode;
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
import com.facebook.presto.sql.relational.SqlToRowExpressionTranslator;
import com.facebook.presto.sql.relational.SymbolToChannelTranslator;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.LambdaArgumentDeclaration;
import com.facebook.presto.sql.tree.LambdaExpression;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.base.VerifyException;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;
import com.google.common.primitives.Ints;
import io.airlift.units.DataSize;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.facebook.presto.SystemSessionProperties.getAggregationOperatorUnspillMemoryLimit;
import static com.facebook.presto.SystemSessionProperties.getFilterAndProjectMinOutputPageRowCount;
import static com.facebook.presto.SystemSessionProperties.getFilterAndProjectMinOutputPageSize;
import static com.facebook.presto.SystemSessionProperties.getTaskConcurrency;
import static com.facebook.presto.SystemSessionProperties.getTaskWriterCount;
import static com.facebook.presto.SystemSessionProperties.isExchangeCompressionEnabled;
import static com.facebook.presto.SystemSessionProperties.isSpillEnabled;
import static com.facebook.presto.execution.warnings.WarningCollector.NOOP;
import static com.facebook.presto.operator.DistinctLimitOperator.DistinctLimitOperatorFactory;
import static com.facebook.presto.operator.NestedLoopBuildOperator.NestedLoopBuildOperatorFactory;
import static com.facebook.presto.operator.NestedLoopJoinOperator.NestedLoopJoinOperatorFactory;
import static com.facebook.presto.operator.PipelineExecutionStrategy.GROUPED_EXECUTION;
import static com.facebook.presto.operator.PipelineExecutionStrategy.UNGROUPED_EXECUTION;
import static com.facebook.presto.operator.TableFinishOperator.TableFinishOperatorFactory;
import static com.facebook.presto.operator.TableFinishOperator.TableFinisher;
import static com.facebook.presto.operator.TableWriterOperator.FRAGMENT_CHANNEL;
import static com.facebook.presto.operator.TableWriterOperator.ROW_COUNT_CHANNEL;
import static com.facebook.presto.operator.TableWriterOperator.STATS_START_CHANNEL;
import static com.facebook.presto.operator.TableWriterOperator.TableWriterOperatorFactory;
import static com.facebook.presto.operator.UnnestOperator.UnnestOperatorFactory;
import static com.facebook.presto.operator.WindowFunctionDefinition.window;
import static com.facebook.presto.spi.StandardErrorCode.COMPILER_ERROR;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.TypeUtils.writeNativeValue;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypes;
import static com.facebook.presto.sql.gen.LambdaBytecodeGenerator.compileLambdaProvider;
import static com.facebook.presto.sql.planner.RowExpressionInterpreter.rowExpressionInterpreter;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.COORDINATOR_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_BROADCAST_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SCALED_WRITER_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.plan.AggregationNode.Step.FINAL;
import static com.facebook.presto.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.FULL;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.RIGHT;
import static com.facebook.presto.sql.planner.plan.TableWriterNode.CreateHandle;
import static com.facebook.presto.sql.planner.plan.TableWriterNode.InsertHandle;
import static com.facebook.presto.sql.planner.plan.TableWriterNode.WriterTarget;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.LogicalRowExpressions.TRUE;
import static com.facebook.presto.sql.relational.RowExpressionNodeInliner.replaceExpression;
import static com.facebook.presto.util.Reflection.constructorMethodHandle;
import static com.facebook.presto.util.SpatialJoinUtils.ST_CONTAINS;
import static com.facebook.presto.util.SpatialJoinUtils.ST_DISTANCE;
import static com.facebook.presto.util.SpatialJoinUtils.ST_INTERSECTS;
import static com.facebook.presto.util.SpatialJoinUtils.ST_WITHIN;
import static com.facebook.presto.util.SpatialJoinUtils.extractSupportedSpatialComparisons;
import static com.facebook.presto.util.SpatialJoinUtils.extractSupportedSpatialFunctions;
import static com.google.common.base.Functions.forMap;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.DiscreteDomain.integers;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Range.closedOpen;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.IntStream.range;

public class LocalExecutionPlanner
{
    private final Metadata metadata;
    private final SqlParser sqlParser;
    private final Optional<ExplainAnalyzeContext> explainAnalyzeContext;
    private final PageSourceProvider pageSourceProvider;
    private final IndexManager indexManager;
    private final NodePartitioningManager nodePartitioningManager;
    private final PageSinkManager pageSinkManager;
    private final ExpressionCompiler expressionCompiler;
    private final PageFunctionCompiler pageFunctionCompiler;
    private final JoinFilterFunctionCompiler joinFilterFunctionCompiler;
    private final DataSize maxIndexMemorySize;
    private final IndexJoinLookupStats indexJoinLookupStats;
    private final DataSize maxPartialAggregationMemorySize;
    private final DataSize maxPagePartitioningBufferSize;
    private final DataSize maxLocalExchangeBufferSize;
    private final SpillerFactory spillerFactory;
    private final SingleStreamSpillerFactory singleStreamSpillerFactory;
    private final PartitioningSpillerFactory partitioningSpillerFactory;
    private final BlockEncodingSerde blockEncodingSerde;
    private final PagesIndex.Factory pagesIndexFactory;
    private final JoinCompiler joinCompiler;
    private final LookupJoinOperators lookupJoinOperators;
    private final OrderingCompiler orderingCompiler;

    @Inject
    public LocalExecutionPlanner(
            Metadata metadata,
            SqlParser sqlParser,
            Optional<ExplainAnalyzeContext> explainAnalyzeContext,
            PageSourceProvider pageSourceProvider,
            IndexManager indexManager,
            NodePartitioningManager nodePartitioningManager,
            PageSinkManager pageSinkManager,
            ExpressionCompiler expressionCompiler,
            PageFunctionCompiler pageFunctionCompiler,
            JoinFilterFunctionCompiler joinFilterFunctionCompiler,
            IndexJoinLookupStats indexJoinLookupStats,
            TaskManagerConfig taskManagerConfig,
            SpillerFactory spillerFactory,
            SingleStreamSpillerFactory singleStreamSpillerFactory,
            PartitioningSpillerFactory partitioningSpillerFactory,
            BlockEncodingSerde blockEncodingSerde,
            PagesIndex.Factory pagesIndexFactory,
            JoinCompiler joinCompiler,
            LookupJoinOperators lookupJoinOperators,
            OrderingCompiler orderingCompiler)
    {
        this.explainAnalyzeContext = requireNonNull(explainAnalyzeContext, "explainAnalyzeContext is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        this.indexManager = requireNonNull(indexManager, "indexManager is null");
        this.nodePartitioningManager = requireNonNull(nodePartitioningManager, "nodePartitioningManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.pageSinkManager = requireNonNull(pageSinkManager, "pageSinkManager is null");
        this.expressionCompiler = requireNonNull(expressionCompiler, "compiler is null");
        this.pageFunctionCompiler = requireNonNull(pageFunctionCompiler, "pageFunctionCompiler is null");
        this.joinFilterFunctionCompiler = requireNonNull(joinFilterFunctionCompiler, "compiler is null");
        this.indexJoinLookupStats = requireNonNull(indexJoinLookupStats, "indexJoinLookupStats is null");
        this.maxIndexMemorySize = requireNonNull(taskManagerConfig, "taskManagerConfig is null").getMaxIndexMemoryUsage();
        this.spillerFactory = requireNonNull(spillerFactory, "spillerFactory is null");
        this.singleStreamSpillerFactory = requireNonNull(singleStreamSpillerFactory, "singleStreamSpillerFactory is null");
        this.partitioningSpillerFactory = requireNonNull(partitioningSpillerFactory, "partitioningSpillerFactory is null");
        this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
        this.maxPartialAggregationMemorySize = taskManagerConfig.getMaxPartialAggregationMemoryUsage();
        this.maxPagePartitioningBufferSize = taskManagerConfig.getMaxPagePartitioningBufferSize();
        this.maxLocalExchangeBufferSize = taskManagerConfig.getMaxLocalExchangeBufferSize();
        this.pagesIndexFactory = requireNonNull(pagesIndexFactory, "pagesIndexFactory is null");
        this.joinCompiler = requireNonNull(joinCompiler, "joinCompiler is null");
        this.lookupJoinOperators = requireNonNull(lookupJoinOperators, "lookupJoinOperators is null");
        this.orderingCompiler = requireNonNull(orderingCompiler, "orderingCompiler is null");
    }

    public LocalExecutionPlan plan(
            TaskContext taskContext,
            PlanNode plan,
            TypeProvider types,
            PartitioningScheme partitioningScheme,
            StageExecutionDescriptor stageExecutionDescriptor,
            List<PlanNodeId> partitionedSourceOrder,
            OutputBuffer outputBuffer,
            TaskExchangeClientManager taskExchangeClientManager)
    {
        List<VariableReferenceExpression> outputLayout = partitioningScheme.getOutputLayout();

        if (partitioningScheme.getPartitioning().getHandle().equals(FIXED_BROADCAST_DISTRIBUTION) ||
                partitioningScheme.getPartitioning().getHandle().equals(FIXED_ARBITRARY_DISTRIBUTION) ||
                partitioningScheme.getPartitioning().getHandle().equals(SCALED_WRITER_DISTRIBUTION) ||
                partitioningScheme.getPartitioning().getHandle().equals(SINGLE_DISTRIBUTION) ||
                partitioningScheme.getPartitioning().getHandle().equals(COORDINATOR_DISTRIBUTION)) {
            return plan(
                    taskContext,
                    stageExecutionDescriptor,
                    plan,
                    outputLayout,
                    types,
                    partitionedSourceOrder,
                    new TaskOutputFactory(outputBuffer),
                    taskExchangeClientManager);
        }

        // We can convert the symbols directly into channels, because the root must be a sink and therefore the layout is fixed
        List<Integer> partitionChannels;
        List<Optional<ConstantExpression>> partitionConstants;
        List<Type> partitionChannelTypes;
        if (partitioningScheme.getHashColumn().isPresent()) {
            partitionChannels = ImmutableList.of(outputLayout.indexOf(partitioningScheme.getHashColumn().get()));
            partitionConstants = ImmutableList.of(Optional.empty());
            partitionChannelTypes = ImmutableList.of(BIGINT);
        }
        else {
            partitionChannels = partitioningScheme.getPartitioning().getArguments().stream()
                    .map(argument -> {
                        if (argument.isConstant()) {
                            return -1;
                        }
                        return outputLayout.indexOf(argument.getVariableReference());
                    })
                    .collect(toImmutableList());
            partitionConstants = partitioningScheme.getPartitioning().getArguments().stream()
                    .map(argument -> {
                        if (argument.isConstant()) {
                            return Optional.of(argument.getConstant());
                        }
                        return Optional.<ConstantExpression>empty();
                    })
                    .collect(toImmutableList());
            partitionChannelTypes = partitioningScheme.getPartitioning().getArguments().stream()
                    .map(argument -> {
                        if (argument.isConstant()) {
                            return argument.getConstant().getType();
                        }
                        return types.get(argument.getColumn());
                    })
                    .collect(toImmutableList());
        }

        PartitionFunction partitionFunction = nodePartitioningManager.getPartitionFunction(taskContext.getSession(), partitioningScheme, partitionChannelTypes);
        OptionalInt nullChannel = OptionalInt.empty();
        Set<VariableReferenceExpression> partitioningColumns = partitioningScheme.getPartitioning().getVariableReferences();

        // partitioningColumns expected to have one column in the normal case, and zero columns when partitioning on a constant
        checkArgument(!partitioningScheme.isReplicateNullsAndAny() || partitioningColumns.size() <= 1);
        if (partitioningScheme.isReplicateNullsAndAny() && partitioningColumns.size() == 1) {
            nullChannel = OptionalInt.of(outputLayout.indexOf(getOnlyElement(partitioningColumns)));
        }

        return plan(
                taskContext,
                stageExecutionDescriptor,
                plan,
                outputLayout,
                types,
                partitionedSourceOrder,
                new PartitionedOutputFactory(
                        partitionFunction,
                        partitionChannels,
                        partitionConstants,
                        partitioningScheme.isReplicateNullsAndAny(),
                        nullChannel,
                        outputBuffer,
                        maxPagePartitioningBufferSize),
                taskExchangeClientManager);
    }

    public LocalExecutionPlan plan(
            TaskContext taskContext,
            StageExecutionDescriptor stageExecutionDescriptor,
            PlanNode plan,
            List<VariableReferenceExpression> outputLayout,
            TypeProvider types,
            List<PlanNodeId> partitionedSourceOrder,
            OutputFactory outputOperatorFactory,
            TaskExchangeClientManager taskExchangeClientManager)
    {
        Session session = taskContext.getSession();
        LocalExecutionPlanContext context = new LocalExecutionPlanContext(taskContext, types, taskExchangeClientManager);

        PhysicalOperation physicalOperation = plan.accept(new Visitor(session, stageExecutionDescriptor), context);

        Function<Page, Page> pagePreprocessor = enforceLayoutProcessor(outputLayout, physicalOperation.getLayout());

        List<Type> outputTypes = outputLayout.stream()
                .map(VariableReferenceExpression::getType)
                .collect(toImmutableList());

        context.addDriverFactory(
                context.isInputDriver(),
                true,
                ImmutableList.<OperatorFactory>builder()
                        .addAll(physicalOperation.getOperatorFactories())
                        .add(outputOperatorFactory.createOutputOperator(
                                context.getNextOperatorId(),
                                plan.getId(),
                                outputTypes,
                                pagePreprocessor,
                                new PagesSerdeFactory(blockEncodingSerde, isExchangeCompressionEnabled(session))))
                        .build(),
                context.getDriverInstanceCount(),
                physicalOperation.getPipelineExecutionStrategy());

        addLookupOuterDrivers(context);

        // notify operator factories that planning has completed
        context.getDriverFactories().stream()
                .map(DriverFactory::getOperatorFactories)
                .flatMap(List::stream)
                .filter(LocalPlannerAware.class::isInstance)
                .map(LocalPlannerAware.class::cast)
                .forEach(LocalPlannerAware::localPlannerComplete);

        return new LocalExecutionPlan(context.getDriverFactories(), partitionedSourceOrder, stageExecutionDescriptor);
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
                Optional<OuterOperatorFactoryResult> outerOperatorFactoryResult = lookupJoin.createOuterOperatorFactory();
                if (outerOperatorFactoryResult.isPresent()) {
                    // Add a new driver to output the unmatched rows in an outer join.
                    // We duplicate all of the factories above the JoinOperator (the ones reading from the joins),
                    // and replace the JoinOperator with the OuterOperator (the one that produces unmatched rows).
                    ImmutableList.Builder<OperatorFactory> newOperators = ImmutableList.builder();
                    newOperators.add(outerOperatorFactoryResult.get().getOuterOperatorFactory());
                    operatorFactories.subList(i + 1, operatorFactories.size()).stream()
                            .map(OperatorFactory::duplicate)
                            .forEach(newOperators::add);

                    context.addDriverFactory(false, factory.isOutputDriver(), newOperators.build(), OptionalInt.of(1), outerOperatorFactoryResult.get().getBuildExecutionStrategy());
                }
            }
        }
    }

    private static class LocalExecutionPlanContext
    {
        private final TaskContext taskContext;
        private final TypeProvider types;
        private final TaskExchangeClientManager taskExchangeClientManager;
        private final List<DriverFactory> driverFactories;
        private final Optional<IndexSourceContext> indexSourceContext;

        // this is shared with all subContexts
        private final AtomicInteger nextPipelineId;

        private int nextOperatorId;
        private boolean inputDriver = true;
        private OptionalInt driverInstanceCount = OptionalInt.empty();

        public LocalExecutionPlanContext(TaskContext taskContext, TypeProvider types, TaskExchangeClientManager taskExchangeClientManager)
        {
            this(taskContext, types, taskExchangeClientManager, new ArrayList<>(), Optional.empty(), new AtomicInteger(0));
        }

        private LocalExecutionPlanContext(
                TaskContext taskContext,
                TypeProvider types,
                TaskExchangeClientManager taskExchangeClientManager,
                List<DriverFactory> driverFactories,
                Optional<IndexSourceContext> indexSourceContext,
                AtomicInteger nextPipelineId)
        {
            this.taskContext = taskContext;
            this.types = types;
            this.taskExchangeClientManager = taskExchangeClientManager;
            this.driverFactories = driverFactories;
            this.indexSourceContext = indexSourceContext;
            this.nextPipelineId = nextPipelineId;
        }

        public void addDriverFactory(boolean inputDriver, boolean outputDriver, List<OperatorFactory> operatorFactories, OptionalInt driverInstances, PipelineExecutionStrategy pipelineExecutionStrategy)
        {
            if (pipelineExecutionStrategy == GROUPED_EXECUTION) {
                OperatorFactory firstOperatorFactory = operatorFactories.get(0);
                if (inputDriver) {
                    checkArgument(firstOperatorFactory instanceof ScanFilterAndProjectOperatorFactory || firstOperatorFactory instanceof TableScanOperatorFactory);
                }
                else {
                    checkArgument(firstOperatorFactory instanceof LocalExchangeSourceOperatorFactory || firstOperatorFactory instanceof LookupOuterOperatorFactory);
                }
            }
            driverFactories.add(new DriverFactory(getNextPipelineId(), inputDriver, outputDriver, operatorFactories, driverInstances, pipelineExecutionStrategy));
        }

        private List<DriverFactory> getDriverFactories()
        {
            return ImmutableList.copyOf(driverFactories);
        }

        public Session getSession()
        {
            return taskContext.getSession();
        }

        public StageId getStageId()
        {
            return taskContext.getTaskId().getStageId();
        }

        public TypeProvider getTypes()
        {
            return types;
        }

        public TaskExchangeClientManager getTaskExchangeClientManager()
        {
            return taskExchangeClientManager;
        }

        public Optional<IndexSourceContext> getIndexSourceContext()
        {
            return indexSourceContext;
        }

        private int getNextPipelineId()
        {
            return nextPipelineId.getAndIncrement();
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
            return new LocalExecutionPlanContext(taskContext, types, taskExchangeClientManager, driverFactories, indexSourceContext, nextPipelineId);
        }

        public LocalExecutionPlanContext createIndexSourceSubContext(IndexSourceContext indexSourceContext)
        {
            return new LocalExecutionPlanContext(taskContext, types, taskExchangeClientManager, driverFactories, Optional.of(indexSourceContext), nextPipelineId);
        }

        public OptionalInt getDriverInstanceCount()
        {
            return driverInstanceCount;
        }

        public void setDriverInstanceCount(int driverInstanceCount)
        {
            checkArgument(driverInstanceCount > 0, "driverInstanceCount must be > 0");
            if (this.driverInstanceCount.isPresent()) {
                checkState(this.driverInstanceCount.getAsInt() == driverInstanceCount, "driverInstance count already set to " + this.driverInstanceCount.getAsInt());
            }
            this.driverInstanceCount = OptionalInt.of(driverInstanceCount);
        }
    }

    private static class IndexSourceContext
    {
        private final SetMultimap<VariableReferenceExpression, Integer> indexLookupToProbeInput;

        public IndexSourceContext(SetMultimap<VariableReferenceExpression, Integer> indexLookupToProbeInput)
        {
            this.indexLookupToProbeInput = ImmutableSetMultimap.copyOf(requireNonNull(indexLookupToProbeInput, "indexLookupToProbeInput is null"));
        }

        private SetMultimap<VariableReferenceExpression, Integer> getIndexLookupToProbeInput()
        {
            return indexLookupToProbeInput;
        }
    }

    public static class LocalExecutionPlan
    {
        private final List<DriverFactory> driverFactories;
        private final List<PlanNodeId> tableScanSourceOrder;
        private final StageExecutionDescriptor stageExecutionDescriptor;

        public LocalExecutionPlan(List<DriverFactory> driverFactories, List<PlanNodeId> tableScanSourceOrder, StageExecutionDescriptor stageExecutionDescriptor)
        {
            this.driverFactories = ImmutableList.copyOf(requireNonNull(driverFactories, "driverFactories is null"));
            this.tableScanSourceOrder = ImmutableList.copyOf(requireNonNull(tableScanSourceOrder, "tableScanSourceOrder is null"));
            this.stageExecutionDescriptor = requireNonNull(stageExecutionDescriptor, "stageExecutionDescriptor is null");
        }

        public List<DriverFactory> getDriverFactories()
        {
            return driverFactories;
        }

        public List<PlanNodeId> getTableScanSourceOrder()
        {
            return tableScanSourceOrder;
        }

        public StageExecutionDescriptor getStageExecutionDescriptor()
        {
            return stageExecutionDescriptor;
        }
    }

    private class Visitor
            extends PlanVisitor<PhysicalOperation, LocalExecutionPlanContext>
    {
        private final Session session;
        private final StageExecutionDescriptor stageExecutionDescriptor;

        private Visitor(Session session, StageExecutionDescriptor stageExecutionDescriptor)
        {
            this.session = session;
            this.stageExecutionDescriptor = stageExecutionDescriptor;
        }

        @Override
        public PhysicalOperation visitRemoteSource(RemoteSourceNode node, LocalExecutionPlanContext context)
        {
            if (node.getOrderingScheme().isPresent()) {
                return createMergeSource(node, context);
            }

            return createRemoteSource(node, context);
        }

        private PhysicalOperation createMergeSource(RemoteSourceNode node, LocalExecutionPlanContext context)
        {
            checkArgument(node.getOrderingScheme().isPresent(), "orderingScheme is absent");

            // merging remote source must have a single driver
            context.setDriverInstanceCount(1);

            OrderingScheme orderingScheme = node.getOrderingScheme().get();
            ImmutableMap<Symbol, Integer> layout = makeLayout(node);
            List<Integer> sortChannels = getChannelsForVariables(orderingScheme.getOrderBy(), layout);
            List<SortOrder> sortOrder = orderingScheme.getOrderingList();

            List<Type> types = getSourceOperatorTypes(node, context.getTypes());
            ImmutableList<Integer> outputChannels = IntStream.range(0, types.size())
                    .boxed()
                    .collect(toImmutableList());

            OperatorFactory operatorFactory = new MergeOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    context.getTaskExchangeClientManager(),
                    new PagesSerdeFactory(blockEncodingSerde, isExchangeCompressionEnabled(session)),
                    orderingCompiler,
                    types,
                    outputChannels,
                    sortChannels,
                    sortOrder);

            return new PhysicalOperation(operatorFactory, makeLayout(node), context, UNGROUPED_EXECUTION);
        }

        private PhysicalOperation createRemoteSource(RemoteSourceNode node, LocalExecutionPlanContext context)
        {
            if (!context.getDriverInstanceCount().isPresent()) {
                context.setDriverInstanceCount(getTaskConcurrency(session));
            }

            OperatorFactory operatorFactory = new ExchangeOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    context.getTaskExchangeClientManager(),
                    new PagesSerdeFactory(blockEncodingSerde, isExchangeCompressionEnabled(session)));

            return new PhysicalOperation(operatorFactory, makeLayout(node), context, UNGROUPED_EXECUTION);
        }

        @Override
        public PhysicalOperation visitExplainAnalyze(ExplainAnalyzeNode node, LocalExecutionPlanContext context)
        {
            ExplainAnalyzeContext analyzeContext = explainAnalyzeContext
                    .orElseThrow(() -> new IllegalStateException("ExplainAnalyze can only run on coordinator"));
            PhysicalOperation source = node.getSource().accept(this, context);
            OperatorFactory operatorFactory = new ExplainAnalyzeOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    analyzeContext.getQueryPerformanceFetcher(),
                    metadata.getFunctionManager(),
                    node.isVerbose());
            return new PhysicalOperation(operatorFactory, makeLayout(node), context, source);
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

            Optional<Integer> hashChannel = node.getHashVariable().map(variableChannelGetter(source));
            OperatorFactory operatorFactory = new RowNumberOperator.RowNumberOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    source.getTypes(),
                    outputChannels.build(),
                    partitionChannels,
                    partitionTypes,
                    node.getMaxRowCountPerPartition(),
                    hashChannel,
                    10_000,
                    joinCompiler);
            return new PhysicalOperation(operatorFactory, outputMappings.build(), context, source);
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

            List<VariableReferenceExpression> orderByVariables = node.getOrderingScheme().getOrderBy();
            List<Integer> sortChannels = getChannelsForVariables(orderByVariables, source.getLayout());
            List<SortOrder> sortOrder = orderByVariables.stream()
                    .map(symbol -> node.getOrderingScheme().getOrdering(symbol))
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

            Optional<Integer> hashChannel = node.getHashVariable().map(variableChannelGetter(source));
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
                    1000,
                    joinCompiler);

            return new PhysicalOperation(operatorFactory, makeLayout(node), context, source);
        }

        @Override
        public PhysicalOperation visitWindow(WindowNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            List<Symbol> partitionBySymbols = node.getPartitionBy();
            List<Integer> partitionChannels = ImmutableList.copyOf(getChannelsForSymbols(partitionBySymbols, source.getLayout()));
            List<Integer> preGroupedChannels = ImmutableList.copyOf(getChannelsForSymbols(ImmutableList.copyOf(node.getPrePartitionedInputs()), source.getLayout()));

            List<Integer> sortChannels = ImmutableList.of();
            List<SortOrder> sortOrder = ImmutableList.of();

            if (node.getOrderingScheme().isPresent()) {
                OrderingScheme orderingScheme = node.getOrderingScheme().get();
                sortChannels = getChannelsForVariables(orderingScheme.getOrderBy(), source.getLayout());
                sortOrder = orderingScheme.getOrderingList();
            }

            ImmutableList.Builder<Integer> outputChannels = ImmutableList.builder();
            for (int i = 0; i < source.getTypes().size(); i++) {
                outputChannels.add(i);
            }

            ImmutableList.Builder<WindowFunctionDefinition> windowFunctionsBuilder = ImmutableList.builder();
            ImmutableList.Builder<VariableReferenceExpression> windowFunctionOutputVariablesBuilder = ImmutableList.builder();
            for (Map.Entry<VariableReferenceExpression, WindowNode.Function> entry : node.getWindowFunctions().entrySet()) {
                Optional<Integer> frameStartChannel = Optional.empty();
                Optional<Integer> frameEndChannel = Optional.empty();

                Frame frame = entry.getValue().getFrame();
                if (frame.getStartValue().isPresent()) {
                    frameStartChannel = Optional.of(source.getLayout().get(frame.getStartValue().get()));
                }
                if (frame.getEndValue().isPresent()) {
                    frameEndChannel = Optional.of(source.getLayout().get(frame.getEndValue().get()));
                }

                FrameInfo frameInfo = new FrameInfo(frame.getType(), frame.getStartType(), frameStartChannel, frame.getEndType(), frameEndChannel);

                CallExpression call = entry.getValue().getFunctionCall();
                FunctionHandle functionHandle = entry.getValue().getFunctionHandle();
                ImmutableList.Builder<Integer> arguments = ImmutableList.builder();
                for (RowExpression argument : call.getArguments()) {
                    checkState(argument instanceof VariableReferenceExpression);
                    Symbol argumentSymbol = new Symbol(((VariableReferenceExpression) argument).getName());
                    arguments.add(source.getLayout().get(argumentSymbol));
                }
                VariableReferenceExpression variable = entry.getKey();
                FunctionManager functionManager = metadata.getFunctionManager();
                WindowFunctionSupplier windowFunctionSupplier = functionManager.getWindowFunctionImplementation(functionHandle);
                Type type = metadata.getType(functionManager.getFunctionMetadata(functionHandle).getReturnType());
                windowFunctionsBuilder.add(window(windowFunctionSupplier, type, frameInfo, arguments.build()));
                windowFunctionOutputVariablesBuilder.add(variable);
            }

            List<VariableReferenceExpression> windowFunctionOutputVariables = windowFunctionOutputVariablesBuilder.build();

            // compute the layout of the output from the window operator
            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            for (Symbol symbol : node.getSource().getOutputSymbols()) {
                outputMappings.put(symbol, source.getLayout().get(symbol));
            }

            // window functions go in remaining channels starting after the last channel from the source operator, one per channel
            int channel = source.getTypes().size();
            for (VariableReferenceExpression variable : windowFunctionOutputVariables) {
                outputMappings.put(new Symbol(variable.getName()), channel);
                channel++;
            }

            OperatorFactory operatorFactory = new WindowOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    source.getTypes(),
                    outputChannels.build(),
                    windowFunctionsBuilder.build(),
                    partitionChannels,
                    preGroupedChannels,
                    sortChannels,
                    sortOrder,
                    node.getPreSortedOrderPrefix(),
                    10_000,
                    pagesIndexFactory);

            return new PhysicalOperation(operatorFactory, outputMappings.build(), context, source);
        }

        @Override
        public PhysicalOperation visitTopN(TopNNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            List<VariableReferenceExpression> orderByVariables = node.getOrderingScheme().getOrderBy();

            List<Integer> sortChannels = new ArrayList<>();
            List<SortOrder> sortOrders = new ArrayList<>();
            for (VariableReferenceExpression variable : orderByVariables) {
                sortChannels.add(source.getLayout().get(new Symbol(variable.getName())));
                sortOrders.add(node.getOrderingScheme().getOrdering(variable));
            }

            OperatorFactory operator = new TopNOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    source.getTypes(),
                    (int) node.getCount(),
                    sortChannels,
                    sortOrders);

            return new PhysicalOperation(operator, source.getLayout(), context, source);
        }

        @Override
        public PhysicalOperation visitSort(SortNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            List<VariableReferenceExpression> orderByVariables = node.getOrderingScheme().getOrderBy();

            List<Integer> orderByChannels = getChannelsForVariables(orderByVariables, source.getLayout());

            ImmutableList.Builder<SortOrder> sortOrder = ImmutableList.builder();
            for (VariableReferenceExpression variable : orderByVariables) {
                sortOrder.add(node.getOrderingScheme().getOrdering(variable));
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
                    sortOrder.build(),
                    pagesIndexFactory);

            return new PhysicalOperation(operator, source.getLayout(), context, source);
        }

        @Override
        public PhysicalOperation visitLimit(LimitNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            OperatorFactory operatorFactory = new LimitOperatorFactory(context.getNextOperatorId(), node.getId(), node.getCount());
            return new PhysicalOperation(operatorFactory, source.getLayout(), context, source);
        }

        @Override
        public PhysicalOperation visitDistinctLimit(DistinctLimitNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            Optional<Integer> hashChannel = node.getHashVariable().map(variableChannelGetter(source));
            List<Integer> distinctChannels = getChannelsForSymbols(node.getDistinctSymbols(), source.getLayout());

            OperatorFactory operatorFactory = new DistinctLimitOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    source.getTypes(),
                    distinctChannels,
                    node.getLimit(),
                    hashChannel,
                    joinCompiler);
            return new PhysicalOperation(operatorFactory, makeLayout(node), context, source);
        }

        @Override
        public PhysicalOperation visitGroupId(GroupIdNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);
            Map<Symbol, Integer> newLayout = new HashMap<>();
            ImmutableList.Builder<Type> outputTypes = ImmutableList.builder();

            int outputChannel = 0;

            for (Symbol output : node.getGroupingSets().stream().flatMap(Collection::stream).collect(Collectors.toSet())) {
                newLayout.put(output, outputChannel++);
                outputTypes.add(source.getTypes().get(source.getLayout().get(node.getGroupingColumns().get(output))));
            }

            Map<Symbol, Integer> argumentMappings = new HashMap<>();
            for (Symbol output : node.getAggregationArguments()) {
                int inputChannel = source.getLayout().get(output);

                newLayout.put(output, outputChannel++);
                outputTypes.add(source.getTypes().get(inputChannel));
                argumentMappings.put(output, inputChannel);
            }

            // for every grouping set, create a mapping of all output to input channels (including arguments)
            ImmutableList.Builder<Map<Integer, Integer>> mappings = ImmutableList.builder();
            for (List<Symbol> groupingSet : node.getGroupingSets()) {
                ImmutableMap.Builder<Integer, Integer> setMapping = ImmutableMap.builder();

                for (Symbol output : groupingSet) {
                    setMapping.put(newLayout.get(output), source.getLayout().get(node.getGroupingColumns().get(output)));
                }

                for (Symbol output : argumentMappings.keySet()) {
                    setMapping.put(newLayout.get(output), argumentMappings.get(output));
                }

                mappings.add(setMapping.build());
            }

            newLayout.put(node.getGroupIdSymbol(), outputChannel);
            outputTypes.add(BIGINT);

            OperatorFactory groupIdOperatorFactory = new GroupIdOperator.GroupIdOperatorFactory(context.getNextOperatorId(),
                    node.getId(),
                    outputTypes.build(),
                    mappings.build());

            return new PhysicalOperation(groupIdOperatorFactory, newLayout, context, source);
        }

        @Override
        public PhysicalOperation visitAggregation(AggregationNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            if (node.getGroupingKeys().isEmpty()) {
                return planGlobalAggregation(node, source, context);
            }

            boolean spillEnabled = isSpillEnabled(context.getSession());
            DataSize unspillMemoryLimit = getAggregationOperatorUnspillMemoryLimit(context.getSession());

            return planGroupByAggregation(node, source, spillEnabled, unspillMemoryLimit, context);
        }

        @Override
        public PhysicalOperation visitMarkDistinct(MarkDistinctNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            List<Integer> channels = getChannelsForSymbols(node.getDistinctSymbols(), source.getLayout());
            Optional<Integer> hashChannel = node.getHashVariable().map(variableChannelGetter(source));
            MarkDistinctOperatorFactory operator = new MarkDistinctOperatorFactory(context.getNextOperatorId(), node.getId(), source.getTypes(), channels, hashChannel, joinCompiler);
            return new PhysicalOperation(operator, makeLayout(node), context, source);
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

            RowExpression filterExpression = node.getPredicate();
            List<Symbol> outputSymbols = node.getOutputSymbols();

            return visitScanFilterAndProject(context, node.getId(), sourceNode, Optional.of(filterExpression), Assignments.identity(outputSymbols), outputSymbols);
        }

        @Override
        public PhysicalOperation visitProject(ProjectNode node, LocalExecutionPlanContext context)
        {
            PlanNode sourceNode;
            Optional<RowExpression> filterExpression = Optional.empty();
            if (node.getSource() instanceof FilterNode) {
                FilterNode filterNode = (FilterNode) node.getSource();
                sourceNode = filterNode.getSource();
                filterExpression = Optional.of(filterNode.getPredicate());
            }
            else {
                sourceNode = node.getSource();
            }

            List<Symbol> outputSymbols = node.getOutputSymbols();

            return visitScanFilterAndProject(context, node.getId(), sourceNode, filterExpression, node.getAssignments(), outputSymbols);
        }

        // TODO: This should be refactored, so that there's an optimizer that merges scan-filter-project into a single PlanNode
        private PhysicalOperation visitScanFilterAndProject(
                LocalExecutionPlanContext context,
                PlanNodeId planNodeId,
                PlanNode sourceNode,
                Optional<RowExpression> filterExpression,
                Assignments assignments,
                List<Symbol> outputSymbols)
        {
            // if source is a table scan we fold it directly into the filter and project
            // otherwise we plan it as a normal operator
            Map<Symbol, Integer> sourceLayout;
            List<ColumnHandle> columns = null;
            PhysicalOperation source = null;
            if (sourceNode instanceof TableScanNode) {
                TableScanNode tableScanNode = (TableScanNode) sourceNode;

                // extract the column handles and channel to type mapping
                sourceLayout = new LinkedHashMap<>();
                columns = new ArrayList<>();
                int channel = 0;
                for (Symbol symbol : tableScanNode.getOutputSymbols()) {
                    columns.add(tableScanNode.getAssignments().get(symbol));

                    Integer input = channel;
                    sourceLayout.put(symbol, input);

                    Type type = requireNonNull(context.getTypes().get(symbol), format("No type for symbol %s", symbol));

                    channel++;
                }
            }
            else {
                // plan source
                source = sourceNode.accept(this, context);
                sourceLayout = source.getLayout();
            }

            // filterExpression may contain large function calls; evaluate them before compiling.
            if (filterExpression.isPresent()) {
                // TODO: theoretically, filterExpression could be a constant value (true or false) after optimization; we could possibly optimize the execution.
                filterExpression = Optional.of(bindChannels(filterExpression.get(), sourceLayout));
            }

            // build output mapping
            ImmutableMap.Builder<Symbol, Integer> outputMappingsBuilder = ImmutableMap.builder();
            for (int i = 0; i < outputSymbols.size(); i++) {
                Symbol symbol = outputSymbols.get(i);
                outputMappingsBuilder.put(symbol, i);
            }
            Map<Symbol, Integer> outputMappings = outputMappingsBuilder.build();

            // compiler uses inputs instead of symbols, so rewrite the expressions first

            List<Expression> projections = new ArrayList<>();
            for (Symbol symbol : outputSymbols) {
                projections.add(assignments.get(symbol));
            }

            Map<NodeRef<Expression>, Type> expressionTypes = getExpressionTypes(
                    context.getSession(),
                    metadata,
                    sqlParser,
                    context.getTypes(),
                    concat(assignments.getExpressions()),
                    emptyList(),
                    NOOP,
                    false);

            List<RowExpression> translatedProjections = projections.stream()
                    .map(expression -> toRowExpression(expression, expressionTypes, sourceLayout))
                    .collect(toImmutableList());

            try {
                if (columns != null) {
                    Supplier<CursorProcessor> cursorProcessor = expressionCompiler.compileCursorProcessor(filterExpression, translatedProjections, sourceNode.getId());
                    Supplier<PageProcessor> pageProcessor = expressionCompiler.compilePageProcessor(filterExpression, translatedProjections, Optional.of(context.getStageId() + "_" + planNodeId));

                    SourceOperatorFactory operatorFactory = new ScanFilterAndProjectOperatorFactory(
                            context.getNextOperatorId(),
                            planNodeId,
                            sourceNode.getId(),
                            pageSourceProvider,
                            cursorProcessor,
                            pageProcessor,
                            columns,
                            getTypes(projections, expressionTypes),
                            getFilterAndProjectMinOutputPageSize(session),
                            getFilterAndProjectMinOutputPageRowCount(session));

                    return new PhysicalOperation(operatorFactory, outputMappings, context, stageExecutionDescriptor.isScanGroupedExecution(sourceNode.getId()) ? GROUPED_EXECUTION : UNGROUPED_EXECUTION);
                }
                else {
                    Supplier<PageProcessor> pageProcessor = expressionCompiler.compilePageProcessor(filterExpression, translatedProjections, Optional.of(context.getStageId() + "_" + planNodeId));

                    OperatorFactory operatorFactory = new FilterAndProjectOperator.FilterAndProjectOperatorFactory(
                            context.getNextOperatorId(),
                            planNodeId,
                            pageProcessor,
                            getTypes(projections, expressionTypes),
                            getFilterAndProjectMinOutputPageSize(session),
                            getFilterAndProjectMinOutputPageRowCount(session));

                    return new PhysicalOperation(operatorFactory, outputMappings, context, source);
                }
            }
            catch (RuntimeException e) {
                throw new PrestoException(COMPILER_ERROR, "Compiler failed", e);
            }
        }

        // TODO: migrate `toRowExpression` to `bindChannels`
        private RowExpression toRowExpression(Expression expression, Map<NodeRef<Expression>, Type> types, Map<Symbol, Integer> sourceLayout)
        {
            return SqlToRowExpressionTranslator.translate(expression, types, sourceLayout, metadata.getFunctionManager(), metadata.getTypeManager(), session, true);
        }

        private RowExpression bindChannels(RowExpression expression, Map<Symbol, Integer> sourceLayout)
        {
            Type type = expression.getType();
            Object value = new RowExpressionInterpreter(expression, metadata, session.toConnectorSession(), true).optimize();
            if (value instanceof RowExpression) {
                RowExpression optimized = (RowExpression) value;
                // building channel info
                expression = SymbolToChannelTranslator.translate(optimized, sourceLayout);
            }
            else {
                expression = constant(value, type);
            }
            return expression;
        }

        @Override
        public PhysicalOperation visitTableScan(TableScanNode node, LocalExecutionPlanContext context)
        {
            List<ColumnHandle> columns = new ArrayList<>();
            for (Symbol symbol : node.getOutputSymbols()) {
                columns.add(node.getAssignments().get(symbol));
            }

            OperatorFactory operatorFactory = new TableScanOperatorFactory(context.getNextOperatorId(), node.getId(), pageSourceProvider, columns);
            return new PhysicalOperation(operatorFactory, makeLayout(node), context, stageExecutionDescriptor.isScanGroupedExecution(node.getId()) ? GROUPED_EXECUTION : UNGROUPED_EXECUTION);
        }

        @Override
        public PhysicalOperation visitValues(ValuesNode node, LocalExecutionPlanContext context)
        {
            // a values node must have a single driver
            context.setDriverInstanceCount(1);

            if (node.getRows().isEmpty()) {
                OperatorFactory operatorFactory = new ValuesOperatorFactory(context.getNextOperatorId(), node.getId(), ImmutableList.of());
                return new PhysicalOperation(operatorFactory, makeLayout(node), context, UNGROUPED_EXECUTION);
            }

            List<Type> outputTypes = getSymbolTypes(node.getOutputSymbols(), context.getTypes());
            PageBuilder pageBuilder = new PageBuilder(node.getRows().size(), outputTypes);
            for (List<RowExpression> row : node.getRows()) {
                pageBuilder.declarePosition();
                for (int i = 0; i < row.size(); i++) {
                    // evaluate the literal value
                    Object result = rowExpressionInterpreter(row.get(i), metadata, context.getSession().toConnectorSession()).evaluate();
                    writeNativeValue(outputTypes.get(i), pageBuilder.getBlockBuilder(i), result);
                }
            }

            OperatorFactory operatorFactory = new ValuesOperatorFactory(context.getNextOperatorId(), node.getId(), ImmutableList.of(pageBuilder.build()));
            return new PhysicalOperation(operatorFactory, makeLayout(node), context, UNGROUPED_EXECUTION);
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
            return new PhysicalOperation(operatorFactory, outputMappings.build(), context, source);
        }

        private ImmutableMap<Symbol, Integer> makeLayout(PlanNode node)
        {
            return makeLayoutFromOutputSymbols(node.getOutputSymbols());
        }

        private ImmutableMap<Symbol, Integer> makeLayoutFromOutputSymbols(List<Symbol> outputSymbols)
        {
            Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            int channel = 0;
            for (Symbol symbol : outputSymbols) {
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

            SetMultimap<VariableReferenceExpression, Integer> indexLookupToProbeInput = indexSourceContext.getIndexLookupToProbeInput();
            checkState(indexLookupToProbeInput.keySet().equals(node.getLookupVariables()));

            // Finalize the symbol lookup layout for the index source
            List<Symbol> lookupSymbolSchema = node.getLookupVariables().stream()
                    .map(VariableReferenceExpression::getName)
                    .map(Symbol::new)
                    .collect(toImmutableList());

            // Identify how to remap the probe key Input to match the source index lookup layout
            ImmutableList.Builder<Integer> remappedProbeKeyChannelsBuilder = ImmutableList.builder();
            // Identify overlapping fields that can produce the same lookup symbol.
            // We will filter incoming keys to ensure that overlapping fields will have the same value.
            ImmutableList.Builder<Set<Integer>> overlappingFieldSetsBuilder = ImmutableList.builder();
            for (VariableReferenceExpression lookupVariable : node.getLookupVariables()) {
                Set<Integer> potentialProbeInputs = indexLookupToProbeInput.get(lookupVariable);
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
                    recordSet = new FieldSetFilteringRecordSet(metadata.getFunctionManager(), recordSet, overlappingFieldSets);
                }
                return new MappedRecordSet(recordSet, remappedProbeKeyChannels);
            };

            // Declare the input and output schemas for the index and acquire the actual Index
            List<ColumnHandle> lookupSchema = Lists.transform(lookupSymbolSchema, forMap(node.getAssignments()));
            List<ColumnHandle> outputSchema = Lists.transform(node.getOutputSymbols(), forMap(node.getAssignments()));
            ConnectorIndex index = indexManager.getIndex(session, node.getIndexHandle(), lookupSchema, outputSchema);

            OperatorFactory operatorFactory = new IndexSourceOperator.IndexSourceOperatorFactory(context.getNextOperatorId(), node.getId(), index, probeKeyNormalizer);
            return new PhysicalOperation(operatorFactory, makeLayout(node), context, UNGROUPED_EXECUTION);
        }

        /**
         * This method creates a mapping from each index source lookup symbol (directly applied to the index)
         * to the corresponding probe key Input
         */
        private SetMultimap<VariableReferenceExpression, Integer> mapIndexSourceLookupSymbolToProbeKeyInput(IndexJoinNode node, Map<VariableReferenceExpression, Integer> probeKeyLayout)
        {
            Set<VariableReferenceExpression> indexJoinVariables = node.getCriteria().stream()
                    .map(IndexJoinNode.EquiJoinClause::getIndex)
                    .collect(toImmutableSet());

            // Trace the index join symbols to the index source lookup symbols
            // Map: Index join symbol => Index source lookup symbol
            Map<VariableReferenceExpression, VariableReferenceExpression> indexKeyTrace = IndexJoinOptimizer.IndexKeyTracer.trace(node.getIndexSource(), indexJoinVariables);

            // Map the index join symbols to the probe key Input
            Multimap<VariableReferenceExpression, Integer> indexToProbeKeyInput = HashMultimap.create();
            for (IndexJoinNode.EquiJoinClause clause : node.getCriteria()) {
                indexToProbeKeyInput.put(clause.getIndex(), probeKeyLayout.get(clause.getProbe()));
            }

            // Create the mapping from index source look up symbol to probe key Input
            ImmutableSetMultimap.Builder<VariableReferenceExpression, Integer> builder = ImmutableSetMultimap.builder();
            for (Map.Entry<VariableReferenceExpression, VariableReferenceExpression> entry : indexKeyTrace.entrySet()) {
                VariableReferenceExpression indexJoinVariable = entry.getKey();
                VariableReferenceExpression indexLookupVariable = entry.getValue();
                builder.putAll(indexJoinVariable, indexToProbeKeyInput.get(indexLookupVariable));
            }
            return builder.build();
        }

        @Override
        public PhysicalOperation visitIndexJoin(IndexJoinNode node, LocalExecutionPlanContext context)
        {
            List<IndexJoinNode.EquiJoinClause> clauses = node.getCriteria();

            List<VariableReferenceExpression> probeVariables = clauses.stream().map(IndexJoinNode.EquiJoinClause::getProbe).collect(toImmutableList());
            List<Symbol> probeSymbols = probeVariables.stream().map(VariableReferenceExpression::getName).map(Symbol::new).collect(toImmutableList());
            List<VariableReferenceExpression> indexVariables = clauses.stream().map(IndexJoinNode.EquiJoinClause::getIndex).collect(toImmutableList());
            List<Symbol> indexSymbols = indexVariables.stream().map(VariableReferenceExpression::getName).map(Symbol::new).collect(toImmutableList());

            // Plan probe side
            PhysicalOperation probeSource = node.getProbeSource().accept(this, context);
            List<Integer> probeChannels = getChannelsForSymbols(probeSymbols, probeSource.getLayout());
            OptionalInt probeHashChannel = node.getProbeHashVariable().map(variableChannelGetter(probeSource))
                    .map(OptionalInt::of).orElse(OptionalInt.empty());

            // The probe key channels will be handed to the index according to probeSymbol order
            Map<VariableReferenceExpression, Integer> probeKeyLayout = new HashMap<>();
            for (int i = 0; i < probeVariables.size(); i++) {
                // Duplicate symbols can appear and we only need to take take one of the Inputs
                probeKeyLayout.put(probeVariables.get(i), i);
            }

            // Plan the index source side
            SetMultimap<VariableReferenceExpression, Integer> indexLookupToProbeInput = mapIndexSourceLookupSymbolToProbeKeyInput(node, probeKeyLayout);
            LocalExecutionPlanContext indexContext = context.createIndexSourceSubContext(new IndexSourceContext(indexLookupToProbeInput));
            PhysicalOperation indexSource = node.getIndexSource().accept(this, indexContext);
            List<Integer> indexOutputChannels = getChannelsForSymbols(indexSymbols, indexSource.getLayout());
            OptionalInt indexHashChannel = node.getIndexHashVariable().map(variableChannelGetter(indexSource))
                    .map(OptionalInt::of).orElse(OptionalInt.empty());

            // Identify just the join keys/channels needed for lookup by the index source (does not have to use all of them).
            Set<VariableReferenceExpression> indexVariablesNeededBySource = IndexJoinOptimizer.IndexKeyTracer.trace(node.getIndexSource(), ImmutableSet.copyOf(indexVariables)).keySet();

            Set<Integer> lookupSourceInputChannels = node.getCriteria().stream()
                    .filter(equiJoinClause -> indexVariablesNeededBySource.contains(equiJoinClause.getIndex()))
                    .map(IndexJoinNode.EquiJoinClause::getProbe)
                    .map(probeKeyLayout::get)
                    .collect(toImmutableSet());

            Optional<DynamicTupleFilterFactory> dynamicTupleFilterFactory = Optional.empty();
            if (lookupSourceInputChannels.size() < probeKeyLayout.values().size()) {
                int[] nonLookupInputChannels = Ints.toArray(node.getCriteria().stream()
                        .filter(equiJoinClause -> !indexVariablesNeededBySource.contains(equiJoinClause.getIndex()))
                        .map(IndexJoinNode.EquiJoinClause::getProbe)
                        .map(probeKeyLayout::get)
                        .collect(toImmutableList()));
                int[] nonLookupOutputChannels = Ints.toArray(node.getCriteria().stream()
                        .filter(equiJoinClause -> !indexVariablesNeededBySource.contains(equiJoinClause.getIndex()))
                        .map(IndexJoinNode.EquiJoinClause::getIndex)
                        .map(variable -> indexSource.getLayout().get(new Symbol(variable.getName())))
                        .collect(toImmutableList()));

                int filterOperatorId = indexContext.getNextOperatorId();
                dynamicTupleFilterFactory = Optional.of(new DynamicTupleFilterFactory(
                        filterOperatorId,
                        node.getId(),
                        nonLookupInputChannels,
                        nonLookupOutputChannels,
                        indexSource.getTypes(),
                        pageFunctionCompiler));
            }

            IndexBuildDriverFactoryProvider indexBuildDriverFactoryProvider = new IndexBuildDriverFactoryProvider(
                    indexContext.getNextPipelineId(),
                    indexContext.getNextOperatorId(),
                    node.getId(),
                    indexContext.isInputDriver(),
                    indexSource.getTypes(),
                    indexSource.getOperatorFactories(),
                    dynamicTupleFilterFactory);

            IndexLookupSourceFactory indexLookupSourceFactory = new IndexLookupSourceFactory(
                    lookupSourceInputChannels,
                    indexOutputChannels,
                    indexHashChannel,
                    indexSource.getTypes(),
                    indexSource.getLayout(),
                    indexBuildDriverFactoryProvider,
                    maxIndexMemorySize,
                    indexJoinLookupStats,
                    SystemSessionProperties.isShareIndexLoading(session),
                    pagesIndexFactory,
                    joinCompiler);

            verify(probeSource.getPipelineExecutionStrategy() == UNGROUPED_EXECUTION);
            verify(indexSource.getPipelineExecutionStrategy() == UNGROUPED_EXECUTION);
            JoinBridgeManager<LookupSourceFactory> lookupSourceFactoryManager = new JoinBridgeManager<>(
                    false,
                    UNGROUPED_EXECUTION,
                    UNGROUPED_EXECUTION,
                    lifespan -> indexLookupSourceFactory,
                    indexLookupSourceFactory.getOutputTypes());

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
            OptionalInt totalOperatorsCount = getJoinOperatorsCountForSpill(context, session);
            switch (node.getType()) {
                case INNER:
                    lookupJoinOperatorFactory = lookupJoinOperators.innerJoin(context.getNextOperatorId(), node.getId(), lookupSourceFactoryManager, probeSource.getTypes(), probeChannels, probeHashChannel, Optional.empty(), totalOperatorsCount, partitioningSpillerFactory);
                    break;
                case SOURCE_OUTER:
                    lookupJoinOperatorFactory = lookupJoinOperators.probeOuterJoin(context.getNextOperatorId(), node.getId(), lookupSourceFactoryManager, probeSource.getTypes(), probeChannels, probeHashChannel, Optional.empty(), totalOperatorsCount, partitioningSpillerFactory);
                    break;
                default:
                    throw new AssertionError("Unknown type: " + node.getType());
            }
            return new PhysicalOperation(lookupJoinOperatorFactory, outputMappings.build(), context, probeSource);
        }

        @Override
        public PhysicalOperation visitJoin(JoinNode node, LocalExecutionPlanContext context)
        {
            if (node.isCrossJoin()) {
                return createNestedLoopJoin(node, context);
            }

            List<JoinNode.EquiJoinClause> clauses = node.getCriteria();
            List<VariableReferenceExpression> leftVariables = Lists.transform(clauses, JoinNode.EquiJoinClause::getLeft);
            List<VariableReferenceExpression> rightVariables = Lists.transform(clauses, JoinNode.EquiJoinClause::getRight);

            switch (node.getType()) {
                case INNER:
                case LEFT:
                case RIGHT:
                case FULL:
                    return createLookupJoin(node, node.getLeft(), leftVariables, node.getLeftHashVariable(), node.getRight(), rightVariables, node.getRightHashVariable(), context);
                default:
                    throw new UnsupportedOperationException("Unsupported join type: " + node.getType());
            }
        }

        @Override
        public PhysicalOperation visitSpatialJoin(SpatialJoinNode node, LocalExecutionPlanContext context)
        {
            RowExpression filterExpression = node.getFilter();
            List<CallExpression> spatialFunctions = extractSupportedSpatialFunctions(filterExpression, metadata.getFunctionManager());
            for (CallExpression spatialFunction : spatialFunctions) {
                Optional<PhysicalOperation> operation = tryCreateSpatialJoin(context, node, removeExpressionFromFilter(filterExpression, spatialFunction), spatialFunction, Optional.empty(), Optional.empty());
                if (operation.isPresent()) {
                    return operation.get();
                }
            }

            List<CallExpression> spatialComparisons = extractSupportedSpatialComparisons(filterExpression, metadata.getFunctionManager());
            for (CallExpression spatialComparison : spatialComparisons) {
                FunctionMetadata functionMetadata = metadata.getFunctionManager().getFunctionMetadata(spatialComparison.getFunctionHandle());
                checkArgument(functionMetadata.getOperatorType().isPresent() && functionMetadata.getOperatorType().get().isComparisonOperator());
                if (functionMetadata.getOperatorType().get() == OperatorType.LESS_THAN || functionMetadata.getOperatorType().get() == OperatorType.LESS_THAN_OR_EQUAL) {
                    // ST_Distance(a, b) <= r
                    RowExpression radius = spatialComparison.getArguments().get(1);
                    if (radius instanceof VariableReferenceExpression && node.getRight().getOutputSymbols().contains(new Symbol(((VariableReferenceExpression) radius).getName()))) {
                        CallExpression spatialFunction = (CallExpression) spatialComparison.getArguments().get(0);
                        Optional<PhysicalOperation> operation = tryCreateSpatialJoin(
                                context,
                                node,
                                removeExpressionFromFilter(filterExpression, spatialComparison),
                                spatialFunction,
                                Optional.of((VariableReferenceExpression) radius),
                                functionMetadata.getOperatorType());
                        if (operation.isPresent()) {
                            return operation.get();
                        }
                    }
                }
            }

            throw new VerifyException("No valid spatial relationship found for spatial join");
        }

        private Optional<PhysicalOperation> tryCreateSpatialJoin(
                LocalExecutionPlanContext context,
                SpatialJoinNode node,
                Optional<RowExpression> filterExpression,
                CallExpression spatialFunction,
                Optional<VariableReferenceExpression> radius,
                Optional<OperatorType> comparisonOperator)
        {
            List<RowExpression> arguments = spatialFunction.getArguments();
            verify(arguments.size() == 2);

            if (!(arguments.get(0) instanceof VariableReferenceExpression) || !(arguments.get(1) instanceof VariableReferenceExpression)) {
                return Optional.empty();
            }

            VariableReferenceExpression firstSymbol = (VariableReferenceExpression) arguments.get(0);
            VariableReferenceExpression secondSymbol = (VariableReferenceExpression) arguments.get(1);

            PlanNode probeNode = node.getLeft();
            Set<SymbolReference> probeSymbols = getSymbolReferences(probeNode.getOutputSymbols());

            PlanNode buildNode = node.getRight();
            Set<SymbolReference> buildSymbols = getSymbolReferences(buildNode.getOutputSymbols());

            if (probeSymbols.contains(new SymbolReference(firstSymbol.getName())) && buildSymbols.contains(new SymbolReference(secondSymbol.getName()))) {
                return Optional.of(createSpatialLookupJoin(
                        node,
                        probeNode,
                        new Symbol(firstSymbol.getName()),
                        buildNode,
                        new Symbol(secondSymbol.getName()),
                        radius.map(r -> new Symbol(r.getName())),
                        spatialTest(spatialFunction, true, comparisonOperator),
                        filterExpression,
                        context));
            }
            else if (probeSymbols.contains(new SymbolReference(secondSymbol.getName())) && buildSymbols.contains(new SymbolReference(firstSymbol.getName()))) {
                return Optional.of(createSpatialLookupJoin(
                        node,
                        probeNode,
                        new Symbol(secondSymbol.getName()),
                        buildNode,
                        new Symbol(firstSymbol.getName()),
                        radius.map(r -> new Symbol(r.getName())),
                        spatialTest(spatialFunction, false, comparisonOperator),
                        filterExpression,
                        context));
            }
            return Optional.empty();
        }

        private Optional<RowExpression> removeExpressionFromFilter(RowExpression filter, RowExpression expression)
        {
            RowExpression updatedJoinFilter = replaceExpression(filter, ImmutableMap.of(expression, TRUE));
            return updatedJoinFilter == TRUE ? Optional.empty() : Optional.of(updatedJoinFilter);
        }

        private SpatialPredicate spatialTest(CallExpression functionCall, boolean probeFirst, Optional<OperatorType> comparisonOperator)
        {
            String functionName = metadata.getFunctionManager().getFunctionMetadata(functionCall.getFunctionHandle()).getName();
            switch (functionName.toLowerCase(Locale.ENGLISH)) {
                case ST_CONTAINS:
                    if (probeFirst) {
                        return (buildGeometry, probeGeometry, radius) -> probeGeometry.contains(buildGeometry);
                    }
                    else {
                        return (buildGeometry, probeGeometry, radius) -> buildGeometry.contains(probeGeometry);
                    }
                case ST_WITHIN:
                    if (probeFirst) {
                        return (buildGeometry, probeGeometry, radius) -> probeGeometry.within(buildGeometry);
                    }
                    else {
                        return (buildGeometry, probeGeometry, radius) -> buildGeometry.within(probeGeometry);
                    }
                case ST_INTERSECTS:
                    return (buildGeometry, probeGeometry, radius) -> buildGeometry.intersects(probeGeometry);
                case ST_DISTANCE:
                    if (comparisonOperator.get() == OperatorType.LESS_THAN) {
                        return (buildGeometry, probeGeometry, radius) -> buildGeometry.distance(probeGeometry) < radius.getAsDouble();
                    }
                    else if (comparisonOperator.get() == OperatorType.LESS_THAN_OR_EQUAL) {
                        return (buildGeometry, probeGeometry, radius) -> buildGeometry.distance(probeGeometry) <= radius.getAsDouble();
                    }
                    else {
                        throw new UnsupportedOperationException("Unsupported comparison operator: " + comparisonOperator.get());
                    }
                default:
                    throw new UnsupportedOperationException("Unsupported spatial function: " + functionName);
            }
        }

        private Set<SymbolReference> getSymbolReferences(Collection<Symbol> symbols)
        {
            return symbols.stream().map(Symbol::toSymbolReference).collect(toImmutableSet());
        }

        private PhysicalOperation createNestedLoopJoin(JoinNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation probeSource = node.getLeft().accept(this, context);

            LocalExecutionPlanContext buildContext = context.createSubContext();
            PhysicalOperation buildSource = node.getRight().accept(this, buildContext);

            checkState(
                    buildSource.getPipelineExecutionStrategy() == UNGROUPED_EXECUTION,
                    "Build source of a nested loop join is expected to be GROUPED_EXECUTION.");
            checkArgument(node.getType() == INNER, "NestedLoopJoin is only used for inner join");

            JoinBridgeManager<NestedLoopJoinBridge> nestedLoopJoinBridgeManager = new JoinBridgeManager<>(
                    false,
                    probeSource.getPipelineExecutionStrategy(),
                    buildSource.getPipelineExecutionStrategy(),
                    lifespan -> new NestedLoopJoinPagesSupplier(),
                    buildSource.getTypes());
            NestedLoopBuildOperatorFactory nestedLoopBuildOperatorFactory = new NestedLoopBuildOperatorFactory(
                    buildContext.getNextOperatorId(),
                    node.getId(),
                    nestedLoopJoinBridgeManager);

            checkArgument(buildContext.getDriverInstanceCount().orElse(1) == 1, "Expected local execution to not be parallel");
            context.addDriverFactory(
                    buildContext.isInputDriver(),
                    false,
                    ImmutableList.<OperatorFactory>builder()
                            .addAll(buildSource.getOperatorFactories())
                            .add(nestedLoopBuildOperatorFactory)
                            .build(),
                    buildContext.getDriverInstanceCount(),
                    buildSource.getPipelineExecutionStrategy());

            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            outputMappings.putAll(probeSource.getLayout());

            // inputs from build side of the join are laid out following the input from the probe side,
            // so adjust the channel ids but keep the field layouts intact
            int offset = probeSource.getTypes().size();
            for (Map.Entry<Symbol, Integer> entry : buildSource.getLayout().entrySet()) {
                outputMappings.put(entry.getKey(), offset + entry.getValue());
            }

            OperatorFactory operatorFactory = new NestedLoopJoinOperatorFactory(context.getNextOperatorId(), node.getId(), nestedLoopJoinBridgeManager);
            return new PhysicalOperation(operatorFactory, outputMappings.build(), context, probeSource);
        }

        private PhysicalOperation createSpatialLookupJoin(
                SpatialJoinNode node,
                PlanNode probeNode,
                Symbol probeSymbol,
                PlanNode buildNode,
                Symbol buildSymbol,
                Optional<Symbol> radiusSymbol,
                SpatialPredicate spatialRelationshipTest,
                Optional<RowExpression> joinFilter,
                LocalExecutionPlanContext context)
        {
            // Plan probe
            PhysicalOperation probeSource = probeNode.accept(this, context);

            // Plan build
            PagesSpatialIndexFactory pagesSpatialIndexFactory = createPagesSpatialIndexFactory(node,
                    buildNode,
                    buildSymbol,
                    radiusSymbol,
                    probeSource.getLayout(),
                    spatialRelationshipTest,
                    joinFilter,
                    context);

            OperatorFactory operator = createSpatialLookupJoin(node, probeNode, probeSource, probeSymbol, pagesSpatialIndexFactory, context);

            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            List<Symbol> outputSymbols = node.getOutputSymbols();
            for (int i = 0; i < outputSymbols.size(); i++) {
                Symbol symbol = outputSymbols.get(i);
                outputMappings.put(symbol, i);
            }

            return new PhysicalOperation(operator, outputMappings.build(), context, probeSource);
        }

        private OperatorFactory createSpatialLookupJoin(SpatialJoinNode node,
                PlanNode probeNode,
                PhysicalOperation probeSource,
                Symbol probeSymbol,
                PagesSpatialIndexFactory pagesSpatialIndexFactory,
                LocalExecutionPlanContext context)
        {
            List<Type> probeTypes = probeSource.getTypes();
            List<Symbol> probeOutputSymbols = node.getOutputSymbols().stream()
                    .filter(symbol -> probeNode.getOutputSymbols().contains(symbol))
                    .collect(toImmutableList());
            List<Integer> probeOutputChannels = ImmutableList.copyOf(getChannelsForSymbols(probeOutputSymbols, probeSource.getLayout()));
            Function<Symbol, Integer> probeChannelGetter = symbolChannelGetter(probeSource);
            int probeChannel = probeChannelGetter.apply(probeSymbol);

            Optional<Integer> partitionChannel = node.getLeftPartitionSymbol().map(probeChannelGetter::apply);

            return new SpatialJoinOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    node.getType(),
                    probeTypes,
                    probeOutputChannels,
                    probeChannel,
                    partitionChannel,
                    pagesSpatialIndexFactory);
        }

        private PagesSpatialIndexFactory createPagesSpatialIndexFactory(
                SpatialJoinNode node,
                PlanNode buildNode,
                Symbol buildSymbol,
                Optional<Symbol> radiusSymbol,
                Map<Symbol, Integer> probeLayout,
                SpatialPredicate spatialRelationshipTest,
                Optional<RowExpression> joinFilter,
                LocalExecutionPlanContext context)
        {
            LocalExecutionPlanContext buildContext = context.createSubContext();
            PhysicalOperation buildSource = buildNode.accept(this, buildContext);
            List<Symbol> buildOutputSymbols = node.getOutputSymbols().stream()
                    .filter(symbol -> buildNode.getOutputSymbols().contains(symbol))
                    .collect(toImmutableList());
            Map<Symbol, Integer> buildLayout = buildSource.getLayout();
            List<Integer> buildOutputChannels = ImmutableList.copyOf(getChannelsForSymbols(buildOutputSymbols, buildLayout));
            Function<Symbol, Integer> buildChannelGetter = symbolChannelGetter(buildSource);
            Integer buildChannel = buildChannelGetter.apply(buildSymbol);
            Optional<Integer> radiusChannel = radiusSymbol.map(buildChannelGetter::apply);

            Optional<JoinFilterFunctionFactory> filterFunctionFactory = joinFilter
                    .map(filterExpression -> compileJoinFilterFunction(
                            filterExpression,
                            probeLayout,
                            buildLayout));

            Optional<Integer> partitionChannel = node.getRightPartitionSymbol().map(buildChannelGetter::apply);

            SpatialIndexBuilderOperatorFactory builderOperatorFactory = new SpatialIndexBuilderOperatorFactory(
                    buildContext.getNextOperatorId(),
                    node.getId(),
                    buildSource.getTypes(),
                    buildOutputChannels,
                    buildChannel,
                    radiusChannel,
                    partitionChannel,
                    spatialRelationshipTest,
                    node.getKdbTree(),
                    filterFunctionFactory,
                    10_000,
                    pagesIndexFactory);

            context.addDriverFactory(
                    buildContext.isInputDriver(),
                    false,
                    ImmutableList.<OperatorFactory>builder()
                            .addAll(buildSource.getOperatorFactories())
                            .add(builderOperatorFactory)
                            .build(),
                    buildContext.getDriverInstanceCount(),
                    buildSource.getPipelineExecutionStrategy());

            return builderOperatorFactory.getPagesSpatialIndexFactory();
        }

        private PhysicalOperation createLookupJoin(JoinNode node,
                PlanNode probeNode,
                List<VariableReferenceExpression> probeVariables,
                Optional<VariableReferenceExpression> probeHashVariable,
                PlanNode buildNode,
                List<VariableReferenceExpression> buildVariables,
                Optional<VariableReferenceExpression> buildHashVariable,
                LocalExecutionPlanContext context)
        {
            // Plan probe
            PhysicalOperation probeSource = probeNode.accept(this, context);

            // Plan build
            JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory =
                    createLookupSourceFactory(node, buildNode, buildVariables, buildHashVariable, probeSource, context);

            OperatorFactory operator = createLookupJoin(node, probeSource, probeVariables, probeHashVariable, lookupSourceFactory, context);

            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            List<Symbol> outputSymbols = node.getOutputSymbols();
            for (int i = 0; i < outputSymbols.size(); i++) {
                Symbol symbol = outputSymbols.get(i);
                outputMappings.put(symbol, i);
            }

            return new PhysicalOperation(operator, outputMappings.build(), context, probeSource);
        }

        private JoinBridgeManager<PartitionedLookupSourceFactory> createLookupSourceFactory(
                JoinNode node,
                PlanNode buildNode,
                List<VariableReferenceExpression> buildVariables,
                Optional<VariableReferenceExpression> buildHashVariable,
                PhysicalOperation probeSource,
                LocalExecutionPlanContext context)
        {
            LocalExecutionPlanContext buildContext = context.createSubContext();
            PhysicalOperation buildSource = buildNode.accept(this, buildContext);

            if (buildSource.getPipelineExecutionStrategy() == GROUPED_EXECUTION) {
                checkState(
                        probeSource.getPipelineExecutionStrategy() == GROUPED_EXECUTION,
                        "Build execution is GROUPED_EXECUTION. Probe execution is expected be GROUPED_EXECUTION, but is UNGROUPED_EXECUTION.");
            }

            List<Symbol> buildOutputSymbols = node.getOutputSymbols().stream()
                    .filter(symbol -> node.getRight().getOutputSymbols().contains(symbol))
                    .collect(toImmutableList());
            List<Integer> buildOutputChannels = ImmutableList.copyOf(getChannelsForSymbols(buildOutputSymbols, buildSource.getLayout()));
            List<Integer> buildChannels = ImmutableList.copyOf(getChannelsForSymbols(buildVariables.stream().map(VariableReferenceExpression::getName).map(Symbol::new).collect(toImmutableList()), buildSource.getLayout()));
            OptionalInt buildHashChannel = buildHashVariable.map(variableChannelGetter(buildSource))
                    .map(OptionalInt::of).orElse(OptionalInt.empty());

            boolean spillEnabled = isSpillEnabled(context.getSession());
            boolean buildOuter = node.getType() == RIGHT || node.getType() == FULL;
            int partitionCount = buildContext.getDriverInstanceCount().orElse(1);

            Optional<JoinFilterFunctionFactory> filterFunctionFactory = node.getFilter()
                    .map(filterExpression -> compileJoinFilterFunction(
                            filterExpression,
                            probeSource.getLayout(),
                            buildSource.getLayout()));

            Optional<SortExpressionContext> sortExpressionContext = node.getSortExpressionContext(metadata.getFunctionManager());

            Optional<Integer> sortChannel = sortExpressionContext
                    .map(SortExpressionContext::getSortExpression)
                    .map(sortExpression -> sortExpressionAsSortChannel(
                            sortExpression,
                            probeSource.getLayout(),
                            buildSource.getLayout()));

            List<JoinFilterFunctionFactory> searchFunctionFactories = sortExpressionContext
                    .map(SortExpressionContext::getSearchExpressions)
                    .map(searchExpressions -> searchExpressions.stream()
                            .map(searchExpression -> compileJoinFilterFunction(
                                    searchExpression,
                                    probeSource.getLayout(),
                                    buildSource.getLayout()))
                            .collect(toImmutableList()))
                    .orElse(ImmutableList.of());

            ImmutableList<Type> buildOutputTypes = buildOutputChannels.stream()
                    .map(buildSource.getTypes()::get)
                    .collect(toImmutableList());
            JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager = new JoinBridgeManager<>(
                    buildOuter,
                    probeSource.getPipelineExecutionStrategy(),
                    buildSource.getPipelineExecutionStrategy(),
                    lifespan -> new PartitionedLookupSourceFactory(
                            buildSource.getTypes(),
                            buildOutputTypes,
                            buildChannels.stream()
                                    .map(buildSource.getTypes()::get)
                                    .collect(toImmutableList()),
                            buildContext.getDriverInstanceCount().orElse(1),
                            buildSource.getLayout(),
                            buildOuter),
                    buildOutputTypes);
            HashBuilderOperatorFactory hashBuilderOperatorFactory = new HashBuilderOperatorFactory(
                    buildContext.getNextOperatorId(),
                    node.getId(),
                    lookupSourceFactoryManager,
                    buildOutputChannels,
                    buildChannels,
                    buildHashChannel,
                    filterFunctionFactory,
                    sortChannel,
                    searchFunctionFactories,
                    10_000,
                    pagesIndexFactory,
                    spillEnabled && !buildOuter && partitionCount > 1,
                    singleStreamSpillerFactory);

            context.addDriverFactory(
                    buildContext.isInputDriver(),
                    false,
                    ImmutableList.<OperatorFactory>builder()
                            .addAll(buildSource.getOperatorFactories())
                            .add(hashBuilderOperatorFactory)
                            .build(),
                    buildContext.getDriverInstanceCount(),
                    buildSource.getPipelineExecutionStrategy());

            return lookupSourceFactoryManager;
        }

        private JoinFilterFunctionFactory compileJoinFilterFunction(
                RowExpression filterExpression,
                Map<Symbol, Integer> probeLayout,
                Map<Symbol, Integer> buildLayout)
        {
            Map<Symbol, Integer> joinSourcesLayout = createJoinSourcesLayout(buildLayout, probeLayout);
            return joinFilterFunctionCompiler.compileJoinFilterFunction(bindChannels(filterExpression, joinSourcesLayout), buildLayout.size());
        }

        private int sortExpressionAsSortChannel(
                RowExpression sortExpression,
                Map<Symbol, Integer> probeLayout,
                Map<Symbol, Integer> buildLayout)
        {
            Map<Symbol, Integer> joinSourcesLayout = createJoinSourcesLayout(buildLayout, probeLayout);
            RowExpression rewrittenSortExpression = bindChannels(sortExpression, joinSourcesLayout);
            checkArgument(rewrittenSortExpression instanceof InputReferenceExpression, "Unsupported expression type [%s]", rewrittenSortExpression);
            return ((InputReferenceExpression) rewrittenSortExpression).getField();
        }

        private OperatorFactory createLookupJoin(
                JoinNode node,
                PhysicalOperation probeSource,
                List<VariableReferenceExpression> probeVariables,
                Optional<VariableReferenceExpression> probeHashVariable,
                JoinBridgeManager<? extends LookupSourceFactory> lookupSourceFactoryManager,
                LocalExecutionPlanContext context)
        {
            List<Type> probeTypes = probeSource.getTypes();
            List<Symbol> probeOutputSymbols = node.getOutputSymbols().stream()
                    .filter(symbol -> node.getLeft().getOutputSymbols().contains(symbol))
                    .collect(toImmutableList());
            List<Integer> probeOutputChannels = ImmutableList.copyOf(getChannelsForSymbols(probeOutputSymbols, probeSource.getLayout()));
            List<Integer> probeJoinChannels = ImmutableList.copyOf(getChannelsForSymbols(probeVariables.stream().map(VariableReferenceExpression::getName).map(Symbol::new).collect(toImmutableList()), probeSource.getLayout()));
            OptionalInt probeHashChannel = probeHashVariable.map(variableChannelGetter(probeSource))
                    .map(OptionalInt::of).orElse(OptionalInt.empty());
            OptionalInt totalOperatorsCount = getJoinOperatorsCountForSpill(context, session);

            switch (node.getType()) {
                case INNER:
                    return lookupJoinOperators.innerJoin(context.getNextOperatorId(), node.getId(), lookupSourceFactoryManager, probeTypes, probeJoinChannels, probeHashChannel, Optional.of(probeOutputChannels), totalOperatorsCount, partitioningSpillerFactory);
                case LEFT:
                    return lookupJoinOperators.probeOuterJoin(context.getNextOperatorId(), node.getId(), lookupSourceFactoryManager, probeTypes, probeJoinChannels, probeHashChannel, Optional.of(probeOutputChannels), totalOperatorsCount, partitioningSpillerFactory);
                case RIGHT:
                    return lookupJoinOperators.lookupOuterJoin(context.getNextOperatorId(), node.getId(), lookupSourceFactoryManager, probeTypes, probeJoinChannels, probeHashChannel, Optional.of(probeOutputChannels), totalOperatorsCount, partitioningSpillerFactory);
                case FULL:
                    return lookupJoinOperators.fullOuterJoin(context.getNextOperatorId(), node.getId(), lookupSourceFactoryManager, probeTypes, probeJoinChannels, probeHashChannel, Optional.of(probeOutputChannels), totalOperatorsCount, partitioningSpillerFactory);
                default:
                    throw new UnsupportedOperationException("Unsupported join type: " + node.getType());
            }
        }

        private OptionalInt getJoinOperatorsCountForSpill(LocalExecutionPlanContext context, Session session)
        {
            OptionalInt driverInstanceCount = context.getDriverInstanceCount();
            if (isSpillEnabled(session)) {
                checkState(driverInstanceCount.isPresent(), "A fixed distribution is required for JOIN when spilling is enabled");
            }
            return driverInstanceCount;
        }

        private Map<Symbol, Integer> createJoinSourcesLayout(Map<Symbol, Integer> lookupSourceLayout, Map<Symbol, Integer> probeSourceLayout)
        {
            Builder<Symbol, Integer> joinSourcesLayout = ImmutableMap.builder();
            joinSourcesLayout.putAll(lookupSourceLayout);
            for (Map.Entry<Symbol, Integer> probeLayoutEntry : probeSourceLayout.entrySet()) {
                joinSourcesLayout.put(probeLayoutEntry.getKey(), probeLayoutEntry.getValue() + lookupSourceLayout.size());
            }
            return joinSourcesLayout.build();
        }

        @Override
        public PhysicalOperation visitSemiJoin(SemiJoinNode node, LocalExecutionPlanContext context)
        {
            // Plan probe
            PhysicalOperation probeSource = node.getSource().accept(this, context);

            // Plan build
            LocalExecutionPlanContext buildContext = context.createSubContext();
            PhysicalOperation buildSource = node.getFilteringSource().accept(this, buildContext);
            checkState(buildSource.getPipelineExecutionStrategy() == probeSource.getPipelineExecutionStrategy(), "build and probe have different pipelineExecutionStrategy");
            checkArgument(buildContext.getDriverInstanceCount().orElse(1) == 1, "Expected local execution to not be parallel");

            int probeChannel = probeSource.getLayout().get(node.getSourceJoinSymbol());
            int buildChannel = buildSource.getLayout().get(node.getFilteringSourceJoinSymbol());

            Optional<Integer> buildHashChannel = node.getFilteringSourceHashVariable().map(variableChannelGetter(buildSource));

            SetBuilderOperatorFactory setBuilderOperatorFactory = new SetBuilderOperatorFactory(
                    buildContext.getNextOperatorId(),
                    node.getId(),
                    buildSource.getTypes().get(buildChannel),
                    buildChannel,
                    buildHashChannel,
                    10_000,
                    joinCompiler);
            SetSupplier setProvider = setBuilderOperatorFactory.getSetProvider();
            context.addDriverFactory(
                    buildContext.isInputDriver(),
                    false,
                    ImmutableList.<OperatorFactory>builder()
                            .addAll(buildSource.getOperatorFactories())
                            .add(setBuilderOperatorFactory)
                            .build(),
                    buildContext.getDriverInstanceCount(),
                    buildSource.getPipelineExecutionStrategy());

            // Source channels are always laid out first, followed by the boolean output symbol
            Map<Symbol, Integer> outputMappings = ImmutableMap.<Symbol, Integer>builder()
                    .putAll(probeSource.getLayout())
                    .put(node.getSemiJoinOutput(), probeSource.getLayout().size())
                    .build();

            HashSemiJoinOperatorFactory operator = new HashSemiJoinOperatorFactory(context.getNextOperatorId(), node.getId(), setProvider, probeSource.getTypes(), probeChannel);
            return new PhysicalOperation(operator, outputMappings, context, probeSource);
        }

        @Override
        public PhysicalOperation visitTableWriter(TableWriterNode node, LocalExecutionPlanContext context)
        {
            // Set table writer count
            if (node.getPartitioningScheme().isPresent()) {
                context.setDriverInstanceCount(1);
            }
            else {
                context.setDriverInstanceCount(getTaskWriterCount(session));
            }

            // serialize writes by forcing data through a single writer
            PhysicalOperation source = node.getSource().accept(this, context);

            ImmutableMap.Builder<Symbol, Integer> outputMapping = ImmutableMap.builder();
            outputMapping.put(node.getOutputSymbols().get(0), ROW_COUNT_CHANNEL);
            outputMapping.put(node.getOutputSymbols().get(1), FRAGMENT_CHANNEL);

            OperatorFactory statisticsAggregation = node.getStatisticsAggregation().map(aggregation -> {
                List<Symbol> groupingSymbols = aggregation.getGroupingSymbols();
                if (groupingSymbols.isEmpty()) {
                    return createAggregationOperatorFactory(
                            node.getId(),
                            aggregation.getAggregations(),
                            PARTIAL,
                            STATS_START_CHANNEL,
                            outputMapping,
                            source,
                            context,
                            true);
                }
                return createHashAggregationOperatorFactory(
                        node.getId(),
                        aggregation.getAggregations(),
                        ImmutableSet.of(),
                        groupingSymbols,
                        PARTIAL,
                        Optional.empty(),
                        Optional.empty(),
                        source,
                        false,
                        false,
                        false,
                        new DataSize(0, BYTE),
                        context,
                        STATS_START_CHANNEL,
                        outputMapping,
                        200,
                        // This aggregation must behave as INTERMEDIATE.
                        // Using INTERMEDIATE aggregation directly
                        // is not possible, as it doesn't accept raw input data.
                        // Disabling partial pre-aggregation memory limit effectively
                        // turns PARTIAL aggregation into INTERMEDIATE.
                        Optional.empty(),
                        true);
            }).orElse(new DevNullOperatorFactory(context.getNextOperatorId(), node.getId()));

            List<Integer> inputChannels = node.getColumns().stream()
                    .map(source::symbolToChannel)
                    .collect(toImmutableList());

            OperatorFactory operatorFactory = new TableWriterOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    pageSinkManager,
                    node.getTarget(),
                    inputChannels,
                    session,
                    statisticsAggregation,
                    getSymbolTypes(node.getOutputSymbols(), context.getTypes()));

            return new PhysicalOperation(operatorFactory, outputMapping.build(), context, source);
        }

        @Override
        public PhysicalOperation visitStatisticsWriterNode(StatisticsWriterNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            StatisticAggregationsDescriptor<Integer> descriptor = node.getDescriptor().map(variable -> source.getLayout().get(new Symbol(variable.getName())));

            OperatorFactory operatorFactory = new StatisticsWriterOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    computedStatistics -> metadata.finishStatisticsCollection(session, ((StatisticsWriterNode.WriteStatisticsHandle) node.getTarget()).getHandle(), computedStatistics),
                    node.isRowCountEnabled(),
                    descriptor);
            return new PhysicalOperation(operatorFactory, makeLayout(node), context, source);
        }

        @Override
        public PhysicalOperation visitTableFinish(TableFinishNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            ImmutableMap.Builder<Symbol, Integer> outputMapping = ImmutableMap.builder();

            OperatorFactory statisticsAggregation = node.getStatisticsAggregation().map(aggregation -> {
                List<Symbol> groupingSymbols = aggregation.getGroupingSymbols();
                if (groupingSymbols.isEmpty()) {
                    return createAggregationOperatorFactory(
                            node.getId(),
                            aggregation.getAggregations(),
                            FINAL,
                            0,
                            outputMapping,
                            source,
                            context,
                            true);
                }
                return createHashAggregationOperatorFactory(
                        node.getId(),
                        aggregation.getAggregations(),
                        ImmutableSet.of(),
                        aggregation.getGroupingSymbols(),
                        FINAL,
                        Optional.empty(),
                        Optional.empty(),
                        source,
                        false,
                        false,
                        false,
                        new DataSize(0, BYTE),
                        context,
                        0,
                        outputMapping,
                        200,
                        // final aggregation ignores partial pre-aggregation memory limit
                        Optional.empty(),
                        true);
            }).orElse(new DevNullOperatorFactory(context.getNextOperatorId(), node.getId()));

            Map<Symbol, Integer> aggregationOutput = outputMapping.build();
            StatisticAggregationsDescriptor<Integer> descriptor = node.getStatisticsAggregationDescriptor()
                    .map(desc -> desc.map(variable -> aggregationOutput.get(new Symbol(variable.getName()))))
                    .orElse(StatisticAggregationsDescriptor.empty());

            OperatorFactory operatorFactory = new TableFinishOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    createTableFinisher(session, node, metadata),
                    statisticsAggregation,
                    descriptor,
                    session);
            Map<Symbol, Integer> layout = ImmutableMap.of(node.getOutputSymbols().get(0), 0);

            return new PhysicalOperation(operatorFactory, layout, context, source);
        }

        @Override
        public PhysicalOperation visitDelete(DeleteNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            OperatorFactory operatorFactory = new DeleteOperatorFactory(context.getNextOperatorId(), node.getId(), source.getLayout().get(node.getRowIdAsSymbol()));

            Map<Symbol, Integer> layout = ImmutableMap.<Symbol, Integer>builder()
                    .put(node.getOutputSymbols().get(0), 0)
                    .put(node.getOutputSymbols().get(1), 1)
                    .build();

            return new PhysicalOperation(operatorFactory, layout, context, source);
        }

        @Override
        public PhysicalOperation visitMetadataDelete(MetadataDeleteNode node, LocalExecutionPlanContext context)
        {
            OperatorFactory operatorFactory = new MetadataDeleteOperatorFactory(context.getNextOperatorId(), node.getId(), metadata, session, node.getTarget().getHandle());

            return new PhysicalOperation(operatorFactory, makeLayout(node), context, UNGROUPED_EXECUTION);
        }

        @Override
        public PhysicalOperation visitUnion(UnionNode node, LocalExecutionPlanContext context)
        {
            throw new UnsupportedOperationException("Union node should not be present in a local execution plan");
        }

        @Override
        public PhysicalOperation visitEnforceSingleRow(EnforceSingleRowNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            OperatorFactory operatorFactory = new EnforceSingleRowOperator.EnforceSingleRowOperatorFactory(context.getNextOperatorId(), node.getId());
            return new PhysicalOperation(operatorFactory, makeLayout(node), context, source);
        }

        @Override
        public PhysicalOperation visitAssignUniqueId(AssignUniqueId node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            OperatorFactory operatorFactory = new AssignUniqueIdOperator.AssignUniqueIdOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId());
            return new PhysicalOperation(operatorFactory, makeLayout(node), context, source);
        }

        @Override
        public PhysicalOperation visitExchange(ExchangeNode node, LocalExecutionPlanContext context)
        {
            checkArgument(node.getScope().isLocal(), "Only local exchanges are supported in the local planner");

            if (node.getOrderingScheme().isPresent()) {
                return createLocalMerge(node, context);
            }

            return createLocalExchange(node, context);
        }

        private PhysicalOperation createLocalMerge(ExchangeNode node, LocalExecutionPlanContext context)
        {
            checkArgument(node.getOrderingScheme().isPresent(), "orderingScheme is absent");
            checkState(node.getSources().size() == 1, "single source is expected");

            // local merge source must have a single driver
            context.setDriverInstanceCount(1);

            PlanNode sourceNode = getOnlyElement(node.getSources());
            LocalExecutionPlanContext subContext = context.createSubContext();
            PhysicalOperation source = sourceNode.accept(this, subContext);

            int operatorsCount = subContext.getDriverInstanceCount().orElse(1);
            List<Type> types = getSourceOperatorTypes(node, context.getTypes());
            LocalExchangeFactory exchangeFactory = new LocalExchangeFactory(
                    node.getPartitioningScheme().getPartitioning().getHandle(),
                    operatorsCount,
                    types,
                    ImmutableList.of(),
                    Optional.empty(),
                    source.getPipelineExecutionStrategy(),
                    maxLocalExchangeBufferSize);

            List<OperatorFactory> operatorFactories = new ArrayList<>(source.getOperatorFactories());
            List<VariableReferenceExpression> expectedLayout = node.getInputs().get(0);
            Function<Page, Page> pagePreprocessor = enforceLayoutProcessor(expectedLayout, source.getLayout());
            operatorFactories.add(new LocalExchangeSinkOperatorFactory(
                    exchangeFactory,
                    subContext.getNextOperatorId(),
                    node.getId(),
                    exchangeFactory.newSinkFactoryId(),
                    pagePreprocessor));
            context.addDriverFactory(subContext.isInputDriver(), false, operatorFactories, subContext.getDriverInstanceCount(), source.getPipelineExecutionStrategy());
            // the main driver is not an input... the exchange sources are the input for the plan
            context.setInputDriver(false);

            OrderingScheme orderingScheme = node.getOrderingScheme().get();
            ImmutableMap<Symbol, Integer> layout = makeLayout(node);
            List<Integer> sortChannels = getChannelsForVariables(orderingScheme.getOrderBy(), layout);
            List<SortOrder> orderings = orderingScheme.getOrderingList();
            OperatorFactory operatorFactory = new LocalMergeSourceOperatorFactory(
                    context.getNextOperatorId(),
                    node.getId(),
                    exchangeFactory,
                    types,
                    orderingCompiler,
                    sortChannels,
                    orderings);
            return new PhysicalOperation(operatorFactory, layout, context, UNGROUPED_EXECUTION);
        }

        private PhysicalOperation createLocalExchange(ExchangeNode node, LocalExecutionPlanContext context)
        {
            int driverInstanceCount;
            if (node.getType() == ExchangeNode.Type.GATHER) {
                driverInstanceCount = 1;
                context.setDriverInstanceCount(1);
            }
            else if (context.getDriverInstanceCount().isPresent()) {
                driverInstanceCount = context.getDriverInstanceCount().getAsInt();
            }
            else {
                driverInstanceCount = getTaskConcurrency(session);
                context.setDriverInstanceCount(driverInstanceCount);
            }

            List<Type> types = getSourceOperatorTypes(node, context.getTypes());
            List<Integer> channels = node.getPartitioningScheme().getPartitioning().getArguments().stream()
                    .map(argument -> node.getOutputSymbols().indexOf(argument.getColumn()))
                    .collect(toImmutableList());
            Optional<Integer> hashChannel = node.getPartitioningScheme().getHashColumn()
                    .map(variable -> node.getOutputSymbols().indexOf(new Symbol(variable.getName())));

            PipelineExecutionStrategy exchangeSourcePipelineExecutionStrategy = GROUPED_EXECUTION;
            List<DriverFactoryParameters> driverFactoryParametersList = new ArrayList<>();
            for (int i = 0; i < node.getSources().size(); i++) {
                PlanNode sourceNode = node.getSources().get(i);

                LocalExecutionPlanContext subContext = context.createSubContext();
                PhysicalOperation source = sourceNode.accept(this, subContext);
                driverFactoryParametersList.add(new DriverFactoryParameters(subContext, source));

                if (source.getPipelineExecutionStrategy() == UNGROUPED_EXECUTION) {
                    exchangeSourcePipelineExecutionStrategy = UNGROUPED_EXECUTION;
                }
            }

            LocalExchangeFactory localExchangeFactory = new LocalExchangeFactory(
                    node.getPartitioningScheme().getPartitioning().getHandle(),
                    driverInstanceCount,
                    types,
                    channels,
                    hashChannel,
                    exchangeSourcePipelineExecutionStrategy,
                    maxLocalExchangeBufferSize);
            for (int i = 0; i < node.getSources().size(); i++) {
                DriverFactoryParameters driverFactoryParameters = driverFactoryParametersList.get(i);
                PhysicalOperation source = driverFactoryParameters.getSource();
                LocalExecutionPlanContext subContext = driverFactoryParameters.getSubContext();

                List<VariableReferenceExpression> expectedLayout = node.getInputs().get(i);
                Function<Page, Page> pagePreprocessor = enforceLayoutProcessor(expectedLayout, source.getLayout());
                List<OperatorFactory> operatorFactories = new ArrayList<>(source.getOperatorFactories());

                operatorFactories.add(new LocalExchangeSinkOperatorFactory(
                        localExchangeFactory,
                        subContext.getNextOperatorId(),
                        node.getId(),
                        localExchangeFactory.newSinkFactoryId(),
                        pagePreprocessor));
                context.addDriverFactory(
                        subContext.isInputDriver(),
                        false,
                        operatorFactories,
                        subContext.getDriverInstanceCount(),
                        exchangeSourcePipelineExecutionStrategy);
            }

            // the main driver is not an input... the exchange sources are the input for the plan
            context.setInputDriver(false);

            // instance count must match the number of partitions in the exchange
            verify(context.getDriverInstanceCount().getAsInt() == localExchangeFactory.getBufferCount(),
                    "driver instance count must match the number of exchange partitions");

            return new PhysicalOperation(new LocalExchangeSourceOperatorFactory(context.getNextOperatorId(), node.getId(), localExchangeFactory), makeLayout(node), context, exchangeSourcePipelineExecutionStrategy);
        }

        @Override
        protected PhysicalOperation visitPlan(PlanNode node, LocalExecutionPlanContext context)
        {
            throw new UnsupportedOperationException("not yet implemented");
        }

        private List<Type> getSourceOperatorTypes(PlanNode node, TypeProvider types)
        {
            return getSymbolTypes(node.getOutputSymbols(), types);
        }

        private List<Type> getSymbolTypes(List<Symbol> symbols, TypeProvider types)
        {
            return symbols.stream()
                    .map(types::get)
                    .collect(toImmutableList());
        }

        private AccumulatorFactory buildAccumulatorFactory(
                PhysicalOperation source,
                Aggregation aggregation)
        {
            FunctionManager functionManager = metadata.getFunctionManager();
            InternalAggregationFunction internalAggregationFunction = functionManager.getAggregateFunctionImplementation(aggregation.getFunctionHandle());

            List<Integer> valueChannels = new ArrayList<>();
            for (Expression argument : aggregation.getArguments()) {
                if (!(argument instanceof LambdaExpression)) {
                    Symbol argumentSymbol = Symbol.from(argument);
                    valueChannels.add(source.getLayout().get(argumentSymbol));
                }
            }

            List<LambdaProvider> lambdaProviders = new ArrayList<>();
            List<LambdaExpression> lambdaExpressions = aggregation.getArguments().stream()
                    .filter(LambdaExpression.class::isInstance)
                    .map(LambdaExpression.class::cast)
                    .collect(toImmutableList());
            if (!lambdaExpressions.isEmpty()) {
                List<FunctionType> functionTypes = functionManager.getFunctionMetadata(aggregation.getFunctionHandle()).getArgumentTypes().stream()
                        .filter(typeSignature -> typeSignature.getBase().equals(FunctionType.NAME))
                        .map(typeSignature -> (FunctionType) (metadata.getTypeManager().getType(typeSignature)))
                        .collect(toImmutableList());
                List<Class> lambdaInterfaces = internalAggregationFunction.getLambdaInterfaces();
                verify(lambdaExpressions.size() == functionTypes.size());
                verify(lambdaExpressions.size() == lambdaInterfaces.size());

                for (int i = 0; i < lambdaExpressions.size(); i++) {
                    LambdaExpression lambdaExpression = lambdaExpressions.get(i);
                    FunctionType functionType = functionTypes.get(i);

                    // To compile lambda, LambdaDefinitionExpression needs to be generated from LambdaExpression,
                    // which requires the types of all sub-expressions.
                    //
                    // In project and filter expression compilation, ExpressionAnalyzer.getExpressionTypesFromInput
                    // is used to generate the types of all sub-expressions. (see visitScanFilterAndProject and visitFilter)
                    //
                    // This does not work here since the function call representation in final aggregation node
                    // is currently a hack: it takes intermediate type as input, and may not be a valid
                    // function call in Presto.
                    //
                    // TODO: Once the final aggregation function call representation is fixed,
                    // the same mechanism in project and filter expression should be used here.
                    verify(lambdaExpression.getArguments().size() == functionType.getArgumentTypes().size());
                    Map<NodeRef<Expression>, Type> lambdaArgumentExpressionTypes = new HashMap<>();
                    Map<Symbol, Type> lambdaArgumentSymbolTypes = new HashMap<>();
                    for (int j = 0; j < lambdaExpression.getArguments().size(); j++) {
                        LambdaArgumentDeclaration argument = lambdaExpression.getArguments().get(j);
                        Type type = functionType.getArgumentTypes().get(j);
                        lambdaArgumentExpressionTypes.put(NodeRef.of(argument), type);
                        lambdaArgumentSymbolTypes.put(new Symbol(argument.getName().getValue()), type);
                    }
                    Map<NodeRef<Expression>, Type> expressionTypes = ImmutableMap.<NodeRef<Expression>, Type>builder()
                            // the lambda expression itself
                            .put(NodeRef.of(lambdaExpression), functionType)
                            // expressions from lambda arguments
                            .putAll(lambdaArgumentExpressionTypes)
                            // expressions from lambda body
                            .putAll(getExpressionTypes(
                                    session,
                                    metadata,
                                    sqlParser,
                                    TypeProvider.copyOf(lambdaArgumentSymbolTypes),
                                    lambdaExpression.getBody(),
                                    emptyList(),
                                    NOOP))
                            .build();

                    LambdaDefinitionExpression lambda = (LambdaDefinitionExpression) toRowExpression(lambdaExpression, expressionTypes, ImmutableMap.of());
                    Class<? extends LambdaProvider> lambdaProviderClass = compileLambdaProvider(lambda, metadata.getFunctionManager(), lambdaInterfaces.get(i));
                    try {
                        lambdaProviders.add((LambdaProvider) constructorMethodHandle(lambdaProviderClass, ConnectorSession.class).invoke(session.toConnectorSession()));
                    }
                    catch (Throwable t) {
                        throw new RuntimeException(t);
                    }
                }
            }

            Optional<Integer> maskChannel = aggregation.getMask().map(value -> source.getLayout().get(new Symbol(value.getName())));
            List<SortOrder> sortOrders = ImmutableList.of();
            List<VariableReferenceExpression> sortKeys = ImmutableList.of();
            if (aggregation.getOrderBy().isPresent()) {
                OrderingScheme orderBy = aggregation.getOrderBy().get();
                sortKeys = orderBy.getOrderBy();
                sortOrders = orderBy.getOrderingList();
            }

            return internalAggregationFunction.bind(
                    valueChannels,
                    maskChannel,
                    source.getTypes(),
                    getChannelsForVariables(sortKeys, source.getLayout()),
                    sortOrders,
                    pagesIndexFactory,
                    aggregation.isDistinct(),
                    joinCompiler,
                    lambdaProviders,
                    session);
        }

        private PhysicalOperation planGlobalAggregation(AggregationNode node, PhysicalOperation source, LocalExecutionPlanContext context)
        {
            ImmutableMap.Builder<Symbol, Integer> outputMappings = ImmutableMap.builder();
            AggregationOperatorFactory operatorFactory = createAggregationOperatorFactory(
                    node.getId(),
                    node.getAggregations(),
                    node.getStep(),
                    0,
                    outputMappings,
                    source,
                    context,
                    node.getStep().isOutputPartial());
            return new PhysicalOperation(operatorFactory, outputMappings.build(), context, source);
        }

        private AggregationOperatorFactory createAggregationOperatorFactory(
                PlanNodeId planNodeId,
                Map<VariableReferenceExpression, Aggregation> aggregations,
                Step step,
                int startOutputChannel,
                ImmutableMap.Builder<Symbol, Integer> outputMappings,
                PhysicalOperation source,
                LocalExecutionPlanContext context,
                boolean useSystemMemory)
        {
            int outputChannel = startOutputChannel;
            ImmutableList.Builder<AccumulatorFactory> accumulatorFactories = ImmutableList.builder();
            for (Map.Entry<VariableReferenceExpression, Aggregation> entry : aggregations.entrySet()) {
                VariableReferenceExpression variable = entry.getKey();
                Aggregation aggregation = entry.getValue();
                accumulatorFactories.add(buildAccumulatorFactory(source, aggregation));
                outputMappings.put(new Symbol(variable.getName()), outputChannel); // one aggregation per channel
                outputChannel++;
            }
            return new AggregationOperatorFactory(context.getNextOperatorId(), planNodeId, step, accumulatorFactories.build(), useSystemMemory);
        }

        private PhysicalOperation planGroupByAggregation(
                AggregationNode node,
                PhysicalOperation source,
                boolean spillEnabled,
                DataSize unspillMemoryLimit,
                LocalExecutionPlanContext context)
        {
            ImmutableMap.Builder<Symbol, Integer> mappings = ImmutableMap.builder();
            OperatorFactory operatorFactory = createHashAggregationOperatorFactory(
                    node.getId(),
                    node.getAggregations(),
                    node.getGlobalGroupingSets(),
                    node.getGroupingKeys(),
                    node.getStep(),
                    node.getHashVariable(),
                    node.getGroupIdSymbol(),
                    source,
                    node.hasDefaultOutput(),
                    spillEnabled,
                    node.isStreamable(),
                    unspillMemoryLimit,
                    context,
                    0,
                    mappings,
                    10_000,
                    Optional.of(maxPartialAggregationMemorySize),
                    node.getStep().isOutputPartial());
            return new PhysicalOperation(operatorFactory, mappings.build(), context, source);
        }

        private OperatorFactory createHashAggregationOperatorFactory(
                PlanNodeId planNodeId,
                Map<VariableReferenceExpression, Aggregation> aggregations,
                Set<Integer> globalGroupingSets,
                List<Symbol> groupbySymbols,
                Step step,
                Optional<VariableReferenceExpression> hashVariable,
                Optional<Symbol> groupIdSymbol,
                PhysicalOperation source,
                boolean hasDefaultOutput,
                boolean spillEnabled,
                boolean isStreamable,
                DataSize unspillMemoryLimit,
                LocalExecutionPlanContext context,
                int startOutputChannel,
                ImmutableMap.Builder<Symbol, Integer> outputMappings,
                int expectedGroups,
                Optional<DataSize> maxPartialAggregationMemorySize,
                boolean useSystemMemory)
        {
            List<VariableReferenceExpression> aggregationOutputSymbols = new ArrayList<>();
            List<AccumulatorFactory> accumulatorFactories = new ArrayList<>();
            for (Map.Entry<VariableReferenceExpression, Aggregation> entry : aggregations.entrySet()) {
                VariableReferenceExpression variable = entry.getKey();
                Aggregation aggregation = entry.getValue();

                accumulatorFactories.add(buildAccumulatorFactory(source, aggregation));
                aggregationOutputSymbols.add(variable);
            }

            // add group-by key fields each in a separate channel
            int channel = startOutputChannel;
            Optional<Integer> groupIdChannel = Optional.empty();
            for (Symbol symbol : groupbySymbols) {
                outputMappings.put(symbol, channel);
                if (groupIdSymbol.isPresent() && groupIdSymbol.get().equals(symbol)) {
                    groupIdChannel = Optional.of(channel);
                }
                channel++;
            }

            // hashChannel follows the group by channels
            if (hashVariable.isPresent()) {
                outputMappings.put(new Symbol(hashVariable.get().getName()), channel++);
            }

            // aggregations go in following channels
            for (VariableReferenceExpression variable : aggregationOutputSymbols) {
                outputMappings.put(new Symbol(variable.getName()), channel);
                channel++;
            }

            List<Integer> groupByChannels = getChannelsForSymbols(groupbySymbols, source.getLayout());
            List<Type> groupByTypes = groupByChannels.stream()
                    .map(entry -> source.getTypes().get(entry))
                    .collect(toImmutableList());

            if (isStreamable) {
                return new StreamingAggregationOperatorFactory(
                        context.getNextOperatorId(),
                        planNodeId,
                        source.getTypes(),
                        groupByTypes,
                        groupByChannels,
                        step,
                        accumulatorFactories,
                        joinCompiler);
            }
            else {
                Optional<Integer> hashChannel = hashVariable.map(variableChannelGetter(source));
                return new HashAggregationOperatorFactory(
                        context.getNextOperatorId(),
                        planNodeId,
                        groupByTypes,
                        groupByChannels,
                        ImmutableList.copyOf(globalGroupingSets),
                        step,
                        hasDefaultOutput,
                        accumulatorFactories,
                        hashChannel,
                        groupIdChannel,
                        expectedGroups,
                        maxPartialAggregationMemorySize,
                        spillEnabled,
                        unspillMemoryLimit,
                        spillerFactory,
                        joinCompiler,
                        useSystemMemory);
            }
        }
    }

    private static List<Type> getTypes(List<Expression> expressions, Map<NodeRef<Expression>, Type> expressionTypes)
    {
        return expressions.stream()
                .map(NodeRef::of)
                .map(expressionTypes::get)
                .collect(toImmutableList());
    }

    private static TableFinisher createTableFinisher(Session session, TableFinishNode node, Metadata metadata)
    {
        WriterTarget target = node.getTarget();
        return (fragments, statistics) -> {
            if (target instanceof CreateHandle) {
                return metadata.finishCreateTable(session, ((CreateHandle) target).getHandle(), fragments, statistics);
            }
            else if (target instanceof InsertHandle) {
                return metadata.finishInsert(session, ((InsertHandle) target).getHandle(), fragments, statistics);
            }
            else if (target instanceof DeleteHandle) {
                metadata.finishDelete(session, ((DeleteHandle) target).getHandle(), fragments);
                return Optional.empty();
            }
            else {
                throw new AssertionError("Unhandled target type: " + target.getClass().getName());
            }
        };
    }

    private static Function<Page, Page> enforceLayoutProcessor(List<VariableReferenceExpression> expectedLayout, Map<Symbol, Integer> inputLayout)
    {
        int[] channels = expectedLayout.stream()
                .peek(variable -> checkArgument(inputLayout.containsKey(new Symbol(variable.getName())), "channel not found for variable: %s", variable))
                .mapToInt(variable -> inputLayout.get(new Symbol(variable.getName())))
                .toArray();

        if (Arrays.equals(channels, range(0, inputLayout.size()).toArray())) {
            // this is an identity mapping
            return Function.identity();
        }

        return new PageChannelSelector(channels);
    }

    private static List<Integer> getChannelsForSymbols(List<Symbol> symbols, Map<Symbol, Integer> layout)
    {
        ImmutableList.Builder<Integer> builder = ImmutableList.builder();
        for (Symbol symbol : symbols) {
            builder.add(layout.get(symbol));
        }
        return builder.build();
    }

    private static List<Integer> getChannelsForVariables(List<VariableReferenceExpression> variables, Map<Symbol, Integer> layout)
    {
        ImmutableList.Builder<Integer> builder = ImmutableList.builder();
        for (VariableReferenceExpression variable : variables) {
            builder.add(layout.get(new Symbol(variable.getName())));
        }
        return builder.build();
    }

    private static Function<VariableReferenceExpression, Integer> variableChannelGetter(PhysicalOperation source)
    {
        return input -> {
            Symbol symbol = new Symbol(input.getName());
            checkArgument(source.getLayout().containsKey(symbol));
            return source.getLayout().get(symbol);
        };
    }

    private static Function<Symbol, Integer> symbolChannelGetter(PhysicalOperation source)
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

        private final PipelineExecutionStrategy pipelineExecutionStrategy;

        public PhysicalOperation(OperatorFactory operatorFactory, Map<Symbol, Integer> layout, LocalExecutionPlanContext context, PipelineExecutionStrategy pipelineExecutionStrategy)
        {
            this(operatorFactory, layout, context, Optional.empty(), pipelineExecutionStrategy);
        }

        public PhysicalOperation(OperatorFactory operatorFactory, Map<Symbol, Integer> layout, LocalExecutionPlanContext context, PhysicalOperation source)
        {
            this(operatorFactory, layout, context, Optional.of(requireNonNull(source, "source is null")), source.getPipelineExecutionStrategy());
        }

        private PhysicalOperation(
                OperatorFactory operatorFactory,
                Map<Symbol, Integer> layout,
                LocalExecutionPlanContext context,
                Optional<PhysicalOperation> source,
                PipelineExecutionStrategy pipelineExecutionStrategy)
        {
            requireNonNull(operatorFactory, "operatorFactory is null");
            requireNonNull(layout, "layout is null");
            requireNonNull(context, "context is null");
            requireNonNull(source, "source is null");
            requireNonNull(pipelineExecutionStrategy, "pipelineExecutionStrategy is null");

            this.operatorFactories = ImmutableList.<OperatorFactory>builder()
                    .addAll(source.map(PhysicalOperation::getOperatorFactories).orElse(ImmutableList.of()))
                    .add(operatorFactory)
                    .build();
            this.layout = ImmutableMap.copyOf(layout);
            this.types = toTypes(layout, context);
            this.pipelineExecutionStrategy = pipelineExecutionStrategy;
        }

        private static List<Type> toTypes(Map<Symbol, Integer> layout, LocalExecutionPlanContext context)
        {
            // verify layout covers all values
            int channelCount = layout.values().stream().mapToInt(Integer::intValue).max().orElse(-1) + 1;
            checkArgument(
                    layout.size() == channelCount && ImmutableSet.copyOf(layout.values()).containsAll(ContiguousSet.create(closedOpen(0, channelCount), integers())),
                    "Layout does not have a symbol for every output channel: %s", layout);
            Map<Integer, Symbol> channelLayout = ImmutableBiMap.copyOf(layout).inverse();

            return range(0, channelCount)
                    .mapToObj(channelLayout::get)
                    .map(context.getTypes()::get)
                    .collect(toImmutableList());
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

        public PipelineExecutionStrategy getPipelineExecutionStrategy()
        {
            return pipelineExecutionStrategy;
        }
    }

    private static class DriverFactoryParameters
    {
        private final LocalExecutionPlanContext subContext;
        private final PhysicalOperation source;

        public DriverFactoryParameters(LocalExecutionPlanContext subContext, PhysicalOperation source)
        {
            this.subContext = subContext;
            this.source = source;
        }

        public LocalExecutionPlanContext getSubContext()
        {
            return subContext;
        }

        public PhysicalOperation getSource()
        {
            return source;
        }
    }
}
