package com.facebook.presto.spark.planner;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.Session;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.execution.ExplainAnalyzeContext;
import com.facebook.presto.execution.TaskManagerConfig;
import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.execution.buffer.PagesSerdeFactory;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.index.IndexManager;
import com.facebook.presto.memory.MemoryManagerConfig;
import com.facebook.presto.metadata.ConnectorMetadataUpdaterManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.DriverFactory;
import com.facebook.presto.operator.FragmentResultCacheManager;
import com.facebook.presto.operator.LocalPlannerAware;
import com.facebook.presto.operator.LookupJoinOperators;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.operator.OutputFactory;
import com.facebook.presto.operator.PagesIndex;
import com.facebook.presto.operator.TableCommitContext;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.operator.index.IndexJoinLookupStats;
import com.facebook.presto.spark.execution.NativeEngineOperator;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.relation.DeterminismEvaluator;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spiller.PartitioningSpillerFactory;
import com.facebook.presto.spiller.SingleStreamSpillerFactory;
import com.facebook.presto.spiller.SpillerFactory;
import com.facebook.presto.spiller.StandaloneSpillerFactory;
import com.facebook.presto.split.PageSinkManager;
import com.facebook.presto.split.PageSourceProvider;
import com.facebook.presto.sql.gen.ExpressionCompiler;
import com.facebook.presto.sql.gen.JoinCompiler;
import com.facebook.presto.sql.gen.JoinFilterFunctionCompiler;
import com.facebook.presto.sql.gen.OrderingCompiler;
import com.facebook.presto.sql.gen.PageFunctionCompiler;
import com.facebook.presto.sql.planner.LocalExecutionPlanner;
import com.facebook.presto.sql.planner.NodePartitioningManager;
import com.facebook.presto.sql.planner.OutputPartitioning;
import com.facebook.presto.sql.planner.PartitioningProviderManager;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.RemoteSourceFactory;
import com.facebook.presto.sql.planner.plan.NativeEngineNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static com.facebook.presto.SystemSessionProperties.isExchangeChecksumEnabled;
import static com.facebook.presto.SystemSessionProperties.isExchangeCompressionEnabled;
import static com.facebook.presto.execution.FragmentResultCacheContext.createFragmentResultCacheContext;
import static com.facebook.presto.operator.PipelineExecutionStrategy.GROUPED_EXECUTION;
import static com.facebook.presto.operator.PipelineExecutionStrategy.UNGROUPED_EXECUTION;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class PrestoSparkLocalExecutionPlanner
        extends LocalExecutionPlanner
{
    private final JsonCodec<TaskSource> taskSourceCodec;
    private final JsonCodec<PlanFragment> planFragmentCodec;
    private final JsonCodec<TableWriteInfo> tableWriteInfoCodec;

    @Inject
    public PrestoSparkLocalExecutionPlanner(
            Metadata metadata,
            Optional<ExplainAnalyzeContext> explainAnalyzeContext,
            PageSourceProvider pageSourceProvider,
            IndexManager indexManager,
            PartitioningProviderManager partitioningProviderManager,
            NodePartitioningManager nodePartitioningManager,
            PageSinkManager pageSinkManager,
            ConnectorMetadataUpdaterManager metadataUpdaterManager,
            ExpressionCompiler expressionCompiler,
            PageFunctionCompiler pageFunctionCompiler,
            JoinFilterFunctionCompiler joinFilterFunctionCompiler,
            IndexJoinLookupStats indexJoinLookupStats,
            TaskManagerConfig taskManagerConfig,
            MemoryManagerConfig memoryManagerConfig,
            SpillerFactory spillerFactory,
            SingleStreamSpillerFactory singleStreamSpillerFactory,
            PartitioningSpillerFactory partitioningSpillerFactory,
            BlockEncodingSerde blockEncodingSerde,
            PagesIndex.Factory pagesIndexFactory,
            JoinCompiler joinCompiler,
            LookupJoinOperators lookupJoinOperators,
            OrderingCompiler orderingCompiler,
            JsonCodec<TableCommitContext> tableCommitContextCodec,
            DeterminismEvaluator determinismEvaluator,
            FragmentResultCacheManager fragmentResultCacheManager,
            ObjectMapper objectMapper,
            StandaloneSpillerFactory standaloneSpillerFactory,
            JsonCodec<TaskSource> taskSourceCodec,
            JsonCodec<TableWriteInfo> tableWriteInfoCodec,
            JsonCodec<PlanFragment> planFragmentCodec)
    {
        super(metadata,
                explainAnalyzeContext,
                pageSourceProvider, indexManager,
                partitioningProviderManager,
                nodePartitioningManager,
                pageSinkManager,
                metadataUpdaterManager,
                expressionCompiler,
                pageFunctionCompiler,
                joinFilterFunctionCompiler,
                indexJoinLookupStats,
                taskManagerConfig,
                memoryManagerConfig,
                spillerFactory,
                singleStreamSpillerFactory,
                partitioningSpillerFactory,
                blockEncodingSerde,
                pagesIndexFactory,
                joinCompiler,
                lookupJoinOperators,
                orderingCompiler,
                tableCommitContextCodec,
                determinismEvaluator,
                fragmentResultCacheManager,
                objectMapper,
                standaloneSpillerFactory);

        this.taskSourceCodec = requireNonNull(taskSourceCodec, "taskSourceCodec is null");
        this.tableWriteInfoCodec = requireNonNull(tableWriteInfoCodec, "tableWriteInfoCodec is null");
        this.planFragmentCodec = requireNonNull(planFragmentCodec, "planFragmentCodec is null");
    }

    public LocalExecutionPlan plan(
            TaskContext taskContext,
            PlanFragment planFragment,
            OutputFactory outputFactory,
            RemoteSourceFactory remoteSourceFactory,
            TableWriteInfo tableWriteInfo,
            boolean pageSinkCommitRequired)
    {
        return plan(
                taskContext,
                planFragment,
                outputFactory,
                createOutputPartitioning(taskContext, planFragment.getPartitioningScheme()),
                remoteSourceFactory,
                tableWriteInfo,
                pageSinkCommitRequired);
    }

    @VisibleForTesting
    public LocalExecutionPlan plan(
            TaskContext taskContext,
            PlanFragment planFragment,
            OutputFactory outputOperatorFactory,
            Optional<OutputPartitioning> outputPartitioning,
            RemoteSourceFactory remoteSourceFactory,
            TableWriteInfo tableWriteInfo,
            boolean pageSinkCommitRequired)
    {
        List<VariableReferenceExpression> outputLayout = planFragment.getPartitioningScheme().getOutputLayout();
        Session session = taskContext.getSession();
        PlanNode plan = planFragment.getRoot();
        PrestoSparkLocalExecutionPlanContext context = new PrestoSparkLocalExecutionPlanContext(taskContext, tableWriteInfo, planFragment);
        PhysicalOperation physicalOperation = plan.accept(new PrestoSparkVisitor(session, planFragment, remoteSourceFactory, pageSinkCommitRequired, taskSourceCodec, tableWriteInfoCodec, planFragmentCodec), context);

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
                                outputPartitioning,
                                new PagesSerdeFactory(blockEncodingSerde, isExchangeCompressionEnabled(session), isExchangeChecksumEnabled(session))))
                        .build(),
                context.getDriverInstanceCount(),
                physicalOperation.getPipelineExecutionStrategy(),
                createFragmentResultCacheContext(fragmentResultCacheManager, plan, planFragment.getPartitioningScheme(), session, objectMapper));

        addLookupOuterDrivers(context);

        // notify operator factories that planning has completed
        context.getDriverFactories().stream()
                .map(DriverFactory::getOperatorFactories)
                .flatMap(List::stream)
                .filter(LocalPlannerAware.class::isInstance)
                .map(LocalPlannerAware.class::cast)
                .forEach(LocalPlannerAware::localPlannerComplete);

        return new LocalExecutionPlan(context.getDriverFactories(), planFragment.getTableScanSchedulingOrder(), planFragment.getStageExecutionDescriptor());
    }

    private static class PrestoSparkLocalExecutionPlanContext
            extends LocalExecutionPlanContext
    {
        private final PlanFragment planFragment;

        public PrestoSparkLocalExecutionPlanContext(TaskContext taskContext, TableWriteInfo tableWriteInfo, PlanFragment planFragment)
        {
            super(taskContext, tableWriteInfo);
            this.planFragment = planFragment;
        }

        private List<DriverFactory> getDriverFactories()
        {
            return ImmutableList.copyOf(driverFactories);
        }

        private boolean isInputDriver()
        {
            return inputDriver;
        }

        private int getNextOperatorId()
        {
            return nextOperatorId++;
        }
    }

    private class PrestoSparkVisitor
            extends Visitor
    {
        private final PlanFragment planFragment;
        private final JsonCodec<TaskSource> taskSourceCodec;
        private final JsonCodec<PlanFragment> planFragmentCodec;
        private final JsonCodec<TableWriteInfo> tableWriteInfoCodec;

        private PrestoSparkVisitor(Session session, PlanFragment planFragment, RemoteSourceFactory remoteSourceFactory, boolean pageSinkCommitRequired, JsonCodec<TaskSource> taskSourceCodec, JsonCodec<TableWriteInfo> tableWriteInfoCodec, JsonCodec<PlanFragment> planFragmentCodec)
        {
            super(session, planFragment.getStageExecutionDescriptor(), remoteSourceFactory, pageSinkCommitRequired);
            this.taskSourceCodec = requireNonNull(taskSourceCodec, "taskSourceCodec is null");
            this.tableWriteInfoCodec = requireNonNull(tableWriteInfoCodec, "tableWriteInfoCodec is null");
            this.planFragmentCodec = requireNonNull(planFragmentCodec, "planFragmentCodec is null");
            this.planFragment = requireNonNull(planFragment, "planFragment is null");
        }

        @Override
        public PhysicalOperation visitNativeEngine(NativeEngineNode node, LocalExecutionPlanContext context)
        {
            OperatorFactory operatorFactory = new NativeEngineOperator.NativeEngineOperatorFactory(((PrestoSparkLocalExecutionPlanContext) context).getNextOperatorId(), node.getId(), taskSourceCodec, planFragmentCodec, tableWriteInfoCodec, planFragment, context.getTableWriteInfo());
            return new PhysicalOperation(operatorFactory, makeLayout(node), context, stageExecutionDescriptor.isScanGroupedExecution(node.getId()) ? GROUPED_EXECUTION : UNGROUPED_EXECUTION);
        }
    }
}
