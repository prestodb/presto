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
package com.facebook.presto.sql.planner.planPrinter;

import com.facebook.presto.Session;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.cost.PlanCostEstimate;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.cost.StatsAndCosts;
import com.facebook.presto.execution.StageExecutionStats;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.expressions.DynamicFilters.DynamicFilterExtractResult;
import com.facebook.presto.expressions.LogicalRowExpressions;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.OperatorNotFoundException;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.SourceLocation;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.plan.AbstractJoinNode;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.CteConsumerNode;
import com.facebook.presto.spi.plan.CteProducerNode;
import com.facebook.presto.spi.plan.CteReferenceNode;
import com.facebook.presto.spi.plan.DeleteNode;
import com.facebook.presto.spi.plan.DistinctLimitNode;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.ExceptNode;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.IndexSourceNode;
import com.facebook.presto.spi.plan.IntersectNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.MarkDistinctNode;
import com.facebook.presto.spi.plan.MergeJoinNode;
import com.facebook.presto.spi.plan.MetadataDeleteNode;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.OutputNode;
import com.facebook.presto.spi.plan.Partitioning;
import com.facebook.presto.spi.plan.PartitioningScheme;
import com.facebook.presto.spi.plan.PlanFragmentId;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.SemiJoinNode;
import com.facebook.presto.spi.plan.SortNode;
import com.facebook.presto.spi.plan.SpatialJoinNode;
import com.facebook.presto.spi.plan.StageExecutionDescriptor;
import com.facebook.presto.spi.plan.StatisticAggregations;
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
import com.facebook.presto.sql.InterpretedFunctionInvoker;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.SubPlan;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.iterative.GroupReference;
import com.facebook.presto.sql.planner.optimizations.JoinNodeUtils;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.AssignUniqueId;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.ExplainAnalyzeNode;
import com.facebook.presto.sql.planner.plan.GroupIdNode;
import com.facebook.presto.sql.planner.plan.IndexJoinNode;
import com.facebook.presto.sql.planner.plan.InternalPlanVisitor;
import com.facebook.presto.sql.planner.plan.LateralJoinNode;
import com.facebook.presto.sql.planner.plan.MergeProcessorNode;
import com.facebook.presto.sql.planner.plan.MergeWriterNode;
import com.facebook.presto.sql.planner.plan.OffsetNode;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.facebook.presto.sql.planner.plan.RowNumberNode;
import com.facebook.presto.sql.planner.plan.SampleNode;
import com.facebook.presto.sql.planner.plan.SequenceNode;
import com.facebook.presto.sql.planner.plan.StatisticsWriterNode;
import com.facebook.presto.sql.planner.plan.TableWriterMergeNode;
import com.facebook.presto.sql.planner.plan.TopNRowNumberNode;
import com.facebook.presto.sql.planner.plan.UpdateNode;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.util.GraphvizPrinter;
import com.google.common.base.CaseFormat;
import com.google.common.base.Functions;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Streams;
import io.airlift.slice.Slice;
import io.airlift.units.Duration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.facebook.presto.SystemSessionProperties.isVerboseOptimizerInfoEnabled;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.execution.StageInfo.getAllStages;
import static com.facebook.presto.expressions.DynamicFilters.extractDynamicFilters;
import static com.facebook.presto.metadata.CastType.CAST;
import static com.facebook.presto.sql.analyzer.ExpressionTreeUtils.createSymbolReference;
import static com.facebook.presto.sql.planner.SortExpressionExtractor.getSortExpressionContext;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.planPrinter.JsonRenderer.JsonPlanFragment;
import static com.facebook.presto.sql.planner.planPrinter.PlanNodeStatsSummarizer.aggregateStageStats;
import static com.facebook.presto.sql.planner.planPrinter.TextRenderer.formatDouble;
import static com.facebook.presto.sql.planner.planPrinter.TextRenderer.formatPositions;
import static com.facebook.presto.sql.planner.planPrinter.TextRenderer.indentString;
import static com.facebook.presto.util.GraphvizPrinter.printDistributedFromFragments;
import static com.google.common.base.CaseFormat.UPPER_UNDERSCORE;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.units.DataSize.succinctBytes;
import static java.lang.String.format;
import static java.util.Arrays.stream;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;

public class PlanPrinter
{
    private final PlanRepresentation representation;
    private final FunctionAndTypeManager functionAndTypeManager;
    private final LogicalRowExpressions logicalRowExpressions;
    private final Function<RowExpression, String> formatter;

    private PlanPrinter(
            PlanNode planRoot,
            TypeProvider types,
            Optional<StageExecutionDescriptor> stageExecutionStrategy,
            StatsAndCosts estimatedStatsAndCosts,
            Optional<Map<PlanNodeId, PlanNodeStats>> stats,
            FunctionAndTypeManager functionAndTypeManager,
            Session session)
    {
        requireNonNull(planRoot, "planRoot is null");
        requireNonNull(types, "types is null");
        requireNonNull(functionAndTypeManager, "functionManager is null");
        requireNonNull(estimatedStatsAndCosts, "estimatedStatsAndCosts is null");
        requireNonNull(stats, "stats is null");

        this.functionAndTypeManager = functionAndTypeManager;
        this.logicalRowExpressions = new LogicalRowExpressions(
                new RowExpressionDeterminismEvaluator(functionAndTypeManager),
                new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver()),
                functionAndTypeManager);

        Optional<Duration> totalCpuTime = stats.map(s -> new Duration(s.values().stream()
                .mapToLong(planNode -> planNode.getPlanNodeCpuTime().toMillis())
                .sum(), MILLISECONDS));

        Optional<Duration> totalScheduledTime = stats.map(s -> new Duration(s.values().stream()
                .mapToLong(planNode -> planNode.getPlanNodeScheduledTime().toMillis())
                .sum(), MILLISECONDS));

        this.representation = new PlanRepresentation(
                planRoot,
                types,
                totalCpuTime,
                totalScheduledTime,
                session.getOptimizerInformationCollector().getOptimizationInfo(),
                session.getCteInformationCollector().getCTEInformationList(),
                session.getOptimizerResultCollector().getOptimizerResults());

        RowExpressionFormatter rowExpressionFormatter = new RowExpressionFormatter(functionAndTypeManager);
        ConnectorSession connectorSession = requireNonNull(session, "session is null").toConnectorSession();
        this.formatter = rowExpression -> rowExpressionFormatter.formatRowExpression(connectorSession, rowExpression);

        Visitor visitor = new Visitor(stageExecutionStrategy, types, estimatedStatsAndCosts, session, stats);
        planRoot.accept(visitor, null);
    }

    public String toText(boolean verbose, int level, boolean verboseOptimizerInfo)
    {
        return new TextRenderer(verbose, level, verboseOptimizerInfo).render(representation);
    }

    public String toJson()
    {
        return new JsonRenderer(functionAndTypeManager).render(representation);
    }

    public static String jsonFragmentPlan(PlanNode root, Set<VariableReferenceExpression> variables, StatsAndCosts estimatedStatsAndCosts, FunctionAndTypeManager functionAndTypeManager, Session session)
    {
        TypeProvider typeProvider = TypeProvider.fromVariables(variables);

        return new PlanPrinter(root, typeProvider, Optional.empty(), estimatedStatsAndCosts, Optional.empty(), functionAndTypeManager, session).toJson();
    }

    public static String textLogicalPlan(PlanNode plan, TypeProvider types, StatsAndCosts estimatedStatsAndCosts, FunctionAndTypeManager functionAndTypeManager, Session session, int level)
    {
        return new PlanPrinter(plan, types,
                Optional.empty(),
                estimatedStatsAndCosts,
                Optional.empty(),
                functionAndTypeManager,
                session)
                .toText(false, level, isVerboseOptimizerInfoEnabled(session));
    }

    public static String textLogicalPlan(
            PlanNode plan,
            TypeProvider types,
            StatsAndCosts estimatedStatsAndCosts,
            FunctionAndTypeManager functionAndTypeManager,
            Session session,
            int level,
            boolean verbose,
            boolean verboseOptimizerInfoEnabled)
    {
        return textLogicalPlan(plan, types, Optional.empty(), estimatedStatsAndCosts, Optional.empty(), functionAndTypeManager, session, level, verbose, verboseOptimizerInfoEnabled);
    }

    public static String textLogicalPlan(
            PlanNode plan,
            TypeProvider types,
            Optional<StageExecutionDescriptor> stageExecutionStrategy,
            StatsAndCosts estimatedStatsAndCosts,
            Optional<Map<PlanNodeId, PlanNodeStats>> stats,
            FunctionAndTypeManager functionAndTypeManager,
            Session session,
            int level,
            boolean verbose,
            boolean verboseOptimizerInfoEnabled)
    {
        return new PlanPrinter(plan, types, stageExecutionStrategy, estimatedStatsAndCosts, stats, functionAndTypeManager, session).toText(verbose, level, verboseOptimizerInfoEnabled);
    }

    public static String textDistributedPlan(StageInfo outputStageInfo, FunctionAndTypeManager functionAndTypeManager, Session session, boolean verbose)
    {
        StringBuilder builder = new StringBuilder();
        List<StageInfo> allStages = getAllStages(Optional.of(outputStageInfo));
        Map<PlanNodeId, PlanNodeStats> aggregatedStats = aggregateStageStats(allStages);
        for (StageInfo stageInfo : allStages) {
            builder.append(formatFragment(
                    functionAndTypeManager,
                    session,
                    stageInfo.getPlan().get(),
                    Optional.of(stageInfo),
                    Optional.of(aggregatedStats),
                    verbose));
        }

        return builder.toString();
    }

    public static String textDistributedPlan(SubPlan plan, FunctionAndTypeManager functionAndTypeManager, Session session, boolean verbose)
    {
        StringBuilder builder = new StringBuilder();
        for (PlanFragment fragment : plan.getAllFragments()) {
            builder.append(formatFragment(
                    functionAndTypeManager,
                    session,
                    fragment,
                    Optional.empty(),
                    Optional.empty(),
                    verbose));
        }

        return builder.toString();
    }

    public static String textPlanFragment(PlanFragment fragment, FunctionAndTypeManager functionAndTypeManager, Session session, boolean verbose)
    {
        return formatFragment(
                functionAndTypeManager,
                session,
                fragment,
                Optional.empty(),
                Optional.empty(),
                verbose);
    }

    public static String jsonLogicalPlan(
            PlanNode plan,
            TypeProvider types,
            FunctionAndTypeManager functionAndTypeManager,
            StatsAndCosts estimatedStatsAndCosts,
            Session session)
    {
        return jsonLogicalPlan(plan, types, Optional.empty(), functionAndTypeManager, estimatedStatsAndCosts, session, Optional.empty());
    }

    public static String jsonLogicalPlan(
            PlanNode plan,
            TypeProvider types,
            Optional<StageExecutionDescriptor> stageExecutionStrategy,
            FunctionAndTypeManager functionAndTypeManager,
            StatsAndCosts estimatedStatsAndCosts,
            Session session,
            Optional<Map<PlanNodeId, PlanNodeStats>> stats)
    {
        return new PlanPrinter(plan, types, stageExecutionStrategy, estimatedStatsAndCosts, stats, functionAndTypeManager, session).toJson();
    }

    public static String jsonDistributedPlan(StageInfo outputStageInfo, FunctionAndTypeManager functionAndTypeManager, Session session)
    {
        List<StageInfo> allStages = getAllStages(Optional.of(outputStageInfo));
        Map<PlanNodeId, PlanNodeStats> aggregatedStats = aggregateStageStats(allStages);
        List<PlanFragment> allFragments = getAllStages(Optional.of(outputStageInfo)).stream()
                .map(StageInfo::getPlan)
                .map(Optional::get)
                .collect(toImmutableList());
        return formatJsonFragmentList(allFragments, Optional.of(aggregatedStats), functionAndTypeManager, session);
    }

    public static String jsonDistributedPlan(SubPlan plan, FunctionAndTypeManager functionAndTypeManager, Session session)
    {
        return formatJsonFragmentList(plan.getAllFragments(), Optional.empty(), functionAndTypeManager, session);
    }

    private String formatSourceLocation(Optional<SourceLocation> sourceLocation1, Optional<SourceLocation> sourceLocation2)
    {
        if (sourceLocation1.isPresent()) {
            return formatSourceLocation(sourceLocation1);
        }

        return formatSourceLocation(sourceLocation2);
    }

    private String formatSourceLocation(Optional<SourceLocation> sourceLocation)
    {
        if (sourceLocation.isPresent()) {
            return " (" + sourceLocation.get().toString() + ")";
        }

        return "";
    }

    private static String formatJsonFragmentList(List<PlanFragment> fragments, Optional<Map<PlanNodeId, PlanNodeStats>> executionStats, FunctionAndTypeManager functionAndTypeManager, Session session)
    {
        ImmutableSortedMap.Builder<PlanFragmentId, JsonPlanFragment> fragmentJsonMap = ImmutableSortedMap.naturalOrder();
        for (PlanFragment fragment : fragments) {
            PlanFragmentId fragmentId = fragment.getId();
            TypeProvider typeProvider = TypeProvider.fromVariables(fragment.getVariables());
            PlanPrinter printer = new PlanPrinter(fragment.getRoot(), typeProvider, Optional.of(fragment.getStageExecutionDescriptor()), fragment.getStatsAndCosts().orElse(StatsAndCosts.empty()), executionStats, functionAndTypeManager, session);
            JsonPlanFragment jsonPlanFragment = new JsonPlanFragment(printer.toJson());
            fragmentJsonMap.put(fragmentId, jsonPlanFragment);
        }
        return new JsonRenderer(functionAndTypeManager).render(fragmentJsonMap.build());
    }

    public static String formattedFragmentString(StageExecutionStats stageExecutionStats,
            double avgPositionsPerTask, double sdAmongTasks, int taskSize)
    {
        return format("CPU: %s, Scheduled: %s, Input: %s (%s); per task: avg.: %s std.dev.: %s, Output: %s (%s), %s tasks%n",
                stageExecutionStats.getTotalCpuTime().convertToMostSuccinctTimeUnit(),
                stageExecutionStats.getTotalScheduledTime().convertToMostSuccinctTimeUnit(),
                formatPositions(stageExecutionStats.getProcessedInputPositions()),
                succinctBytes(stageExecutionStats.getProcessedInputDataSizeInBytes()),
                formatDouble(avgPositionsPerTask),
                formatDouble(sdAmongTasks),
                formatPositions(stageExecutionStats.getOutputPositions()),
                succinctBytes(stageExecutionStats.getOutputDataSizeInBytes()),
                taskSize);
    }

    private static String formatFragment(
            FunctionAndTypeManager functionAndTypeManager,
            Session session,
            PlanFragment fragment,
            Optional<StageInfo> stageInfo,
            Optional<Map<PlanNodeId, PlanNodeStats>> planNodeStats,
            boolean verbose)
    {
        StringBuilder builder = new StringBuilder();
        builder.append(format("Fragment %s [%s]%n",
                fragment.getId(),
                fragment.getPartitioning()));

        if (stageInfo.isPresent()) {
            StageExecutionStats stageExecutionStats = stageInfo.get().getLatestAttemptExecutionInfo().getStats();
            List<TaskInfo> tasks = stageInfo.get().getLatestAttemptExecutionInfo().getTasks();

            double avgPositionsPerTask = tasks.stream().mapToLong(task -> task.getStats().getProcessedInputPositions()).average().orElse(Double.NaN);
            double squaredDifferences = tasks.stream().mapToDouble(task -> Math.pow(task.getStats().getProcessedInputPositions() - avgPositionsPerTask, 2)).sum();
            double sdAmongTasks = Math.sqrt(squaredDifferences / tasks.size());

            builder.append(indentString(1))
                    .append(formattedFragmentString(stageExecutionStats, avgPositionsPerTask, sdAmongTasks, tasks.size()));
        }

        PartitioningScheme partitioningScheme = fragment.getPartitioningScheme();
        builder.append(indentString(1))
                .append(format("Output layout: [%s]%n",
                        Joiner.on(", ").join(partitioningScheme.getOutputLayout())));

        builder.append(indentString(1));
        boolean replicateNullsAndAny = partitioningScheme.isReplicateNullsAndAny();
        if (replicateNullsAndAny) {
            builder.append(format("Output partitioning: %s (replicate nulls and any) [%s]%s%n",
                    partitioningScheme.getPartitioning().getHandle(),
                    Joiner.on(", ").join(partitioningScheme.getPartitioning().getArguments()),
                    formatHash(partitioningScheme.getHashColumn())));
        }
        else {
            builder.append(format("Output partitioning: %s [%s]%s%n",
                    partitioningScheme.getPartitioning().getHandle(),
                    Joiner.on(", ").join(partitioningScheme.getPartitioning().getArguments()),
                    formatHash(partitioningScheme.getHashColumn())));
        }
        builder.append(indentString(1)).append(format("Output encoding: %s%n", fragment.getPartitioningScheme().getEncoding()));
        builder.append(indentString(1)).append(format("Stage Execution Strategy: %s%n", fragment.getStageExecutionDescriptor().getStageExecutionStrategy()));

        TypeProvider typeProvider = TypeProvider.fromVariables(fragment.getVariables());
        builder.append(
                        textLogicalPlan(
                                fragment.getRoot(),
                                typeProvider,
                                Optional.of(fragment.getStageExecutionDescriptor()),
                                fragment.getStatsAndCosts().orElse(StatsAndCosts.empty()),
                                planNodeStats,
                                functionAndTypeManager,
                                session,
                                1,
                                verbose,
                                isVerboseOptimizerInfoEnabled(session)))
                .append("\n");

        return builder.toString();
    }

    public static String graphvizLogicalPlan(PlanNode plan, TypeProvider types, StatsAndCosts estimatedStatsAndCosts, FunctionAndTypeManager functionAndTypeManager, Session session)
    {
        // TODO: This should move to something like GraphvizRenderer
        PlanFragment fragment = new PlanFragment(
                new PlanFragmentId(0),
                plan,
                types.allVariables(),
                SINGLE_DISTRIBUTION,
                ImmutableList.of(plan.getId()),
                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), plan.getOutputVariables()),
                StageExecutionDescriptor.ungroupedExecution(),
                false,
                Optional.of(estimatedStatsAndCosts),
                Optional.empty());
        return GraphvizPrinter.printLogical(ImmutableList.of(fragment), functionAndTypeManager, session);
    }

    public static String graphvizDistributedPlan(SubPlan plan, FunctionAndTypeManager functionAndTypeManager, Session session)
    {
        return GraphvizPrinter.printDistributed(plan, functionAndTypeManager, session);
    }

    public static String graphvizDistributedPlan(StageInfo stageInfo, FunctionAndTypeManager functionAndTypeManager, Session session)
    {
        List<PlanFragment> allFragments = getAllStages(Optional.of(stageInfo)).stream()
                .map(StageInfo::getPlan)
                .map(Optional::get)
                .sorted(Comparator.comparing(PlanFragment::getId))
                .collect(toImmutableList());
        return printDistributedFromFragments(allFragments, functionAndTypeManager, session);
    }

    private class Visitor
            extends InternalPlanVisitor<Void, Void>
    {
        private final Optional<StageExecutionDescriptor> stageExecutionStrategy;
        private final TypeProvider types;
        private final StatsAndCosts estimatedStatsAndCosts;
        private final Optional<Map<PlanNodeId, PlanNodeStats>> stats;
        private final Session session;

        public Visitor(Optional<StageExecutionDescriptor> stageExecutionStrategy, TypeProvider types, StatsAndCosts estimatedStatsAndCosts, Session session, Optional<Map<PlanNodeId, PlanNodeStats>> stats)
        {
            this.stageExecutionStrategy = requireNonNull(stageExecutionStrategy, "stageExecutionStrategy is null");
            this.types = requireNonNull(types, "types is null");
            this.estimatedStatsAndCosts = requireNonNull(estimatedStatsAndCosts, "estimatedStatsAndCosts is null");
            this.stats = requireNonNull(stats, "stats is null");
            this.session = requireNonNull(session, "session is null");
        }

        @Override
        public Void visitExplainAnalyze(ExplainAnalyzeNode node, Void context)
        {
            addNode(node, "ExplainAnalyze");
            return processChildren(node, context);
        }

        @Override
        public Void visitJoin(JoinNode node, Void context)
        {
            List<String> joinExpressions = new ArrayList<>();
            for (EquiJoinClause clause : node.getCriteria()) {
                joinExpressions.add(JoinNodeUtils.toExpression(clause).toString());
            }
            node.getFilter().map(formatter::apply).ifPresent(joinExpressions::add);

            NodeRepresentation nodeOutput;
            if (node.isCrossJoin()) {
                checkState(joinExpressions.isEmpty());
                nodeOutput = addNode(node, "CrossJoin");
            }
            else {
                nodeOutput = addNode(node,
                        node.getType().getJoinLabel(),
                        format("[%s]%s", Joiner.on(" AND ").join(joinExpressions), formatHash(node.getLeftHashVariable(), node.getRightHashVariable())));
            }

            node.getDistributionType().ifPresent(distributionType -> nodeOutput.appendDetailsLine("Distribution: %s", distributionType));
            if (!node.getDynamicFilters().isEmpty()) {
                nodeOutput.appendDetails(getDynamicFilterAssignments(node));
            }

            getSortExpressionContext(node, functionAndTypeManager)
                    .ifPresent(sortContext -> nodeOutput.appendDetails("SortExpression[%s]", formatter.apply(sortContext.getSortExpression())));
            node.getLeft().accept(this, context);
            node.getRight().accept(this, context);

            return null;
        }

        @Override
        public Void visitSpatialJoin(SpatialJoinNode node, Void context)
        {
            NodeRepresentation nodeOutput = addNode(node,
                    node.getType().getJoinLabel(),
                    format("[%s]", formatter.apply(node.getFilter())));

            nodeOutput.appendDetailsLine("Distribution: %s", node.getDistributionType());
            node.getLeft().accept(this, context);
            node.getRight().accept(this, context);

            return null;
        }

        @Override
        public Void visitSemiJoin(SemiJoinNode node, Void context)
        {
            NodeRepresentation nodeOutput = addNode(node,
                    "SemiJoin",
                    format("[%s = %s]%s",
                            node.getSourceJoinVariable(),
                            node.getFilteringSourceJoinVariable(),
                            formatHash(node.getSourceHashVariable(), node.getFilteringSourceHashVariable())));
            node.getDistributionType().ifPresent(distributionType -> nodeOutput.appendDetailsLine("Distribution: %s", distributionType));
            if (!node.getDynamicFilters().isEmpty()) {
                nodeOutput.appendDetails(getDynamicFilterAssignments(node));
            }
            node.getSource().accept(this, context);
            node.getFilteringSource().accept(this, context);

            return null;
        }

        @Override
        public Void visitIndexSource(IndexSourceNode node, Void context)
        {
            NodeRepresentation nodeOutput = addNode(node,
                    "IndexSource",
                    format("[%s, lookup = %s]", node.getIndexHandle(), node.getLookupVariables()));

            nodeOutput.appendDetailsLine("TableHandle: %s", node.getTableHandle().getConnectorHandle().toString());

            for (Map.Entry<VariableReferenceExpression, ColumnHandle> entry : node.getAssignments().entrySet()) {
                if (node.getOutputVariables().contains(entry.getKey())) {
                    nodeOutput.appendDetailsLine("%s := %s%s", entry.getKey(), entry.getValue(), formatSourceLocation(entry.getKey().getSourceLocation()));
                }
            }
            return null;
        }

        @Override
        public Void visitIndexJoin(IndexJoinNode node, Void context)
        {
            List<String> joinExpressions = new ArrayList<>();
            for (IndexJoinNode.EquiJoinClause clause : node.getCriteria()) {
                joinExpressions.add(new ComparisonExpression(ComparisonExpression.Operator.EQUAL,
                        createSymbolReference(clause.getProbe()),
                        createSymbolReference(clause.getIndex())).toString());
            }
            node.getFilter().map(formatter).ifPresent(joinExpressions::add);

            addNode(node,
                    format("%sIndexJoin", node.getType().getJoinLabel()),
                    format("[%s]%s", Joiner.on(" AND ").join(joinExpressions), formatHash(node.getProbeHashVariable(), node.getIndexHashVariable())));
            node.getProbeSource().accept(this, context);
            node.getIndexSource().accept(this, context);

            return null;
        }

        @Override
        public Void visitMergeJoin(MergeJoinNode node, Void context)
        {
            List<String> joinExpressions = new ArrayList<>();
            for (EquiJoinClause clause : node.getCriteria()) {
                joinExpressions.add(JoinNodeUtils.toExpression(clause).toString());
            }
            node.getFilter().map(formatter::apply).ifPresent(joinExpressions::add);

            addNode(node,
                    "MergeJoin",
                    format("[type: %s], [%s]%s", node.getType().getJoinLabel(), Joiner.on(" AND ").join(joinExpressions), formatHash(node.getLeftHashVariable(), node.getRightHashVariable())));
            node.getLeft().accept(this, context);
            node.getRight().accept(this, context);
            return null;
        }

        @Override
        public Void visitLimit(LimitNode node, Void context)
        {
            addNode(node,
                    format("Limit%s", node.isPartial() ? "Partial" : ""),
                    format("[%s]", node.getCount()));
            return processChildren(node, context);
        }

        @Override
        public Void visitDistinctLimit(DistinctLimitNode node, Void context)
        {
            addNode(node,
                    format("DistinctLimit%s", node.isPartial() ? "Partial" : ""),
                    format("[%s]%s", node.getLimit(), formatHash(node.getHashVariable())));
            return processChildren(node, context);
        }

        @Override
        public Void visitAggregation(AggregationNode node, Void context)
        {
            String type = "";
            if (node.getStep() != AggregationNode.Step.SINGLE) {
                type = format("(%s)", node.getStep().toString());
            }
            if (node.isSegmentedAggregationEligible()) {
                type = format("%s(SEGMENTED, %s)", type, node.getPreGroupedVariables());
            }
            if (node.isStreamable()) {
                type = format("%s(STREAMING)", type);
            }
            String key = "";
            if (!node.getGroupingKeys().isEmpty()) {
                key = node.getGroupingKeys().toString();
            }

            NodeRepresentation nodeOutput = addNode(node,
                    format("Aggregate%s%s%s", type, key, formatHash(node.getHashVariable())));

            for (Map.Entry<VariableReferenceExpression, AggregationNode.Aggregation> entry : node.getAggregations().entrySet()) {
                nodeOutput.appendDetailsLine("%s := %s%s", entry.getKey(), formatAggregation(entry.getValue()), formatSourceLocation(entry.getValue().getCall().getSourceLocation(), entry.getKey().getSourceLocation()));
            }

            return processChildren(node, context);
        }

        private String formatAggregation(AggregationNode.Aggregation aggregation)
        {
            StringBuilder builder = new StringBuilder();
            builder.append("\"");
            builder.append(functionAndTypeManager.getFunctionMetadata(aggregation.getFunctionHandle()).getName());
            builder.append("\"");
            builder.append("(");
            if (aggregation.isDistinct()) {
                builder.append("DISTINCT ");
            }
            if (aggregation.getArguments().isEmpty()) {
                builder.append("*");
            }
            else {
                builder.append("(" + Joiner.on(",").join(aggregation.getArguments().stream().map(formatter::apply).collect(toImmutableList())) + ")");
            }
            builder.append(")");
            aggregation.getFilter().ifPresent(filter -> builder.append(" WHERE " + formatter.apply(filter)));
            aggregation.getOrderBy().ifPresent(orderingScheme -> builder.append(" ORDER BY " + orderingScheme.toString()));
            aggregation.getMask().ifPresent(mask -> builder.append(" (mask = " + mask + ")"));
            return builder.toString();
        }

        @Override
        public Void visitGroupId(GroupIdNode node, Void context)
        {
            // grouping sets are easier to understand in terms of inputs
            List<List<VariableReferenceExpression>> inputGroupingSetSymbols = node.getGroupingSets().stream()
                    .map(set -> set.stream()
                            .map(symbol -> node.getGroupingColumns().get(symbol))
                            .collect(Collectors.toList()))
                    .collect(Collectors.toList());

            NodeRepresentation nodeOutput = addNode(node, "GroupId", format("%s", inputGroupingSetSymbols));

            for (Map.Entry<VariableReferenceExpression, VariableReferenceExpression> mapping : node.getGroupingColumns().entrySet()) {
                nodeOutput.appendDetailsLine("%s := %s%s", mapping.getKey(), mapping.getValue(), formatSourceLocation(mapping.getValue().getSourceLocation(), mapping.getKey().getSourceLocation()));
            }

            return processChildren(node, context);
        }

        @Override
        public Void visitMarkDistinct(MarkDistinctNode node, Void context)
        {
            addNode(node,
                    "MarkDistinct",
                    format("[distinct=%s marker=%s]%s", formatOutputs(node.getDistinctVariables()), node.getMarkerVariable(), formatHash(node.getHashVariable())));

            return processChildren(node, context);
        }

        @Override
        public Void visitWindow(WindowNode node, Void context)
        {
            List<String> partitionBy = Lists.transform(node.getPartitionBy(), Functions.toStringFunction());

            List<String> args = new ArrayList<>();
            if (!partitionBy.isEmpty()) {
                List<VariableReferenceExpression> prePartitioned = node.getPartitionBy().stream()
                        .filter(node.getPrePartitionedInputs()::contains)
                        .collect(toImmutableList());

                List<VariableReferenceExpression> notPrePartitioned = node.getPartitionBy().stream()
                        .filter(column -> !node.getPrePartitionedInputs().contains(column))
                        .collect(toImmutableList());

                StringBuilder builder = new StringBuilder();
                if (!prePartitioned.isEmpty()) {
                    builder.append("<")
                            .append(Joiner.on(", ").join(prePartitioned))
                            .append(">");
                    if (!notPrePartitioned.isEmpty()) {
                        builder.append(", ");
                    }
                }
                if (!notPrePartitioned.isEmpty()) {
                    builder.append(Joiner.on(", ").join(notPrePartitioned));
                }
                args.add(format("partition by (%s)", builder));
            }
            if (node.getOrderingScheme().isPresent()) {
                OrderingScheme orderingScheme = node.getOrderingScheme().get();
                args.add(format("order by (%s)", Stream.concat(
                                orderingScheme.getOrderByVariables().stream()
                                        .limit(node.getPreSortedOrderPrefix())
                                        .map(symbol -> "<" + symbol + " " + orderingScheme.getOrdering(symbol) + ">"),
                                orderingScheme.getOrderByVariables().stream()
                                        .skip(node.getPreSortedOrderPrefix())
                                        .map(symbol -> symbol + " " + orderingScheme.getOrdering(symbol)))
                        .collect(Collectors.joining(", "))));
            }

            NodeRepresentation nodeOutput = addNode(node, "Window", format("[%s]%s", Joiner.on(", ").join(args), formatHash(node.getHashVariable())));

            for (Map.Entry<VariableReferenceExpression, WindowNode.Function> entry : node.getWindowFunctions().entrySet()) {
                CallExpression call = entry.getValue().getFunctionCall();
                String frameInfo = formatFrame(entry.getValue().getFrame());

                nodeOutput.appendDetailsLine(
                        "%s := %s(%s) %s%s",
                        entry.getKey(),
                        call.getDisplayName(),
                        Joiner.on(", ").join(call.getArguments().stream().map(formatter::apply).collect(toImmutableList())),
                        frameInfo,
                        formatSourceLocation(entry.getValue().getFunctionCall().getSourceLocation(), entry.getKey().getSourceLocation()));
            }
            return processChildren(node, context);
        }

        @Override
        public Void visitTopNRowNumber(TopNRowNumberNode node, Void context)
        {
            List<String> partitionBy = node.getPartitionBy().stream()
                    .map(Functions.toStringFunction())
                    .collect(toImmutableList());

            List<String> orderBy = node.getOrderingScheme().getOrderByVariables().stream()
                    .map(input -> input + " " + node.getOrderingScheme().getOrdering(input))
                    .collect(toImmutableList());

            List<String> args = new ArrayList<>();
            args.add(format("partition by (%s)", Joiner.on(", ").join(partitionBy)));
            args.add(format("order by (%s)", Joiner.on(", ").join(orderBy)));

            NodeRepresentation nodeOutput = addNode(node,
                    format("TopNRowNumber%s", node.isPartial() ? "Partial" : ""),
                    format("[%s limit %s]%s", Joiner.on(", ").join(args), node.getMaxRowCountPerPartition(), formatHash(node.getHashVariable())));

            nodeOutput.appendDetailsLine("%s := %s%s", node.getRowNumberVariable(), "row_number()", formatSourceLocation(node.getRowNumberVariable().getSourceLocation()));

            return processChildren(node, context);
        }

        @Override
        public Void visitRowNumber(RowNumberNode node, Void context)
        {
            List<String> partitionBy = Lists.transform(node.getPartitionBy(), Functions.toStringFunction());
            List<String> args = new ArrayList<>();
            if (!partitionBy.isEmpty()) {
                args.add(format("partition by (%s)", Joiner.on(", ").join(partitionBy)));
            }

            if (node.getMaxRowCountPerPartition().isPresent()) {
                args.add(format("limit = %s", node.getMaxRowCountPerPartition().get()));
            }

            NodeRepresentation nodeOutput = addNode(node,
                    format("RowNumber%s", node.isPartial() ? "Partial" : ""),
                    format("[%s]%s", Joiner.on(", ").join(args), formatHash(node.getHashVariable())));
            nodeOutput.appendDetailsLine("%s := %s%s", node.getRowNumberVariable(), "row_number()", formatSourceLocation(node.getRowNumberVariable().getSourceLocation()));

            return processChildren(node, context);
        }

        @Override
        public Void visitTableScan(TableScanNode node, Void context)
        {
            TableHandle table = node.getTable();
            NodeRepresentation nodeOutput;
            if (stageExecutionStrategy.isPresent()) {
                nodeOutput = addNode(node,
                        "TableScan",
                        format("[%s, grouped = %s]", table, stageExecutionStrategy.get().isScanGroupedExecution(node.getId())));
            }
            else {
                nodeOutput = addNode(node, "TableScan", format("[%s]", table));
            }
            PlanNodeStats nodeStats = stats.map(s -> s.get(node.getId())).orElse(null);
            printTableScanInfo(nodeOutput, node, nodeStats);
            return null;
        }

        @Override
        public Void visitSequence(SequenceNode node, Void context)
        {
            NodeRepresentation nodeOutput;
            nodeOutput = addNode(node, "Sequence");
            nodeOutput.appendDetails(getCteExecutionOrder(node));

            return processChildren(node, context);
        }

        @Override
        public Void visitCteConsumer(CteConsumerNode node, Void context)
        {
            NodeRepresentation nodeOutput;
            nodeOutput = addNode(node, "CteConsumer");
            nodeOutput.appendDetailsLine("CTE_NAME: %s", node.getCteId());
            return processChildren(node, context);
        }

        @Override
        public Void visitCteProducer(CteProducerNode node, Void context)
        {
            NodeRepresentation nodeOutput;
            nodeOutput = addNode(node, "CteProducer");
            nodeOutput.appendDetailsLine("CTE_NAME: %s", node.getCteId());
            return processChildren(node, context);
        }

        @Override
        public Void visitCteReference(CteReferenceNode node, Void context)
        {
            addNode(node, "CteReference");
            return processChildren(node, context);
        }

        @Override
        public Void visitValues(ValuesNode node, Void context)
        {
            NodeRepresentation nodeOutput;
            if (node.getValuesNodeLabel().isPresent()) {
                nodeOutput = addNode(node, format("Values converted from TableScan[%s]", node.getValuesNodeLabel().get()));
            }
            else {
                nodeOutput = addNode(node, "Values");
            }
            for (List<RowExpression> row : node.getRows()) {
                nodeOutput.appendDetailsLine("(" + row.stream().map(formatter::apply).collect(Collectors.joining(", ")) + ")");
            }
            return null;
        }

        @Override
        public Void visitFilter(FilterNode node, Void context)
        {
            return visitScanFilterAndProjectInfo(node, Optional.of(node), Optional.empty(), context);
        }

        @Override
        public Void visitProject(ProjectNode node, Void context)
        {
            if (node.getSource() instanceof FilterNode) {
                return visitScanFilterAndProjectInfo(node, Optional.of((FilterNode) node.getSource()), Optional.of(node), context);
            }

            return visitScanFilterAndProjectInfo(node, Optional.empty(), Optional.of(node), context);
        }

        private Void visitScanFilterAndProjectInfo(
                PlanNode node,
                Optional<FilterNode> filterNode,
                Optional<ProjectNode> projectNode,
                Void context)
        {
            checkState(projectNode.isPresent() || filterNode.isPresent());

            PlanNode sourceNode;
            if (filterNode.isPresent()) {
                sourceNode = filterNode.get().getSource();
            }
            else {
                sourceNode = projectNode.get().getSource();
            }

            Optional<TableScanNode> scanNode;
            if (sourceNode instanceof TableScanNode) {
                scanNode = Optional.of((TableScanNode) sourceNode);
            }
            else {
                scanNode = Optional.empty();
            }

            String formatString = "[";
            String operatorName = "";
            List<Object> arguments = new LinkedList<>();

            if (scanNode.isPresent()) {
                operatorName += "Scan";
                formatString += "table = %s, ";
                TableHandle table = scanNode.get().getTable();
                arguments.add(table);
                if (stageExecutionStrategy.isPresent()) {
                    formatString += "grouped = %s, ";
                    arguments.add(stageExecutionStrategy.get().isScanGroupedExecution(scanNode.get().getId()));
                }
            }

            if (filterNode.isPresent()) {
                operatorName += "Filter";
                formatString += "filterPredicate = %s, ";
                RowExpression predicate = filterNode.get().getPredicate();
                DynamicFilterExtractResult dynamicFilterExtractResult = extractDynamicFilters(predicate);
                arguments.add(formatter.apply(logicalRowExpressions.combineConjuncts(dynamicFilterExtractResult.getStaticConjuncts())));

                if (!dynamicFilterExtractResult.getDynamicConjuncts().isEmpty()) {
                    formatString += "dynamicFilter = %s, ";
                    String dynamicConjuncts = dynamicFilterExtractResult.getDynamicConjuncts().stream()
                            .map(filter -> filter.getId() + " -> " + filter.getInput())
                            .collect(Collectors.joining(", ", "{", "}"));
                    arguments.add(dynamicConjuncts);
                }
            }

            if (projectNode.isPresent()) {
                operatorName += "Project";
                formatString += "projectLocality = %s, ";
                arguments.add(projectNode.get().getLocality());
            }

            if (formatString.length() > 1) {
                formatString = formatString.substring(0, formatString.length() - 2);
            }
            formatString += "]";

            List<PlanNodeId> allNodes = Stream.of(scanNode, filterNode, projectNode)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .map(PlanNode::getId)
                    .collect(toList());

            NodeRepresentation nodeOutput = addNode(
                    node,
                    operatorName,
                    format(formatString, arguments.toArray(new Object[0])),
                    allNodes,
                    ImmutableList.of(sourceNode),
                    ImmutableList.of());

            if (projectNode.isPresent()) {
                printAssignments(nodeOutput, projectNode.get().getAssignments());
            }

            if (scanNode.isPresent()) {
                PlanNodeStats nodeStats = stats.map(s -> s.get(node.getId())).orElse(null);
                printTableScanInfo(nodeOutput, scanNode.get(), nodeStats);
                return null;
            }

            sourceNode.accept(this, context);
            return null;
        }

        private void printTableScanInfo(NodeRepresentation nodeOutput, TableScanNode node, PlanNodeStats nodeStats)
        {
            TableHandle table = node.getTable();

            if (table.getLayout().isPresent()) {
                // TODO: find a better way to do this
                ConnectorTableLayoutHandle layout = table.getLayout().get();
                if (!table.getConnectorHandle().toString().equals(layout.toString())) {
                    nodeOutput.appendDetailsLine("LAYOUT: %s", layout);
                }
            }

            if (!node.getTableConstraints().isEmpty()) {
                nodeOutput.appendDetailsLine("Table Constraints: %s", node.getTableConstraints());
            }

            TupleDomain<ColumnHandle> predicate = node.getCurrentConstraint();
            if (predicate == null) {
                // This happens when printing the plan fragment on worker for debug purpose
                nodeOutput.appendDetailsLine(":: PREDICATE INFORMATION UNAVAILABLE");
            }
            else if (predicate.isNone()) {
                nodeOutput.appendDetailsLine(":: NONE");
            }
            else {
                // first, print output columns and their constraints
                for (Map.Entry<VariableReferenceExpression, ColumnHandle> assignment : node.getAssignments().entrySet()) {
                    ColumnHandle column = assignment.getValue();
                    nodeOutput.appendDetailsLine("%s := %s%s", assignment.getKey(), column, formatSourceLocation(assignment.getKey().getSourceLocation()));
                    printConstraint(nodeOutput, column, predicate);
                }

                // then, print constraints for columns that are not in the output
                if (!predicate.isAll()) {
                    Set<ColumnHandle> outputs = ImmutableSet.copyOf(node.getAssignments().values());

                    predicate.getDomains().get()
                            .entrySet().stream()
                            .filter(entry -> !outputs.contains(entry.getKey()))
                            .forEach(entry -> {
                                ColumnHandle column = entry.getKey();
                                nodeOutput.appendDetailsLine("%s", column);
                                printConstraint(nodeOutput, column, predicate);
                            });
                }
            }

            if (nodeStats != null) {
                // Add to 'details' rather than 'statistics', since these stats are node-specific
                long inputPositions = nodeStats.getPlanNodeInputPositions();
                nodeOutput.appendDetails("Input: %s (%s)", formatPositions(inputPositions), nodeStats.getPlanNodeInputDataSize().toString());
                double filtered = 100.0d * (inputPositions - nodeStats.getPlanNodeOutputPositions()) / inputPositions;
                nodeOutput.appendDetailsLine(", Filtered: %s%%", formatDouble(filtered));

                long rawInputPositions = nodeStats.getPlanNodeRawInputPositions();
                if (rawInputPositions != inputPositions) {
                    nodeOutput.appendDetails("Raw input: %s (%s)", formatPositions(rawInputPositions), nodeStats.getPlanNodeRawInputDataSize().toString());
                    double rawFiltered = 100.0d * (rawInputPositions - inputPositions) / rawInputPositions;
                    nodeOutput.appendDetailsLine(", Filtered: %s%%", formatDouble(rawFiltered));
                }
            }
        }

        @Override
        public Void visitUnnest(UnnestNode node, Void context)
        {
            addNode(node,
                    "Unnest",
                    format("[replicate=%s, unnest=%s]", formatOutputs(node.getReplicateVariables()), formatOutputs(node.getUnnestVariables().keySet())));
            return processChildren(node, context);
        }

        @Override
        public Void visitOutput(OutputNode node, Void context)
        {
            NodeRepresentation nodeOutput = addNode(node, "Output", format("[%s]", Joiner.on(", ").join(node.getColumnNames())));
            for (int i = 0; i < node.getColumnNames().size(); i++) {
                String name = node.getColumnNames().get(i);
                VariableReferenceExpression variable = node.getOutputVariables().get(i);
                if (!name.equals(variable.toString())) {
                    nodeOutput.appendDetailsLine("%s := %s%s", name, variable, formatSourceLocation(variable.getSourceLocation()));
                }
            }
            return processChildren(node, context);
        }

        @Override
        public Void visitTopN(TopNNode node, Void context)
        {
            Iterable<String> keys = Iterables.transform(node.getOrderingScheme().getOrderByVariables(), input -> input + " " + node.getOrderingScheme().getOrdering(input));

            addNode(node,
                    format("TopN%s", node.getStep() == TopNNode.Step.PARTIAL ? "Partial" : ""),
                    format("[%s by (%s)]", node.getCount(), Joiner.on(", ").join(keys)));
            return processChildren(node, context);
        }

        @Override
        public Void visitSort(SortNode node, Void context)
        {
            Iterable<String> keys = Iterables.transform(node.getOrderingScheme().getOrderByVariables(), input -> input + " " + node.getOrderingScheme().getOrdering(input));

            String detail = format("[%s]", Joiner.on(", ").join(keys));
            if (!node.getPartitionBy().isEmpty()) {
                detail = format("%s[Partition by %s]", detail, Joiner.on(", ").join(node.getPartitionBy()));
            }
            addNode(node, format("%sSort", node.isPartial() ? "Partial" : ""), detail);

            return processChildren(node, context);
        }

        @Override
        public Void visitRemoteSource(RemoteSourceNode node, Void context)
        {
            addNode(node,
                    format("Remote%s", node.getOrderingScheme().isPresent() ? "Merge" : "Source"),
                    format("[%s]", Joiner.on(',').join(node.getSourceFragmentIds())),
                    ImmutableList.of(),
                    ImmutableList.of(),
                    node.getSourceFragmentIds());

            return null;
        }

        @Override
        public Void visitUnion(UnionNode node, Void context)
        {
            addNode(node, "Union");

            return processChildren(node, context);
        }

        @Override
        public Void visitIntersect(IntersectNode node, Void context)
        {
            addNode(node, "Intersect");

            return processChildren(node, context);
        }

        @Override
        public Void visitExcept(ExceptNode node, Void context)
        {
            addNode(node, "Except");

            return processChildren(node, context);
        }

        @Override
        public Void visitTableWriter(TableWriterNode node, Void context)
        {
            NodeRepresentation nodeOutput = addNode(node, "TableWriter");
            for (int i = 0; i < node.getColumnNames().size(); i++) {
                String name = node.getColumnNames().get(i);
                VariableReferenceExpression variable = node.getColumns().get(i);
                nodeOutput.appendDetailsLine("%s := %s%s", name, variable, formatSourceLocation(variable.getSourceLocation()));
            }

            int statisticsCollected = node.getStatisticsAggregation()
                    .map(StatisticAggregations::getAggregations)
                    .map(Map::size)
                    .orElse(0);
            nodeOutput.appendDetailsLine("Statistics collected: %s", statisticsCollected);

            return processChildren(node, context);
        }

        @Override
        public Void visitTableWriteMerge(TableWriterMergeNode node, Void context)
        {
            addNode(node, "TableWriterMerge");
            return processChildren(node, context);
        }

        @Override
        public Void visitStatisticsWriterNode(StatisticsWriterNode node, Void context)
        {
            addNode(node, "StatisticsWriter", format("[%s]", node.getTableHandle()));
            return processChildren(node, context);
        }

        @Override
        public Void visitTableFinish(TableFinishNode node, Void context)
        {
            addNode(node, "TableCommit", format("[%s]", node.getTarget()));
            return processChildren(node, context);
        }

        @Override
        public Void visitSample(SampleNode node, Void context)
        {
            addNode(node, "Sample", format("[%s: %s]", node.getSampleType(), node.getSampleRatio()));

            return processChildren(node, context);
        }

        @Override
        public Void visitExchange(ExchangeNode node, Void context)
        {
            if (node.getOrderingScheme().isPresent()) {
                OrderingScheme orderingScheme = node.getOrderingScheme().get();
                List<String> orderBy = orderingScheme.getOrderByVariables()
                        .stream()
                        .map(input -> input + " " + orderingScheme.getOrdering(input))
                        .collect(toImmutableList());

                addNode(node,
                        format("%sMerge", UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, node.getScope().toString())),
                        format("[%s]", Joiner.on(", ").join(orderBy)));
            }
            else if (node.getScope().isLocal()) {
                addNode(node,
                        "LocalExchange",
                        format("[%s%s]%s (%s)",
                                node.getPartitioningScheme().getPartitioning().getHandle(),
                                node.getPartitioningScheme().isReplicateNullsAndAny() ? " - REPLICATE NULLS AND ANY" : "",
                                formatHash(node.getPartitioningScheme().getHashColumn()),
                                Joiner.on(", ").join(node.getPartitioningScheme().getPartitioning().getArguments())));
            }
            else {
                addNode(node,
                        format("%sExchange", UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, node.getScope().toString())),
                        format("[%s - %s%s]%s",
                                node.getType(),
                                node.getPartitioningScheme().getEncoding(),
                                node.getPartitioningScheme().isReplicateNullsAndAny() ? " - REPLICATE NULLS AND ANY" : "",
                                formatHash(node.getPartitioningScheme().getHashColumn())));
            }
            return processChildren(node, context);
        }

        @Override
        public Void visitDelete(DeleteNode node, Void context)
        {
            addNode(node, "Delete");

            return processChildren(node, context);
        }

        @Override
        public Void visitUpdate(UpdateNode node, Void context)
        {
            addNode(node, "Update");

            return processChildren(node, context);
        }

        @Override
        public Void visitMetadataDelete(MetadataDeleteNode node, Void context)
        {
            addNode(node, "MetadataDelete", format("[%s]", node.getTableHandle()));

            return processChildren(node, context);
        }

        @Override
        public Void visitMergeWriter(MergeWriterNode node, Void context)
        {
            addNode(node, "MergeWriter", format("table: %s", node.getTarget().toString()));
            return processChildren(node, context);
        }

        @Override
        public Void visitMergeProcessor(MergeProcessorNode node, Void context)
        {
            NodeRepresentation nodeOutput = addNode(node, "MergeProcessor");
            nodeOutput.appendDetails("target: %s", node.getTarget());
            nodeOutput.appendDetails("merge row column: %s", node.getMergeRowSymbol());
            nodeOutput.appendDetails("row id column: %s", node.getRowIdSymbol());
            nodeOutput.appendDetails("redistribution columns: %s", node.getRedistributionColumnSymbols());
            nodeOutput.appendDetails("data columns: %s", node.getDataColumnSymbols());

            return processChildren(node, context);
        }

        @Override
        public Void visitEnforceSingleRow(EnforceSingleRowNode node, Void context)
        {
            addNode(node, "EnforceSingleRow");

            return processChildren(node, context);
        }

        @Override
        public Void visitAssignUniqueId(AssignUniqueId node, Void context)
        {
            addNode(node, "AssignUniqueId");

            return processChildren(node, context);
        }

        @Override
        public Void visitGroupReference(GroupReference node, Void context)
        {
            addNode(node, "GroupReference", format("[%s]", node.getGroupId()), ImmutableList.of());

            return null;
        }

        @Override
        public Void visitApply(ApplyNode node, Void context)
        {
            NodeRepresentation nodeOutput = addNode(node, "Apply", format("[%s]", node.getCorrelation()));
            printAssignments(nodeOutput, node.getSubqueryAssignments());

            return processChildren(node, context);
        }

        @Override
        public Void visitLateralJoin(LateralJoinNode node, Void context)
        {
            addNode(node, "Lateral", format("[%s]", node.getCorrelation()));

            return processChildren(node, context);
        }

        @Override
        public Void visitOffset(OffsetNode node, Void context)
        {
            addNode(node, "Offset", format("[%s]", node.getCount()));

            return processChildren(node, context);
        }

        @Override
        public Void visitPlan(PlanNode node, Void context)
        {
            throw new UnsupportedOperationException("not yet implemented: " + node.getClass().getName());
        }

        private Void processChildren(PlanNode node, Void context)
        {
            for (PlanNode child : node.getSources()) {
                child.accept(this, context);
            }

            return null;
        }

        private void printAssignments(NodeRepresentation nodeOutput, Assignments assignments)
        {
            for (Map.Entry<VariableReferenceExpression, RowExpression> entry : assignments.getMap().entrySet()) {
                if (entry.getValue() instanceof VariableReferenceExpression && ((VariableReferenceExpression) entry.getValue()).getName().equals(entry.getKey().getName())) {
                    // skip identity assignments
                    continue;
                }

                nodeOutput.appendDetailsLine("%s := %s%s", entry.getKey(), formatter.apply(entry.getValue()), formatSourceLocation(entry.getValue().getSourceLocation(), entry.getValue().getSourceLocation()));
            }
        }

        private void printConstraint(NodeRepresentation nodeOutput, ColumnHandle column, TupleDomain<ColumnHandle> constraint)
        {
            checkArgument(!constraint.isNone());
            Map<ColumnHandle, Domain> domains = constraint.getDomains().get();
            if (!constraint.isAll() && domains.containsKey(column)) {
                nodeOutput.appendDetailsLine("    :: %s", formatDomain(domains.get(column).simplify()));
            }
        }

        private String formatDomain(Domain domain)
        {
            ImmutableList.Builder<String> parts = ImmutableList.builder();

            if (domain.isNullAllowed()) {
                parts.add("NULL");
            }

            Type type = domain.getType();

            domain.getValues().getValuesProcessor().consume(
                    ranges -> {
                        for (Range range : ranges.getOrderedRanges()) {
                            parts.add(range.toString(session.getSqlFunctionProperties()));
                        }
                    },
                    discreteValues -> discreteValues.getValues().stream()
                            .map(value -> castToVarchar(type, value, functionAndTypeManager, session))
                            .sorted() // Sort so the values will be printed in predictable order
                            .forEach(parts::add),
                    allOrNone -> {
                        if (allOrNone.isAll()) {
                            parts.add("ALL VALUES");
                        }
                    });

            return "[" + Joiner.on(", ").join(parts.build()) + "]";
        }

        public NodeRepresentation addNode(PlanNode node, String name)
        {
            return addNode(node, name, "");
        }

        public NodeRepresentation addNode(PlanNode node, String name, String identifier)
        {
            return addNode(node, name, identifier, node.getSources());
        }

        public NodeRepresentation addNode(PlanNode node, String name, String identifier, List<PlanNode> children)
        {
            return addNode(node, name, identifier, ImmutableList.of(node.getId()), children, ImmutableList.of());
        }

        public NodeRepresentation addNode(PlanNode node, String name, List<PlanNode> children)
        {
            return addNode(node, name, "", ImmutableList.of(node.getId()), children, ImmutableList.of());
        }

        public NodeRepresentation addNode(PlanNode rootNode, String name, String identifier, List<PlanNodeId> allNodes, List<PlanNode> children, List<PlanFragmentId> remoteSources)
        {
            List<PlanNodeId> childrenIds = children.stream().map(PlanNode::getId).collect(toImmutableList());
            List<PlanNodeStatsEstimate> estimatedStats = allNodes.stream()
                    .map(nodeId -> estimatedStatsAndCosts.getStats().getOrDefault(nodeId, PlanNodeStatsEstimate.unknown()))
                    .collect(toList());
            List<PlanCostEstimate> estimatedCosts = allNodes.stream()
                    .map(nodeId -> estimatedStatsAndCosts.getCosts().getOrDefault(nodeId, PlanCostEstimate.unknown()))
                    .collect(toList());

            NodeRepresentation nodeOutput = new NodeRepresentation(
                    Optional.empty(),
                    rootNode.getId(),
                    name,
                    rootNode.getClass().getSimpleName(),
                    identifier,
                    rootNode.getOutputVariables(),
                    stats.map(s -> s.get(rootNode.getId())),
                    estimatedStats,
                    estimatedCosts,
                    childrenIds,
                    remoteSources,
                    allNodes);

            representation.addNode(nodeOutput);
            return nodeOutput;
        }
    }

    public static String getCteExecutionOrder(SequenceNode node)
    {
        List<CteProducerNode> cteProducers = node.getCteProducers().stream()
                .filter(c -> (c instanceof CteProducerNode))
                .map(CteProducerNode.class::cast)
                .collect(Collectors.toList());
        if (cteProducers.isEmpty()) {
            return "";
        }
        Collections.reverse(cteProducers);
        return format("executionOrder = %s",
                cteProducers.stream()
                        .map(CteProducerNode::getCteId)
                        .collect(Collectors.joining(" -> ", "{", "}")));
    }

    public static String getDynamicFilterAssignments(AbstractJoinNode node)
    {
        if (node.getDynamicFilters().isEmpty()) {
            return "";
        }
        return format("dynamicFilterAssignments = %s",
                node.getDynamicFilters().entrySet().stream()
                        .map(filter -> filter.getValue() + " -> " + filter.getKey())
                        .collect(Collectors.joining(", ", "{", "}")));
    }

    private static String castToVarchar(Type type, Object value, FunctionAndTypeManager functionAndTypeManager, Session session)
    {
        if (value == null) {
            return "NULL";
        }

        try {
            FunctionHandle cast = functionAndTypeManager.lookupCast(CAST, type, VARCHAR);
            Slice coerced = (Slice) new InterpretedFunctionInvoker(functionAndTypeManager).invoke(cast, session.getSqlFunctionProperties(), value);
            return "\"" + coerced.toStringUtf8().replace("\"", "\\\"") + "\"";
        }
        catch (OperatorNotFoundException e) {
            return "<UNREPRESENTABLE VALUE>";
        }
    }

    private static String formatFrame(WindowNode.Frame frame)
    {
        StringBuilder builder = new StringBuilder(frame.getType().toString());

        frame.getOriginalStartValue().ifPresent(value -> builder.append(" ").append(value));
        builder.append(" ").append(frame.getStartType());

        frame.getOriginalEndValue().ifPresent(value -> builder.append(" ").append(value));
        builder.append(" ").append(frame.getEndType());

        return builder.toString();
    }

    private static String formatHash(Optional<VariableReferenceExpression>... hashes)
    {
        List<VariableReferenceExpression> variables = stream(hashes)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toList());

        if (variables.isEmpty()) {
            return "";
        }

        return "[" + Joiner.on(", ").join(variables) + "]";
    }

    private static String formatOutputs(Iterable<VariableReferenceExpression> outputs)
    {
        return Streams.stream(outputs)
                .map(input -> input + ":" + input.getType().getDisplayName())
                .collect(Collectors.joining(", "));
    }
}
