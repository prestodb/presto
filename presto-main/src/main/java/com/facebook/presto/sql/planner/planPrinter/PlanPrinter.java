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
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.cost.PlanNodeCostEstimate;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.cost.StatsAndCosts;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.execution.StageStats;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.OperatorNotFoundException;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.operator.StageExecutionStrategy;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Marker;
import com.facebook.presto.spi.predicate.NullableValue;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.statistics.ColumnStatisticMetadata;
import com.facebook.presto.spi.statistics.TableStatisticType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.InterpretedFunctionInvoker;
import com.facebook.presto.sql.planner.OrderingScheme;
import com.facebook.presto.sql.planner.Partitioning;
import com.facebook.presto.sql.planner.PartitioningScheme;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.SubPlan;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.iterative.GroupReference;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.AggregationNode.Aggregation;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.AssignUniqueId;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.DeleteNode;
import com.facebook.presto.sql.planner.plan.DistinctLimitNode;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.ExceptNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode.Scope;
import com.facebook.presto.sql.planner.plan.ExplainAnalyzeNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.GroupIdNode;
import com.facebook.presto.sql.planner.plan.IndexJoinNode;
import com.facebook.presto.sql.planner.plan.IndexSourceNode;
import com.facebook.presto.sql.planner.plan.IntersectNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LateralJoinNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.MarkDistinctNode;
import com.facebook.presto.sql.planner.plan.MetadataDeleteNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
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
import com.facebook.presto.sql.planner.plan.StatisticAggregations;
import com.facebook.presto.sql.planner.plan.StatisticAggregationsDescriptor;
import com.facebook.presto.sql.planner.plan.TableFinishNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.TableWriterNode;
import com.facebook.presto.sql.planner.plan.TopNNode;
import com.facebook.presto.sql.planner.plan.TopNRowNumberNode;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.facebook.presto.sql.planner.plan.UnnestNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.SymbolReference;
import com.facebook.presto.util.GraphvizPrinter;
import com.google.common.base.CaseFormat;
import com.google.common.base.Functions;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.airlift.slice.Slice;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.facebook.presto.execution.StageInfo.getAllStages;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.planPrinter.PlanNodeStatsSummarizer.aggregatePlanNodeStats;
import static com.google.common.base.CaseFormat.UPPER_UNDERSCORE;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.units.DataSize.succinctBytes;
import static java.lang.Double.isFinite;
import static java.lang.Double.isNaN;
import static java.lang.String.format;
import static java.util.Arrays.stream;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public class PlanPrinter
{
    private final StringBuilder output = new StringBuilder();
    private final FunctionRegistry functionRegistry;
    private final Optional<Map<PlanNodeId, PlanNodeStats>> stats;
    private final boolean verbose;

    static class Indent
    {
        private final String node;
        private final String continuation;

        private Indent(String node, String continuation)
        {
            this.node = node;
            this.continuation = continuation;
        }

        /**
         * @return a new Indent context appropriate for use with all but the terminal child
         */
        public Indent intermediateChild()
        {
            return new Indent(continuation + "\u251c\u2500\u2500 ", continuation + "\u2502   ");
        }

        /**
         * @return a new Indent context for the last child
         */
        public Indent lastChild()
        {
            return new Indent(continuation + "\u2514\u2500\u2500 ", continuation + "    ");
        }

        /**
         * @return the indentation prefix to use when printing the first line of a block, which will be the operator node.
         */
        public String getNode()
        {
            return node;
        }

        /**
         * Indentation prefixes for details of an operator will be different if the operator has a child or not.
         *
         * @return the indentation prefix to use
         */
        public String getDetailIndent(boolean hasChildren)
        {
            if (hasChildren) {
                return continuation + "\u2502   ";
            }
            else {
                return continuation + "    ";
            }
        }
    }

    private PlanPrinter(
            PlanNode plan,
            TypeProvider types,
            Optional<StageExecutionStrategy> stageExecutionStrategy,
            FunctionRegistry functionRegistry,
            StatsAndCosts estimatedStatsAndCosts,
            Session session,
            Optional<Map<PlanNodeId, PlanNodeStats>> stats,
            String indent,
            boolean verbose)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(types, "types is null");
        requireNonNull(functionRegistry, "functionRegistry is null");
        requireNonNull(estimatedStatsAndCosts, "estimatedStatsAndCosts is null");

        this.functionRegistry = functionRegistry;
        this.stats = stats;
        this.verbose = verbose;

        Visitor visitor = new Visitor(stageExecutionStrategy, types, estimatedStatsAndCosts, session);
        plan.accept(visitor, new Indent(indent, indent));
    }

    @Override
    public String toString()
    {
        return output.toString();
    }

    public static String textLogicalPlan(PlanNode plan, TypeProvider types, FunctionRegistry functionRegistry, StatsAndCosts estimatedStatsAndCosts, Session session, String indent)
    {
        return new PlanPrinter(plan, types, Optional.empty(), functionRegistry, estimatedStatsAndCosts, session, Optional.empty(), indent, false).toString();
    }

    public static String textLogicalPlan(PlanNode plan, TypeProvider types, FunctionRegistry functionRegistry, StatsAndCosts estimatedStatsAndCosts, Session session, String indent, boolean verbose)
    {
        return textLogicalPlan(plan, types, Optional.empty(), functionRegistry, estimatedStatsAndCosts, session, Optional.empty(), indent, verbose);
    }

    public static String textLogicalPlan(PlanNode plan, TypeProvider types, Optional<StageExecutionStrategy> stageExecutionStrategy, FunctionRegistry functionRegistry, StatsAndCosts estimatedStatsAndCosts, Session session, Optional<Map<PlanNodeId, PlanNodeStats>> stats, String indent, boolean verbose)
    {
        return new PlanPrinter(plan, types, stageExecutionStrategy, functionRegistry, estimatedStatsAndCosts, session, stats, indent, verbose).toString();
    }

    public static String textDistributedPlan(StageInfo outputStageInfo, FunctionRegistry functionRegistry, Session session, boolean verbose)
    {
        StringBuilder builder = new StringBuilder();
        List<StageInfo> allStages = getAllStages(Optional.of(outputStageInfo));
        List<PlanFragment> allFragments = allStages.stream()
                .map(StageInfo::getPlan)
                .collect(toImmutableList());
        Map<PlanNodeId, PlanNodeStats> aggregatedStats = aggregatePlanNodeStats(allStages);
        for (StageInfo stageInfo : allStages) {
            builder.append(formatFragment(functionRegistry, session, stageInfo.getPlan(), Optional.of(stageInfo), Optional.of(aggregatedStats), verbose, allFragments, ""));
        }

        return builder.toString();
    }

    public static String textDistributedPlan(SubPlan plan, FunctionRegistry functionRegistry, Session session, boolean verbose, String indent)
    {
        StringBuilder builder = new StringBuilder();
        for (PlanFragment fragment : plan.getAllFragments()) {
            builder.append(formatFragment(functionRegistry, session, fragment, Optional.empty(), Optional.empty(), verbose, plan.getAllFragments(), indent));
        }

        return builder.toString();
    }

    private static String formatFragment(FunctionRegistry functionRegistry, Session session, PlanFragment fragment, Optional<StageInfo> stageInfo, Optional<Map<PlanNodeId, PlanNodeStats>> planNodeStats, boolean verbose, List<PlanFragment> allFragments, String indent)
    {
        StringBuilder builder = new StringBuilder();
        builder.append(format("Fragment %s [%s]\n",
                fragment.getId(),
                fragment.getPartitioning()));

        if (stageInfo.isPresent()) {
            StageStats stageStats = stageInfo.get().getStageStats();

            double avgPositionsPerTask = stageInfo.get().getTasks().stream().mapToLong(task -> task.getStats().getProcessedInputPositions()).average().orElse(Double.NaN);
            double squaredDifferences = stageInfo.get().getTasks().stream().mapToDouble(task -> Math.pow(task.getStats().getProcessedInputPositions() - avgPositionsPerTask, 2)).sum();
            double sdAmongTasks = Math.sqrt(squaredDifferences / stageInfo.get().getTasks().size());

            builder.append(indent)
                    .append(format("CPU: %s, Scheduled: %s, Input: %s (%s); per task: avg.: %s std.dev.: %s, Output: %s (%s)\n",
                            stageStats.getTotalCpuTime().convertToMostSuccinctTimeUnit(),
                            stageStats.getTotalScheduledTime().convertToMostSuccinctTimeUnit(),
                            formatPositions(stageStats.getProcessedInputPositions()),
                            stageStats.getProcessedInputDataSize(),
                            formatDouble(avgPositionsPerTask),
                            formatDouble(sdAmongTasks),
                            formatPositions(stageStats.getOutputPositions()),
                            stageStats.getOutputDataSize()));
        }

        PartitioningScheme partitioningScheme = fragment.getPartitioningScheme();
        builder.append(indent)
                .append(format("Output layout: [%s]\n",
                        Joiner.on(", ").join(partitioningScheme.getOutputLayout())));

        boolean replicateNullsAndAny = partitioningScheme.isReplicateNullsAndAny();
        List<String> arguments = partitioningScheme.getPartitioning().getArguments().stream()
                .map(argument -> {
                    if (argument.isConstant()) {
                        NullableValue constant = argument.getConstant();
                        String printableValue = castToVarchar(constant.getType(), constant.getValue(), functionRegistry, session);
                        return constant.getType().getDisplayName() + "(" + printableValue + ")";
                    }
                    return argument.getColumn().toString();
                })
                .collect(toImmutableList());
        builder.append(indent);
        if (replicateNullsAndAny) {
            builder.append(format("Output partitioning: %s (replicate nulls and any) [%s]%s\n",
                    partitioningScheme.getPartitioning().getHandle(),
                    Joiner.on(", ").join(arguments),
                    formatHash(partitioningScheme.getHashColumn())));
        }
        else {
            builder.append(format("Output partitioning: %s [%s]%s\n",
                    partitioningScheme.getPartitioning().getHandle(),
                    Joiner.on(", ").join(arguments),
                    formatHash(partitioningScheme.getHashColumn())));
        }
        builder.append(indent).append(format("Grouped Execution: %s\n", fragment.getStageExecutionStrategy().isAnyScanGroupedExecution()));

        TypeProvider typeProvider = TypeProvider.copyOf(allFragments.stream()
                .flatMap(f -> f.getSymbols().entrySet().stream())
                .distinct()
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue)));
        builder.append(textLogicalPlan(fragment.getRoot(), typeProvider, Optional.of(fragment.getStageExecutionStrategy()), functionRegistry, fragment.getStatsAndCosts(), session, planNodeStats, indent, verbose))
                .append("\n");

        return builder.toString();
    }

    public static String graphvizLogicalPlan(PlanNode plan, TypeProvider types)
    {
        PlanFragment fragment = new PlanFragment(
                new PlanFragmentId("graphviz_plan"),
                plan,
                types.allTypes(),
                SINGLE_DISTRIBUTION,
                ImmutableList.of(plan.getId()),
                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), plan.getOutputSymbols()),
                StageExecutionStrategy.ungroupedExecution(),
                StatsAndCosts.empty());
        return GraphvizPrinter.printLogical(ImmutableList.of(fragment));
    }

    public static String graphvizDistributedPlan(SubPlan plan)
    {
        return GraphvizPrinter.printDistributed(plan);
    }

    private void print(String indent, String format, Object... args)
    {
        String value;

        if (args.length == 0) {
            value = format;
        }
        else {
            value = format(format, args);
        }
        output.append(indent).append(value).append('\n');
    }

    private void print(String indent, String format, List<Object> args)
    {
        print(indent, format, args.toArray(new Object[args.size()]));
    }

    private void printStats(String indent, PlanNodeId planNodeId)
    {
        printStats(indent, planNodeId, false, false);
    }

    private void printStats(String indent, PlanNodeId planNodeId, boolean printInput, boolean printFiltered)
    {
        if (!stats.isPresent()) {
            return;
        }

        long totalScheduledMillis = stats.get().values().stream()
                .mapToLong(node -> node.getPlanNodeScheduledTime().toMillis())
                .sum();

        long totalCpuMillis = stats.get().values().stream()
                .mapToLong(node -> node.getPlanNodeCpuTime().toMillis())
                .sum();

        PlanNodeStats nodeStats = stats.get().get(planNodeId);
        if (nodeStats == null) {
            output.append(indent);
            output.append("Cost: ?");
            if (printInput) {
                output.append(", Input: ? rows (?B)");
            }
            output.append(", Output: ? rows (?B)");
            if (printFiltered) {
                output.append(", Filtered: ?%");
            }
            output.append('\n');
            return;
        }

        double scheduledTimeFraction = 100.0d * nodeStats.getPlanNodeScheduledTime().toMillis() / totalScheduledMillis;
        double cpuTimeFraction = 100.0d * nodeStats.getPlanNodeCpuTime().toMillis() / totalCpuMillis;

        output.append(indent);
        output.append(format(
                "CPU: %s (%s%%), Scheduled: %s (%s%%)",
                nodeStats.getPlanNodeCpuTime().convertToMostSuccinctTimeUnit(),
                formatDouble(cpuTimeFraction),
                nodeStats.getPlanNodeScheduledTime().convertToMostSuccinctTimeUnit(),
                formatDouble(scheduledTimeFraction)));

        if (printInput) {
            output.append(format(", Input: %s (%s)",
                    formatPositions(nodeStats.getPlanNodeInputPositions()),
                    nodeStats.getPlanNodeInputDataSize().toString()));
        }
        output.append(format(", Output: %s (%s)",
                formatPositions(nodeStats.getPlanNodeOutputPositions()),
                nodeStats.getPlanNodeOutputDataSize().toString()));
        if (printFiltered) {
            double filtered = 100.0d * (nodeStats.getPlanNodeInputPositions() - nodeStats.getPlanNodeOutputPositions()) / nodeStats.getPlanNodeInputPositions();
            output.append(", Filtered: " + formatDouble(filtered) + "%");
        }
        output.append('\n');

        printDistributions(indent, nodeStats);

        if (nodeStats.getWindowOperatorStats().isPresent()) {
            // TODO: Once PlanNodeStats becomes broken into smaller classes, we should rely on toString() method of WindowOperatorStats here
            printWindowOperatorStats(indent, nodeStats.getWindowOperatorStats().get());
        }
    }

    private void printDistributions(String indent, PlanNodeStats nodeStats)
    {
        Map<String, Double> inputAverages = nodeStats.getOperatorInputPositionsAverages();
        Map<String, Double> inputStdDevs = nodeStats.getOperatorInputPositionsStdDevs();

        Map<String, Double> hashCollisionsAverages = nodeStats.getOperatorHashCollisionsAverages();
        Map<String, Double> hashCollisionsStdDevs = nodeStats.getOperatorHashCollisionsStdDevs();
        Map<String, Double> expectedHashCollisionsAverages = nodeStats.getOperatorExpectedCollisionsAverages();

        Map<String, String> translatedOperatorTypes = translateOperatorTypes(nodeStats.getOperatorTypes());

        for (String operator : translatedOperatorTypes.keySet()) {
            String translatedOperatorType = translatedOperatorTypes.get(operator);
            double inputAverage = inputAverages.get(operator);

            output.append(indent);
            output.append(translatedOperatorType);
            output.append(format(Locale.US, "Input avg.: %s rows, Input std.dev.: %s%%",
                    formatDouble(inputAverage), formatDouble(100.0d * inputStdDevs.get(operator) / inputAverage)));
            output.append('\n');

            double hashCollisionsAverage = hashCollisionsAverages.getOrDefault(operator, 0.0d);
            double expectedHashCollisionsAverage = expectedHashCollisionsAverages.getOrDefault(operator, 0.0d);
            if (hashCollisionsAverage != 0.0d) {
                double hashCollisionsStdDevRatio = hashCollisionsStdDevs.get(operator) / hashCollisionsAverage;

                if (translatedOperatorType.isEmpty()) {
                    output.append(indent);
                }
                else {
                    output.append(indent + "  ");
                }

                if (expectedHashCollisionsAverage != 0.0d) {
                    double hashCollisionsRatio = hashCollisionsAverage / expectedHashCollisionsAverage;
                    output.append(format(Locale.US, "Collisions avg.: %s (%s%% est.), Collisions std.dev.: %s%%",
                            formatDouble(hashCollisionsAverage), formatDouble(hashCollisionsRatio * 100.0d), formatDouble(hashCollisionsStdDevRatio * 100.0d)));
                }
                else {
                    output.append(format(Locale.US, "Collisions avg.: %s, Collisions std.dev.: %s%%",
                            formatDouble(hashCollisionsAverage), formatDouble(hashCollisionsStdDevRatio * 100.0d)));
                }

                output.append('\n');
            }
        }
    }

    private static Map<String, String> translateOperatorTypes(Set<String> operators)
    {
        if (operators.size() == 1) {
            // don't display operator (plan node) name again
            return ImmutableMap.of(getOnlyElement(operators), "");
        }

        if (operators.contains("LookupJoinOperator") && operators.contains("HashBuilderOperator")) {
            // join plan node
            return ImmutableMap.of(
                    "LookupJoinOperator", "Left (probe) ",
                    "HashBuilderOperator", "Right (build) ");
        }

        return ImmutableMap.of();
    }

    private void printWindowOperatorStats(String indent, WindowOperatorStats stats)
    {
        if (!verbose) {
            // these stats are too detailed for non-verbose mode
            return;
        }

        output.append(indent);
        output.append(format("Active Drivers: [ %d / %d ]", stats.getActiveDrivers(), stats.getTotalDrivers()));
        output.append('\n');

        output.append(indent);
        output.append(format("Index size: std.dev.: %s bytes , %s rows", formatDouble(stats.getIndexSizeStdDev()), formatDouble(stats.getIndexPositionsStdDev())));
        output.append('\n');

        output.append(indent);
        output.append(format("Index count per driver: std.dev.: %s", formatDouble(stats.getIndexCountPerDriverStdDev())));
        output.append('\n');

        output.append(indent);
        output.append(format("Rows per driver: std.dev.: %s", formatDouble(stats.getRowsPerDriverStdDev())));
        output.append('\n');

        output.append(indent);
        output.append(format("Size of partition: std.dev.: %s", formatDouble(stats.getPartitionRowsStdDev())));
        output.append('\n');
    }

    private static String formatDouble(double value)
    {
        if (isFinite(value)) {
            return format(Locale.US, "%.2f", value);
        }

        return "?";
    }

    private static String formatAsLong(double value)
    {
        if (isFinite(value)) {
            return format(Locale.US, "%d", Math.round(value));
        }

        return "?";
    }

    private static String formatPositions(long positions)
    {
        if (positions == 1) {
            return "1 row";
        }

        return positions + " rows";
    }

    private class Visitor
            extends PlanVisitor<Void, Indent>
    {
        private final Optional<StageExecutionStrategy> stageExecutionStrategy;
        private final TypeProvider types;
        private final StatsAndCosts estimatedStatsAndCosts;
        private final Session session;

        public Visitor(Optional<StageExecutionStrategy> stageExecutionStrategy, TypeProvider types, StatsAndCosts estimatedStatsAndCosts, Session session)
        {
            this.stageExecutionStrategy = stageExecutionStrategy;
            this.types = types;
            this.estimatedStatsAndCosts = estimatedStatsAndCosts;
            this.session = session;
        }

        @Override
        public Void visitExplainAnalyze(ExplainAnalyzeNode node, Indent indent)
        {
            print(indent.getNode(), "ExplainAnalyze => [%s]", formatOutputs(node.getOutputSymbols()));
            printPlanNodesStatsAndCost(indent.getDetailIndent(true), node);
            printStats(indent.getDetailIndent(true), node.getId());
            return processChildren(node, indent);
        }

        @Override
        public Void visitJoin(JoinNode node, Indent indent)
        {
            List<Expression> joinExpressions = new ArrayList<>();
            for (JoinNode.EquiJoinClause clause : node.getCriteria()) {
                joinExpressions.add(clause.toExpression());
            }
            node.getFilter().ifPresent(joinExpressions::add);

            if (node.isCrossJoin()) {
                checkState(joinExpressions.isEmpty());
                print(indent.getNode(), "CrossJoin => [%s]", formatOutputs(node.getOutputSymbols()));
            }
            else {
                print(indent.getNode(), "%s[%s]%s => [%s]",
                        node.getType().getJoinLabel(),
                        Joiner.on(" AND ").join(joinExpressions),
                        formatHash(node.getLeftHashSymbol(), node.getRightHashSymbol()),
                        formatOutputs(node.getOutputSymbols()));
            }

            node.getDistributionType().ifPresent(distributionType -> print(indent.getDetailIndent(true), "Distribution: %s", distributionType));
            node.getSortExpressionContext().ifPresent(context -> print(indent.getDetailIndent(true), "SortExpression[%s]", context.getSortExpression()));
            printPlanNodesStatsAndCost(indent.getDetailIndent(true), node);
            printStats(indent.getDetailIndent(true), node.getId());
            node.getLeft().accept(this, indent.intermediateChild());
            node.getRight().accept(this, indent.lastChild());

            return null;
        }

        @Override
        public Void visitSpatialJoin(SpatialJoinNode node, Indent indent)
        {
            print(indent.getNode(), "%s[%s] => [%s]",
                    node.getType().getJoinLabel(),
                    node.getFilter(),
                    formatOutputs(node.getOutputSymbols()));

            print(indent.getDetailIndent(true), "Distribution: %s", node.getDistributionType());
            printPlanNodesStatsAndCost(indent.getDetailIndent(true), node);
            printStats(indent.getDetailIndent(true), node.getId());
            node.getLeft().accept(this, indent.intermediateChild());
            node.getRight().accept(this, indent.lastChild());

            return null;
        }

        @Override
        public Void visitSemiJoin(SemiJoinNode node, Indent indent)
        {
            print(indent.getNode(), "SemiJoin[%s = %s]%s => [%s]",
                    node.getSourceJoinSymbol(),
                    node.getFilteringSourceJoinSymbol(),
                    formatHash(node.getSourceHashSymbol(), node.getFilteringSourceHashSymbol()),
                    formatOutputs(node.getOutputSymbols()));
            node.getDistributionType().ifPresent(distributionType -> print(indent.getDetailIndent(true), "Distribution: %s", distributionType));
            printPlanNodesStatsAndCost(indent.getDetailIndent(true), node);
            printStats(indent.getDetailIndent(true), node.getId());
            node.getSource().accept(this, indent.intermediateChild());
            node.getFilteringSource().accept(this, indent.lastChild());

            return null;
        }

        @Override
        public Void visitIndexSource(IndexSourceNode node, Indent indent)
        {
            print(indent.getNode(), "IndexSource[%s, lookup = %s] => [%s]", node.getIndexHandle(), node.getLookupSymbols(), formatOutputs(node.getOutputSymbols()));
            printPlanNodesStatsAndCost(indent.getDetailIndent(false), node);
            printStats(indent.getDetailIndent(false), node.getId());
            for (Map.Entry<Symbol, ColumnHandle> entry : node.getAssignments().entrySet()) {
                if (node.getOutputSymbols().contains(entry.getKey())) {
                    print(indent.getDetailIndent(false), "%s := %s", entry.getKey(), entry.getValue());
                }
            }
            return null;
        }

        @Override
        public Void visitIndexJoin(IndexJoinNode node, Indent indent)
        {
            List<Expression> joinExpressions = new ArrayList<>();
            for (IndexJoinNode.EquiJoinClause clause : node.getCriteria()) {
                joinExpressions.add(new ComparisonExpression(ComparisonExpression.Operator.EQUAL,
                        clause.getProbe().toSymbolReference(),
                        clause.getIndex().toSymbolReference()));
            }

            print(indent.getNode(), "%sIndexJoin[%s]%s => [%s]",
                    node.getType().getJoinLabel(),
                    Joiner.on(" AND ").join(joinExpressions),
                    formatHash(node.getProbeHashSymbol(), node.getIndexHashSymbol()),
                    formatOutputs(node.getOutputSymbols()));
            printPlanNodesStatsAndCost(indent.getDetailIndent(true), node);
            printStats(indent.getDetailIndent(true), node.getId());
            node.getProbeSource().accept(this, indent.intermediateChild());
            node.getIndexSource().accept(this, indent.lastChild());

            return null;
        }

        @Override
        public Void visitLimit(LimitNode node, Indent indent)
        {
            print(indent.getNode(), "Limit%s[%s] => [%s]", node.isPartial() ? "Partial" : "", node.getCount(), formatOutputs(node.getOutputSymbols()));
            printPlanNodesStatsAndCost(indent.getDetailIndent(true), node);
            printStats(indent.getDetailIndent(true), node.getId());
            return processChildren(node, indent);
        }

        @Override
        public Void visitDistinctLimit(DistinctLimitNode node, Indent indent)
        {
            print(indent.getNode(), "DistinctLimit%s[%s]%s => [%s]",
                    node.isPartial() ? "Partial" : "",
                    node.getLimit(),
                    formatHash(node.getHashSymbol()),
                    formatOutputs(node.getOutputSymbols()));
            printPlanNodesStatsAndCost(indent.getDetailIndent(true), node);
            printStats(indent.getDetailIndent(true), node.getId());
            return processChildren(node, indent);
        }

        @Override
        public Void visitAggregation(AggregationNode node, Indent indent)
        {
            String type = "";
            if (node.getStep() != AggregationNode.Step.SINGLE) {
                type = format("(%s)", node.getStep().toString());
            }
            if (node.isStreamable()) {
                type = format("%s(STREAMING)", type);
            }
            String key = "";
            if (!node.getGroupingKeys().isEmpty()) {
                key = node.getGroupingKeys().toString();
            }

            print(indent.getNode(), "Aggregate%s%s%s => [%s]", type, key, formatHash(node.getHashSymbol()), formatOutputs(node.getOutputSymbols()));
            printPlanNodesStatsAndCost(indent.getDetailIndent(true), node);
            printStats(indent.getDetailIndent(true), node.getId());

            for (Map.Entry<Symbol, Aggregation> entry : node.getAggregations().entrySet()) {
                if (entry.getValue().getMask().isPresent()) {
                    print(indent.getDetailIndent(true), "%s := %s (mask = %s)", entry.getKey(), entry.getValue().getCall(), entry.getValue().getMask().get());
                }
                else {
                    print(indent.getDetailIndent(true), "%s := %s", entry.getKey(), entry.getValue().getCall());
                }
            }

            return processChildren(node, indent);
        }

        @Override
        public Void visitGroupId(GroupIdNode node, Indent indent)
        {
            // grouping sets are easier to understand in terms of inputs
            List<List<Symbol>> inputGroupingSetSymbols = node.getGroupingSets().stream()
                    .map(set -> set.stream()
                            .map(symbol -> node.getGroupingColumns().get(symbol))
                            .collect(Collectors.toList()))
                    .collect(Collectors.toList());

            print(indent.getNode(), "GroupId%s => [%s]", inputGroupingSetSymbols, formatOutputs(node.getOutputSymbols()));
            printPlanNodesStatsAndCost(indent.getDetailIndent(true), node);
            printStats(indent.getDetailIndent(true), node.getId());

            for (Map.Entry<Symbol, Symbol> mapping : node.getGroupingColumns().entrySet()) {
                print(indent.getDetailIndent(true), "%s := %s", mapping.getKey(), mapping.getValue());
            }

            return processChildren(node, indent);
        }

        @Override
        public Void visitMarkDistinct(MarkDistinctNode node, Indent indent)
        {
            print(indent.getNode(), "MarkDistinct[distinct=%s marker=%s]%s => [%s]",
                    formatOutputs(node.getDistinctSymbols()),
                    node.getMarkerSymbol(),
                    formatHash(node.getHashSymbol()),
                    formatOutputs(node.getOutputSymbols()));

            printPlanNodesStatsAndCost(indent.getDetailIndent(true), node);
            printStats(indent.getDetailIndent(true), node.getId());
            return processChildren(node, indent);
        }

        @Override
        public Void visitWindow(WindowNode node, Indent indent)
        {
            List<String> partitionBy = Lists.transform(node.getPartitionBy(), Functions.toStringFunction());

            List<String> args = new ArrayList<>();
            if (!partitionBy.isEmpty()) {
                List<Symbol> prePartitioned = node.getPartitionBy().stream()
                        .filter(node.getPrePartitionedInputs()::contains)
                        .collect(toImmutableList());

                List<Symbol> notPrePartitioned = node.getPartitionBy().stream()
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
                        orderingScheme.getOrderBy().stream()
                                .limit(node.getPreSortedOrderPrefix())
                                .map(symbol -> "<" + symbol + " " + orderingScheme.getOrdering(symbol) + ">"),
                        orderingScheme.getOrderBy().stream()
                                .skip(node.getPreSortedOrderPrefix())
                                .map(symbol -> symbol + " " + orderingScheme.getOrdering(symbol)))
                        .collect(Collectors.joining(", "))));
            }

            print(indent.getNode(), "Window[%s]%s => [%s]", Joiner.on(", ").join(args), formatHash(node.getHashSymbol()), formatOutputs(node.getOutputSymbols()));
            printPlanNodesStatsAndCost(indent.getDetailIndent(true), node);
            printStats(indent.getDetailIndent(true), node.getId());

            for (Map.Entry<Symbol, WindowNode.Function> entry : node.getWindowFunctions().entrySet()) {
                FunctionCall call = entry.getValue().getFunctionCall();
                String frameInfo = formatFrame(entry.getValue().getFrame());

                print(indent.getDetailIndent(true), "%s := %s(%s) %s", entry.getKey(), call.getName(), Joiner.on(", ").join(call.getArguments()), frameInfo);
            }
            return processChildren(node, indent);
        }

        @Override
        public Void visitTopNRowNumber(TopNRowNumberNode node, Indent indent)
        {
            List<String> partitionBy = node.getPartitionBy().stream()
                    .map(Functions.toStringFunction())
                    .collect(toImmutableList());

            List<String> orderBy = node.getOrderingScheme().getOrderBy().stream()
                    .map(input -> input + " " + node.getOrderingScheme().getOrdering(input))
                    .collect(toImmutableList());

            List<String> args = new ArrayList<>();
            args.add(format("partition by (%s)", Joiner.on(", ").join(partitionBy)));
            args.add(format("order by (%s)", Joiner.on(", ").join(orderBy)));

            print(indent.getNode(), "TopNRowNumber[%s limit %s]%s => [%s]",
                    Joiner.on(", ").join(args),
                    node.getMaxRowCountPerPartition(),
                    formatHash(node.getHashSymbol()),
                    formatOutputs(node.getOutputSymbols()));
            printPlanNodesStatsAndCost(indent.getDetailIndent(true), node);
            printStats(indent.getDetailIndent(true), node.getId());

            print(indent.getDetailIndent(true), "%s := %s", node.getRowNumberSymbol(), "row_number()");
            return processChildren(node, indent);
        }

        @Override
        public Void visitRowNumber(RowNumberNode node, Indent indent)
        {
            List<String> partitionBy = Lists.transform(node.getPartitionBy(), Functions.toStringFunction());
            List<String> args = new ArrayList<>();
            if (!partitionBy.isEmpty()) {
                args.add(format("partition by (%s)", Joiner.on(", ").join(partitionBy)));
            }

            if (node.getMaxRowCountPerPartition().isPresent()) {
                args.add(format("limit = %s", node.getMaxRowCountPerPartition().get()));
            }

            print(indent.getNode(), "RowNumber[%s]%s => [%s]",
                    Joiner.on(", ").join(args),
                    formatHash(node.getHashSymbol()),
                    formatOutputs(node.getOutputSymbols()));
            printPlanNodesStatsAndCost(indent.getDetailIndent(true), node);
            printStats(indent.getDetailIndent(true), node.getId());

            print(indent.getDetailIndent(true), "%s := %s", node.getRowNumberSymbol(), "row_number()");
            return processChildren(node, indent);
        }

        @Override
        public Void visitTableScan(TableScanNode node, Indent indent)
        {
            TableHandle table = node.getTable();
            if (stageExecutionStrategy.isPresent()) {
                print(indent.getNode(), "TableScan[%s, grouped = %s] => [%s]",
                        table,
                        stageExecutionStrategy.get().isGroupedExecution(node.getId()),
                        formatOutputs(node.getOutputSymbols()));
            }
            else {
                print(indent.getNode(), "TableScan[%s] => [%s]",
                        table,
                        formatOutputs(node.getOutputSymbols()));
            }
            printPlanNodesStatsAndCost(indent.getDetailIndent(false), node);
            printStats(indent.getDetailIndent(false), node.getId());
            printTableScanInfo(node, indent);

            return null;
        }

        @Override
        public Void visitValues(ValuesNode node, Indent indent)
        {
            print(indent.getNode(), "Values => [%s]", formatOutputs(node.getOutputSymbols()));
            printPlanNodesStatsAndCost(indent.getDetailIndent(false), node);
            printStats(indent.getDetailIndent(false), node.getId());
            for (List<Expression> row : node.getRows()) {
                print(indent.getDetailIndent(false), "(" + Joiner.on(", ").join(row) + ")");
            }
            return null;
        }

        @Override
        public Void visitFilter(FilterNode node, Indent indent)
        {
            return visitScanFilterAndProjectInfo(node.getId(), Optional.of(node), Optional.empty(), indent);
        }

        @Override
        public Void visitProject(ProjectNode node, Indent indent)
        {
            if (node.getSource() instanceof FilterNode) {
                return visitScanFilterAndProjectInfo(node.getId(), Optional.of((FilterNode) node.getSource()), Optional.of(node), indent);
            }

            return visitScanFilterAndProjectInfo(node.getId(), Optional.empty(), Optional.of(node), indent);
        }

        private Void visitScanFilterAndProjectInfo(
                PlanNodeId planNodeId,
                Optional<FilterNode> filterNode,
                Optional<ProjectNode> projectNode,
                Indent indent)
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

            String format = "[";
            String operatorName = "";
            List<Object> arguments = new LinkedList<>();

            if (scanNode.isPresent()) {
                operatorName += "Scan";
                format += "table = %s, ";
                TableHandle table = scanNode.get().getTable();
                arguments.add(table);
                if (stageExecutionStrategy.isPresent()) {
                    format += "grouped = %s, ";
                    arguments.add(stageExecutionStrategy.get().isGroupedExecution(scanNode.get().getId()));
                }
            }

            if (filterNode.isPresent()) {
                operatorName += "Filter";
                format += "filterPredicate = %s, ";
                arguments.add(filterNode.get().getPredicate());
            }

            if (format.length() > 1) {
                format = format.substring(0, format.length() - 2);
            }
            format += "] => [%s]";

            if (projectNode.isPresent()) {
                operatorName += "Project";
                arguments.add(formatOutputs(projectNode.get().getOutputSymbols()));
            }
            else {
                arguments.add(formatOutputs(filterNode.get().getOutputSymbols()));
            }

            format = operatorName + format;
            print(indent.getNode(), format, arguments);
            String detailIndent;
            if (scanNode.isPresent()) {
                detailIndent = indent.getDetailIndent(false);
            }
            else {
                detailIndent = indent.getDetailIndent(true);
            }

            printPlanNodesStatsAndCost(detailIndent,
                    Stream.of(scanNode, filterNode, projectNode)
                            .filter(Optional::isPresent)
                            .map(Optional::get)
                            .toArray(PlanNode[]::new));
            printStats(detailIndent, planNodeId, true, true);

            if (projectNode.isPresent()) {
                printAssignments(projectNode.get().getAssignments(), detailIndent);
            }

            if (scanNode.isPresent()) {
                printTableScanInfo(scanNode.get(), indent);
                return null;
            }
            sourceNode.accept(this, indent.lastChild());
            return null;
        }

        private void printTableScanInfo(TableScanNode node, Indent indent)
        {
            TableHandle table = node.getTable();

            if (node.getLayout().isPresent()) {
                // TODO: find a better way to do this
                ConnectorTableLayoutHandle layout = node.getLayout().get().getConnectorHandle();
                if (!table.getConnectorHandle().toString().equals(layout.toString())) {
                    print(indent.getDetailIndent(false), "LAYOUT: %s", layout);
                }
            }

            TupleDomain<ColumnHandle> predicate = node.getCurrentConstraint();
            if (predicate.isNone()) {
                print(indent.getDetailIndent(false), ":: NONE");
            }
            else {
                // first, print output columns and their constraints
                for (Map.Entry<Symbol, ColumnHandle> assignment : node.getAssignments().entrySet()) {
                    ColumnHandle column = assignment.getValue();
                    print(indent.getDetailIndent(false), "%s := %s", assignment.getKey(), column);
                    printConstraint(indent.getDetailIndent(false) + "  ", column, predicate);
                }

                // then, print constraints for columns that are not in the output
                if (!predicate.isAll()) {
                    Set<ColumnHandle> outputs = ImmutableSet.copyOf(node.getAssignments().values());

                    predicate.getDomains().get()
                            .entrySet().stream()
                            .filter(entry -> !outputs.contains(entry.getKey()))
                            .forEach(entry -> {
                                ColumnHandle column = entry.getKey();
                                print(indent.getDetailIndent(false), "%s", column);
                                printConstraint(indent.getDetailIndent(false) + "  " + "  ", column, predicate);
                            });
                }
            }
        }

        @Override
        public Void visitUnnest(UnnestNode node, Indent indent)
        {
            print(indent.getNode(), "Unnest [replicate=%s, unnest=%s] => [%s]", formatOutputs(node.getReplicateSymbols()), formatOutputs(node.getUnnestSymbols().keySet()), formatOutputs(node.getOutputSymbols()));
            printPlanNodesStatsAndCost(indent.getDetailIndent(true), node);
            printStats(indent.getDetailIndent(true), node.getId());

            return processChildren(node, indent);
        }

        @Override
        public Void visitOutput(OutputNode node, Indent indent)
        {
            print(indent.getNode(), "Output[%s] => [%s]", Joiner.on(", ").join(node.getColumnNames()), formatOutputs(node.getOutputSymbols()));
            printPlanNodesStatsAndCost(indent.getDetailIndent(true), node);
            printStats(indent.getDetailIndent(true), node.getId());
            for (int i = 0; i < node.getColumnNames().size(); i++) {
                String name = node.getColumnNames().get(i);
                Symbol symbol = node.getOutputSymbols().get(i);
                if (!name.equals(symbol.toString())) {
                    print(indent.getDetailIndent(true), "%s := %s", name, symbol);
                }
            }

            return processChildren(node, indent);
        }

        @Override
        public Void visitTopN(TopNNode node, Indent indent)
        {
            Iterable<String> keys = Iterables.transform(node.getOrderingScheme().getOrderBy(), input -> input + " " + node.getOrderingScheme().getOrdering(input));

            print(indent.getNode(), "TopN%s[%s by (%s)] => [%s]",
                    node.getStep() == TopNNode.Step.PARTIAL ? "Partial" : "",
                    node.getCount(),
                    Joiner.on(", ").join(keys),
                    formatOutputs(node.getOutputSymbols()));
            printPlanNodesStatsAndCost(indent.getDetailIndent(true), node);
            printStats(indent.getDetailIndent(true), node.getId());
            return processChildren(node, indent);
        }

        @Override
        public Void visitSort(SortNode node, Indent indent)
        {
            Iterable<String> keys = Iterables.transform(node.getOrderingScheme().getOrderBy(), input -> input + " " + node.getOrderingScheme().getOrdering(input));
            boolean isPartial = false;
            if (SystemSessionProperties.isDistributedSortEnabled(session)) {
                isPartial = true;
            }

            print(indent.getNode(), "%sSort[%s] => [%s]",
                    isPartial ? "Partial" : "",
                    Joiner.on(", ").join(keys),
                    formatOutputs(node.getOutputSymbols()));
            printPlanNodesStatsAndCost(indent.getDetailIndent(true), node);
            printStats(indent.getDetailIndent(true), node.getId());
            return processChildren(node, indent);
        }

        @Override
        public Void visitRemoteSource(RemoteSourceNode node, Indent indent)
        {
            print(indent.getNode(), "Remote%s[%s] => [%s]",
                    node.getOrderingScheme().isPresent() ? "Merge" : "Source",
                    Joiner.on(',').join(node.getSourceFragmentIds()),
                    formatOutputs(node.getOutputSymbols()));
            printPlanNodesStatsAndCost(indent.getDetailIndent(false), node);
            printStats(indent.getDetailIndent(false), node.getId());

            return null;
        }

        @Override
        public Void visitUnion(UnionNode node, Indent indent)
        {
            print(indent.getNode(), "Union => [%s]", formatOutputs(node.getOutputSymbols()));
            printPlanNodesStatsAndCost(indent.getDetailIndent(true), node);
            printStats(indent.getDetailIndent(true), node.getId());

            return processChildren(node, indent);
        }

        @Override
        public Void visitIntersect(IntersectNode node, Indent indent)
        {
            print(indent.getNode(), "Intersect => [%s]", formatOutputs(node.getOutputSymbols()));
            printPlanNodesStatsAndCost(indent.getDetailIndent(true), node);
            printStats(indent.getDetailIndent(true), node.getId());

            return processChildren(node, indent);
        }

        @Override
        public Void visitExcept(ExceptNode node, Indent indent)
        {
            print(indent.getNode(), "Except => [%s]", formatOutputs(node.getOutputSymbols()));
            printPlanNodesStatsAndCost(indent.getDetailIndent(true), node);
            printStats(indent.getDetailIndent(true), node.getId());

            return processChildren(node, indent);
        }

        @Override
        public Void visitTableWriter(TableWriterNode node, Indent indent)
        {
            print(indent.getNode(), "TableWriter => [%s]", formatOutputs(node.getOutputSymbols()));
            printPlanNodesStatsAndCost(indent.getDetailIndent(true), node);
            printStats(indent.getDetailIndent(true), node.getId());
            for (int i = 0; i < node.getColumnNames().size(); i++) {
                String name = node.getColumnNames().get(i);
                Symbol symbol = node.getColumns().get(i);
                print(indent.getDetailIndent(true), "%s := %s", name, symbol);
            }

            if (node.getStatisticsAggregation().isPresent()) {
                verify(node.getStatisticsAggregationDescriptor().isPresent(), "statisticsAggregationDescriptor is not present");
                printStatisticAggregations(node.getStatisticsAggregation().get(), node.getStatisticsAggregationDescriptor().get(), indent.getDetailIndent(true));
            }

            return processChildren(node, indent);
        }

        @Override
        public Void visitTableFinish(TableFinishNode node, Indent indent)
        {
            print(indent.getNode(), "TableCommit[%s] => [%s]", node.getTarget(), formatOutputs(node.getOutputSymbols()));
            printPlanNodesStatsAndCost(indent.getDetailIndent(true), node);
            printStats(indent.getDetailIndent(true), node.getId());

            if (node.getStatisticsAggregation().isPresent()) {
                verify(node.getStatisticsAggregationDescriptor().isPresent(), "statisticsAggregationDescriptor is not present");
                printStatisticAggregations(node.getStatisticsAggregation().get(), node.getStatisticsAggregationDescriptor().get(), indent.getDetailIndent(true));
            }

            return processChildren(node, indent);
        }

        private void printStatisticAggregations(StatisticAggregations aggregations, StatisticAggregationsDescriptor<Symbol> descriptor, String indent)
        {
            print(indent, "Collected statistics:");
            printStatisticAggregationsInfo(descriptor.getTableStatistics(), descriptor.getColumnStatistics(), aggregations.getAggregations(), indent + "  ");
            print(indent + "  ", "grouped by => [%s]", getStatisticGroupingSetsInfo(descriptor.getGrouping()));
        }

        private String getStatisticGroupingSetsInfo(Map<String, Symbol> columnMappings)
        {
            return columnMappings.entrySet().stream()
                    .map(entry -> format("%s := %s", entry.getValue(), entry.getKey()))
                    .collect(joining(", "));
        }

        private void printStatisticAggregationsInfo(
                Map<TableStatisticType, Symbol> tableStatistics,
                Map<ColumnStatisticMetadata, Symbol> columnStatistics,
                Map<Symbol, Aggregation> aggregations,
                String indent)
        {
            print(indent, "aggregations =>");
            for (Map.Entry<TableStatisticType, Symbol> tableStatistic : tableStatistics.entrySet()) {
                print(
                        indent + "  ",
                        "%s => [%s := %s]",
                        tableStatistic.getValue(),
                        tableStatistic.getKey(),
                        aggregations.get(tableStatistic.getValue()).getCall());
            }

            for (Map.Entry<ColumnStatisticMetadata, Symbol> columnStatistic : columnStatistics.entrySet()) {
                print(
                        indent + "  ",
                        "%s[%s] => [%s := %s]",
                        columnStatistic.getKey().getStatisticType(),
                        columnStatistic.getKey().getColumnName(),
                        columnStatistic.getValue(),
                        aggregations.get(columnStatistic.getValue()).getCall());
            }
        }

        @Override
        public Void visitSample(SampleNode node, Indent indent)
        {
            print(indent.getNode(), "Sample[%s: %s] => [%s]", node.getSampleType(), node.getSampleRatio(), formatOutputs(node.getOutputSymbols()));
            printPlanNodesStatsAndCost(indent.getDetailIndent(true), node);
            printStats(indent.getDetailIndent(true), node.getId());

            return processChildren(node, indent);
        }

        @Override
        public Void visitExchange(ExchangeNode node, Indent indent)
        {
            if (node.getOrderingScheme().isPresent()) {
                OrderingScheme orderingScheme = node.getOrderingScheme().get();
                List<String> orderBy = orderingScheme.getOrderBy()
                        .stream()
                        .map(input -> input + " " + orderingScheme.getOrdering(input))
                        .collect(toImmutableList());

                print(indent.getNode(), "%sMerge[%s] => [%s]",
                        UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, node.getScope().toString()),
                        Joiner.on(", ").join(orderBy),
                        formatOutputs(node.getOutputSymbols()));
            }
            else if (node.getScope() == Scope.LOCAL) {
                print(indent.getNode(), "LocalExchange[%s%s]%s (%s) => %s",
                        node.getPartitioningScheme().getPartitioning().getHandle(),
                        node.getPartitioningScheme().isReplicateNullsAndAny() ? " - REPLICATE NULLS AND ANY" : "",
                        formatHash(node.getPartitioningScheme().getHashColumn()),
                        Joiner.on(", ").join(node.getPartitioningScheme().getPartitioning().getArguments()),
                        formatOutputs(node.getOutputSymbols()));
            }
            else {
                print(indent.getNode(), "%sExchange[%s%s]%s => %s",
                        UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, node.getScope().toString()),
                        node.getType(),
                        node.getPartitioningScheme().isReplicateNullsAndAny() ? " - REPLICATE NULLS AND ANY" : "",
                        formatHash(node.getPartitioningScheme().getHashColumn()),
                        formatOutputs(node.getOutputSymbols()));
            }
            printPlanNodesStatsAndCost(indent.getDetailIndent(true), node);
            printStats(indent.getDetailIndent(true), node.getId());

            return processChildren(node, indent);
        }

        @Override
        public Void visitDelete(DeleteNode node, Indent indent)
        {
            print(indent.getNode(), "Delete[%s] => [%s]", node.getTarget(), formatOutputs(node.getOutputSymbols()));
            printPlanNodesStatsAndCost(indent.getDetailIndent(true), node);
            printStats(indent.getDetailIndent(true), node.getId());

            return processChildren(node, indent);
        }

        @Override
        public Void visitMetadataDelete(MetadataDeleteNode node, Indent indent)
        {
            print(indent.getNode(), "MetadataDelete[%s] => [%s]", node.getTarget(), formatOutputs(node.getOutputSymbols()));
            printPlanNodesStatsAndCost(indent.getDetailIndent(true), node);
            printStats(indent.getDetailIndent(true), node.getId());

            return processChildren(node, indent);
        }

        @Override
        public Void visitEnforceSingleRow(EnforceSingleRowNode node, Indent indent)
        {
            print(indent.getNode(), "Scalar => [%s]", formatOutputs(node.getOutputSymbols()));
            printPlanNodesStatsAndCost(indent.getDetailIndent(true), node);
            printStats(indent.getDetailIndent(true), node.getId());

            return processChildren(node, indent);
        }

        @Override
        public Void visitAssignUniqueId(AssignUniqueId node, Indent indent)
        {
            print(indent.getNode(), "AssignUniqueId => [%s]", formatOutputs(node.getOutputSymbols()));
            printPlanNodesStatsAndCost(indent.getDetailIndent(true), node);
            printStats(indent.getDetailIndent(true), node.getId());

            return processChildren(node, indent);
        }

        @Override
        public Void visitGroupReference(GroupReference node, Indent indent)
        {
            print(indent.getNode(), "GroupReference[%s] => [%s]", node.getGroupId(), formatOutputs(node.getOutputSymbols()));

            return null;
        }

        @Override
        public Void visitApply(ApplyNode node, Indent indent)
        {
            print(indent.getNode(), "Apply[%s] => [%s]", node.getCorrelation(), formatOutputs(node.getOutputSymbols()));
            printPlanNodesStatsAndCost(indent.getDetailIndent(true), node);
            printStats(indent.getDetailIndent(true), node.getId());
            printAssignments(node.getSubqueryAssignments(), indent.getDetailIndent(true) + "  " + "  ");

            return processChildren(node, indent);
        }

        @Override
        public Void visitLateralJoin(LateralJoinNode node, Indent indent)
        {
            print(indent.getNode(), "Lateral[%s] => [%s]", node.getCorrelation(), formatOutputs(node.getOutputSymbols()));
            printStats(indent.getDetailIndent(true), node.getId());

            return processChildren(node, indent);
        }

        @Override
        protected Void visitPlan(PlanNode node, Indent indent)
        {
            throw new UnsupportedOperationException("not yet implemented: " + node.getClass().getName());
        }

        private Void processChildren(PlanNode node, Indent indent)
        {
            List<PlanNode> sources = node.getSources();
            for (int i = 0; i < sources.size(); i++) {
                if (i < sources.size() - 1) {
                    sources.get(i).accept(this, indent.intermediateChild());
                }
                else {
                    sources.get(i).accept(this, indent.lastChild());
                }
            }

            return null;
        }

        private void printAssignments(Assignments assignments, String indent)
        {
            for (Map.Entry<Symbol, Expression> entry : assignments.getMap().entrySet()) {
                if (entry.getValue() instanceof SymbolReference && ((SymbolReference) entry.getValue()).getName().equals(entry.getKey().getName())) {
                    // skip identity assignments
                    continue;
                }
                print(indent, "%s := %s", entry.getKey(), entry.getValue());
            }
        }

        private String formatOutputs(Iterable<Symbol> symbols)
        {
            return Joiner.on(", ").join(Iterables.transform(symbols, input -> input + ":" + types.get(input).getDisplayName()));
        }

        private void printConstraint(String indent, ColumnHandle column, TupleDomain<ColumnHandle> constraint)
        {
            checkArgument(!constraint.isNone());
            Map<ColumnHandle, Domain> domains = constraint.getDomains().get();
            if (!constraint.isAll() && domains.containsKey(column)) {
                print(indent, ":: %s", formatDomain(domains.get(column).simplify()));
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
                            StringBuilder builder = new StringBuilder();
                            if (range.isSingleValue()) {
                                String value = castToVarchar(type, range.getSingleValue(), functionRegistry, session);
                                builder.append('[').append(value).append(']');
                            }
                            else {
                                builder.append((range.getLow().getBound() == Marker.Bound.EXACTLY) ? '[' : '(');

                                if (range.getLow().isLowerUnbounded()) {
                                    builder.append("<min>");
                                }
                                else {
                                    builder.append(castToVarchar(type, range.getLow().getValue(), functionRegistry, session));
                                }

                                builder.append(", ");

                                if (range.getHigh().isUpperUnbounded()) {
                                    builder.append("<max>");
                                }
                                else {
                                    builder.append(castToVarchar(type, range.getHigh().getValue(), functionRegistry, session));
                                }

                                builder.append((range.getHigh().getBound() == Marker.Bound.EXACTLY) ? ']' : ')');
                            }
                            parts.add(builder.toString());
                        }
                    },
                    discreteValues -> discreteValues.getValues().stream()
                            .map(value -> castToVarchar(type, value, functionRegistry, session))
                            .sorted() // Sort so the values will be printed in predictable order
                            .forEach(parts::add),
                    allOrNone -> {
                        if (allOrNone.isAll()) {
                            parts.add("ALL VALUES");
                        }
                    });

            return "[" + Joiner.on(", ").join(parts.build()) + "]";
        }

        private void printPlanNodesStatsAndCost(String indent, PlanNode... nodes)
        {
            if (stream(nodes).allMatch(this::isPlanNodeStatsAndCostsUnknown)) {
                return;
            }
            print(indent, "Cost: %s", stream(nodes).map(this::formatPlanNodeStatsAndCost).collect(joining("/")));
        }

        private boolean isPlanNodeStatsAndCostsUnknown(PlanNode node)
        {
            PlanNodeStatsEstimate stats = estimatedStatsAndCosts.getStats().getOrDefault(node.getId(), PlanNodeStatsEstimate.unknown());
            PlanNodeCostEstimate cost = estimatedStatsAndCosts.getCosts().getOrDefault(node.getId(), PlanNodeCostEstimate.unknown());
            return stats.isOutputRowCountUnknown() || cost.equals(PlanNodeCostEstimate.unknown());
        }

        private String formatPlanNodeStatsAndCost(PlanNode node)
        {
            PlanNodeStatsEstimate stats = estimatedStatsAndCosts.getStats().getOrDefault(node.getId(), PlanNodeStatsEstimate.unknown());
            PlanNodeCostEstimate cost = estimatedStatsAndCosts.getCosts().getOrDefault(node.getId(), PlanNodeCostEstimate.unknown());
            return format("{rows: %s (%s), cpu: %s, memory: %s, network: %s}",
                    formatAsLong(stats.getOutputRowCount()),
                    formatEstimateAsDataSize(stats.getOutputSizeInBytes(node.getOutputSymbols(), types)),
                    formatDouble(cost.getCpuCost()),
                    formatDouble(cost.getMemoryCost()),
                    formatDouble(cost.getNetworkCost()));
        }
    }

    private static String formatEstimateAsDataSize(double value)
    {
        return isNaN(value) ? "?" : succinctBytes((long) value).toString();
    }

    private static String formatHash(Optional<Symbol>... hashes)
    {
        List<Symbol> symbols = stream(hashes)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toList());

        if (symbols.isEmpty()) {
            return "";
        }

        return "[" + Joiner.on(", ").join(symbols) + "]";
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

    private static String castToVarchar(Type type, Object value, FunctionRegistry functionRegistry, Session session)
    {
        if (value == null) {
            return "NULL";
        }

        try {
            Signature coercion = functionRegistry.getCoercion(type, VARCHAR);
            Slice coerced = (Slice) new InterpretedFunctionInvoker(functionRegistry).invoke(coercion, session.toConnectorSession(), value);
            return coerced.toStringUtf8();
        }
        catch (OperatorNotFoundException e) {
            return "<UNREPRESENTABLE VALUE>";
        }
    }
}
