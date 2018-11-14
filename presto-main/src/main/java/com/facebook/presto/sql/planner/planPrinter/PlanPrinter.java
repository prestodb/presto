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
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.airlift.slice.Slice;

import java.util.ArrayList;
import java.util.HashMap;
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
    private final PrintOutput output;
    private final FunctionRegistry functionRegistry;
    private final Optional<Map<PlanNodeId, PlanNodeStats>> stats;
    private final boolean verbose;

    private PlanPrinter(
            PlanNode plan,
            TypeProvider types,
            Optional<StageExecutionStrategy> stageExecutionStrategy,
            FunctionRegistry functionRegistry,
            StatsAndCosts estimatedStatsAndCosts,
            Session session,
            Optional<Map<PlanNodeId, PlanNodeStats>> stats,
            int indent,
            boolean verbose)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(types, "types is null");
        requireNonNull(functionRegistry, "functionRegistry is null");
        requireNonNull(estimatedStatsAndCosts, "estimatedStatsAndCosts is null");

        this.functionRegistry = functionRegistry;
        this.stats = stats;
        this.verbose = verbose;
        this.output = new PrintOutput(types);

        Visitor visitor = new Visitor(stageExecutionStrategy, types, estimatedStatsAndCosts, session);
        plan.accept(visitor, indent);
    }

    @Override
    public String toString()
    {
        return output.getTextOutput();
    }

    public static String textLogicalPlan(PlanNode plan, TypeProvider types, FunctionRegistry functionRegistry, StatsAndCosts estimatedStatsAndCosts, Session session, int indent)
    {
        return new PlanPrinter(plan, types, Optional.empty(), functionRegistry, estimatedStatsAndCosts, session, Optional.empty(), indent, false).toString();
    }

    public static String textLogicalPlan(PlanNode plan, TypeProvider types, FunctionRegistry functionRegistry, StatsAndCosts estimatedStatsAndCosts, Session session, int indent, boolean verbose)
    {
        return textLogicalPlan(plan, types, Optional.empty(), functionRegistry, estimatedStatsAndCosts, session, Optional.empty(), indent, verbose);
    }

    public static String textLogicalPlan(PlanNode plan, TypeProvider types, Optional<StageExecutionStrategy> stageExecutionStrategy, FunctionRegistry functionRegistry, StatsAndCosts estimatedStatsAndCosts, Session session, Optional<Map<PlanNodeId, PlanNodeStats>> stats, int indent, boolean verbose)
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
            builder.append(formatFragment(functionRegistry, session, stageInfo.getPlan(), Optional.of(stageInfo), Optional.of(aggregatedStats), verbose, allFragments));
        }

        return builder.toString();
    }

    public static String textDistributedPlan(SubPlan plan, FunctionRegistry functionRegistry, Session session, boolean verbose)
    {
        StringBuilder builder = new StringBuilder();
        for (PlanFragment fragment : plan.getAllFragments()) {
            builder.append(formatFragment(functionRegistry, session, fragment, Optional.empty(), Optional.empty(), verbose, plan.getAllFragments()));
        }

        return builder.toString();
    }

    private static String formatFragment(FunctionRegistry functionRegistry, Session session, PlanFragment fragment, Optional<StageInfo> stageInfo, Optional<Map<PlanNodeId, PlanNodeStats>> planNodeStats, boolean verbose, List<PlanFragment> allFragments)
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

            builder.append(indentString(1))
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
        builder.append(indentString(1))
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
        builder.append(indentString(1));
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
        builder.append(indentString(1)).append(format("Grouped Execution: %s\n", fragment.getStageExecutionStrategy().isAnyScanGroupedExecution()));

        TypeProvider typeProvider = TypeProvider.copyOf(allFragments.stream()
                .flatMap(f -> f.getSymbols().entrySet().stream())
                .distinct()
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue)));
        builder.append(textLogicalPlan(fragment.getRoot(), typeProvider, Optional.of(fragment.getStageExecutionStrategy()), functionRegistry, fragment.getStatsAndCosts(), session, planNodeStats, 1, verbose))
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

    private static String formatOutputs(TypeProvider types, Iterable<Symbol> outputs)
    {
        return Joiner.on(", ").join(Iterables.transform(outputs, input -> input + ":" + types.get(input).getDisplayName()));
    }

    private static class PrintOutput
    {
        private final Map<PlanNodeId, PlanNodeInfo> nodeInfo = new HashMap<>();
        private final StringBuilder output = new StringBuilder();
        private final TypeProvider types;

        private PlanNodeId currentNode;

        public PrintOutput(TypeProvider types)
        {
            this.types = requireNonNull(types, "types is null");
        }

        private String format(String format, Object... args)
        {
            String value;

            if (args.length == 0) {
                value = format;
            }
            else {
                value = String.format(format, args);
            }

            return value;
        }

        public void addNode(PlanNode node, int level, String name)
        {
            addNode(node, level, name, "");
        }

        public void addNode(PlanNode node, int level, String name, String identifier, Object... args)
        {
            addNode(node, level, name, node.getOutputSymbols(), identifier, args);
        }

        public void addNode(PlanNode node, int level, String name, List<Symbol> outputs, String identifier, Object... args)
        {
            writeCurrentNode();
            String formattedIdentifier = format(identifier, args);
            String formattedOutputs = formatOutputs(types, outputs);

            output.append(indentString(level))
                    .append("- ")
                    .append(name)
                    .append(formattedIdentifier)
                    .append(" => [")
                    .append(formattedOutputs)
                    .append("]\n");

            nodeInfo.put(node.getId(), new PlanNodeInfo(level));
            currentNode = node.getId();
        }

        public void appendDetails(PlanNode node, String string, Object... args)
        {
            String formatted = format(string, args);
            nodeInfo.get(node.getId()).details.append(formatted);
        }

        public void appendDetailsLine(PlanNode node, String string, Object... args)
        {
            String formatted = format(string, args);
            nodeInfo.get(node.getId()).details.append(formatted).append('\n');
        }

        private void writeCurrentNode()
        {
            if (currentNode == null) {
                return;
            }
            PlanNodeInfo info = nodeInfo.get(currentNode);
            String details = info.details.toString();
            String indentedDetails = details.replaceAll("(?m)^", indentString(info.level + 2));

            if (indentedDetails.length() > 0) {
                output.append(indentedDetails);

                if (!indentedDetails.endsWith("\n")) {
                    output.append('\n');
                }
            }
        }

        public String getTextOutput()
        {
            writeCurrentNode();
            return output.toString();
        }

        private static class PlanNodeInfo
        {
            private final int level;
            private final StringBuilder details = new StringBuilder();

            public PlanNodeInfo(int level)
            {
                this.level = level;
            }
        }
    }

    private void printStats(PlanNode node)
    {
        printStats(node, false, false);
    }

    private void printStats(PlanNode node, boolean printInput, boolean printFiltered)
    {
        if (!stats.isPresent()) {
            return;
        }

        long totalScheduledMillis = stats.get().values().stream()
                .mapToLong(planNode -> planNode.getPlanNodeScheduledTime().toMillis())
                .sum();

        long totalCpuMillis = stats.get().values().stream()
                .mapToLong(planNode -> planNode.getPlanNodeCpuTime().toMillis())
                .sum();

        PlanNodeStats nodeStats = stats.get().get(node.getId());
        if (nodeStats == null) {
            output.appendDetails(node, "Cost: ?");
            if (printInput) {
                output.appendDetails(node, ", Input: ? rows (?B)");
            }
            output.appendDetails(node, ", Output: ? rows (?B)");
            if (printFiltered) {
                output.appendDetails(node, ", Filtered: ?%");
            }
            output.appendDetails(node, "\n");
            return;
        }

        double scheduledTimeFraction = 100.0d * nodeStats.getPlanNodeScheduledTime().toMillis() / totalScheduledMillis;
        double cpuTimeFraction = 100.0d * nodeStats.getPlanNodeCpuTime().toMillis() / totalCpuMillis;

        output.appendDetails(node,
                "CPU: %s (%s%%), Scheduled: %s (%s%%)",
                nodeStats.getPlanNodeCpuTime().convertToMostSuccinctTimeUnit(),
                formatDouble(cpuTimeFraction),
                nodeStats.getPlanNodeScheduledTime().convertToMostSuccinctTimeUnit(),
                formatDouble(scheduledTimeFraction));

        if (printInput) {
            output.appendDetails(node, ", Input: %s (%s)", formatPositions(nodeStats.getPlanNodeInputPositions()), nodeStats.getPlanNodeInputDataSize().toString());
        }
        output.appendDetails(node, ", Output: %s (%s)", formatPositions(nodeStats.getPlanNodeOutputPositions()), nodeStats.getPlanNodeOutputDataSize().toString());
        if (printFiltered) {
            double filtered = 100.0d * (nodeStats.getPlanNodeInputPositions() - nodeStats.getPlanNodeOutputPositions()) / nodeStats.getPlanNodeInputPositions();
            output.appendDetails(node, ", Filtered: " + formatDouble(filtered) + "%");
        }
        output.appendDetails(node, "\n");

        printDistributions(node, nodeStats);

        if (nodeStats.getWindowOperatorStats().isPresent()) {
            // TODO: Once PlanNodeStats becomes broken into smaller classes, we should rely on toString() method of WindowOperatorStats here
            printWindowOperatorStats(node, nodeStats.getWindowOperatorStats().get());
        }
    }

    private void printDistributions(PlanNode node, PlanNodeStats stats)
    {
        Map<String, Double> inputAverages = stats.getOperatorInputPositionsAverages();
        Map<String, Double> inputStdDevs = stats.getOperatorInputPositionsStdDevs();

        Map<String, Double> hashCollisionsAverages = stats.getOperatorHashCollisionsAverages();
        Map<String, Double> hashCollisionsStdDevs = stats.getOperatorHashCollisionsStdDevs();
        Map<String, Double> expectedHashCollisionsAverages = stats.getOperatorExpectedCollisionsAverages();

        Map<String, String> translatedOperatorTypes = translateOperatorTypes(stats.getOperatorTypes());

        for (String operator : translatedOperatorTypes.keySet()) {
            String translatedOperatorType = translatedOperatorTypes.get(operator);
            double inputAverage = inputAverages.get(operator);

            output.appendDetails(node, translatedOperatorType);
            output.appendDetailsLine(node, format(Locale.US, "Input avg.: %s rows, Input std.dev.: %s%%",
                    formatDouble(inputAverage), formatDouble(100.0d * inputStdDevs.get(operator) / inputAverage)));

            double hashCollisionsAverage = hashCollisionsAverages.getOrDefault(operator, 0.0d);
            double expectedHashCollisionsAverage = expectedHashCollisionsAverages.getOrDefault(operator, 0.0d);
            if (hashCollisionsAverage != 0.0d) {
                double hashCollisionsStdDevRatio = hashCollisionsStdDevs.get(operator) / hashCollisionsAverage;

                if (!translatedOperatorType.isEmpty()) {
                    output.appendDetails(node, indentString(2));
                }

                if (expectedHashCollisionsAverage != 0.0d) {
                    double hashCollisionsRatio = hashCollisionsAverage / expectedHashCollisionsAverage;
                    output.appendDetails(node, format(Locale.US, "Collisions avg.: %s (%s%% est.), Collisions std.dev.: %s%%",
                            formatDouble(hashCollisionsAverage), formatDouble(hashCollisionsRatio * 100.0d), formatDouble(hashCollisionsStdDevRatio * 100.0d)));
                }
                else {
                    output.appendDetails(node, format(Locale.US, "Collisions avg.: %s, Collisions std.dev.: %s%%",
                            formatDouble(hashCollisionsAverage), formatDouble(hashCollisionsStdDevRatio * 100.0d)));
                }

                output.appendDetails(node, "\n");
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

    private void printWindowOperatorStats(PlanNode node, WindowOperatorStats stats)
    {
        if (!verbose) {
            // these stats are too detailed for non-verbose mode
            return;
        }

        output.appendDetailsLine(node, "Active Drivers: [ %d / %d ]", stats.getActiveDrivers(), stats.getTotalDrivers());
        output.appendDetailsLine(node, "Index size: std.dev.: %s bytes , %s rows", formatDouble(stats.getIndexSizeStdDev()), formatDouble(stats.getIndexPositionsStdDev()));
        output.appendDetailsLine(node, "Index count per driver: std.dev.: %s", formatDouble(stats.getIndexCountPerDriverStdDev()));
        output.appendDetailsLine(node, "Rows per driver: std.dev.: %s", formatDouble(stats.getRowsPerDriverStdDev()));
        output.appendDetailsLine(node, "Size of partition: std.dev.: %s", formatDouble(stats.getPartitionRowsStdDev()));
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

    private static String indentString(int indent)
    {
        return Strings.repeat("    ", indent);
    }

    private class Visitor
            extends PlanVisitor<Void, Integer>
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
        public Void visitExplainAnalyze(ExplainAnalyzeNode node, Integer indent)
        {
            output.addNode(node, indent, "ExplainAnalyze");
            printPlanNodesStatsAndCost(node);
            printStats(node);
            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitJoin(JoinNode node, Integer indent)
        {
            List<Expression> joinExpressions = new ArrayList<>();
            for (JoinNode.EquiJoinClause clause : node.getCriteria()) {
                joinExpressions.add(clause.toExpression());
            }
            node.getFilter().ifPresent(joinExpressions::add);

            if (node.isCrossJoin()) {
                checkState(joinExpressions.isEmpty());
                output.addNode(node, indent, "CrossJoin");
            }
            else {
                output.addNode(node,
                        indent,
                        node.getType().getJoinLabel(),
                        "[%s]%s",
                        Joiner.on(" AND ").join(joinExpressions),
                        formatHash(node.getLeftHashSymbol(), node.getRightHashSymbol()));
            }

            node.getDistributionType().ifPresent(distributionType -> output.appendDetails(node, "Distribution: %s", distributionType));
            node.getSortExpressionContext().ifPresent(context -> output.appendDetails(node, "SortExpression[%s]", context.getSortExpression()));
            printPlanNodesStatsAndCost(node);
            printStats(node);
            node.getLeft().accept(this, indent + 1);
            node.getRight().accept(this, indent + 1);

            return null;
        }

        @Override
        public Void visitSpatialJoin(SpatialJoinNode node, Integer indent)
        {
            output.addNode(node,
                    indent,
                    node.getType().getJoinLabel(),
                    "[%s]",
                    node.getFilter());

            output.appendDetailsLine(node, "Distribution: %s", node.getDistributionType());
            printPlanNodesStatsAndCost(node);
            printStats(node);
            node.getLeft().accept(this, indent + 1);
            node.getRight().accept(this, indent + 1);

            return null;
        }

        @Override
        public Void visitSemiJoin(SemiJoinNode node, Integer indent)
        {
            output.addNode(node,
                    indent,
                    "SemiJoin",
                    "[%s = %s]%s",
                    node.getSourceJoinSymbol(),
                    node.getFilteringSourceJoinSymbol(),
                    formatHash(node.getSourceHashSymbol(), node.getFilteringSourceHashSymbol()));
            node.getDistributionType().ifPresent(distributionType -> output.appendDetailsLine(node, "Distribution: %s", distributionType));
            printPlanNodesStatsAndCost(node);
            printStats(node);
            node.getSource().accept(this, indent + 1);
            node.getFilteringSource().accept(this, indent + 1);

            return null;
        }

        @Override
        public Void visitIndexSource(IndexSourceNode node, Integer indent)
        {
            output.addNode(node,
                    indent,
                    "IndexSource",
                    "[%s, lookup = %s]",
                    node.getIndexHandle(),
                    node.getLookupSymbols());
            
            printPlanNodesStatsAndCost(node);
            printStats(node);
            for (Map.Entry<Symbol, ColumnHandle> entry : node.getAssignments().entrySet()) {
                if (node.getOutputSymbols().contains(entry.getKey())) {
                    output.appendDetailsLine(node, "%s := %s", entry.getKey(), entry.getValue());
                }
            }
            return null;
        }

        @Override
        public Void visitIndexJoin(IndexJoinNode node, Integer indent)
        {
            List<Expression> joinExpressions = new ArrayList<>();
            for (IndexJoinNode.EquiJoinClause clause : node.getCriteria()) {
                joinExpressions.add(new ComparisonExpression(ComparisonExpression.Operator.EQUAL,
                        clause.getProbe().toSymbolReference(),
                        clause.getIndex().toSymbolReference()));
            }

            output.addNode(node,
                    indent,
                    format("%sIndexJoin", node.getType().getJoinLabel()),
                    "[%s]%s => [%s]",
                    Joiner.on(" AND ").join(joinExpressions),
                    formatHash(node.getProbeHashSymbol(), node.getIndexHashSymbol()));
            printPlanNodesStatsAndCost(node);
            printStats(node);
            node.getProbeSource().accept(this, indent + 1);
            node.getIndexSource().accept(this, indent + 1);

            return null;
        }

        @Override
        public Void visitLimit(LimitNode node, Integer indent)
        {
            output.addNode(node,
                    indent,
                    format("Limit%s", node.isPartial() ? "Partial" : ""),
                    "[%s]",
                    node.getCount());
            printPlanNodesStatsAndCost(node);
            printStats(node);
            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitDistinctLimit(DistinctLimitNode node, Integer indent)
        {
            output.addNode(node,
                    indent,
                    format("DistinctLimit%s", node.isPartial() ? "Partial" : ""),
                    "[%s]%s",
                    node.getLimit(),
                    formatHash(node.getHashSymbol()));
            printPlanNodesStatsAndCost(node);
            printStats(node);
            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitAggregation(AggregationNode node, Integer indent)
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

            output.addNode(node,
                    indent,
                    format("Aggregate%s%s%s", type, key, formatHash(node.getHashSymbol())));
            printPlanNodesStatsAndCost(node);
            printStats(node);

            for (Map.Entry<Symbol, AggregationNode.Aggregation> entry : node.getAggregations().entrySet()) {
                if (entry.getValue().getMask().isPresent()) {
                    output.appendDetailsLine(node, "%s := %s (mask = %s)", entry.getKey(), entry.getValue().getCall(), entry.getValue().getMask().get());
                }
                else {
                    output.appendDetailsLine(node, "%s := %s", entry.getKey(), entry.getValue().getCall());
                }
            }

            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitGroupId(GroupIdNode node, Integer indent)
        {
            // grouping sets are easier to understand in terms of inputs
            List<List<Symbol>> inputGroupingSetSymbols = node.getGroupingSets().stream()
                    .map(set -> set.stream()
                            .map(symbol -> node.getGroupingColumns().get(symbol))
                            .collect(Collectors.toList()))
                    .collect(Collectors.toList());

            output.addNode(node, indent, "GroupId", "%s", inputGroupingSetSymbols);
            printPlanNodesStatsAndCost(node);
            printStats(node);

            for (Map.Entry<Symbol, Symbol> mapping : node.getGroupingColumns().entrySet()) {
                output.appendDetailsLine(node, "%s := %s", mapping.getKey(), mapping.getValue());
            }

            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitMarkDistinct(MarkDistinctNode node, Integer indent)
        {
            output.addNode(node,
                    indent,
                    "MarkDistinct",
                    "[distinct=%s marker=%s]%s",
                    formatOutputs(types, node.getDistinctSymbols()),
                    node.getMarkerSymbol(),
                    formatHash(node.getHashSymbol()));

            printPlanNodesStatsAndCost(node);
            printStats(node);
            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitWindow(WindowNode node, Integer indent)
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

            output.addNode(node, indent, "Window", "[%s]%s", Joiner.on(", ").join(args), formatHash(node.getHashSymbol()));
            printPlanNodesStatsAndCost(node);
            printStats(node);

            for (Map.Entry<Symbol, WindowNode.Function> entry : node.getWindowFunctions().entrySet()) {
                FunctionCall call = entry.getValue().getFunctionCall();
                String frameInfo = formatFrame(entry.getValue().getFrame());

                output.appendDetailsLine(node, "%s := %s(%s) %s", entry.getKey(), call.getName(), Joiner.on(", ").join(call.getArguments()), frameInfo);
            }
            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitTopNRowNumber(TopNRowNumberNode node, Integer indent)
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

            output.addNode(node,
                    indent,
                    "TopNRowNumber",
                    "[%s limit %s]%s",
                    Joiner.on(", ").join(args),
                    node.getMaxRowCountPerPartition(),
                    formatHash(node.getHashSymbol()));
            printPlanNodesStatsAndCost(node);
            printStats(node);

            output.appendDetailsLine(node, "%s := %s", node.getRowNumberSymbol(), "row_number()");
            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitRowNumber(RowNumberNode node, Integer indent)
        {
            List<String> partitionBy = Lists.transform(node.getPartitionBy(), Functions.toStringFunction());
            List<String> args = new ArrayList<>();
            if (!partitionBy.isEmpty()) {
                args.add(format("partition by (%s)", Joiner.on(", ").join(partitionBy)));
            }

            if (node.getMaxRowCountPerPartition().isPresent()) {
                args.add(format("limit = %s", node.getMaxRowCountPerPartition().get()));
            }

            output.addNode(node,
                    indent,
                    "RowNumber",
                    "[%s]%s",
                    Joiner.on(", ").join(args),
                    formatHash(node.getHashSymbol()));
            printPlanNodesStatsAndCost(node);
            printStats(node);

            output.appendDetailsLine(node, "%s := %s", node.getRowNumberSymbol(), "row_number()");
            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitTableScan(TableScanNode node, Integer indent)
        {
            TableHandle table = node.getTable();
            if (stageExecutionStrategy.isPresent()) {
                output.addNode(node,
                        indent,
                        "TableScan",
                        "[%s, grouped = %s]",
                        table,
                        stageExecutionStrategy.get().isGroupedExecution(node.getId()));
            }
            else {
                output.addNode(node, indent, "TableScan", "[%s]", table);
            }
            printPlanNodesStatsAndCost(node);
            printStats(node);
            printTableScanInfo(node, node);
            return null;
        }

        @Override
        public Void visitValues(ValuesNode node, Integer indent)
        {
            output.addNode(node, indent, "Values");
            printPlanNodesStatsAndCost(node);
            printStats(node);
            for (List<Expression> row : node.getRows()) {
                output.appendDetailsLine(node, "(" + Joiner.on(", ").join(row) + ")");
            }
            return null;
        }

        @Override
        public Void visitFilter(FilterNode node, Integer indent)
        {
            return visitScanFilterAndProjectInfo(node, Optional.of(node), Optional.empty(), indent);
        }

        @Override
        public Void visitProject(ProjectNode node, Integer indent)
        {
            if (node.getSource() instanceof FilterNode) {
                return visitScanFilterAndProjectInfo(node, Optional.of((FilterNode) node.getSource()), Optional.of(node), indent);
            }

            return visitScanFilterAndProjectInfo(node, Optional.empty(), Optional.of(node), indent);
        }

        private Void visitScanFilterAndProjectInfo(
                PlanNode rootNode,
                Optional<FilterNode> filterNode,
                Optional<ProjectNode> projectNode,
                int indent)
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
            List<Symbol> outputs;
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
            format += "]";

            if (projectNode.isPresent()) {
                operatorName += "Project";
                outputs = projectNode.get().getOutputSymbols();
            }
            else {
                outputs = filterNode.get().getOutputSymbols();
            }

            output.addNode(rootNode, indent, operatorName, outputs, format, arguments.toArray(new Object[arguments.size()]));
            printPlanNodesStatsAndCost(rootNode,
                    Stream.of(scanNode, filterNode, projectNode)
                            .filter(Optional::isPresent)
                            .map(Optional::get)
                            .toArray(PlanNode[]::new));
            printStats(rootNode, true, true);

            if (projectNode.isPresent()) {
                printAssignments(rootNode, projectNode.get().getAssignments());
            }

            if (scanNode.isPresent()) {
                printTableScanInfo(rootNode, scanNode.get());
                return null;
            }

            sourceNode.accept(this, indent + 1);
            return null;
        }

        private void printTableScanInfo(PlanNode rootNode, TableScanNode node)
        {
            TableHandle table = node.getTable();

            if (node.getLayout().isPresent()) {
                // TODO: find a better way to do this
                ConnectorTableLayoutHandle layout = node.getLayout().get().getConnectorHandle();
                if (!table.getConnectorHandle().toString().equals(layout.toString())) {
                    output.appendDetailsLine(rootNode, "LAYOUT: %s", layout);
                }
            }

            TupleDomain<ColumnHandle> predicate = node.getCurrentConstraint();
            if (predicate.isNone()) {
                output.appendDetailsLine(rootNode, ":: NONE");
            }
            else {
                // first, print output columns and their constraints
                for (Map.Entry<Symbol, ColumnHandle> assignment : node.getAssignments().entrySet()) {
                    ColumnHandle column = assignment.getValue();
                    output.appendDetailsLine(rootNode, "%s := %s", assignment.getKey(), column);
                    printConstraint(rootNode, column, predicate);
                }

                // then, print constraints for columns that are not in the output
                if (!predicate.isAll()) {
                    Set<ColumnHandle> outputs = ImmutableSet.copyOf(node.getAssignments().values());

                    predicate.getDomains().get()
                            .entrySet().stream()
                            .filter(entry -> !outputs.contains(entry.getKey()))
                            .forEach(entry -> {
                                ColumnHandle column = entry.getKey();
                                output.appendDetailsLine(rootNode, "%s", column);
                                printConstraint(rootNode, column, predicate);
                            });
                }
            }
        }

        @Override
        public Void visitUnnest(UnnestNode node, Integer indent)
        {
            output.addNode(node, indent, "Unnest", "[replicate=%s, unnest=%s]", formatOutputs(types, node.getReplicateSymbols()), formatOutputs(types, node.getUnnestSymbols().keySet()));
            printPlanNodesStatsAndCost(node);
            printStats(node);

            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitOutput(OutputNode node, Integer indent)
        {
            output.addNode(node, indent, "Output", "[%s]", Joiner.on(", ").join(node.getColumnNames()));
            printPlanNodesStatsAndCost(node);
            printStats(node);
            for (int i = 0; i < node.getColumnNames().size(); i++) {
                String name = node.getColumnNames().get(i);
                Symbol symbol = node.getOutputSymbols().get(i);
                if (!name.equals(symbol.toString())) {
                    output.appendDetailsLine(node, "%s := %s", name, symbol);
                }
            }
            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitTopN(TopNNode node, Integer indent)
        {
            Iterable<String> keys = Iterables.transform(node.getOrderingScheme().getOrderBy(), input -> input + " " + node.getOrderingScheme().getOrdering(input));

            output.addNode(node, indent, "TopN", "[%s by (%s)]", node.getCount(), Joiner.on(", ").join(keys));
            printPlanNodesStatsAndCost(node);
            printStats(node);
            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitSort(SortNode node, Integer indent)
        {
            Iterable<String> keys = Iterables.transform(node.getOrderingScheme().getOrderBy(), input -> input + " " + node.getOrderingScheme().getOrdering(input));
            boolean isPartial = false;
            if (SystemSessionProperties.isDistributedSortEnabled(session)) {
                isPartial = true;
            }

            output.addNode(node,
                    indent,
                    format("%sSort", isPartial ? "Partial" : ""),
                    "[%s]",
                    Joiner.on(", ").join(keys));
            printPlanNodesStatsAndCost(node);
            printStats(node);
            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitRemoteSource(RemoteSourceNode node, Integer indent)
        {
            output.addNode(node,
                    indent,
                    format("Remote%s", node.getOrderingScheme().isPresent() ? "Merge" : "Source"),
                    "[%s]",
                    Joiner.on(',').join(node.getSourceFragmentIds()));
            printPlanNodesStatsAndCost(node);
            printStats(node);

            return null;
        }

        @Override
        public Void visitUnion(UnionNode node, Integer indent)
        {
            output.addNode(node, indent, "Union");
            printPlanNodesStatsAndCost(node);
            printStats(node);

            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitIntersect(IntersectNode node, Integer indent)
        {
            output.addNode(node, indent, "Intersect");
            printPlanNodesStatsAndCost(node);
            printStats(node);

            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitExcept(ExceptNode node, Integer indent)
        {
            output.addNode(node, indent, "Except");
            printPlanNodesStatsAndCost(node);
            printStats(node);

            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitTableWriter(TableWriterNode node, Integer indent)
        {
            output.addNode(node, indent, "TableWriter");
            printPlanNodesStatsAndCost(node);
            printStats(node);
            for (int i = 0; i < node.getColumnNames().size(); i++) {
                String name = node.getColumnNames().get(i);
                Symbol symbol = node.getColumns().get(i);
                output.appendDetailsLine(node, "%s := %s", name, symbol);
            }

            if (node.getStatisticsAggregation().isPresent()) {
                verify(node.getStatisticsAggregationDescriptor().isPresent(), "statisticsAggregationDescriptor is not present");
                printStatisticAggregations(node, node.getStatisticsAggregation().get(), node.getStatisticsAggregationDescriptor().get());
            }

            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitTableFinish(TableFinishNode node, Integer indent)
        {
            output.addNode(node, indent, "TableCommit", "[%s]", node.getTarget());
            printPlanNodesStatsAndCost(node);
            printStats(node);

            if (node.getStatisticsAggregation().isPresent()) {
                verify(node.getStatisticsAggregationDescriptor().isPresent(), "statisticsAggregationDescriptor is not present");
                printStatisticAggregations(node, node.getStatisticsAggregation().get(), node.getStatisticsAggregationDescriptor().get());
            }

            return processChildren(node, indent + 1);
        }

        private void printStatisticAggregations(PlanNode node, StatisticAggregations aggregations, StatisticAggregationsDescriptor<Symbol> descriptor)
        {
            output.appendDetailsLine(node, "Collected statistics:");
            printStatisticAggregationsInfo(node, descriptor.getTableStatistics(), descriptor.getColumnStatistics(), aggregations.getAggregations());
            output.appendDetailsLine(node, indentString(1) + "grouped by => [%s]", getStatisticGroupingSetsInfo(descriptor.getGrouping()));
        }

        private String getStatisticGroupingSetsInfo(Map<String, Symbol> columnMappings)
        {
            return columnMappings.entrySet().stream()
                    .map(entry -> format("%s := %s", entry.getValue(), entry.getKey()))
                    .collect(joining(", "));
        }

        private void printStatisticAggregationsInfo(
                PlanNode node,
                Map<TableStatisticType, Symbol> tableStatistics,
                Map<ColumnStatisticMetadata, Symbol> columnStatistics,
                Map<Symbol, AggregationNode.Aggregation> aggregations)
        {
            output.appendDetailsLine(node, "aggregations =>");
            for (Map.Entry<TableStatisticType, Symbol> tableStatistic : tableStatistics.entrySet()) {
                output.appendDetailsLine(
                        node,
                        indentString(1) + "%s => [%s := %s]",
                        tableStatistic.getValue(),
                        tableStatistic.getKey(),
                        aggregations.get(tableStatistic.getValue()).getCall());
            }

            for (Map.Entry<ColumnStatisticMetadata, Symbol> columnStatistic : columnStatistics.entrySet()) {
                output.appendDetailsLine(
                        node,
                        indentString(1) + "%s[%s] => [%s := %s]",
                        columnStatistic.getKey().getStatisticType(),
                        columnStatistic.getKey().getColumnName(),
                        columnStatistic.getValue(),
                        aggregations.get(columnStatistic.getValue()).getCall());
            }
        }

        @Override
        public Void visitSample(SampleNode node, Integer indent)
        {
            output.addNode(node, indent, "Sample", "[%s: %s]", node.getSampleType(), node.getSampleRatio());
            printPlanNodesStatsAndCost(node);
            printStats(node);

            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitExchange(ExchangeNode node, Integer indent)
        {
            if (node.getOrderingScheme().isPresent()) {
                OrderingScheme orderingScheme = node.getOrderingScheme().get();
                List<String> orderBy = orderingScheme.getOrderBy()
                        .stream()
                        .map(input -> input + " " + orderingScheme.getOrdering(input))
                        .collect(toImmutableList());

                output.addNode(node,
                        indent,
                        format("%sMerge", UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, node.getScope().toString())),
                        "[%s]",
                        Joiner.on(", ").join(orderBy));
            }
            else if (node.getScope() == Scope.LOCAL) {
                output.addNode(node,
                        indent,
                        "LocalExchange",
                        "[%s%s]%s (%s)",
                        node.getPartitioningScheme().getPartitioning().getHandle(),
                        node.getPartitioningScheme().isReplicateNullsAndAny() ? " - REPLICATE NULLS AND ANY" : "",
                        formatHash(node.getPartitioningScheme().getHashColumn()),
                        Joiner.on(", ").join(node.getPartitioningScheme().getPartitioning().getArguments()));
            }
            else {
                output.addNode(node,
                        indent,
                        format("%sExchange", UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, node.getScope().toString())),
                        "[%s%s]%s",
                        node.getType(),
                        node.getPartitioningScheme().isReplicateNullsAndAny() ? " - REPLICATE NULLS AND ANY" : "",
                        formatHash(node.getPartitioningScheme().getHashColumn()));
            }
            printPlanNodesStatsAndCost(node);
            printStats(node);
            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitDelete(DeleteNode node, Integer indent)
        {
            output.addNode(node, indent, "Delete", "[%s]", node.getTarget());
            printPlanNodesStatsAndCost(node);
            printStats(node);

            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitMetadataDelete(MetadataDeleteNode node, Integer indent)
        {
            output.addNode(node, indent, "MetadataDelete", "[%s]", node.getTarget());
            printPlanNodesStatsAndCost(node);
            printStats(node);

            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitEnforceSingleRow(EnforceSingleRowNode node, Integer indent)
        {
            output.addNode(node, indent, "Scalar");
            printPlanNodesStatsAndCost(node);
            printStats(node);

            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitAssignUniqueId(AssignUniqueId node, Integer indent)
        {
            output.addNode(node, indent, "AssignUniqueId");
            printPlanNodesStatsAndCost(node);
            printStats(node);

            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitGroupReference(GroupReference node, Integer indent)
        {
            output.addNode(node, indent, "GroupReference", "[%s]", node.getGroupId());

            return null;
        }

        @Override
        public Void visitApply(ApplyNode node, Integer indent)
        {
            output.addNode(node, indent, "Apply", "[%s]", node.getCorrelation());
            printPlanNodesStatsAndCost(node);
            printStats(node);
            printAssignments(node, node.getSubqueryAssignments());

            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitLateralJoin(LateralJoinNode node, Integer indent)
        {
            output.addNode(node, indent, "Lateral", "[%s]", node.getCorrelation());
            printStats(node);

            return processChildren(node, indent + 1);
        }

        @Override
        protected Void visitPlan(PlanNode node, Integer indent)
        {
            throw new UnsupportedOperationException("not yet implemented: " + node.getClass().getName());
        }

        private Void processChildren(PlanNode node, Integer indent)
        {
            for (PlanNode child : node.getSources()) {
                child.accept(this, indent);
            }

            return null;
        }

        private void printAssignments(PlanNode node, Assignments assignments)
        {
            for (Map.Entry<Symbol, Expression> entry : assignments.getMap().entrySet()) {
                if (entry.getValue() instanceof SymbolReference && ((SymbolReference) entry.getValue()).getName().equals(entry.getKey().getName())) {
                    // skip identity assignments
                    continue;
                }
                output.appendDetailsLine(node, "%s := %s", entry.getKey(), entry.getValue());
            }
        }

        private void printConstraint(PlanNode node, ColumnHandle column, TupleDomain<ColumnHandle> constraint)
        {
            checkArgument(!constraint.isNone());
            Map<ColumnHandle, Domain> domains = constraint.getDomains().get();
            if (!constraint.isAll() && domains.containsKey(column)) {
                output.appendDetailsLine(node, "    :: %s", formatDomain(domains.get(column).simplify()));
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

        private void printPlanNodesStatsAndCost(PlanNode rootNode, PlanNode... otherNodes)
        {
            Stream<PlanNode> allNodes = Stream.concat(Stream.of(rootNode), Stream.of(otherNodes));
            if (allNodes.allMatch(this::isPlanNodeStatsAndCostsUnknown)) {
                return;
            }
            output.appendDetails(rootNode, "Cost: %s", allNodes.map(this::formatPlanNodeStatsAndCost).collect(joining("/")));
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
