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
import com.facebook.presto.cost.CachingCostProvider;
import com.facebook.presto.cost.CachingStatsProvider;
import com.facebook.presto.cost.CostCalculator;
import com.facebook.presto.cost.CostProvider;
import com.facebook.presto.cost.PlanNodeCostEstimate;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.cost.StatsCalculator;
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.execution.StageStats;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.OperatorNotFoundException;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Marker;
import com.facebook.presto.spi.predicate.NullableValue;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.FunctionInvoker;
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
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.facebook.presto.cost.PlanNodeCostEstimate.UNKNOWN_COST;
import static com.facebook.presto.cost.PlanNodeStatsEstimate.UNKNOWN_STATS;
import static com.facebook.presto.execution.StageInfo.getAllStages;
import static com.facebook.presto.operator.PipelineExecutionStrategy.UNGROUPED_EXECUTION;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.planPrinter.PlanNodeStatsSummarizer.aggregatePlanNodeStats;
import static com.google.common.base.CaseFormat.UPPER_UNDERSCORE;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.units.DataSize.succinctBytes;
import static java.lang.Double.isFinite;
import static java.lang.Double.isNaN;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class PlanPrinter
{
    private final StringBuilder output = new StringBuilder();
    private final FunctionRegistry functionRegistry;
    private final Optional<Map<PlanNodeId, PlanNodeStats>> stats;
    private final boolean verbose;

    private PlanPrinter(PlanNode plan, TypeProvider types, FunctionRegistry functionRegistry, StatsCalculator statsCalculator, CostCalculator costCalculator, Session session)
    {
        this(plan, types, functionRegistry, statsCalculator, costCalculator, session, 0, false);
    }

    private PlanPrinter(
            PlanNode plan,
            TypeProvider types,
            FunctionRegistry functionRegistry,
            StatsCalculator statsCalculator,
            CostCalculator costCalculator,
            Session session,
            int indent,
            boolean verbose)
    {
        this(plan, types, functionRegistry, new CachingStatsProvider(statsCalculator, session, types), costCalculator, session, indent, verbose);
    }

    private PlanPrinter(
            PlanNode plan,
            TypeProvider types,
            FunctionRegistry functionRegistry,
            StatsProvider statsProvider,
            CostCalculator costCalculator,
            Session session,
            int indent,
            boolean verbose)
    {
        this(
                plan,
                types,
                functionRegistry,
                statsProvider,
                new CachingCostProvider(costCalculator, statsProvider, session, types),
                session,
                indent,
                verbose);
    }

    private PlanPrinter(
            PlanNode plan,
            TypeProvider types,
            FunctionRegistry functionRegistry,
            StatsProvider statsProvider,
            CostProvider costProvider,
            Session session,
            int indent,
            boolean verbose)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(types, "types is null");
        requireNonNull(functionRegistry, "functionRegistry is null");
        requireNonNull(statsProvider, "statsProvider is null");

        this.functionRegistry = functionRegistry;
        this.stats = Optional.empty();
        this.verbose = verbose;

        Visitor visitor = new Visitor(statsProvider, costProvider, types, session);
        plan.accept(visitor, indent);
    }

    private PlanPrinter(
            PlanNode plan,
            TypeProvider types,
            FunctionRegistry functionRegistry,
            StatsCalculator statsCalculator,
            CostCalculator costCalculator,
            Session session,
            Map<PlanNodeId, PlanNodeStats> stats,
            int indent,
            boolean verbose)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(types, "types is null");
        requireNonNull(functionRegistry, "functionRegistry is null");
        requireNonNull(statsCalculator, "statsCalculator is null");

        this.functionRegistry = functionRegistry;
        this.stats = Optional.of(stats);
        this.verbose = verbose;

        StatsProvider statsProvider = new CachingStatsProvider(statsCalculator, session, types);
        CostProvider costProvider = new CachingCostProvider(costCalculator, statsProvider, session, types);
        Visitor visitor = new Visitor(statsProvider, costProvider, types, session);
        plan.accept(visitor, indent);
    }

    @Override
    public String toString()
    {
        return output.toString();
    }

    public static String textLogicalPlan(PlanNode plan, TypeProvider types, FunctionRegistry functionRegistry, StatsCalculator statsCalculator, CostCalculator costCalculator, Session session)
    {
        return new PlanPrinter(plan, types, functionRegistry, statsCalculator, costCalculator, session).toString();
    }

    public static String textLogicalPlan(PlanNode plan, TypeProvider types, FunctionRegistry functionRegistry, StatsCalculator statsCalculator, CostCalculator costCalculator, Session session, int indent)
    {
        return textLogicalPlan(plan, types, functionRegistry, statsCalculator, costCalculator, session, indent, false);
    }

    public static String textLogicalPlan(PlanNode plan, TypeProvider types, FunctionRegistry functionRegistry, StatsProvider statsProvider, CostProvider costProvider, Session session, int indent)
    {
        return new PlanPrinter(plan, types, functionRegistry, statsProvider, costProvider, session, indent, false).toString();
    }

    public static String textLogicalPlan(PlanNode plan, TypeProvider types, FunctionRegistry functionRegistry, StatsCalculator statsCalculator, CostCalculator costCalculator, Session session, int indent, boolean verbose)
    {
        return new PlanPrinter(plan, types, functionRegistry, statsCalculator, costCalculator, session, indent, verbose).toString();
    }

    public static String textLogicalPlan(PlanNode plan, TypeProvider types, FunctionRegistry functionRegistry, StatsCalculator statsCalculator, CostCalculator costCalculator, Session session, Map<PlanNodeId, PlanNodeStats> stats, int indent, boolean verbose)
    {
        return new PlanPrinter(plan, types, functionRegistry, statsCalculator, costCalculator, session, stats, indent, verbose).toString();
    }

    public static String textDistributedPlan(StageInfo outputStageInfo, FunctionRegistry functionRegistry, StatsCalculator statsCalculator, CostCalculator costCalculator, Session session, boolean verbose)
    {
        StringBuilder builder = new StringBuilder();
        List<StageInfo> allStages = getAllStages(Optional.of(outputStageInfo));
        for (StageInfo stageInfo : allStages) {
            Map<PlanNodeId, PlanNodeStats> aggregatedStats = aggregatePlanNodeStats(stageInfo);
            builder.append(formatFragment(functionRegistry, statsCalculator, costCalculator, session, stageInfo.getPlan(), Optional.of(stageInfo), Optional.of(aggregatedStats), verbose));
        }

        return builder.toString();
    }

    public static String textDistributedPlan(SubPlan plan, FunctionRegistry functionRegistry, StatsCalculator statsCalculator, CostCalculator costCalculator, Session session)
    {
        return textDistributedPlan(plan, functionRegistry, statsCalculator, costCalculator, session, false);
    }

    public static String textDistributedPlan(SubPlan plan, FunctionRegistry functionRegistry, StatsCalculator statsCalculator, CostCalculator costCalculator, Session session, boolean verbose)
    {
        StringBuilder builder = new StringBuilder();
        for (PlanFragment fragment : plan.getAllFragments()) {
            builder.append(formatFragment(functionRegistry, statsCalculator, costCalculator, session, fragment, Optional.empty(), Optional.empty(), verbose));
        }

        return builder.toString();
    }

    public static String textPlanFragment(PlanFragment fragment, FunctionRegistry functionRegistry, StatsCalculator statsCalculator, CostCalculator costCalculator, Session session)
    {
        return formatFragment(functionRegistry, statsCalculator, costCalculator, session, fragment, Optional.empty(), Optional.empty(), false);
    }

    private static String formatFragment(FunctionRegistry functionRegistry, StatsCalculator statsCalculator, CostCalculator costCalculator, Session session, PlanFragment fragment, Optional<StageInfo> stageInfo, Optional<Map<PlanNodeId, PlanNodeStats>> planNodeStats, boolean verbose)
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
                    .append(format("CPU: %s, Input: %s (%s); per task: avg.: %s std.dev.: %s, Output: %s (%s)\n",
                            stageStats.getTotalCpuTime(),
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
        builder.append(indentString(1)).append(format("Execution Flow: %s\n", fragment.getPipelineExecutionStrategy()));

        if (stageInfo.isPresent()) {
            builder.append(textLogicalPlan(fragment.getRoot(), TypeProvider.copyOf(fragment.getSymbols()), functionRegistry, statsCalculator, costCalculator, session, planNodeStats.get(), 1, verbose))
                    .append("\n");
        }
        else {
            builder.append(textLogicalPlan(fragment.getRoot(), TypeProvider.copyOf(fragment.getSymbols()), functionRegistry, statsCalculator, costCalculator, session, 1, verbose))
                    .append("\n");
        }

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
                UNGROUPED_EXECUTION);
        return GraphvizPrinter.printLogical(ImmutableList.of(fragment));
    }

    public static String graphvizDistributedPlan(SubPlan plan)
    {
        return GraphvizPrinter.printDistributed(plan);
    }

    private void print(int indent, String format, Object... args)
    {
        String value;

        if (args.length == 0) {
            value = format;
        }
        else {
            value = format(format, args);
        }
        output.append(indentString(indent)).append(value).append('\n');
    }

    private void print(int indent, String format, List<Object> args)
    {
        print(indent, format, args.toArray(new Object[args.size()]));
    }

    private void printStats(int intent, PlanNodeId planNodeId)
    {
        printStats(intent, planNodeId, false, false);
    }

    private void printStats(int indent, PlanNodeId planNodeId, boolean printInput, boolean printFiltered)
    {
        if (!stats.isPresent()) {
            return;
        }

        long totalMillis = stats.get().values().stream()
                .mapToLong(node -> node.getPlanNodeWallTime().toMillis())
                .sum();

        PlanNodeStats nodeStats = stats.get().get(planNodeId);
        if (nodeStats == null) {
            output.append(indentString(indent));
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

        double fraction = 100.0d * nodeStats.getPlanNodeWallTime().toMillis() / totalMillis;

        output.append(indentString(indent));
        output.append("CPU fraction: " + formatDouble(fraction) + "%");
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

    private void printDistributions(int indent, PlanNodeStats nodeStats)
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

            output.append(indentString(indent));
            output.append(translatedOperatorType);
            output.append(format(Locale.US, "Input avg.: %s rows, Input std.dev.: %s%%",
                    formatDouble(inputAverage), formatDouble(100.0d * inputStdDevs.get(operator) / inputAverage)));
            output.append('\n');

            double hashCollisionsAverage = hashCollisionsAverages.getOrDefault(operator, 0.0d);
            double expectedHashCollisionsAverage = expectedHashCollisionsAverages.getOrDefault(operator, 0.0d);
            if (hashCollisionsAverage != 0.0d) {
                double hashCollisionsStdDevRatio = hashCollisionsStdDevs.get(operator) / hashCollisionsAverage;

                if (translatedOperatorType.isEmpty()) {
                    output.append(indentString(indent));
                }
                else {
                    output.append(indentString(indent + 2));
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

    private void printWindowOperatorStats(int indent, WindowOperatorStats stats)
    {
        if (!verbose) {
            // these stats are too detailed for non-verbose mode
            return;
        }

        output.append(indentString(indent));
        output.append(format("Active Drivers: [ %d / %d ]", stats.getActiveDrivers(), stats.getTotalDrivers()));
        output.append('\n');

        output.append(indentString(indent));
        output.append(format("Index size: std.dev.: %s bytes , %s rows", formatDouble(stats.getIndexSizeStdDev()), formatDouble(stats.getIndexPositionsStdDev())));
        output.append('\n');

        output.append(indentString(indent));
        output.append(format("Index count per driver: std.dev.: %s", formatDouble(stats.getIndexCountPerDriverStdDev())));
        output.append('\n');

        output.append(indentString(indent));
        output.append(format("Rows per driver: std.dev.: %s", formatDouble(stats.getRowsPerDriverStdDev())));
        output.append('\n');

        output.append(indentString(indent));
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

    private static String indentString(int indent)
    {
        return Strings.repeat("    ", indent);
    }

    private class Visitor
            extends PlanVisitor<Void, Integer>
    {
        private final TypeProvider types;
        private final StatsProvider statsProvider;
        private final CostProvider costProvider;
        private final Session session;

        public Visitor(StatsProvider statsProvider, CostProvider costProvider, TypeProvider types, Session session)
        {
            this.types = types;
            this.statsProvider = statsProvider;
            this.costProvider = costProvider;
            this.session = session;
        }

        @Override
        public Void visitExplainAnalyze(ExplainAnalyzeNode node, Integer indent)
        {
            print(indent, "- ExplainAnalyze => [%s]", formatOutputs(node.getOutputSymbols()));
            printPlanNodesStatsAndCost(indent + 2, node);
            printStats(indent + 2, node.getId());
            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitJoin(JoinNode node, Integer indent)
        {
            List<Expression> joinExpressions = new ArrayList<>();
            for (JoinNode.EquiJoinClause clause : node.getCriteria()) {
                joinExpressions.add(clause.toExpression());
            }
            node.getFilter().ifPresent(expression -> joinExpressions.add(expression));

            if (node.isSpatialJoin()) {
                print(indent, "- Spatial%s[%s] => [%s]",
                        node.getType().getJoinLabel(),
                        Joiner.on(" AND ").join(joinExpressions),
                        formatOutputs(node.getOutputSymbols()));
            }
            else if (node.isCrossJoin()) {
                checkState(joinExpressions.isEmpty());
                print(indent, "- CrossJoin => [%s]", formatOutputs(node.getOutputSymbols()));
            }
            else {
                print(indent, "- %s[%s]%s => [%s]",
                        node.getType().getJoinLabel(),
                        Joiner.on(" AND ").join(joinExpressions),
                        formatHash(node.getLeftHashSymbol(), node.getRightHashSymbol()),
                        formatOutputs(node.getOutputSymbols()));
            }

            node.getDistributionType().ifPresent(distributionType -> print(indent + 2, "Distribution: %s", distributionType));
            node.getSortExpressionContext().ifPresent(context -> print(indent + 2, "SortExpression[%s]", context.getSortExpression()));
            printPlanNodesStatsAndCost(indent + 2, node);
            printStats(indent + 2, node.getId());
            node.getLeft().accept(this, indent + 1);
            node.getRight().accept(this, indent + 1);

            return null;
        }

        @Override
        public Void visitSemiJoin(SemiJoinNode node, Integer indent)
        {
            print(indent, "- SemiJoin[%s = %s]%s => [%s]",
                    node.getSourceJoinSymbol(),
                    node.getFilteringSourceJoinSymbol(),
                    formatHash(node.getSourceHashSymbol(), node.getFilteringSourceHashSymbol()),
                    formatOutputs(node.getOutputSymbols()));
            printPlanNodesStatsAndCost(indent + 2, node);
            printStats(indent + 2, node.getId());
            node.getSource().accept(this, indent + 1);
            node.getFilteringSource().accept(this, indent + 1);

            return null;
        }

        @Override
        public Void visitIndexSource(IndexSourceNode node, Integer indent)
        {
            print(indent, "- IndexSource[%s, lookup = %s] => [%s]", node.getIndexHandle(), node.getLookupSymbols(), formatOutputs(node.getOutputSymbols()));
            printPlanNodesStatsAndCost(indent + 2, node);
            printStats(indent + 2, node.getId());
            for (Map.Entry<Symbol, ColumnHandle> entry : node.getAssignments().entrySet()) {
                if (node.getOutputSymbols().contains(entry.getKey())) {
                    print(indent + 2, "%s := %s", entry.getKey(), entry.getValue());
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

            print(indent, "- %sIndexJoin[%s]%s => [%s]",
                    node.getType().getJoinLabel(),
                    Joiner.on(" AND ").join(joinExpressions),
                    formatHash(node.getProbeHashSymbol(), node.getIndexHashSymbol()),
                    formatOutputs(node.getOutputSymbols()));
            printPlanNodesStatsAndCost(indent + 2, node);
            printStats(indent + 2, node.getId());
            node.getProbeSource().accept(this, indent + 1);
            node.getIndexSource().accept(this, indent + 1);

            return null;
        }

        @Override
        public Void visitLimit(LimitNode node, Integer indent)
        {
            print(indent, "- Limit%s[%s] => [%s]", node.isPartial() ? "Partial" : "", node.getCount(), formatOutputs(node.getOutputSymbols()));
            printPlanNodesStatsAndCost(indent + 2, node);
            printStats(indent + 2, node.getId());
            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitDistinctLimit(DistinctLimitNode node, Integer indent)
        {
            print(indent, "- DistinctLimit%s[%s]%s => [%s]",
                    node.isPartial() ? "Partial" : "",
                    node.getLimit(),
                    formatHash(node.getHashSymbol()),
                    formatOutputs(node.getOutputSymbols()));
            printPlanNodesStatsAndCost(indent + 2, node);
            printStats(indent + 2, node.getId());
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

            print(indent, "- Aggregate%s%s%s => [%s]", type, key, formatHash(node.getHashSymbol()), formatOutputs(node.getOutputSymbols()));
            printPlanNodesStatsAndCost(indent + 2, node);
            printStats(indent + 2, node.getId());

            for (Map.Entry<Symbol, Aggregation> entry : node.getAggregations().entrySet()) {
                if (entry.getValue().getMask().isPresent()) {
                    print(indent + 2, "%s := %s (mask = %s)", entry.getKey(), entry.getValue().getCall(), entry.getValue().getMask().get());
                }
                else {
                    print(indent + 2, "%s := %s", entry.getKey(), entry.getValue().getCall());
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

            print(indent, "- GroupId%s => [%s]", inputGroupingSetSymbols, formatOutputs(node.getOutputSymbols()));
            printPlanNodesStatsAndCost(indent + 2, node);
            printStats(indent + 2, node.getId());

            for (Map.Entry<Symbol, Symbol> mapping : node.getGroupingColumns().entrySet()) {
                print(indent + 2, "%s := %s", mapping.getKey(), mapping.getValue());
            }

            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitMarkDistinct(MarkDistinctNode node, Integer indent)
        {
            print(indent, "- MarkDistinct[distinct=%s marker=%s]%s => [%s]",
                    formatOutputs(node.getDistinctSymbols()),
                    node.getMarkerSymbol(),
                    formatHash(node.getHashSymbol()),
                    formatOutputs(node.getOutputSymbols()));

            printPlanNodesStatsAndCost(indent + 2, node);
            printStats(indent + 2, node.getId());
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

            print(indent, "- Window[%s]%s => [%s]", Joiner.on(", ").join(args), formatHash(node.getHashSymbol()), formatOutputs(node.getOutputSymbols()));
            printPlanNodesStatsAndCost(indent + 2, node);
            printStats(indent + 2, node.getId());

            for (Map.Entry<Symbol, WindowNode.Function> entry : node.getWindowFunctions().entrySet()) {
                FunctionCall call = entry.getValue().getFunctionCall();
                String frameInfo = formatFrame(entry.getValue().getFrame());

                print(indent + 2, "%s := %s(%s) %s", entry.getKey(), call.getName(), Joiner.on(", ").join(call.getArguments()), frameInfo);
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

            print(indent, "- TopNRowNumber[%s limit %s]%s => [%s]",
                    Joiner.on(", ").join(args),
                    node.getMaxRowCountPerPartition(),
                    formatHash(node.getHashSymbol()),
                    formatOutputs(node.getOutputSymbols()));
            printPlanNodesStatsAndCost(indent + 2, node);
            printStats(indent + 2, node.getId());

            print(indent + 2, "%s := %s", node.getRowNumberSymbol(), "row_number()");
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

            print(indent, "- RowNumber[%s]%s => [%s]",
                    Joiner.on(", ").join(args),
                    formatHash(node.getHashSymbol()),
                    formatOutputs(node.getOutputSymbols()));
            printPlanNodesStatsAndCost(indent + 2, node);
            printStats(indent + 2, node.getId());

            print(indent + 2, "%s := %s", node.getRowNumberSymbol(), "row_number()");
            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitTableScan(TableScanNode node, Integer indent)
        {
            TableHandle table = node.getTable();
            print(indent, "- TableScan[%s, originalConstraint = %s] => [%s]", table, node.getOriginalConstraint(), formatOutputs(node.getOutputSymbols()));
            printPlanNodesStatsAndCost(indent + 2, node);
            printStats(indent + 2, node.getId());
            printTableScanInfo(node, indent);

            return null;
        }

        @Override
        public Void visitValues(ValuesNode node, Integer indent)
        {
            print(indent, "- Values => [%s]", formatOutputs(node.getOutputSymbols()));
            printPlanNodesStatsAndCost(indent + 2, node);
            printStats(indent + 2, node.getId());
            for (List<Expression> row : node.getRows()) {
                print(indent + 2, "(" + Joiner.on(", ").join(row) + ")");
            }
            return null;
        }

        @Override
        public Void visitFilter(FilterNode node, Integer indent)
        {
            return visitScanFilterAndProjectInfo(node.getId(), Optional.of(node), Optional.empty(), indent);
        }

        @Override
        public Void visitProject(ProjectNode node, Integer indent)
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
            String operatorName = "- ";
            List<Object> arguments = new LinkedList<>();

            if (scanNode.isPresent()) {
                operatorName += "Scan";
                format += "table = %s, originalConstraint = %s";
                if (filterNode.isPresent()) {
                    format += ", ";
                }
                TableHandle table = scanNode.get().getTable();
                arguments.add(table);
                arguments.add(scanNode.get().getOriginalConstraint());
            }

            if (filterNode.isPresent()) {
                operatorName += "Filter";
                format += "filterPredicate = %s";
                arguments.add(filterNode.get().getPredicate());
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
            print(indent, format, arguments);
            printPlanNodesStatsAndCost(indent + 2,
                    Stream.of(scanNode, filterNode, projectNode)
                            .filter(Optional::isPresent)
                            .map(Optional::get)
                            .toArray(PlanNode[]::new));
            printStats(indent + 2, planNodeId, true, true);

            if (projectNode.isPresent()) {
                printAssignments(projectNode.get().getAssignments(), indent + 2);
            }

            if (scanNode.isPresent()) {
                printTableScanInfo(scanNode.get(), indent);
                return null;
            }

            sourceNode.accept(this, indent + 1);
            return null;
        }

        private void printTableScanInfo(TableScanNode node, int indent)
        {
            TableHandle table = node.getTable();

            if (node.getLayout().isPresent()) {
                // TODO: find a better way to do this
                ConnectorTableLayoutHandle layout = node.getLayout().get().getConnectorHandle();
                if (!table.getConnectorHandle().toString().equals(layout.toString())) {
                    print(indent + 2, "LAYOUT: %s", layout);
                }
            }

            TupleDomain<ColumnHandle> predicate = node.getCurrentConstraint();
            if (predicate.isNone()) {
                print(indent + 2, ":: NONE");
            }
            else {
                // first, print output columns and their constraints
                for (Map.Entry<Symbol, ColumnHandle> assignment : node.getAssignments().entrySet()) {
                    ColumnHandle column = assignment.getValue();
                    print(indent + 2, "%s := %s", assignment.getKey(), column);
                    printConstraint(indent + 3, column, predicate);
                }

                // then, print constraints for columns that are not in the output
                if (!predicate.isAll()) {
                    Set<ColumnHandle> outputs = ImmutableSet.copyOf(node.getAssignments().values());

                    predicate.getDomains().get()
                            .entrySet().stream()
                            .filter(entry -> !outputs.contains(entry.getKey()))
                            .forEach(entry -> {
                                ColumnHandle column = entry.getKey();
                                print(indent + 2, "%s", column);
                                printConstraint(indent + 3, column, predicate);
                            });
                }
            }
        }

        @Override
        public Void visitUnnest(UnnestNode node, Integer indent)
        {
            print(indent, "- Unnest [replicate=%s, unnest=%s] => [%s]", formatOutputs(node.getReplicateSymbols()), formatOutputs(node.getUnnestSymbols().keySet()), formatOutputs(node.getOutputSymbols()));
            printPlanNodesStatsAndCost(indent + 2, node);
            printStats(indent + 2, node.getId());

            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitOutput(OutputNode node, Integer indent)
        {
            print(indent, "- Output[%s] => [%s]", Joiner.on(", ").join(node.getColumnNames()), formatOutputs(node.getOutputSymbols()));
            printPlanNodesStatsAndCost(indent + 2, node);
            printStats(indent + 2, node.getId());
            for (int i = 0; i < node.getColumnNames().size(); i++) {
                String name = node.getColumnNames().get(i);
                Symbol symbol = node.getOutputSymbols().get(i);
                if (!name.equals(symbol.toString())) {
                    print(indent + 2, "%s := %s", name, symbol);
                }
            }

            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitTopN(TopNNode node, Integer indent)
        {
            Iterable<String> keys = Iterables.transform(node.getOrderingScheme().getOrderBy(), input -> input + " " + node.getOrderingScheme().getOrdering(input));

            print(indent, "- TopN[%s by (%s)] => [%s]", node.getCount(), Joiner.on(", ").join(keys), formatOutputs(node.getOutputSymbols()));
            printPlanNodesStatsAndCost(indent + 2, node);
            printStats(indent + 2, node.getId());
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

            print(indent, "- %sSort[%s] => [%s]",
                    isPartial ? "Partial" : "",
                    Joiner.on(", ").join(keys),
                    formatOutputs(node.getOutputSymbols()));
            printPlanNodesStatsAndCost(indent + 2, node);
            printStats(indent + 2, node.getId());
            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitRemoteSource(RemoteSourceNode node, Integer indent)
        {
            print(indent, "- Remote%s[%s] => [%s]",
                    node.getOrderingScheme().isPresent() ? "Merge" : "Source",
                    Joiner.on(',').join(node.getSourceFragmentIds()),
                    formatOutputs(node.getOutputSymbols()));
            printPlanNodesStatsAndCost(indent + 2, node);
            printStats(indent + 2, node.getId());

            return null;
        }

        @Override
        public Void visitUnion(UnionNode node, Integer indent)
        {
            print(indent, "- Union => [%s]", formatOutputs(node.getOutputSymbols()));
            printPlanNodesStatsAndCost(indent + 2, node);
            printStats(indent + 2, node.getId());

            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitIntersect(IntersectNode node, Integer indent)
        {
            print(indent, "- Intersect => [%s]", formatOutputs(node.getOutputSymbols()));
            printPlanNodesStatsAndCost(indent + 2, node);
            printStats(indent + 2, node.getId());

            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitExcept(ExceptNode node, Integer indent)
        {
            print(indent, "- Except => [%s]", formatOutputs(node.getOutputSymbols()));
            printPlanNodesStatsAndCost(indent + 2, node);
            printStats(indent + 2, node.getId());

            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitTableWriter(TableWriterNode node, Integer indent)
        {
            print(indent, "- TableWriter => [%s]", formatOutputs(node.getOutputSymbols()));
            printPlanNodesStatsAndCost(indent + 2, node);
            printStats(indent + 2, node.getId());
            for (int i = 0; i < node.getColumnNames().size(); i++) {
                String name = node.getColumnNames().get(i);
                Symbol symbol = node.getColumns().get(i);
                print(indent + 2, "%s := %s", name, symbol);
            }

            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitTableFinish(TableFinishNode node, Integer indent)
        {
            print(indent, "- TableCommit[%s] => [%s]", node.getTarget(), formatOutputs(node.getOutputSymbols()));
            printPlanNodesStatsAndCost(indent + 2, node);
            printStats(indent + 2, node.getId());

            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitSample(SampleNode node, Integer indent)
        {
            print(indent, "- Sample[%s: %s] => [%s]", node.getSampleType(), node.getSampleRatio(), formatOutputs(node.getOutputSymbols()));
            printPlanNodesStatsAndCost(indent + 2, node);
            printStats(indent + 2, node.getId());

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

                print(indent, "- %sMerge[%s] => [%s]",
                        UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, node.getScope().toString()),
                        Joiner.on(", ").join(orderBy),
                        formatOutputs(node.getOutputSymbols()));
            }
            else if (node.getScope() == Scope.LOCAL) {
                print(indent, "- LocalExchange[%s%s]%s (%s) => %s",
                        node.getPartitioningScheme().getPartitioning().getHandle(),
                        node.getPartitioningScheme().isReplicateNullsAndAny() ? " - REPLICATE NULLS AND ANY" : "",
                        formatHash(node.getPartitioningScheme().getHashColumn()),
                        Joiner.on(", ").join(node.getPartitioningScheme().getPartitioning().getArguments()),
                        formatOutputs(node.getOutputSymbols()));
            }
            else {
                print(indent, "- %sExchange[%s%s]%s => %s",
                        UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, node.getScope().toString()),
                        node.getType(),
                        node.getPartitioningScheme().isReplicateNullsAndAny() ? " - REPLICATE NULLS AND ANY" : "",
                        formatHash(node.getPartitioningScheme().getHashColumn()),
                        formatOutputs(node.getOutputSymbols()));
            }
            printPlanNodesStatsAndCost(indent + 2, node);
            printStats(indent + 2, node.getId());

            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitDelete(DeleteNode node, Integer indent)
        {
            print(indent, "- Delete[%s] => [%s]", node.getTarget(), formatOutputs(node.getOutputSymbols()));
            printPlanNodesStatsAndCost(indent + 2, node);
            printStats(indent + 2, node.getId());

            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitMetadataDelete(MetadataDeleteNode node, Integer indent)
        {
            print(indent, "- MetadataDelete[%s] => [%s]", node.getTarget(), formatOutputs(node.getOutputSymbols()));
            printPlanNodesStatsAndCost(indent + 2, node);
            printStats(indent + 2, node.getId());

            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitEnforceSingleRow(EnforceSingleRowNode node, Integer indent)
        {
            print(indent, "- Scalar => [%s]", formatOutputs(node.getOutputSymbols()));
            printPlanNodesStatsAndCost(indent + 2, node);
            printStats(indent + 2, node.getId());

            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitAssignUniqueId(AssignUniqueId node, Integer indent)
        {
            print(indent, "- AssignUniqueId => [%s]", formatOutputs(node.getOutputSymbols()));
            printPlanNodesStatsAndCost(indent + 2, node);
            printStats(indent + 2, node.getId());

            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitGroupReference(GroupReference node, Integer indent)
        {
            print(indent, "- GroupReference[%s] => [%s]", node.getGroupId(), formatOutputs(node.getOutputSymbols()));

            return null;
        }

        @Override
        public Void visitApply(ApplyNode node, Integer indent)
        {
            print(indent, "- Apply[%s] => [%s]", node.getCorrelation(), formatOutputs(node.getOutputSymbols()));
            printPlanNodesStatsAndCost(indent + 2, node);
            printStats(indent + 2, node.getId());
            printAssignments(node.getSubqueryAssignments(), indent + 4);

            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitLateralJoin(LateralJoinNode node, Integer indent)
        {
            print(indent, "- Lateral[%s] => [%s]", node.getCorrelation(), formatOutputs(node.getOutputSymbols()));
            printStats(indent + 2, node.getId());

            return processChildren(node, indent + 1);
        }

        @Override
        protected Void visitPlan(PlanNode node, Integer indent)
        {
            throw new UnsupportedOperationException("not yet implemented: " + node.getClass().getName());
        }

        private Void processChildren(PlanNode node, int indent)
        {
            for (PlanNode child : node.getSources()) {
                child.accept(this, indent);
            }

            return null;
        }

        private void printAssignments(Assignments assignments, int indent)
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

        private void printConstraint(int indent, ColumnHandle column, TupleDomain<ColumnHandle> constraint)
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

        private void printPlanNodesStatsAndCost(int indent, PlanNode... nodes)
        {
            if (Arrays.stream(nodes).anyMatch(this::isKnownPlanNodeStatsOrCost)) {
                String formattedStatsAndCost = Joiner.on("/").join(Arrays.stream(nodes)
                        .map(this::formatPlanNodeStatsAndCost)
                        .collect(toImmutableList()));
                print(indent, "Cost: %s", formattedStatsAndCost);
            }
        }

        private boolean isKnownPlanNodeStatsOrCost(PlanNode node)
        {
            return !UNKNOWN_STATS.equals(statsProvider.getStats(node))
                    || !UNKNOWN_COST.equals(costProvider.getCumulativeCost(node));
        }

        private String formatPlanNodeStatsAndCost(PlanNode node)
        {
            PlanNodeStatsEstimate stats = statsProvider.getStats(node);
            PlanNodeCostEstimate cost = costProvider.getCumulativeCost(node);
            return String.format("{rows: %s (%s), cpu: %s, memory: %s, network: %s}",
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
        List<Symbol> symbols = Arrays.stream(hashes)
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
            Slice coerced = (Slice) new FunctionInvoker(functionRegistry).invoke(coercion, session.toConnectorSession(), value);
            return coerced.toStringUtf8();
        }
        catch (OperatorNotFoundException e) {
            return "<UNREPRESENTABLE VALUE>";
        }
    }
}
