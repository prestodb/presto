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
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.execution.StageStats;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.OperatorNotFoundException;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.metadata.TableLayout;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.PipelineStats;
import com.facebook.presto.operator.TaskStats;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Marker;
import com.facebook.presto.spi.predicate.NullableValue;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.FunctionInvoker;
import com.facebook.presto.sql.planner.plan.AggregationNode;
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
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.util.GraphvizPrinter;
import com.google.common.base.Functions;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.facebook.presto.execution.StageInfo.getAllStages;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.planner.DomainUtils.simplifyDomain;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Lists.reverse;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.succinctBytes;
import static io.airlift.units.DataSize.succinctDataSize;
import static java.lang.Double.isNaN;
import static java.lang.Double.max;
import static java.lang.Math.sqrt;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class PlanPrinter
{
    private final StringBuilder output = new StringBuilder();
    private final Metadata metadata;
    private final Optional<Map<PlanNodeId, PlanNodeStats>> stats;

    private PlanPrinter(PlanNode plan, Map<Symbol, Type> types, Metadata metadata, Session sesion)
    {
        this(plan, types, metadata, sesion, 0);
    }

    private PlanPrinter(PlanNode plan, Map<Symbol, Type> types, Metadata metadata, Session session, int indent)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(types, "types is null");
        requireNonNull(metadata, "metadata is null");

        this.metadata = metadata;
        this.stats = Optional.empty();

        Visitor visitor = new Visitor(types, session);
        plan.accept(visitor, indent);
    }

    private PlanPrinter(PlanNode plan, Map<Symbol, Type> types, Metadata metadata, Session session, Map<PlanNodeId, PlanNodeStats> stats, int indent)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(types, "types is null");
        requireNonNull(metadata, "metadata is null");

        this.metadata = metadata;
        this.stats = Optional.of(stats);

        Visitor visitor = new Visitor(types, session);
        plan.accept(visitor, indent);
    }

    @Override
    public String toString()
    {
        return output.toString();
    }

    public static String textLogicalPlan(PlanNode plan, Map<Symbol, Type> types, Metadata metadata, Session session)
    {
        return new PlanPrinter(plan, types, metadata, session).toString();
    }

    public static String textLogicalPlan(PlanNode plan, Map<Symbol, Type> types, Metadata metadata, Session session, int indent)
    {
        return new PlanPrinter(plan, types, metadata, session, indent).toString();
    }

    public static String textLogicalPlan(PlanNode plan, Map<Symbol, Type> types, Metadata metadata, Session session, Map<PlanNodeId, PlanNodeStats> stats, int indent)
    {
        return new PlanPrinter(plan, types, metadata, session, stats, indent).toString();
    }

    public static String textDistributedPlan(List<StageInfo> stages, Metadata metadata, Session session)
    {
        StringBuilder builder = new StringBuilder();
        List<StageInfo> allStages = stages.stream()
                .flatMap(stage -> getAllStages(stage).stream())
                .collect(toImmutableList());
        for (StageInfo stageInfo : allStages) {
            Map<PlanNodeId, PlanNodeStats> aggregatedStats = new HashMap<>();
            List<PlanNodeStats> planNodeStats = stageInfo.getTasks().stream()
                    .map(TaskInfo::getStats)
                    .flatMap(taskStats -> getPlanNodeStats(taskStats).stream())
                    .collect(toList());
            for (PlanNodeStats stats : planNodeStats) {
                aggregatedStats.merge(stats.getPlanNodeId(), stats, PlanNodeStats::merge);
            }

            builder.append(formatFragment(metadata, session, stageInfo.getPlan(), Optional.of(stageInfo.getStageStats()), Optional.of(aggregatedStats)));
        }

        return builder.toString();
    }

    private static List<PlanNodeStats> getPlanNodeStats(TaskStats taskStats)
    {
        Map<PlanNodeId, Long> planNodeInputPositions = new HashMap<>();
        Map<PlanNodeId, Long> planNodeInputBytes = new HashMap<>();
        Map<PlanNodeId, Long> planNodeOutputPositions = new HashMap<>();
        Map<PlanNodeId, Long> planNodeOutputBytes = new HashMap<>();
        Map<PlanNodeId, Long> planNodeWallMillis = new HashMap<>();

        Map<PlanNodeId, Map<String, Long>> operatorTotalDrivers = new HashMap<>();
        Map<PlanNodeId, Map<String, Long>> operatorInputPositions = new HashMap<>();
        Map<PlanNodeId, Map<String, Double>> operatorInputPositionsSquared = new HashMap<>();
        Map<PlanNodeId, Map<String, Double>> operatorWeightedHashCollisions = new HashMap<>();
        Map<PlanNodeId, Map<String, Double>> operatorWeightedHashCollisionsSquared = new HashMap<>();
        Map<PlanNodeId, Map<String, Double>> operatorWeightedExpectedHashCollisions = new HashMap<>();

        for (PipelineStats pipelineStats : taskStats.getPipelines()) {
            // Due to eventual consistently collected stats, these could be empty
            if (pipelineStats.getOperatorSummaries().isEmpty()) {
                continue;
            }

            // Gather input statistics
            Set<PlanNodeId> processedInputNode = new HashSet<>();
            PlanNodeId inputPlanNode = pipelineStats.getOperatorSummaries().iterator().next().getPlanNodeId();
            for (OperatorStats operatorStats : pipelineStats.getOperatorSummaries()) {
                PlanNodeId planNodeId = operatorStats.getPlanNodeId();

                long wall = operatorStats.getAddInputWall().toMillis() + operatorStats.getGetOutputWall().toMillis() + operatorStats.getFinishWall().toMillis();
                planNodeWallMillis.merge(planNodeId, wall, Long::sum);

                operatorTotalDrivers.merge(planNodeId,
                        ImmutableMap.of(
                                operatorStats.getOperatorType(),
                                operatorStats.getTotalDrivers()),
                        PlanPrinter::sumLongsMap);

                operatorInputPositions.merge(planNodeId,
                        ImmutableMap.of(
                                operatorStats.getOperatorType(),
                                operatorStats.getInputPositions()),
                        PlanPrinter::sumLongsMap);
                operatorInputPositionsSquared.merge(planNodeId,
                        ImmutableMap.of(
                                operatorStats.getOperatorType(),
                                operatorStats.getInputPositionsSquared()
                        ),
                        PlanPrinter::sumDoublesMap);

                operatorWeightedHashCollisions.merge(planNodeId,
                        ImmutableMap.of(
                                operatorStats.getOperatorType(),
                                operatorStats.getWeightedHashCollisions()),
                        PlanPrinter::sumDoublesMap);
                operatorWeightedHashCollisionsSquared.merge(planNodeId,
                        ImmutableMap.of(
                                operatorStats.getOperatorType(),
                                operatorStats.getWeightedHashCollisionsSquared()
                        ),
                        PlanPrinter::sumDoublesMap);
                operatorWeightedExpectedHashCollisions.merge(planNodeId,
                        ImmutableMap.of(
                                operatorStats.getOperatorType(),
                                operatorStats.getWeightedExpectedHashCollisions()
                        ),
                        PlanPrinter::sumDoublesMap);

                // An "internal" pipeline like a hash build, links to another pipeline which is the actual output for this plan node
                if (operatorStats.getPlanNodeId().equals(inputPlanNode) && !pipelineStats.isInputPipeline()) {
                    continue;
                }
                if (processedInputNode.contains(planNodeId)) {
                    continue;
                }

                planNodeInputPositions.merge(planNodeId, operatorStats.getInputPositions(), Long::sum);
                planNodeInputBytes.merge(planNodeId, operatorStats.getInputDataSize().toBytes(), Long::sum);
                processedInputNode.add(planNodeId);
            }

            // Gather output statistics
            Set<PlanNodeId> processedOutputNode = new HashSet<>();
            PlanNodeId outputPlanNode = Iterables.getLast(pipelineStats.getOperatorSummaries()).getPlanNodeId();
            for (OperatorStats operatorStats : reverse(pipelineStats.getOperatorSummaries())) {
                PlanNodeId planNodeId = operatorStats.getPlanNodeId();

                // An "internal" pipeline like a hash build, links to another pipeline which is the actual output for this plan node
                if (operatorStats.getPlanNodeId().equals(outputPlanNode) && !pipelineStats.isOutputPipeline()) {
                    continue;
                }
                if (processedOutputNode.contains(planNodeId)) {
                    continue;
                }

                planNodeOutputPositions.merge(planNodeId, operatorStats.getOutputPositions(), Long::sum);
                planNodeOutputBytes.merge(planNodeId, operatorStats.getOutputDataSize().toBytes(), Long::sum);
                processedOutputNode.add(planNodeId);
            }
        }

        List<PlanNodeStats> stats = new ArrayList<>();
        for (PlanNodeId planNodeId : planNodeWallMillis.keySet()) {
            checkState(operatorInputPositions.containsKey(planNodeId), "no input statistics for %s", planNodeId);
            checkState(planNodeOutputPositions.containsKey(planNodeId), "no output statistics for %s", planNodeId);

            stats.add(new PlanNodeStats(planNodeId,
                    new Duration(planNodeWallMillis.get(planNodeId), MILLISECONDS),
                    planNodeInputPositions.get(planNodeId), succinctDataSize(planNodeInputBytes.get(planNodeId), BYTE),
                    planNodeOutputPositions.get(planNodeId), succinctDataSize(planNodeOutputBytes.get(planNodeId), BYTE),
                    operatorTotalDrivers.get(planNodeId),
                    operatorInputPositions.get(planNodeId), operatorInputPositionsSquared.get(planNodeId),
                    operatorWeightedHashCollisions.get(planNodeId), operatorWeightedHashCollisionsSquared.get(planNodeId),
                    operatorWeightedExpectedHashCollisions.get(planNodeId)));
        }
        return stats;
    }

    public static String textDistributedPlan(SubPlan plan, Metadata metadata, Session session)
    {
        StringBuilder builder = new StringBuilder();
        for (PlanFragment fragment : plan.getAllFragments()) {
            builder.append(formatFragment(metadata, session, fragment, Optional.empty(), Optional.empty()));
        }

        return builder.toString();
    }

    private static String formatFragment(Metadata metadata, Session session, PlanFragment fragment, Optional<StageStats> stageStats, Optional<Map<PlanNodeId, PlanNodeStats>> planNodeStats)
    {
        StringBuilder builder = new StringBuilder();
        builder.append(format("Fragment %s [%s]\n",
                fragment.getId(),
                fragment.getPartitioning()));

        if (stageStats.isPresent()) {
            builder.append(indentString(1))
                    .append(format("Cost: CPU %s, Input: %s (%s), Output: %s (%s)\n",
                            stageStats.get().getTotalCpuTime(),
                            formatPositions(stageStats.get().getProcessedInputPositions()),
                            stageStats.get().getProcessedInputDataSize(),
                            formatPositions(stageStats.get().getOutputPositions()),
                            stageStats.get().getOutputDataSize()));
        }

        PartitionFunctionBinding partitionFunction = fragment.getPartitionFunction();
        builder.append(indentString(1))
                .append(format("Output layout: [%s]\n",
                        Joiner.on(", ").join(partitionFunction.getOutputLayout())));

        boolean replicateNulls = partitionFunction.isReplicateNulls();
        List<String> arguments = partitionFunction.getPartitionFunctionArguments().stream()
                .map(argument -> {
                    if (argument.isConstant()) {
                        NullableValue constant = argument.getConstant();
                        String printableValue = castToVarchar(constant.getType(), constant.getValue(), metadata, session);
                        return constant.getType().getDisplayName() + "(" + printableValue + ")";
                    }
                    return argument.getColumn().toString();
                })
                .collect(toImmutableList());
        builder.append(indentString(1));
        if (replicateNulls) {
            builder.append(format("Output partitioning: %s (replicate nulls) [%s]\n",
                    partitionFunction.getPartitioningHandle(),
                    Joiner.on(", ").join(arguments)));
        }
        else {
            builder.append(format("Output partitioning: %s [%s]\n",
                    partitionFunction.getPartitioningHandle(),
                    Joiner.on(", ").join(arguments)));
        }

        if (stageStats.isPresent()) {
            builder.append(textLogicalPlan(fragment.getRoot(), fragment.getSymbols(), metadata, session, planNodeStats.get(), 1))
                    .append("\n");
        }
        else {
            builder.append(textLogicalPlan(fragment.getRoot(), fragment.getSymbols(), metadata, session, 1))
                    .append("\n");
        }

        return builder.toString();
    }

    public static String graphvizLogicalPlan(PlanNode plan, Map<Symbol, Type> types)
    {
        PlanFragment fragment = new PlanFragment(
                new PlanFragmentId("graphviz_plan"),
                plan,
                types,
                SINGLE_DISTRIBUTION,
                plan.getId(),
                new PartitionFunctionBinding(SINGLE_DISTRIBUTION, plan.getOutputSymbols(), ImmutableList.of()));
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
                output.append(", Input: ? lines (?B)");
            }
            output.append(", Output: ? lines (?B)");
            if (printFiltered) {
                output.append(", Filtered: ? %");
            }
            output.append('\n');
            return;
        }

        double fraction = (nodeStats.getPlanNodeWallTime().toMillis()) / (double) totalMillis;
        if (isNaN(fraction)) {
            fraction = 0.0;
        }

        output.append(indentString(indent));
        output.append(format(Locale.US, "Cost: %.2f%%", 100.0 * fraction));
        if (printInput) {
            output.append(format(", Input: %s (%s)",
                    formatPositions(nodeStats.getPlanNodeInputPositions()),
                    nodeStats.getPlanNodeInputDataSize().toString()));
        }
        output.append(format(", Output: %s (%s)",
                formatPositions(nodeStats.getPlanNodeOutputPositions()),
                nodeStats.getPlanNodeOutputDataSize().toString()));
        if (printFiltered) {
            double filtered = 100.0 * (nodeStats.getPlanNodeInputPositions() - nodeStats.getPlanNodeOutputPositions()) / nodeStats.getPlanNodeInputPositions();
            if (isNaN(filtered)) {
                filtered = 0.0;
            }
            output.append(format(Locale.US, ", Filtered: %.2f%%", filtered));
        }
        output.append('\n');

        printDistributions(indent, nodeStats);
    }

    private void printDistributions(int indent, PlanNodeStats nodeStats)
    {
        Map<String, Long> totalDrivers = nodeStats.getOperatorTotalDrivers();

        Map<String, Double> inputAverages = nodeStats.getOperatorInputPositionsAverages();
        Map<String, Double> inputStdDevs = nodeStats.getOperatorInputPositionsStdDevs();

        Map<String, Double> hashCollisionsAverages = nodeStats.getOperatorHashCollisionsAverages();
        Map<String, Double> hashCollisionsStdDevs = nodeStats.getOperatorHashCollisionsStdDevs();
        Map<String, Double> expectedHashCollisionsAverages = nodeStats.getOperatorExpectedCollisionsAverages();

        for (String operator : inputAverages.keySet()) {
            output.append(indentString(indent));
            double inputAverage = inputAverages.get(operator);
            output.append(format(Locale.US, "%s := Drivers: %d, Input avg.: %.2f lines, Input std.dev.: %.2f%%",
                    operator, totalDrivers.get(operator), inputAverage, inputStdDevs.get(operator) * 100.0d / inputAverage));
            output.append('\n');

            double hashCollisionsAverage = hashCollisionsAverages.get(operator);
            double expectedHashCollisionsAverage = expectedHashCollisionsAverages.get(operator);
            if (hashCollisionsAverage != 0.0d) {
                output.append(indentString(indent));

                double hashCollisionsStdDevRatio = hashCollisionsStdDevs.get(operator) / hashCollisionsAverage;
                if (expectedHashCollisionsAverage != 0.0d) {
                    double hashCollisionsRatio = hashCollisionsAverage / expectedHashCollisionsAverage;
                    output.append(format(Locale.US, "%s := Collisions avg.: %.2f (%.2f%%), Collisions std.dev.: %.2f%%",
                            operator, hashCollisionsAverage, hashCollisionsRatio * 100.0d, hashCollisionsStdDevRatio * 100.0d));
                }
                else {
                    output.append(format(Locale.US, "%s := Collisions avg.: %.2f, Collisions std.dev.: %.2f%%",
                            operator, hashCollisionsAverage, hashCollisionsStdDevRatio * 100.0d));
                }

                output.append('\n');
            }
        }
    }

    private static String formatPositions(long positions)
    {
        if (positions == 1) {
            return "1 line";
        }
        else {
            return positions + " lines";
        }
    }

    private static String indentString(int indent)
    {
        return Strings.repeat("    ", indent);
    }

    private class Visitor
            extends PlanVisitor<Integer, Void>
    {
        private final Map<Symbol, Type> types;
        private final Session session;

        @SuppressWarnings("AssignmentToCollectionOrArrayFieldFromParameter")
        public Visitor(Map<Symbol, Type> types, Session session)
        {
            this.types = types;
            this.session = session;
        }

        @Override
        public Void visitExplainAnalyze(ExplainAnalyzeNode node, Integer indent)
        {
            print(indent, "- ExplainAnalyze => [%s]", formatOutputs(node.getOutputSymbols()));
            printStats(indent + 2, node.getId());
            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitJoin(JoinNode node, Integer indent)
        {
            List<Expression> joinExpressions = new ArrayList<>();
            for (JoinNode.EquiJoinClause clause : node.getCriteria()) {
                joinExpressions.add(new ComparisonExpression(ComparisonExpression.Type.EQUAL,
                        new QualifiedNameReference(clause.getLeft().toQualifiedName()),
                        new QualifiedNameReference(clause.getRight().toQualifiedName())));
            }

            print(indent, "- %s[%s] => [%s]", node.getType().getJoinLabel(), Joiner.on(" AND ").join(joinExpressions), formatOutputs(node.getOutputSymbols()));
            printStats(indent + 2, node.getId());
            node.getLeft().accept(this, indent + 1);
            node.getRight().accept(this, indent + 1);

            return null;
        }

        @Override
        public Void visitSemiJoin(SemiJoinNode node, Integer indent)
        {
            print(indent, "- SemiJoin[%s = %s] => [%s]", node.getSourceJoinSymbol(), node.getFilteringSourceJoinSymbol(), formatOutputs(node.getOutputSymbols()));
            printStats(indent + 2, node.getId());
            node.getSource().accept(this, indent + 1);
            node.getFilteringSource().accept(this, indent + 1);

            return null;
        }

        @Override
        public Void visitIndexSource(IndexSourceNode node, Integer indent)
        {
            print(indent, "- IndexSource[%s, lookup = %s] => [%s]", node.getIndexHandle(), node.getLookupSymbols(), formatOutputs(node.getOutputSymbols()));
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
                joinExpressions.add(new ComparisonExpression(ComparisonExpression.Type.EQUAL,
                        new QualifiedNameReference(clause.getProbe().toQualifiedName()),
                        new QualifiedNameReference(clause.getIndex().toQualifiedName())));
            }

            print(indent, "- %sIndexJoin[%s] => [%s]", node.getType().getJoinLabel(), Joiner.on(" AND ").join(joinExpressions), formatOutputs(node.getOutputSymbols()));
            printStats(indent + 2, node.getId());
            node.getProbeSource().accept(this, indent + 1);
            node.getIndexSource().accept(this, indent + 1);

            return null;
        }

        @Override
        public Void visitLimit(LimitNode node, Integer indent)
        {
            print(indent, "- Limit[%s] => [%s]", node.getCount(), formatOutputs(node.getOutputSymbols()));
            printStats(indent + 2, node.getId());
            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitDistinctLimit(DistinctLimitNode node, Integer indent)
        {
            print(indent, "- DistinctLimit[%s] => [%s]", node.getLimit(), formatOutputs(node.getOutputSymbols()));
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
            String key = "";
            if (!node.getGroupBy().isEmpty()) {
                key = node.getGroupBy().toString();
            }
            String sampleWeight = "";
            if (node.getSampleWeight().isPresent()) {
                sampleWeight = format("[sampleWeight = %s]", node.getSampleWeight().get());
            }

            print(indent, "- Aggregate%s%s%s => [%s]", type, key, sampleWeight, formatOutputs(node.getOutputSymbols()));
            printStats(indent + 2, node.getId());

            for (Map.Entry<Symbol, FunctionCall> entry : node.getAggregations().entrySet()) {
                if (node.getMasks().containsKey(entry.getKey())) {
                    print(indent + 2, "%s := %s (mask = %s)", entry.getKey(), entry.getValue(), node.getMasks().get(entry.getKey()));
                }
                else {
                    print(indent + 2, "%s := %s", entry.getKey(), entry.getValue());
                }
            }

            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitGroupId(GroupIdNode node, Integer indent)
        {
            print(indent, "- GroupId%s => [%s]", node.getGroupingSets(), formatOutputs(node.getOutputSymbols()));
            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitMarkDistinct(MarkDistinctNode node, Integer indent)
        {
            print(indent, "- MarkDistinct[distinct=%s marker=%s] => [%s]", formatOutputs(node.getDistinctSymbols()), node.getMarkerSymbol(), formatOutputs(node.getOutputSymbols()));
            printStats(indent + 2, node.getId());
            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitWindow(WindowNode node, Integer indent)
        {
            List<String> partitionBy = Lists.transform(node.getPartitionBy(), Functions.toStringFunction());

            List<String> orderBy = Lists.transform(node.getOrderBy(), input -> input + " " + node.getOrderings().get(input));

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
            if (!orderBy.isEmpty()) {
                args.add(format("order by (%s)", Stream.concat(
                        node.getOrderBy().stream()
                                .limit(node.getPreSortedOrderPrefix())
                                .map(symbol -> "<" + symbol + ">"),
                        node.getOrderBy().stream()
                                .skip(node.getPreSortedOrderPrefix())
                                .map(Symbol::toString))
                        .collect(Collectors.joining(", "))));
            }

            print(indent, "- Window[%s] => [%s]", Joiner.on(", ").join(args), formatOutputs(node.getOutputSymbols()));
            printStats(indent + 2, node.getId());

            for (Map.Entry<Symbol, FunctionCall> entry : node.getWindowFunctions().entrySet()) {
                print(indent + 2, "%s := %s(%s)", entry.getKey(), entry.getValue().getName(), Joiner.on(", ").join(entry.getValue().getArguments()));
            }
            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitTopNRowNumber(TopNRowNumberNode node, Integer indent)
        {
            List<String> partitionBy = Lists.transform(node.getPartitionBy(), Functions.toStringFunction());

            List<String> orderBy = Lists.transform(node.getOrderBy(), input -> input + " " + node.getOrderings().get(input));

            List<String> args = new ArrayList<>();
            args.add(format("partition by (%s)", Joiner.on(", ").join(partitionBy)));
            args.add(format("order by (%s)", Joiner.on(", ").join(orderBy)));

            print(indent, "- TopNRowNumber[%s limit %s] => [%s]", Joiner.on(", ").join(args), node.getMaxRowCountPerPartition(), formatOutputs(node.getOutputSymbols()));
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

            print(indent, "- RowNumber[%s] => [%s]", Joiner.on(", ").join(args), formatOutputs(node.getOutputSymbols()));
            printStats(indent + 2, node.getId());

            print(indent + 2, "%s := %s", node.getRowNumberSymbol(), "row_number()");
            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitTableScan(TableScanNode node, Integer indent)
        {
            TableHandle table = node.getTable();
            print(indent, "- TableScan[%s, originalConstraint = %s] => [%s]", table, node.getOriginalConstraint(), formatOutputs(node.getOutputSymbols()));
            printStats(indent + 2, node.getId());
            printTableScanInfo(node, indent);

            return null;
        }

        @Override
        public Void visitValues(ValuesNode node, Integer indent)
        {
            print(indent, "- Values => [%s]", formatOutputs(node.getOutputSymbols()));
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
                Optional<FilterNode> filterNode, Optional<ProjectNode> projectNode,
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

            String format = "- ScanFilterAndProject[";
            List<Object> arguments = new LinkedList<>();

            if (scanNode.isPresent()) {
                format += "table = %s, originalConstraint = %s";
                if (filterNode.isPresent()) {
                    format += ", ";
                }
                TableHandle table = scanNode.get().getTable();
                arguments.add(table);
                arguments.add(scanNode.get().getOriginalConstraint());
            }

            if (filterNode.isPresent()) {
                format += "filterPredicate = %s";
                arguments.add(filterNode.get().getPredicate());
            }

            format += "] => [%s]";
            if (projectNode.isPresent()) {
                arguments.add(formatOutputs(projectNode.get().getOutputSymbols()));
            }
            else {
                arguments.add(formatOutputs(filterNode.get().getOutputSymbols()));
            }

            print(indent, format, arguments);
            printStats(indent + 2, planNodeId, true, true);

            if (projectNode.isPresent()) {
                printProjectInfo(projectNode.get(), indent);
            }

            if (scanNode.isPresent()) {
                printTableScanInfo(scanNode.get(), indent);
                return null;
            }

            sourceNode.accept(this, indent + 1);
            return null;
        }

        private void printProjectInfo(ProjectNode node, int indent)
        {
            for (Map.Entry<Symbol, Expression> entry : node.getAssignments().entrySet()) {
                if (entry.getValue() instanceof QualifiedNameReference && ((QualifiedNameReference) entry.getValue()).getName().equals(entry.getKey().toQualifiedName())) {
                    // skip identity assignments
                    continue;
                }
                print(indent + 2, "%s := %s", entry.getKey(), entry.getValue());
            }
        }

        private void printTableScanInfo(TableScanNode node, int indent)
        {
            TableHandle table = node.getTable();

            TupleDomain<ColumnHandle> predicate = node.getLayout()
                    .map(layoutHandle -> metadata.getLayout(session, layoutHandle))
                    .map(TableLayout::getPredicate)
                    .orElse(TupleDomain.<ColumnHandle>all());

            if (node.getLayout().isPresent()) {
                // TODO: find a better way to do this
                ConnectorTableLayoutHandle layout = node.getLayout().get().getConnectorHandle();
                if (!table.getConnectorHandle().toString().equals(layout.toString())) {
                    print(indent + 2, "LAYOUT: %s", layout);
                }
            }

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
            printStats(indent + 2, node.getId());

            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitOutput(OutputNode node, Integer indent)
        {
            print(indent, "- Output[%s] => [%s]", Joiner.on(", ").join(node.getColumnNames()), formatOutputs(node.getOutputSymbols()));
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
            Iterable<String> keys = Iterables.transform(node.getOrderBy(), input -> input + " " + node.getOrderings().get(input));

            print(indent, "- TopN[%s by (%s)] => [%s]", node.getCount(), Joiner.on(", ").join(keys), formatOutputs(node.getOutputSymbols()));
            printStats(indent + 2, node.getId());
            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitSort(SortNode node, Integer indent)
        {
            Iterable<String> keys = Iterables.transform(node.getOrderBy(), input -> input + " " + node.getOrderings().get(input));

            print(indent, "- Sort[%s] => [%s]", Joiner.on(", ").join(keys), formatOutputs(node.getOutputSymbols()));
            printStats(indent + 2, node.getId());
            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitRemoteSource(RemoteSourceNode node, Integer indent)
        {
            print(indent, "- RemoteSource[%s] => [%s]", Joiner.on(',').join(node.getSourceFragmentIds()), formatOutputs(node.getOutputSymbols()));
            printStats(indent + 2, node.getId());

            return null;
        }

        @Override
        public Void visitUnion(UnionNode node, Integer indent)
        {
            print(indent, "- Union => [%s]", formatOutputs(node.getOutputSymbols()));
            printStats(indent + 2, node.getId());

            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitTableWriter(TableWriterNode node, Integer indent)
        {
            print(indent, "- TableWriter => [%s]", formatOutputs(node.getOutputSymbols()));
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
            printStats(indent + 2, node.getId());

            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitSample(SampleNode node, Integer indent)
        {
            print(indent, "- Sample[%s: %s] => [%s]", node.getSampleType(), node.getSampleRatio(), formatOutputs(node.getOutputSymbols()));
            printStats(indent + 2, node.getId());

            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitExchange(ExchangeNode node, Integer indent)
        {
            print(indent, "- Exchange[%s] => %s", node.getType(), formatOutputs(node.getOutputSymbols()));
            printStats(indent + 2, node.getId());

            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitDelete(DeleteNode node, Integer indent)
        {
            print(indent, "- Delete[%s] => [%s]", node.getTarget(), formatOutputs(node.getOutputSymbols()));
            printStats(indent + 2, node.getId());

            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitMetadataDelete(MetadataDeleteNode node, Integer indent)
        {
            print(indent, "- MetadataDelete[%s] => [%s]", node.getTarget(), formatOutputs(node.getOutputSymbols()));
            printStats(indent + 2, node.getId());

            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitEnforceSingleRow(EnforceSingleRowNode node, Integer indent)
        {
            print(indent, "- Scalar => [%s]", formatOutputs(node.getOutputSymbols()));
            printStats(indent + 2, node.getId());

            return processChildren(node, indent + 1);
        }

        @Override
        protected Void visitPlan(PlanNode node, Integer context)
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

        private String formatOutputs(Iterable<Symbol> symbols)
        {
            return Joiner.on(", ").join(Iterables.transform(symbols, input -> input + ":" + types.get(input).getDisplayName()));
        }

        private void printConstraint(int indent, ColumnHandle column, TupleDomain<ColumnHandle> constraint)
        {
            checkArgument(!constraint.isNone());
            Map<ColumnHandle, Domain> domains = constraint.getDomains().get();
            if (!constraint.isAll() && domains.containsKey(column)) {
                print(indent, ":: %s", formatDomain(simplifyDomain(domains.get(column))));
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
                                String value = castToVarchar(type, range.getSingleValue(), PlanPrinter.this.metadata, session);
                                builder.append('[').append(value).append(']');
                            }
                            else {
                                builder.append((range.getLow().getBound() == Marker.Bound.EXACTLY) ? '[' : '(');

                                if (range.getLow().isLowerUnbounded()) {
                                    builder.append("<min>");
                                }
                                else {
                                    builder.append(castToVarchar(type, range.getLow().getValue(), PlanPrinter.this.metadata, session));
                                }

                                builder.append(", ");

                                if (range.getHigh().isUpperUnbounded()) {
                                    builder.append("<max>");
                                }
                                else {
                                    builder.append(castToVarchar(type, range.getHigh().getValue(), PlanPrinter.this.metadata, session));
                                }

                                builder.append((range.getHigh().getBound() == Marker.Bound.EXACTLY) ? ']' : ')');
                            }
                            parts.add(builder.toString());
                        }
                    },
                    discreteValues -> discreteValues.getValues().stream()
                            .map(value -> castToVarchar(type, value, PlanPrinter.this.metadata, session))
                            .sorted() // Sort so the values will be printed in predictable order
                            .forEach(parts::add),
                    allOrNone -> {
                        if (allOrNone.isAll()) {
                            parts.add("ALL VALUES");
                        }
                    });

            return "[" + Joiner.on(", ").join(parts.build()) + "]";
        }
    }

    private static String castToVarchar(Type type, Object value, Metadata metadata, Session session)
    {
        if (value == null) {
            return "NULL";
        }

        Signature coercion = metadata.getFunctionRegistry().getCoercion(type, VARCHAR);

        try {
            Slice coerced = (Slice) new FunctionInvoker(metadata.getFunctionRegistry()).invoke(coercion, session.toConnectorSession(), value);
            return coerced.toStringUtf8();
        }
        catch (OperatorNotFoundException e) {
            return "<UNREPRESENTABLE VALUE>";
        }
        catch (Throwable throwable) {
            throw Throwables.propagate(throwable);
        }
    }

    private static class PlanNodeStats
    {
        private final PlanNodeId planNodeId;

        private final Duration planNodeWallTime;
        private final long planNodeInputPositions;
        private final DataSize planNodeInputDataSize;
        private final long planNodeOutputPositions;
        private final DataSize planNodeOutputDataSize;

        private final Map<String, Long> operatorTotalDrivers;
        private final Map<String, Long> operatorInputPositions;
        private final Map<String, Double> operatorInputPositionsSquared;
        private final Map<String, Double> operatorWeightedHashCollisions;
        private final Map<String, Double> operatorWeightedHashCollisionsSquared;
        private final Map<String, Double> operatorWeightedExpectedHashCollisions;

        private PlanNodeStats(
                PlanNodeId planNodeId,
                Duration planNodeWallTime,
                long planNodeInputPositions, DataSize planNodeInputDataSize,
                long planNodeOutputPositions, DataSize planNodeOutputDataSize,
                Map<String, Long> operatorTotalDrivers,
                Map<String, Long> operatorInputPositions, Map<String, Double> operatorInputPositionsSquared,
                Map<String, Double> operatorWeightedHashCollisions, Map<String, Double> operatorWeightedHashCollisionsSquared,
                Map<String, Double> operatorWeightedExpectedHashCollisions)
        {
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");

            this.planNodeWallTime = requireNonNull(planNodeWallTime, "planNodeWallTime is null");
            this.planNodeInputPositions = planNodeInputPositions;
            this.planNodeInputDataSize = planNodeInputDataSize;
            this.planNodeOutputPositions = planNodeOutputPositions;
            this.planNodeOutputDataSize = planNodeOutputDataSize;

            this.operatorTotalDrivers = requireNonNull(operatorTotalDrivers, "operatorTotalDrivers is null");
            this.operatorInputPositions = requireNonNull(operatorInputPositions, "operatorInputPositions is null");
            this.operatorInputPositionsSquared = requireNonNull(operatorInputPositionsSquared, "operatorInputPositionsSquared is null");
            this.operatorWeightedHashCollisions = requireNonNull(operatorWeightedHashCollisions, "operatorWeightedHashCollisions is null");
            this.operatorWeightedHashCollisionsSquared = requireNonNull(operatorWeightedHashCollisionsSquared, "operatorWeightedHashCollisionsSquared is null");
            this.operatorWeightedExpectedHashCollisions = requireNonNull(operatorWeightedExpectedHashCollisions, "operatorWeightedExpectedHashCollisions is null");
        }

        public PlanNodeId getPlanNodeId()
        {
            return planNodeId;
        }

        public Duration getPlanNodeWallTime()
        {
            return planNodeWallTime;
        }

        public Map<String, Long> getOperatorTotalDrivers()
        {
            return operatorTotalDrivers;
        }

        public long getPlanNodeInputPositions()
        {
            return planNodeInputPositions;
        }

        public DataSize getPlanNodeInputDataSize()
        {
            return planNodeInputDataSize;
        }

        public long getPlanNodeOutputPositions()
        {
            return planNodeOutputPositions;
        }

        public DataSize getPlanNodeOutputDataSize()
        {
            return planNodeOutputDataSize;
        }

        public Map<String, Double> getOperatorInputPositionsAverages()
        {
            return operatorInputPositions.entrySet().stream()
                    .collect(toMap(
                            Map.Entry::getKey,
                            entry -> (double) entry.getValue() / operatorTotalDrivers.get(entry.getKey())));
        }

        public Map<String, Double> getOperatorInputPositionsStdDevs()
        {
            return operatorInputPositions.keySet().stream()
                    .collect(toMap(
                            key -> key,
                            key -> computerStdDev(
                                    operatorInputPositionsSquared.get(key),
                                    operatorInputPositions.get(key),
                                    operatorTotalDrivers.get(key))));
        }

        public Map<String, Double> getOperatorHashCollisionsAverages()
        {
            return operatorWeightedHashCollisions.entrySet().stream()
                    .collect(toMap(
                            Map.Entry::getKey,
                            entry -> entry.getValue() / operatorInputPositions.get(entry.getKey())));
        }

        public Map<String, Double> getOperatorHashCollisionsStdDevs()
        {
            return operatorWeightedHashCollisions.keySet().stream()
                    .collect(toMap(
                            key -> key,
                            key -> computedWeightedStdDev(
                                    operatorWeightedHashCollisionsSquared.get(key),
                                    operatorWeightedHashCollisions.get(key),
                                    operatorInputPositions.get(key))));
        }

        public Map<String, Double> getOperatorExpectedCollisionsAverages()
        {
            return operatorWeightedExpectedHashCollisions.entrySet().stream()
                    .collect(toMap(
                            Map.Entry::getKey,
                            entry -> entry.getValue() / operatorInputPositions.get(entry.getKey())));
        }

        public static PlanNodeStats merge(PlanNodeStats planNodeStats1, PlanNodeStats planNodeStats2)
        {
            checkArgument(planNodeStats1.getPlanNodeId().equals(planNodeStats2.getPlanNodeId()), "planNodeIds do not match. %s != %s", planNodeStats1.getPlanNodeId(), planNodeStats2.getPlanNodeId());

            long planNodeInputPositions = planNodeStats1.planNodeInputPositions + planNodeStats2.planNodeInputPositions;
            DataSize planNodeInputDataSize = succinctBytes(planNodeStats1.planNodeInputDataSize.toBytes() + planNodeStats2.planNodeInputDataSize.toBytes());
            long planNodeOutputPositions = planNodeStats1.planNodeOutputPositions + planNodeStats2.planNodeOutputPositions;
            DataSize planNodeOutputDataSize = succinctBytes(planNodeStats1.planNodeOutputDataSize.toBytes() + planNodeStats2.planNodeOutputDataSize.toBytes());

            Map<String, Long> operatorTotalDrivers = sumLongsMap(planNodeStats1.operatorTotalDrivers, planNodeStats2.operatorTotalDrivers);
            Map<String, Long> operatorInputPositions = sumLongsMap(planNodeStats1.operatorInputPositions, planNodeStats2.operatorInputPositions);
            Map<String, Double> operatorInputPositionsSquared = sumDoublesMap(planNodeStats1.operatorInputPositionsSquared, planNodeStats2.operatorInputPositionsSquared);
            Map<String, Double> operatorWeightedHashCollisions = sumDoublesMap(planNodeStats1.operatorWeightedHashCollisions, planNodeStats2.operatorWeightedHashCollisions);
            Map<String, Double> operatorWeightedHashCollisionsSquared = sumDoublesMap(planNodeStats1.operatorWeightedHashCollisionsSquared, planNodeStats2.operatorWeightedHashCollisionsSquared);
            Map<String, Double> operatorWeightedExpectedHashCollisions = sumDoublesMap(planNodeStats1.operatorWeightedExpectedHashCollisions, planNodeStats2.operatorWeightedExpectedHashCollisions);

            return new PlanNodeStats(
                    planNodeStats1.getPlanNodeId(),
                    new Duration(planNodeStats1.getPlanNodeWallTime().toMillis() + planNodeStats2.getPlanNodeWallTime().toMillis(), MILLISECONDS),
                    planNodeInputPositions, planNodeInputDataSize,
                    planNodeOutputPositions, planNodeOutputDataSize,
                    operatorTotalDrivers,
                    operatorInputPositions, operatorInputPositionsSquared,
                    operatorWeightedHashCollisions, operatorWeightedHashCollisionsSquared,
                    operatorWeightedExpectedHashCollisions);
        }
    }

    private static double computerStdDev(double sumSquared, double sum, long n)
    {
        double average = sum / n;
        double variance = (sumSquared - 2 * sum * average + average * average * n) / n;
        // variance might be negative because of numeric inaccuracy, therefore we need to use max
        return sqrt(max(variance, 0d));
    }

    private static double computedWeightedStdDev(double sumSquared, double sum, double totalWeight)
    {
        double average = sum / totalWeight;
        double variance = (sumSquared - 2 * sum * average) / totalWeight + average * average;
        // variance might be negative because of numeric inaccuracy, therefore we need to use max
        return sqrt(max(variance, 0d));
    }

    private static <K> Map<K, Long> sumLongsMap(Map<K, Long> map1, Map<K, Long> map2)
    {
        return mergeMaps(map1, map2, (left, right) -> left + right);
    }

    private static <K> Map<K, Double> sumDoublesMap(Map<K, Double> map1, Map<K, Double> map2)
    {
        return mergeMaps(map1, map2, (left, right) -> left + right);
    }

    private static <K, V> Map<K, V> mergeMaps(Map<K, V> map1, Map<K, V> map2, BinaryOperator<V> merger)
    {
        return Stream.of(map1, map2)
                .map(Map::entrySet)
                .flatMap(Collection::stream)
                .collect(toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        merger::apply));
    }
}
