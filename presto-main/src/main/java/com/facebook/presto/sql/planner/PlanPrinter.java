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
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.facebook.presto.execution.StageInfo.getAllStages;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.planner.DomainUtils.simplifyDomain;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.CaseFormat.UPPER_UNDERSCORE;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.succinctBytes;
import static io.airlift.units.DataSize.succinctDataSize;
import static java.lang.Double.isFinite;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;

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
                .flatMap(stage -> getAllStages(Optional.of(stage)).stream())
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
        // Best effort to reconstruct the plan nodes from operators.
        // Because stats are collected separately from query execution,
        // it's possible that some or all of them are missing or out of date.
        // For example, a LIMIT clause can cause a query to finish before stats
        // are collected from the leaf stages.
        Map<PlanNodeId, Long> outputPositions = new HashMap<>();
        Map<PlanNodeId, Long> outputBytes = new HashMap<>();
        Map<PlanNodeId, Long> wallMillis = new HashMap<>();

        for (PipelineStats pipelineStats : taskStats.getPipelines()) {
            Map<PlanNodeId, Long> pipelineOutputPositions = new HashMap<>();
            Map<PlanNodeId, Long> pipelineOutputBytes = new HashMap<>();

            List<OperatorStats> operatorSummaries = pipelineStats.getOperatorSummaries();
            for (int i = 0; i < operatorSummaries.size(); i++) {
                OperatorStats operatorStats = operatorSummaries.get(i);
                PlanNodeId planNodeId = operatorStats.getPlanNodeId();
                long wall = operatorStats.getAddInputWall().toMillis() + operatorStats.getGetOutputWall().toMillis() + operatorStats.getFinishWall().toMillis();
                wallMillis.merge(planNodeId, wall, Long::sum);

                // An "internal" pipeline like a hash build, links to another pipeline which is the actual output for this plan node
                if (i == operatorSummaries.size() - 1 && !pipelineStats.isOutputPipeline()) {
                    pipelineOutputBytes.remove(planNodeId);
                    pipelineOutputPositions.remove(planNodeId);
                }
                else {
                    // Overwrite whatever we currently have, to get the last operator's stats for this plan node in this pipeline
                    pipelineOutputPositions.put(planNodeId, operatorStats.getOutputPositions());
                    pipelineOutputBytes.put(planNodeId, operatorStats.getOutputDataSize().toBytes());
                }
            }

            for (Map.Entry<PlanNodeId, Long> entry : pipelineOutputPositions.entrySet()) {
                outputBytes.merge(entry.getKey(), pipelineOutputBytes.get(entry.getKey()), Long::sum);
                outputPositions.merge(entry.getKey(), entry.getValue(), Long::sum);
            }
        }

        List<PlanNodeStats> stats = new ArrayList<>();
        for (Map.Entry<PlanNodeId, Long> entry : wallMillis.entrySet()) {
            if (outputPositions.containsKey(entry.getKey())) {
                stats.add(new PlanNodeStats(entry.getKey(), new Duration(entry.getValue(), MILLISECONDS), outputPositions.get(entry.getKey()), succinctDataSize(outputBytes.get(entry.getKey()), BYTE)));
            }
            else {
                // It's possible there will be no output stats because all the pipelines that we observed were non-output.
                // For example in a query like SELECT * FROM a JOIN b ON c = d LIMIT 1
                // It's possible to observe stats after the build starts, but before the probe does
                // and therefore only have wall time, but no output stats
                stats.add(new PlanNodeStats(entry.getKey(), new Duration(entry.getValue(), MILLISECONDS)));
            }
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
                    .append(format("Cost: CPU %s, Input %d (%s), Output %d (%s)\n",
                            stageStats.get().getTotalCpuTime(),
                            stageStats.get().getProcessedInputPositions(),
                            stageStats.get().getProcessedInputDataSize(),
                            stageStats.get().getOutputPositions(),
                            stageStats.get().getOutputDataSize()));
        }

        PartitioningScheme partitioningScheme = fragment.getPartitioningScheme();
        builder.append(indentString(1))
                .append(format("Output layout: [%s]\n",
                        Joiner.on(", ").join(partitioningScheme.getOutputLayout())));

        boolean replicateNulls = partitioningScheme.isReplicateNulls();
        List<String> arguments = partitioningScheme.getPartitioning().getArguments().stream()
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
                    partitioningScheme.getPartitioning().getHandle(),
                    Joiner.on(", ").join(arguments)));
        }
        else {
            builder.append(format("Output partitioning: %s [%s]\n",
                    partitioningScheme.getPartitioning().getHandle(),
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
                ImmutableList.of(plan.getId()),
                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), plan.getOutputSymbols()));
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

    private void printStats(int indent, PlanNodeId planNodeId)
    {
        if (!stats.isPresent()) {
            return;
        }
        long totalMillis = stats.get().values().stream()
                .mapToLong(node -> node.getWallTime().toMillis())
                .sum();
        PlanNodeStats stats = this.stats.get().get(planNodeId);
        if (stats == null) {
            output.append(indentString(indent))
                    .append("Cost: unknown, Output: unknown \n");
            return;
        }
        double fraction = (stats.getWallTime().toMillis()) / (double) totalMillis;
        String fractionString;
        if (isFinite(fraction)) {
            fractionString = format("%.2f%%", 100.0 * fraction);
        }
        else {
            fractionString = "unknown";
        }

        String outputString;
        if (stats.getOutputPositions().isPresent() && stats.getOutputDataSize().isPresent()) {
            outputString = format("%s rows (%s)", stats.getOutputPositions().get(), stats.getOutputDataSize().get());
        }
        else {
            outputString = "unknown";
        }
        output.append(indentString(indent))
                .append(format("Cost: %s, Output: %s\n", fractionString, outputString));
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
                        clause.getLeft().toSymbolReference(),
                        clause.getRight().toSymbolReference()));
            }
            node.getFilter().ifPresent(expression -> joinExpressions.add(expression));

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
                        clause.getProbe().toSymbolReference(),
                        clause.getIndex().toSymbolReference()));
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
            print(indent, "- Limit%s[%s] => [%s]", node.isPartial() ? "Partial" : "", node.getCount(), formatOutputs(node.getOutputSymbols()));
            printStats(indent + 2, node.getId());
            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitDistinctLimit(DistinctLimitNode node, Integer indent)
        {
            print(indent, "- DistinctLimit%s[%s] => [%s]", node.isPartial() ? "Partial" : "", node.getLimit(), formatOutputs(node.getOutputSymbols()));
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
            print(indent, "- Filter[%s] => [%s]", node.getPredicate(), formatOutputs(node.getOutputSymbols()));
            printStats(indent + 2, node.getId());
            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitProject(ProjectNode node, Integer indent)
        {
            print(indent, "- Project => [%s]", formatOutputs(node.getOutputSymbols()));
            printStats(indent + 2, node.getId());
            for (Map.Entry<Symbol, Expression> entry : node.getAssignments().entrySet()) {
                if (entry.getValue() instanceof SymbolReference && ((SymbolReference) entry.getValue()).getName().equals(entry.getKey().getName())) {
                    // skip identity assignments
                    continue;
                }
                print(indent + 2, "%s := %s", entry.getKey(), entry.getValue());
            }

            return processChildren(node, indent + 1);
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
        public Void visitIntersect(IntersectNode node, Integer indent)
        {
            print(indent, "- Intersect => [%s]", formatOutputs(node.getOutputSymbols()));
            printStats(indent + 2, node.getId());

            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitExcept(ExceptNode node, Integer indent)
        {
            print(indent, "- Except => [%s]", formatOutputs(node.getOutputSymbols()));
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
            if (node.getScope() == Scope.LOCAL) {
                print(indent, "- LocalExchange[%s%s] (%s) => %s",
                        node.getPartitioningScheme().getPartitioning().getHandle(),
                        node.getPartitioningScheme().isReplicateNulls() ? " - REPLICATE NULLS" : "",
                        Joiner.on(", ").join(node.getPartitioningScheme().getPartitioning().getArguments()),
                        formatOutputs(node.getOutputSymbols()));
            }
            else {
                print(indent, "- %sExchange[%s%s] => %s",
                        UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, node.getScope().toString()),
                        node.getType(),
                        node.getPartitioningScheme().isReplicateNulls() ? " - REPLICATE NULLS" : "",
                        formatOutputs(node.getOutputSymbols()));
            }
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
        private final Duration wallTime;
        private final Optional<Long> outputPositions;
        private final Optional<DataSize> outputDataSize;

        public PlanNodeStats(PlanNodeId planNodeId, Duration wallTime)
        {
            this(planNodeId, wallTime, Optional.empty(), Optional.empty());
        }

        public PlanNodeStats(PlanNodeId planNodeId, Duration wallTime, long outputPositions, DataSize outputDataSize)
        {
            this(planNodeId, wallTime, Optional.of(outputPositions), Optional.of(outputDataSize));
        }

        private PlanNodeStats(PlanNodeId planNodeId, Duration wallTime, Optional<Long> outputPositions, Optional<DataSize> outputDataSize)
        {
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.wallTime = requireNonNull(wallTime, "wallTime is null");
            this.outputPositions = outputPositions;
            this.outputDataSize = outputDataSize;
        }

        public PlanNodeId getPlanNodeId()
        {
            return planNodeId;
        }

        public Duration getWallTime()
        {
            return wallTime;
        }

        public Optional<Long> getOutputPositions()
        {
            return outputPositions;
        }

        public Optional<DataSize> getOutputDataSize()
        {
            return outputDataSize;
        }

        public static PlanNodeStats merge(PlanNodeStats planNodeStats1, PlanNodeStats planNodeStats2)
        {
            checkArgument(planNodeStats1.getPlanNodeId().equals(planNodeStats2.getPlanNodeId()), "planNodeIds do not match. %s != %s", planNodeStats1.getPlanNodeId(), planNodeStats2.getPlanNodeId());
            Optional<Long> outputPositions;
            if (planNodeStats1.getOutputPositions().isPresent() && planNodeStats2.getOutputPositions().isPresent()) {
                outputPositions = Optional.of(planNodeStats1.getOutputPositions().get() + planNodeStats2.getOutputPositions().get());
            }
            else if (planNodeStats1.getOutputPositions().isPresent()) {
                outputPositions = planNodeStats1.getOutputPositions();
            }
            else {
                outputPositions = planNodeStats2.getOutputPositions();
            }
            Optional<DataSize> outputDataSize;
            if (planNodeStats1.getOutputDataSize().isPresent() && planNodeStats2.getOutputDataSize().isPresent()) {
                outputDataSize = Optional.of(succinctBytes(planNodeStats1.getOutputDataSize().get().toBytes() + planNodeStats2.getOutputDataSize().get().toBytes()));
            }
            else if (planNodeStats1.getOutputDataSize().isPresent()) {
                outputDataSize = planNodeStats1.getOutputDataSize();
            }
            else {
                outputDataSize = planNodeStats2.getOutputDataSize();
            }
            return new PlanNodeStats(
                    planNodeStats1.getPlanNodeId(),
                    new Duration(planNodeStats1.getWallTime().toMillis() + planNodeStats2.getWallTime().toMillis(), MILLISECONDS),
                    outputPositions,
                    outputDataSize);
        }
    }
}
