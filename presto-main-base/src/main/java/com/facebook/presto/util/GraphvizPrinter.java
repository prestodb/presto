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
package com.facebook.presto.util;

import com.facebook.presto.Session;
import com.facebook.presto.cost.PlanCostEstimate;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.cost.StatsAndCosts;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.plan.AbstractJoinNode;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.AggregationNode.Aggregation;
import com.facebook.presto.spi.plan.DeleteNode;
import com.facebook.presto.spi.plan.DistinctLimitNode;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.IndexSourceNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.MarkDistinctNode;
import com.facebook.presto.spi.plan.MergeJoinNode;
import com.facebook.presto.spi.plan.MetadataDeleteNode;
import com.facebook.presto.spi.plan.OutputNode;
import com.facebook.presto.spi.plan.PlanFragmentId;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.SemiJoinNode;
import com.facebook.presto.spi.plan.SortNode;
import com.facebook.presto.spi.plan.SpatialJoinNode;
import com.facebook.presto.spi.plan.TableFinishNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.TableWriterNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.plan.UnionNode;
import com.facebook.presto.spi.plan.UnnestNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.plan.WindowNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.SubPlan;
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
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.facebook.presto.sql.planner.plan.RowNumberNode;
import com.facebook.presto.sql.planner.plan.SampleNode;
import com.facebook.presto.sql.planner.plan.SequenceNode;
import com.facebook.presto.sql.planner.plan.StatisticsWriterNode;
import com.facebook.presto.sql.planner.plan.TableWriterMergeNode;
import com.facebook.presto.sql.planner.plan.TopNRowNumberNode;
import com.facebook.presto.sql.planner.plan.UpdateNode;
import com.facebook.presto.sql.planner.planPrinter.RowExpressionFormatter;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.facebook.presto.sql.analyzer.ExpressionTreeUtils.createSymbolReference;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static com.facebook.presto.sql.planner.planPrinter.PlanPrinter.getCteExecutionOrder;
import static com.facebook.presto.sql.planner.planPrinter.PlanPrinter.getDynamicFilterAssignments;
import static com.facebook.presto.sql.planner.planPrinter.TextRenderer.formatAsLong;
import static com.facebook.presto.sql.planner.planPrinter.TextRenderer.formatDouble;
import static com.facebook.presto.sql.planner.planPrinter.TextRenderer.formatEstimateAsDataSize;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Maps.immutableEnumMap;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class GraphvizPrinter
{
    private enum NodeType
    {
        EXCHANGE,
        AGGREGATE,
        FILTER,
        PROJECT,
        TOPN,
        OUTPUT,
        LIMIT,
        TABLESCAN,
        VALUES,
        JOIN,
        MERGE_JOIN,
        SINK,
        WINDOW,
        UNION,
        DELETE,

        SEQUENCE,
        SORT,
        SAMPLE,
        MARK_DISTINCT,
        TABLE_WRITER,
        TABLE_WRITER_MERGE,
        TABLE_FINISH,
        METADATA_DELETE,
        INDEX_SOURCE,
        UNNEST,
        ANALYZE_FINISH,
        EXPLAIN_ANALYZE,
        UPDATE,
        MERGE
    }

    private static final Map<NodeType, String> NODE_COLORS = immutableEnumMap(ImmutableMap.<NodeType, String>builder()
            .put(NodeType.EXCHANGE, "gold")
            .put(NodeType.AGGREGATE, "chartreuse3")
            .put(NodeType.FILTER, "yellow")
            .put(NodeType.PROJECT, "bisque")
            .put(NodeType.TOPN, "darksalmon")
            .put(NodeType.OUTPUT, "white")
            .put(NodeType.LIMIT, "gray83")
            .put(NodeType.TABLESCAN, "deepskyblue")
            .put(NodeType.VALUES, "deepskyblue")
            .put(NodeType.JOIN, "orange")
            .put(NodeType.MERGE_JOIN, "grey")
            .put(NodeType.SORT, "aliceblue")
            .put(NodeType.SINK, "indianred1")
            .put(NodeType.WINDOW, "darkolivegreen4")
            .put(NodeType.UNION, "turquoise4")
            .put(NodeType.DELETE, "tomato")
            .put(NodeType.SEQUENCE, "turquoise4")
            .put(NodeType.MARK_DISTINCT, "violet")
            .put(NodeType.TABLE_WRITER, "cyan")
            .put(NodeType.TABLE_WRITER_MERGE, "cyan4")
            .put(NodeType.TABLE_FINISH, "hotpink")
            .put(NodeType.METADATA_DELETE, "garnet")
            .put(NodeType.INDEX_SOURCE, "dodgerblue3")
            .put(NodeType.UNNEST, "crimson")
            .put(NodeType.SAMPLE, "goldenrod4")
            .put(NodeType.ANALYZE_FINISH, "plum")
            .put(NodeType.EXPLAIN_ANALYZE, "cadetblue1")
            .put(NodeType.UPDATE, "blue")
            .put(NodeType.MERGE, "lightblue")
            .build());

    static {
        Preconditions.checkState(NODE_COLORS.size() == NodeType.values().length);
    }

    private GraphvizPrinter() {}

    public static String printLogical(List<PlanFragment> fragments, FunctionAndTypeManager functionAndTypeManager, Session session)
    {
        Map<PlanFragmentId, PlanFragment> fragmentsById = Maps.uniqueIndex(fragments, PlanFragment::getId);
        PlanNodeIdGenerator idGenerator = new PlanNodeIdGenerator();

        StringBuilder output = new StringBuilder();
        output.append("digraph logical_plan {\n");

        for (PlanFragment fragment : fragments) {
            printFragmentNodes(output, fragment, idGenerator, functionAndTypeManager, session);
        }

        for (PlanFragment fragment : fragments) {
            fragment.getRoot().accept(new EdgePrinter(output, fragmentsById, idGenerator), null);
        }

        output.append("}\n");

        return output.toString();
    }

    public static String printDistributed(SubPlan plan, FunctionAndTypeManager functionAndTypeManager, Session session)
    {
        List<PlanFragment> fragments = plan.getAllFragments();
        Map<PlanFragmentId, PlanFragment> fragmentsById = Maps.uniqueIndex(fragments, PlanFragment::getId);
        PlanNodeIdGenerator idGenerator = new PlanNodeIdGenerator();

        StringBuilder output = new StringBuilder();
        output.append("digraph distributed_plan {\n");

        printSubPlan(plan, fragmentsById, idGenerator, output, functionAndTypeManager, session);

        output.append("}\n");

        return output.toString();
    }

    public static String printDistributedFromFragments(List<PlanFragment> allFragments, FunctionAndTypeManager functionAndTypeManager, Session session)
    {
        PlanNodeIdGenerator idGenerator = new PlanNodeIdGenerator();
        Map<PlanFragmentId, PlanFragment> fragmentsById = Maps.uniqueIndex(allFragments, PlanFragment::getId);

        StringBuilder output = new StringBuilder();
        output.append("digraph distributed_plan {\n");

        for (PlanFragment planFragment : allFragments) {
            printFragmentNodes(output, planFragment, idGenerator, functionAndTypeManager, session);
            planFragment.getRoot().accept(new EdgePrinter(output, fragmentsById, idGenerator), null);
        }

        output.append("}\n");
        return output.toString();
    }

    private static void printSubPlan(
            SubPlan plan,
            Map<PlanFragmentId, PlanFragment> fragmentsById,
            PlanNodeIdGenerator idGenerator,
            StringBuilder output,
            FunctionAndTypeManager functionAndTypeManager,
            Session session)
    {
        PlanFragment fragment = plan.getFragment();
        printFragmentNodes(output, fragment, idGenerator, functionAndTypeManager, session);
        fragment.getRoot().accept(new EdgePrinter(output, fragmentsById, idGenerator), null);

        for (SubPlan child : plan.getChildren()) {
            printSubPlan(child, fragmentsById, idGenerator, output, functionAndTypeManager, session);
        }
    }

    private static void printFragmentNodes(StringBuilder output, PlanFragment fragment, PlanNodeIdGenerator idGenerator, FunctionAndTypeManager functionAndTypeManager, Session session)
    {
        String clusterId = "cluster_" + fragment.getId();
        output.append("subgraph ")
                .append(clusterId)
                .append(" {")
                .append('\n');

        output.append(format("label = \"%s\"", fragment.getPartitioning()))
                .append('\n');

        PlanNode plan = fragment.getRoot();
        plan.accept(new NodePrinter(output, idGenerator, fragment.getStatsAndCosts().orElse(StatsAndCosts.empty()), functionAndTypeManager, session), null);

        output.append("}")
                .append('\n');
    }

    private static class NodePrinter
            extends InternalPlanVisitor<Void, Void>
    {
        private static final int MAX_NAME_WIDTH = 100;
        private final StringBuilder output;
        private final PlanNodeIdGenerator idGenerator;
        private final Function<RowExpression, String> formatter;
        StatsAndCosts estimatedStatsAndCosts;

        public NodePrinter(StringBuilder output, PlanNodeIdGenerator idGenerator, StatsAndCosts estimatedStatsAndCosts, FunctionAndTypeManager functionAndTypeManager, Session session)
        {
            this.output = output;
            this.idGenerator = idGenerator;
            RowExpressionFormatter rowExpressionFormatter = new RowExpressionFormatter(functionAndTypeManager);
            ConnectorSession connectorSession = requireNonNull(session, "session is null").toConnectorSession();
            this.formatter = rowExpression -> rowExpressionFormatter.formatRowExpression(connectorSession, rowExpression);
            this.estimatedStatsAndCosts = estimatedStatsAndCosts;
        }

        @Override
        public Void visitPlan(PlanNode node, Void context)
        {
            throw new UnsupportedOperationException(format("Node %s does not have a Graphviz visitor", node.getClass().getName()));
        }

        @Override
        public Void visitSequence(SequenceNode node, Void context)
        {
            String expression = getCteExecutionOrder(node);
            printNode(node, "Sequence", expression, NODE_COLORS.get(NodeType.SEQUENCE));
            for (PlanNode planNode : node.getSources()) {
                planNode.accept(this, context);
            }

            return null;
        }

        @Override
        public Void visitTableWriter(TableWriterNode node, Void context)
        {
            List<String> columns = new ArrayList<>();
            for (int i = 0; i < node.getColumnNames().size(); i++) {
                columns.add(node.getColumnNames().get(i) + " := " + node.getColumns().get(i));
            }
            printNode(node, format("TableWriter[%s]", Joiner.on(", ").join(columns)), NODE_COLORS.get(NodeType.TABLE_WRITER));
            return node.getSource().accept(this, context);
        }

        @Override
        public Void visitTableWriteMerge(TableWriterMergeNode node, Void context)
        {
            printNode(node, "TableWriterMerge", NODE_COLORS.get(NodeType.TABLE_WRITER_MERGE));
            return node.getSource().accept(this, context);
        }

        @Override
        public Void visitUpdate(UpdateNode node, Void context)
        {
            printNode(node, format("UpdateNode[%s]", Joiner.on(", ").join(node.getOutputVariables())), NODE_COLORS.get(NodeType.UPDATE));
            return node.getSource().accept(this, context);
        }

        @Override
        public Void visitMergeWriter(MergeWriterNode node, Void context)
        {
            printNode(node, format("MergeWriterNode[%s]", Joiner.on(", ").join(node.getOutputVariables())), NODE_COLORS.get(NodeType.MERGE));
            return node.getSource().accept(this, context);
        }

        @Override
        public Void visitMergeProcessor(MergeProcessorNode node, Void context)
        {
            printNode(node, format("MergeProcessorNode[%s]", Joiner.on(", ").join(node.getOutputVariables())), NODE_COLORS.get(NodeType.MERGE));
            return node.getSource().accept(this, context);
        }

        @Override
        public Void visitStatisticsWriterNode(StatisticsWriterNode node, Void context)
        {
            printNode(node, format("StatisticsWriterNode[%s]", Joiner.on(", ").join(node.getOutputVariables())), NODE_COLORS.get(NodeType.ANALYZE_FINISH));
            return node.getSource().accept(this, context);
        }

        @Override
        public Void visitTableFinish(TableFinishNode node, Void context)
        {
            printNode(node, format("TableFinish[%s]", Joiner.on(", ").join(node.getOutputVariables())), NODE_COLORS.get(NodeType.TABLE_FINISH));
            return node.getSource().accept(this, context);
        }

        @Override
        public Void visitMetadataDelete(MetadataDeleteNode node, Void context)
        {
            printNode(node, format("MetadataDeleteNode[%s]", Joiner.on(", ").join(node.getOutputVariables())), NODE_COLORS.get(NodeType.METADATA_DELETE));
            return null;
        }

        @Override
        public Void visitExplainAnalyze(ExplainAnalyzeNode node, Void context)
        {
            printNode(node, format("ExplainAnalyze[%s]", Joiner.on(", ").join(node.getOutputVariables())), NODE_COLORS.get(NodeType.EXPLAIN_ANALYZE));
            return node.getSource().accept(this, context);
        }

        @Override
        public Void visitSample(SampleNode node, Void context)
        {
            printNode(node, format("Sample[type=%s, ratio=%f]", node.getSampleType(), node.getSampleRatio()), NODE_COLORS.get(NodeType.SAMPLE));
            return node.getSource().accept(this, context);
        }

        @Override
        public Void visitSort(SortNode node, Void context)
        {
            printNode(node, format("Sort[%s]", Joiner.on(", ").join(node.getOrderingScheme().getOrderByVariables())), NODE_COLORS.get(NodeType.SORT));
            return node.getSource().accept(this, context);
        }

        @Override
        public Void visitMarkDistinct(MarkDistinctNode node, Void context)
        {
            printNode(node, format("MarkDistinct[%s]", node.getMarkerVariable()), format("%s => %s", node.getDistinctVariables(), node.getMarkerVariable()), NODE_COLORS.get(NodeType.MARK_DISTINCT));
            return node.getSource().accept(this, context);
        }

        @Override
        public Void visitWindow(WindowNode node, Void context)
        {
            printNode(node, "Window", format("partition by = %s|order by = %s",
                            Joiner.on(", ").join(node.getPartitionBy()),
                            node.getOrderingScheme()
                                    .map(orderingScheme -> Joiner.on(", ").join(orderingScheme.getOrderByVariables()))
                                    .orElse("")),
                    NODE_COLORS.get(NodeType.WINDOW));
            return node.getSource().accept(this, context);
        }

        @Override
        public Void visitRowNumber(RowNumberNode node, Void context)
        {
            printNode(node,
                    "RowNumber",
                    format("partition by = %s", Joiner.on(", ").join(node.getPartitionBy())),
                    NODE_COLORS.get(NodeType.WINDOW));
            return node.getSource().accept(this, context);
        }

        @Override
        public Void visitTopNRowNumber(TopNRowNumberNode node, Void context)
        {
            printNode(node,
                    "TopNRowNumber",
                    format("partition by = %s|order by = %s|n = %s",
                            Joiner.on(", ").join(node.getPartitionBy()),
                            Joiner.on(", ").join(node.getOrderingScheme().getOrderByVariables()), node.getMaxRowCountPerPartition()),
                    NODE_COLORS.get(NodeType.WINDOW));
            return node.getSource().accept(this, context);
        }

        @Override
        public Void visitUnion(UnionNode node, Void context)
        {
            printNode(node, "Union", NODE_COLORS.get(NodeType.UNION));

            for (PlanNode planNode : node.getSources()) {
                planNode.accept(this, context);
            }

            return null;
        }

        @Override
        public Void visitRemoteSource(RemoteSourceNode node, Void context)
        {
            printNode(node, (node.getOrderingScheme().isPresent() ? "Merge" : "Exchange") + " 1:N", NODE_COLORS.get(NodeType.EXCHANGE));
            return null;
        }

        @Override
        public Void visitExchange(ExchangeNode node, Void context)
        {
            String columns;
            if (node.getType() == REPARTITION) {
                columns = Joiner.on(", ").join(node.getPartitioningScheme().getPartitioning().getArguments());
            }
            else {
                columns = Joiner.on(", ").join(node.getOutputVariables());
            }
            printNode(node, format("ExchangeNode[%s]", node.getType()), columns, NODE_COLORS.get(NodeType.EXCHANGE));
            for (PlanNode planNode : node.getSources()) {
                planNode.accept(this, context);
            }
            return null;
        }

        @Override
        public Void visitAggregation(AggregationNode node, Void context)
        {
            StringBuilder builder = new StringBuilder();
            for (Map.Entry<VariableReferenceExpression, Aggregation> entry : node.getAggregations().entrySet()) {
                builder.append(format("%s := %s\\n", entry.getKey(), formatAggregation(entry.getValue())));
            }
            printNode(node, format("Aggregate[%s]", node.getStep()), builder.toString(), NODE_COLORS.get(NodeType.AGGREGATE));
            return node.getSource().accept(this, context);
        }

        private String formatAggregation(AggregationNode.Aggregation aggregation)
        {
            return String.format("%s(%s)%s%s%s",
                    aggregation.getCall().getDisplayName(),
                    Joiner.on(",").join(aggregation.getArguments().stream().map(RowExpression::toString).collect(toImmutableList())),
                    aggregation.getFilter().map(filter -> format(" WHERE %s", filter)).orElse(""),
                    aggregation.getOrderBy().map(orderingScheme -> format(" ORDER BY %s", orderingScheme)).orElse(""),
                    aggregation.getMask().map(mask -> format(" (mask = %s)", mask)).orElse(""));
        }

        @Override
        public Void visitGroupId(GroupIdNode node, Void context)
        {
            // grouping sets are easier to understand in terms of inputs
            List<String> inputGroupingSetSymbols = node.getGroupingSets().stream()
                    .map(set -> "(" + Joiner.on(", ").join(set.stream()
                            .map(symbol -> node.getGroupingColumns().get(symbol))
                            .collect(Collectors.toList())) + ")")
                    .collect(Collectors.toList());

            printNode(node, "GroupId", Joiner.on(", ").join(inputGroupingSetSymbols), NODE_COLORS.get(NodeType.AGGREGATE));
            return node.getSource().accept(this, context);
        }

        @Override
        public Void visitFilter(FilterNode node, Void context)
        {
            String expression = formatter.apply(node.getPredicate());
            printNode(node, "Filter", expression, NODE_COLORS.get(NodeType.FILTER));
            return node.getSource().accept(this, context);
        }

        @Override
        public Void visitProject(ProjectNode node, Void context)
        {
            StringBuilder builder = new StringBuilder();
            for (Map.Entry<VariableReferenceExpression, RowExpression> entry : node.getAssignments().entrySet()) {
                if ((entry.getValue() instanceof VariableReferenceExpression) &&
                        ((VariableReferenceExpression) entry.getValue()).getName().equals(entry.getKey().getName())) {
                    // skip identity assignments
                    continue;
                }
                builder.append(format("%s := %s\\n", entry.getKey(), formatter.apply(entry.getValue())));
            }

            printNode(node, "Project", builder.toString(), NODE_COLORS.get(NodeType.PROJECT));
            return node.getSource().accept(this, context);
        }

        @Override
        public Void visitUnnest(UnnestNode node, Void context)
        {
            if (!node.getOrdinalityVariable().isPresent()) {
                printNode(node, format("Unnest[%s]", node.getUnnestVariables().keySet()), NODE_COLORS.get(NodeType.UNNEST));
            }
            else {
                printNode(node, format("Unnest[%s (ordinality)]", node.getUnnestVariables().keySet()), NODE_COLORS.get(NodeType.UNNEST));
            }
            return node.getSource().accept(this, context);
        }

        @Override
        public Void visitTopN(final TopNNode node, Void context)
        {
            Iterable<String> keys = Iterables.transform(node.getOrderingScheme().getOrderByVariables(), input -> input + " " + node.getOrderingScheme().getOrdering(input));
            printNode(node, format("TopN[%s]", node.getCount()), Joiner.on(", ").join(keys), NODE_COLORS.get(NodeType.TOPN));
            return node.getSource().accept(this, context);
        }

        @Override
        public Void visitOutput(OutputNode node, Void context)
        {
            String columns = getColumns(node);
            printNode(node, format("Output[%s]", columns), NODE_COLORS.get(NodeType.OUTPUT));
            return node.getSource().accept(this, context);
        }

        @Override
        public Void visitDistinctLimit(DistinctLimitNode node, Void context)
        {
            printNode(node, format("DistinctLimit[%s]", node.getLimit()), NODE_COLORS.get(NodeType.LIMIT));
            return node.getSource().accept(this, context);
        }

        @Override
        public Void visitLimit(LimitNode node, Void context)
        {
            printNode(node, format("Limit[%s]", node.getCount()), NODE_COLORS.get(NodeType.LIMIT));
            return node.getSource().accept(this, context);
        }

        @Override
        public Void visitTableScan(TableScanNode node, Void context)
        {
            printNode(node, format("TableScan | [%s]", node.getTable()), NODE_COLORS.get(NodeType.TABLESCAN));
            return null;
        }

        @Override
        public Void visitValues(ValuesNode node, Void context)
        {
            if (node.getValuesNodeLabel().isPresent()) {
                printNode(node, format("Values converted from TableScan[%s]", node.getValuesNodeLabel().get()), NODE_COLORS.get(NodeType.TABLESCAN));
            }
            else {
                printNode(node, "Values", NODE_COLORS.get(NodeType.TABLESCAN));
            }
            return null;
        }

        @Override
        public Void visitEnforceSingleRow(EnforceSingleRowNode node, Void context)
        {
            printNode(node, "Scalar", NODE_COLORS.get(NodeType.PROJECT));
            return node.getSource().accept(this, context);
        }

        @Override
        public Void visitJoin(JoinNode node, Void context)
        {
            List<Expression> joinExpressions = new ArrayList<>();
            for (EquiJoinClause clause : node.getCriteria()) {
                joinExpressions.add(JoinNodeUtils.toExpression(clause));
            }
            String joinCriteria = Joiner.on(" AND ").join(joinExpressions);
            StringBuilder details = new StringBuilder(joinCriteria);
            if (!node.getDynamicFilters().isEmpty()) {
                details.append(", ");
                details.append(getDynamicFilterAssignments(node));
            }

            String distributionType = node.getDistributionType().isPresent() ? node.getDistributionType().get().toString() : "UNKNOWN";
            final String joinType = node.isCrossJoin() ? "CrossJoin" : node.getType().getJoinLabel();
            String label = format("%s[%s]", joinType, distributionType);

            printNode(node, label, details.toString(), NODE_COLORS.get(NodeType.JOIN));

            node.getLeft().accept(this, context);
            node.getRight().accept(this, context);

            return null;
        }

        @Override
        public Void visitSemiJoin(SemiJoinNode node, Void context)
        {
            String joinExpression = format("%s = %s", node.getSourceJoinVariable(), node.getFilteringSourceJoinVariable());
            StringBuilder details = new StringBuilder(joinExpression);
            if (!node.getDynamicFilters().isEmpty()) {
                details.append(", ");
                details.append(getDynamicFilterAssignments(node));
            }

            String label = format("SemiJoin[%s]", node.getDistributionType().isPresent() ? node.getDistributionType().get().toString() : "UNKNOWN");

            printNode(node, label, details.toString(), NODE_COLORS.get(NodeType.JOIN));

            node.getSource().accept(this, context);
            node.getFilteringSource().accept(this, context);

            return null;
        }

        @Override
        public Void visitSpatialJoin(SpatialJoinNode node, Void context)
        {
            printNode(node, node.getType().getJoinLabel(), formatter.apply(node.getFilter()), NODE_COLORS.get(NodeType.JOIN));

            node.getLeft().accept(this, context);
            node.getRight().accept(this, context);

            return null;
        }

        @Override
        public Void visitMergeJoin(MergeJoinNode node, Void context)
        {
            printNode(node, "MergeJoin", NODE_COLORS.get(NodeType.MERGE_JOIN));

            node.getLeft().accept(this, context);
            node.getRight().accept(this, context);

            return null;
        }

        @Override
        public Void visitApply(ApplyNode node, Void context)
        {
            String parameters = Joiner.on(",").join(node.getCorrelation());
            printNode(node, "Apply", parameters, NODE_COLORS.get(NodeType.JOIN));

            node.getInput().accept(this, context);
            node.getSubquery().accept(this, context);

            return null;
        }

        @Override
        public Void visitAssignUniqueId(AssignUniqueId node, Void context)
        {
            printNode(node, "AssignUniqueId", NODE_COLORS.get(NodeType.PROJECT));
            node.getSource().accept(this, context);

            return null;
        }

        @Override
        public Void visitLateralJoin(LateralJoinNode node, Void context)
        {
            String parameters = Joiner.on(",").join(node.getCorrelation());
            printNode(node, "LateralJoin", parameters, NODE_COLORS.get(NodeType.JOIN));

            node.getInput().accept(this, context);
            node.getSubquery().accept(this, context);

            return null;
        }

        @Override
        public Void visitIndexSource(IndexSourceNode node, Void context)
        {
            printNode(node, format("IndexSource[%s]", node.getIndexHandle()), NODE_COLORS.get(NodeType.INDEX_SOURCE));
            return null;
        }

        @Override
        public Void visitIndexJoin(IndexJoinNode node, Void context)
        {
            List<Expression> joinExpressions = new ArrayList<>();
            for (IndexJoinNode.EquiJoinClause clause : node.getCriteria()) {
                joinExpressions.add(new ComparisonExpression(ComparisonExpression.Operator.EQUAL,
                        createSymbolReference(clause.getProbe()),
                        createSymbolReference(clause.getIndex())));
            }

            String criteria = Joiner.on(" AND ").join(joinExpressions);
            String joinLabel = format("%sIndexJoin", node.getType().getJoinLabel());
            printNode(node, joinLabel, criteria, NODE_COLORS.get(NodeType.JOIN));

            node.getProbeSource().accept(this, context);
            node.getIndexSource().accept(this, context);

            return null;
        }

        @Override
        public Void visitDelete(DeleteNode node, Void context)
        {
            String deleteLabel = format("Delete[%s]", Joiner.on(",").join(node.getOutputVariables()));
            printNode(node, deleteLabel, NODE_COLORS.get(NodeType.DELETE));

            node.getSource().accept(this, context);
            return null;
        }

        private void printNode(PlanNode node, String label, String color)
        {
            String nodeEstimate = addStatsEstimate(node);
            String nodeId = idGenerator.getNodeId(node);
            label = escapeSpecialCharacters(label);
            nodeEstimate = escapeSpecialCharacters(nodeEstimate);
            output.append(nodeId)
                    .append(format("[label=\"{%s|%s}\", style=\"rounded, filled\", shape=record, fillcolor=%s]", label, nodeEstimate, color))
                    .append(';')
                    .append('\n');
        }

        private void printNode(PlanNode node, String label, String details, String color)
        {
            if (details.isEmpty()) {
                printNode(node, label, color);
            }
            else {
                String nodeEstimate = addStatsEstimate(node);
                String nodeId = idGenerator.getNodeId(node);
                label = escapeSpecialCharacters(label);
                details = escapeSpecialCharacters(details);
                nodeEstimate = escapeSpecialCharacters(nodeEstimate);
                output.append(nodeId)
                        .append(format("[label=\"{%s|%s|%s}\", style=\"rounded, filled\", shape=record, fillcolor=%s]", label, details, nodeEstimate, color))
                        .append(';')
                        .append('\n');
            }
        }

        private String addStatsEstimate(PlanNode node)
        {
            PlanNodeStatsEstimate stats = estimatedStatsAndCosts.getStats().getOrDefault(node.getId(), PlanNodeStatsEstimate.unknown());
            PlanCostEstimate cost = estimatedStatsAndCosts.getCosts().getOrDefault(node.getId(), PlanCostEstimate.unknown());
            StringBuilder output = new StringBuilder();
            output.append("Estimates: ");
            output.append(format("{rows: %s (%s), cpu: %s, memory: %s, network: %s}",
                    formatAsLong(stats.getOutputRowCount()),
                    formatEstimateAsDataSize(stats.getOutputSizeInBytes(node)),
                    formatDouble(cost.getCpuCost()),
                    formatDouble(cost.getMaxMemory()),
                    formatDouble(cost.getNetworkCost())));
            output.append("\n");
            return output.toString();
        }

        private static String getColumns(OutputNode node)
        {
            Iterator<String> columnNames = node.getColumnNames().iterator();
            String columns = "";
            int nameWidth = 0;
            while (columnNames.hasNext()) {
                String columnName = columnNames.next();
                columns += columnName;
                nameWidth += columnName.length();
                if (columnNames.hasNext()) {
                    columns += ", ";
                }
                if (nameWidth >= MAX_NAME_WIDTH) {
                    columns += "\\n";
                    nameWidth = 0;
                }
            }
            return columns;
        }

        /**
         * Escape characters that are special to graphviz.
         */
        private static String escapeSpecialCharacters(String label)
        {
            return label
                    .replace("<", "\\<")
                    .replace(">", "\\>")
                    .replace("\"", "\\\"")
                    .replace("{", "\\{")
                    .replace("}", "\\}");
        }
    }

    private static class EdgePrinter
            extends InternalPlanVisitor<Void, Void>
    {
        private final StringBuilder output;
        private final Map<PlanFragmentId, PlanFragment> fragmentsById;
        private final PlanNodeIdGenerator idGenerator;

        public EdgePrinter(StringBuilder output, Map<PlanFragmentId, PlanFragment> fragmentsById, PlanNodeIdGenerator idGenerator)
        {
            this.output = output;
            this.fragmentsById = ImmutableMap.copyOf(fragmentsById);
            this.idGenerator = idGenerator;
        }

        @Override
        public Void visitSemiJoin(SemiJoinNode node, Void context)
        {
            return visitBuildAndProbe(node, context, node.getBuild(), node.getProbe());
        }

        @Override
        public Void visitJoin(JoinNode node, Void context)
        {
            return visitBuildAndProbe(node, context, node.getBuild(), node.getProbe());
        }

        private Void visitBuildAndProbe(AbstractJoinNode node, Void context, PlanNode build, PlanNode probe)
        {
            printEdge(node, build, "Build");
            build.accept(this, context);

            printEdge(node, probe, "Probe");
            probe.accept(this, context);

            return null;
        }

        @Override
        public Void visitPlan(PlanNode node, Void context)
        {
            for (PlanNode child : node.getSources()) {
                printEdge(node, child);

                child.accept(this, context);
            }

            return null;
        }

        @Override
        public Void visitRemoteSource(RemoteSourceNode node, Void context)
        {
            for (PlanFragmentId planFragmentId : node.getSourceFragmentIds()) {
                PlanFragment target = fragmentsById.get(planFragmentId);
                printEdge(node, target.getRoot());
            }

            return null;
        }

        private void printEdge(PlanNode from, PlanNode to)
        {
            printEdge(from, to, "");
        }

        private void printEdge(PlanNode from, PlanNode to, String label)
        {
            String fromId = idGenerator.getNodeId(from);
            String toId = idGenerator.getNodeId(to);

            output.append(fromId)
                    .append(" -> ")
                    .append(toId)
                    .append(label.isEmpty() ? "" : String.format(" [label = \"%s\"]", label))
                    .append(';')
                    .append('\n');
        }
    }

    private static class PlanNodeIdGenerator
    {
        private final Map<PlanNode, Integer> planNodeIds;
        private int idCount;

        public PlanNodeIdGenerator()
        {
            planNodeIds = new HashMap<>();
        }

        public String getNodeId(PlanNode from)
        {
            int nodeId;

            if (planNodeIds.containsKey(from)) {
                nodeId = planNodeIds.get(from);
            }
            else {
                idCount++;
                planNodeIds.put(from, idCount);
                nodeId = idCount;
            }
            return ("plannode_" + nodeId);
        }
    }
}
