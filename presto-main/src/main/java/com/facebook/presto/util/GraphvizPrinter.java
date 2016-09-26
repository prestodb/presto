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

import com.facebook.presto.sql.planner.Partitioning.ArgumentBinding;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.SubPlan;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.AssignUniqueId;
import com.facebook.presto.sql.planner.plan.DistinctLimitNode;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.GroupIdNode;
import com.facebook.presto.sql.planner.plan.IndexJoinNode;
import com.facebook.presto.sql.planner.plan.IndexSourceNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.MarkDistinctNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.PlanNode;
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
import java.util.stream.Collectors;

import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.collect.Maps.immutableEnumMap;
import static java.lang.String.format;

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
        SINK,
        WINDOW,
        UNION,
        SORT,
        SAMPLE,
        MARK_DISTINCT,
        TABLE_WRITER,
        TABLE_FINISH,
        INDEX_SOURCE,
        UNNEST
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
            .put(NodeType.SORT, "aliceblue")
            .put(NodeType.SINK, "indianred1")
            .put(NodeType.WINDOW, "darkolivegreen4")
            .put(NodeType.UNION, "turquoise4")
            .put(NodeType.MARK_DISTINCT, "violet")
            .put(NodeType.TABLE_WRITER, "cyan")
            .put(NodeType.TABLE_FINISH, "hotpink")
            .put(NodeType.INDEX_SOURCE, "dodgerblue3")
            .put(NodeType.UNNEST, "crimson")
            .put(NodeType.SAMPLE, "goldenrod4")
            .build());

    static {
        Preconditions.checkState(NODE_COLORS.size() == NodeType.values().length);
    }

    private GraphvizPrinter() {}

    public static String printLogical(List<PlanFragment> fragments)
    {
        Map<PlanFragmentId, PlanFragment> fragmentsById = Maps.uniqueIndex(fragments, PlanFragment::getId);
        PlanNodeIdGenerator idGenerator = new PlanNodeIdGenerator();

        StringBuilder output = new StringBuilder();
        output.append("digraph logical_plan {\n");

        for (PlanFragment fragment : fragments) {
            printFragmentNodes(output, fragment, idGenerator);
        }

        for (PlanFragment fragment : fragments) {
            fragment.getRoot().accept(new EdgePrinter(output, fragmentsById, idGenerator), null);
        }

        output.append("}\n");

        return output.toString();
    }

    public static String printDistributed(SubPlan plan)
    {
        List<PlanFragment> fragments = plan.getAllFragments();
        Map<PlanFragmentId, PlanFragment> fragmentsById = Maps.uniqueIndex(fragments, PlanFragment::getId);
        PlanNodeIdGenerator idGenerator = new PlanNodeIdGenerator();

        StringBuilder output = new StringBuilder();
        output.append("digraph distributed_plan {\n");

        printSubPlan(plan, fragmentsById, idGenerator, output);

        output.append("}\n");

        return output.toString();
    }

    private static void printSubPlan(SubPlan plan, Map<PlanFragmentId, PlanFragment> fragmentsById, PlanNodeIdGenerator idGenerator, StringBuilder output)
    {
        PlanFragment fragment = plan.getFragment();
        printFragmentNodes(output, fragment, idGenerator);
        fragment.getRoot().accept(new EdgePrinter(output, fragmentsById, idGenerator), null);

        for (SubPlan child : plan.getChildren()) {
            printSubPlan(child, fragmentsById, idGenerator, output);
        }
    }

    private static void printFragmentNodes(StringBuilder output, PlanFragment fragment, PlanNodeIdGenerator idGenerator)
    {
        String clusterId = "cluster_" + fragment.getId();
        output.append("subgraph ")
                .append(clusterId)
                .append(" {")
                .append('\n');

        output.append(format("label = \"%s\"", fragment.getPartitioning()))
                .append('\n');

        PlanNode plan = fragment.getRoot();
        plan.accept(new NodePrinter(output, idGenerator), null);

        output.append("}")
                .append('\n');
    }

    private static class NodePrinter
            extends PlanVisitor<Void, Void>
    {
        private static final int MAX_NAME_WIDTH = 100;
        private final StringBuilder output;
        private final PlanNodeIdGenerator idGenerator;

        public NodePrinter(StringBuilder output, PlanNodeIdGenerator idGenerator)
        {
            this.output = output;
            this.idGenerator = idGenerator;
        }

        @Override
        protected Void visitPlan(PlanNode node, Void context)
        {
            throw new UnsupportedOperationException(format("Node %s does not have a Graphviz visitor", node.getClass().getName()));
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
        public Void visitTableFinish(TableFinishNode node, Void context)
        {
            printNode(node, format("TableFinish[%s]", Joiner.on(", ").join(node.getOutputSymbols())), NODE_COLORS.get(NodeType.TABLE_FINISH));
            return node.getSource().accept(this, context);
        }

        @Override
        public Void visitSample(SampleNode node, Void context)
        {
            printNode(node, format("Sample[type=%s, ratio=%f, rescaled=%s]", node.getSampleType(), node.getSampleRatio(), node.isRescaled()), NODE_COLORS.get(NodeType.SAMPLE));
            return node.getSource().accept(this, context);
        }

        @Override
        public Void visitSort(SortNode node, Void context)
        {
            printNode(node, format("Sort[%s]", Joiner.on(", ").join(node.getOrderBy())), NODE_COLORS.get(NodeType.SORT));
            return node.getSource().accept(this, context);
        }

        @Override
        public Void visitMarkDistinct(MarkDistinctNode node, Void context)
        {
            printNode(node, format("MarkDistinct[%s]", node.getMarkerSymbol()), format("%s => %s", node.getDistinctSymbols(), node.getMarkerSymbol()), NODE_COLORS.get(NodeType.MARK_DISTINCT));
            return node.getSource().accept(this, context);
        }

        @Override
        public Void visitWindow(WindowNode node, Void context)
        {
            printNode(node, "Window", format("partition by = %s|order by = %s", Joiner.on(", ").join(node.getPartitionBy()), Joiner.on(", ").join(node.getOrderBy())), NODE_COLORS.get(NodeType.WINDOW));
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
                    format("partition by = %s|order by = %s|n = %s", Joiner.on(", ").join(node.getPartitionBy()), Joiner.on(", ").join(node.getOrderBy()), node.getMaxRowCountPerPartition()),
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
            printNode(node, "Exchange 1:N", NODE_COLORS.get(NodeType.EXCHANGE));
            return null;
        }

        @Override
        public Void visitExchange(ExchangeNode node, Void context)
        {
            List<ArgumentBinding> symbols = node.getOutputSymbols().stream()
                    .map(ArgumentBinding::columnBinding)
                    .collect(toImmutableList());
            if (node.getType() == REPARTITION) {
                symbols = node.getPartitioningScheme().getPartitioning().getArguments();
            }
            String columns = Joiner.on(", ").join(symbols);
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
            for (Map.Entry<Symbol, FunctionCall> entry : node.getAggregations().entrySet()) {
                if (node.getMasks().containsKey(entry.getKey())) {
                    builder.append(format("%s := %s (mask = %s)\\n", entry.getKey(), entry.getValue(), node.getMasks().get(entry.getKey())));
                }
                else {
                    builder.append(format("%s := %s\\n", entry.getKey(), entry.getValue()));
                }
            }
            printNode(node, format("Aggregate[%s]", node.getStep()), builder.toString(), NODE_COLORS.get(NodeType.AGGREGATE));
            return node.getSource().accept(this, context);
        }

        @Override
        public Void visitGroupId(GroupIdNode node, Void context)
        {
            List<String> groupingSets = node.getGroupingSets().stream()
                    .map(groupingSet -> "(" + Joiner.on(", ").join(groupingSet) + ")")
                    .collect(Collectors.toList());

            printNode(node, "GroupId", Joiner.on(", ").join(groupingSets), NODE_COLORS.get(NodeType.AGGREGATE));
            return node.getSource().accept(this, context);
        }

        @Override
        public Void visitFilter(FilterNode node, Void context)
        {
            String expression = node.getPredicate().toString();
            printNode(node, "Filter", expression, NODE_COLORS.get(NodeType.FILTER));
            return node.getSource().accept(this, context);
        }

        @Override
        public Void visitProject(ProjectNode node, Void context)
        {
            StringBuilder builder = new StringBuilder();
            for (Map.Entry<Symbol, Expression> entry : node.getAssignments().entrySet()) {
                if ((entry.getValue() instanceof SymbolReference) &&
                        ((SymbolReference) entry.getValue()).getName().equals(entry.getKey().getName())) {
                    // skip identity assignments
                    continue;
                }
                builder.append(format("%s := %s\\n", entry.getKey(), entry.getValue()));
            }

            printNode(node, "Project", builder.toString(), NODE_COLORS.get(NodeType.PROJECT));
            return node.getSource().accept(this, context);
        }

        @Override
        public Void visitUnnest(UnnestNode node, Void context)
        {
            if (node.getOrdinalitySymbol() == null) {
                printNode(node, format("Unnest[%s]", node.getUnnestSymbols().keySet()), NODE_COLORS.get(NodeType.UNNEST));
            }
            else {
                printNode(node, format("Unnest[%s (ordinality)]", node.getUnnestSymbols().keySet()), NODE_COLORS.get(NodeType.UNNEST));
            }
            return node.getSource().accept(this, context);
        }

        @Override
        public Void visitTopN(final TopNNode node, Void context)
        {
            Iterable<String> keys = Iterables.transform(node.getOrderBy(), input -> input + " " + node.getOrderings().get(input));
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
            printNode(node, format("TableScan[%s]", node.getTable()), format("original constraint=%s", node.getOriginalConstraint()), NODE_COLORS.get(NodeType.TABLESCAN));
            return null;
        }

        @Override
        public Void visitValues(ValuesNode node, Void context)
        {
            printNode(node, "Values", NODE_COLORS.get(NodeType.TABLESCAN));
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
            for (JoinNode.EquiJoinClause clause : node.getCriteria()) {
                joinExpressions.add(new ComparisonExpression(ComparisonExpression.Type.EQUAL,
                        clause.getLeft().toSymbolReference(),
                        clause.getRight().toSymbolReference()));
            }

            String criteria = Joiner.on(" AND ").join(joinExpressions);
            printNode(node, node.getType().getJoinLabel(), criteria, NODE_COLORS.get(NodeType.JOIN));

            node.getLeft().accept(this, context);
            node.getRight().accept(this, context);

            return null;
        }

        @Override
        public Void visitSemiJoin(SemiJoinNode node, Void context)
        {
            printNode(node, "SemiJoin", format("%s = %s", node.getSourceJoinSymbol(), node.getFilteringSourceJoinSymbol()), NODE_COLORS.get(NodeType.JOIN));

            node.getSource().accept(this, context);
            node.getFilteringSource().accept(this, context);

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
                joinExpressions.add(new ComparisonExpression(ComparisonExpression.Type.EQUAL,
                        clause.getProbe().toSymbolReference(),
                        clause.getIndex().toSymbolReference()));
            }

            String criteria = Joiner.on(" AND ").join(joinExpressions);
            String joinLabel = format("%sIndexJoin", node.getType().getJoinLabel());
            printNode(node, joinLabel, criteria, NODE_COLORS.get(NodeType.JOIN));

            node.getProbeSource().accept(this, context);
            node.getIndexSource().accept(this, context);

            return null;
        }

        private void printNode(PlanNode node, String label, String color)
        {
            String nodeId = idGenerator.getNodeId(node);
            label = escapeSpecialCharacters(label);
            output.append(nodeId)
                    .append(format("[label=\"{%s}\", style=\"rounded, filled\", shape=record, fillcolor=%s]", label, color))
                    .append(';')
                    .append('\n');
        }

        private void printNode(PlanNode node, String label, String details, String color)
        {
            if (details.isEmpty()) {
                printNode(node, label, color);
            }
            else {
                String nodeId = idGenerator.getNodeId(node);
                label = escapeSpecialCharacters(label);
                details = escapeSpecialCharacters(details);
                output.append(nodeId)
                        .append(format("[label=\"{%s|%s}\", style=\"rounded, filled\", shape=record, fillcolor=%s]", label, details, color))
                        .append(';')
                        .append('\n');
            }
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
                    .replace("\"", "\\\"");
        }
    }

    private static class EdgePrinter
            extends PlanVisitor<Void, Void>
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
        protected Void visitPlan(PlanNode node, Void context)
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
            String fromId = idGenerator.getNodeId(from);
            String toId = idGenerator.getNodeId(to);

            output.append(fromId)
                    .append(" -> ")
                    .append(toId)
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
