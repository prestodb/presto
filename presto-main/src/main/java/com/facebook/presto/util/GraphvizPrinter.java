package com.facebook.presto.util;

import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.TopNNode;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.google.common.base.Function;
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

import static com.google.common.collect.Maps.immutableEnumMap;
import static java.lang.String.format;
import static java.lang.System.identityHashCode;

public final class GraphvizPrinter
{
    enum NodeType {
        EXCHANGE,
        AGGREGATE,
        FILTER,
        PROJECT,
        TOPN,
        OUTPUT,
        LIMIT,
        TABLESCAN,
        JOIN
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
            .put(NodeType.JOIN, "orange")
            .build());

    static {
        Preconditions.checkState(NODE_COLORS.size() == NodeType.values().length);
    }

    public static String print(List<PlanFragment> fragments)
    {
        Map<PlanFragmentId, PlanFragment> fragmentsById = Maps.uniqueIndex(fragments, new Function<PlanFragment, PlanFragmentId>()
        {
            @Override
            public PlanFragmentId apply(PlanFragment input)
            {
                return input.getId();
            }
        });
        PlanNodeIdGenerator idGenerator = new PlanNodeIdGenerator();

        StringBuilder output = new StringBuilder();
        output.append("digraph Plan {")
                .append('\n');

        for (PlanFragment fragment : fragments) {
            printFragmentNodes(output, fragment, idGenerator);
        }

        for (PlanFragment fragment : fragments) {
            fragment.getRoot().accept(new EdgePrinter(output, fragmentsById, idGenerator), null);
        }

        output.append("}")
                .append('\n');

        return output.toString();
    }

    private static void printFragmentNodes(StringBuilder output, PlanFragment fragment, PlanNodeIdGenerator idGenerator)
    {
        String clusterId = "cluster_" + fragment.getId();
        output.append("subgraph ")
                .append(clusterId)
                .append(" {")
                .append('\n');

        output.append(format("label = \"%s\"", fragment.isPartitioned() ? "Partitioned" : "Unpartitioned"))
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
        private PlanNodeIdGenerator idGenerator;

        public NodePrinter(StringBuilder output, PlanNodeIdGenerator idGenerator)
        {
            this.output = output;
            this.idGenerator = idGenerator;
        }

        @Override
        protected Void visitPlan(PlanNode node, Void context)
        {
            output.append(format("/* plannode_%s: %s*/\n", identityHashCode(node), node.getClass().getName()));
            return null;
        }

        @Override
        public Void visitExchange(ExchangeNode node, Void context)
        {
            printNode(node, "Exchange 1:N", NODE_COLORS.get(NodeType.EXCHANGE));
            return null;
        }

        @Override
        public Void visitAggregation(AggregationNode node, Void context)
        {
            StringBuilder builder = new StringBuilder();
            for (Map.Entry<Symbol, FunctionCall> entry : node.getAggregations().entrySet()) {
                builder.append(format("%s := %s\\n", entry.getKey(), entry.getValue()));
            }
            printNode(node, format("Aggregate[%s]", node.getStep()), builder.toString(), NODE_COLORS.get(NodeType.AGGREGATE));
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
            for (Map.Entry<Symbol, Expression> entry : node.getOutputMap().entrySet()) {
                if ((entry.getValue() instanceof QualifiedNameReference) &&
                        ((QualifiedNameReference) entry.getValue()).getName().equals(entry.getKey().toQualifiedName())) {
                    // skip identity assignments
                    continue;
                }
                builder.append(format("%s := %s\\n", entry.getKey(), entry.getValue()));
            }

            printNode(node, "Project", builder.toString(), NODE_COLORS.get(NodeType.PROJECT));
            return node.getSource().accept(this, context);
        }

        @Override
        public Void visitTopN(final TopNNode node, Void context)
        {
            Iterable<String> keys = Iterables.transform(node.getOrderBy(), new Function<Symbol, String>()
            {
                @Override
                public String apply(Symbol input)
                {
                    return input + " " + node.getOrderings().get(input);
                }
            });
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
        public Void visitLimit(LimitNode node, Void context)
        {
            printNode(node, format("Limit[%s]", node.getCount()), NODE_COLORS.get(NodeType.LIMIT));
            return node.getSource().accept(this, context);
        }

        @Override
        public Void visitTableScan(TableScanNode node, Void context)
        {
            printNode(node, format("TableScan[%s]", node.getTable()), format("partition predicate=%s", node.getPartitionPredicate()), NODE_COLORS.get(NodeType.TABLESCAN));
            return null;
        }

        @Override
        public Void visitJoin(JoinNode node, Void context)
        {
            List<Expression> joinExpressions = new ArrayList<>();
            for (JoinNode.EquiJoinClause clause : node.getCriteria()) {
                joinExpressions.add(new ComparisonExpression(ComparisonExpression.Type.EQUAL,
                        new QualifiedNameReference(clause.getLeft().toQualifiedName()),
                        new QualifiedNameReference(clause.getRight().toQualifiedName())));
            }

            String criteria = Joiner.on(" AND ").join(joinExpressions);
            printNode(node, node.getType().getJoinLabel(), criteria, NODE_COLORS.get(NodeType.JOIN));

            node.getLeft().accept(this, context);
            node.getRight().accept(this, context);

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
            if (details.length() == 0) {
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

        private String getColumns(OutputNode node)
        {
            Iterator<String> columnNames = node.getColumnNames().iterator();
            String columns = "";
            String columnName = "";
            int nameWidth = 0;
            while(columnNames.hasNext()) {
                columnName = columnNames.next();
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
        public Void visitExchange(ExchangeNode node, Void context)
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
        private int idCount = 0;

        public PlanNodeIdGenerator()
        {
            planNodeIds = new HashMap<PlanNode, Integer>();
        }

        public String getNodeId (PlanNode from)
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
