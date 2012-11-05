package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.ExpressionFormatter;
import com.facebook.presto.sql.compiler.Slot;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;

import java.util.List;
import java.util.Map;

public class PlanPrinter
{
    public void print(PlanNode plan)
    {
        Visitor visitor = new Visitor();
        plan.accept(visitor, 0);
    }

    private void print(int indent, String format, Object... args)
    {
        String value;

        if (args.length == 0) {
            value = format;
        }
        else {
            value = String.format(format, args);
        }

        System.out.println(Strings.repeat("    ", indent) + value);
    }

    private String formatOutputs(List<Slot> slots)
    {
        return Joiner.on(", ").join(Iterables.transform(slots, new Function<Slot, String>()
        {
            @Override
            public String apply(Slot input)
            {
                return input.getName() + ":" + input.getType();
            }
        }));
    }

    private class Visitor
            extends PlanVisitor<Integer, Void>
    {
        @Override
        public Void visitAggregation(AggregationNode node, Integer indent)
        {
            print(indent, "- Aggregate => [%s]", formatOutputs(node.getOutputs()));

            if (!node.getGroupBy().isEmpty()) {
                print(indent + 2, "key = %s", Joiner.on(", ").join(node.getGroupBy()));
            }

            for (Map.Entry<Slot, FunctionCall> entry : node.getAggregations().entrySet()) {
                print(indent + 2, "%s := %s", entry.getKey(), ExpressionFormatter.toString(entry.getValue()));

            }

            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitAlign(AlignNode node, Integer indent)
        {
            print(indent, "- Align => [%s]", formatOutputs(node.getOutputs()));

            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitColumnScan(ColumnScan node, Integer indent)
        {
            print(indent, "- ColumnScan => [%s]", formatOutputs(node.getOutputs()));
            print(indent + 2, "%s := %s.%s.%s.%s", Iterables.getOnlyElement(node.getOutputs()), node.getCatalogName(), node.getSchemaName(), node.getTableName(), node.getAttributeName());

            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitFilter(FilterNode node, Integer indent)
        {
            print(indent, "- Filter => [%s]", formatOutputs(node.getOutputs()));
            print(indent + 2, "predicate = %s", ExpressionFormatter.toString(node.getPredicate()));

            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitProject(ProjectNode node, Integer indent)
        {
            print(indent, "- Project => [%s]", formatOutputs(node.getOutputs()));
            for (Map.Entry<Slot, Expression> entry : node.getOutputMap().entrySet()) {
                print(indent + 2, "%s := %s", entry.getKey(), ExpressionFormatter.toString(entry.getValue()));
            }

            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitOutput(OutputPlan node, Integer indent)
        {
            print(indent, "- Output[%s]", Joiner.on(", ").join(node.getColumnNames()));
            for (int i = 0; i < node.getColumnNames().size(); i++) {
                print(indent + 2, "%s := %s", node.getColumnNames().get(i), node.getOutputs().get(i));
            }

            return processChildren(node, indent + 1);
        }

        @Override
        protected Void visitPlan(PlanNode node, Integer context)
        {
            throw new UnsupportedOperationException("not yet implemented");
        }

        private Void processChildren(PlanNode node, int indent)
        {
            for (PlanNode child : node.getSources()) {
                child.accept(this, indent);
            }

            return null;
        }
    }
}
