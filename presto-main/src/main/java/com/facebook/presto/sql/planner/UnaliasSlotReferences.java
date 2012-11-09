package com.facebook.presto.sql.planner;

import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.sql.compiler.Slot;
import com.facebook.presto.sql.compiler.SlotReference;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NodeRewriter;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.TreeRewriter;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Re-maps slot references that are just aliases of each other (e.g., due to projects like $0 := $1)
 *
 * E.g.,
 *
 * Output[$0, $1] -> Project[$0 := $2, $1 := $3 * 100] -> Aggregate[$2, $3 := sum($4)] -> ...
 *
 * gets rewritten as
 *
 * Output[$2, $1] -> Project[$2, $1 := $3 * 100] -> Aggregate[$2, $3 := sum($4)] -> ...
 */
public class UnaliasSlotReferences
        extends PlanOptimizer
{
    @Override
    public PlanNode optimize(PlanNode plan)
    {
        Visitor visitor = new Visitor(new HashMap<Slot, Slot>());
        return plan.accept(visitor, null);
    }

    private static class Visitor
            extends PlanVisitor<Void, PlanNode>
    {
        private final Map<Slot, Slot> mapping;

        public Visitor(Map<Slot, Slot> mapping)
        {
            this.mapping = mapping;
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, Void context)
        {
            PlanNode source = node.getSource().accept(this, context);

            ImmutableMap.Builder<Slot, FunctionInfo> functionInfos = ImmutableMap.builder();
            ImmutableMap.Builder<Slot, FunctionCall> functionCalls = ImmutableMap.builder();
            for (Map.Entry<Slot, FunctionCall> entry : node.getAggregations().entrySet()) {
                Slot slot = entry.getKey();
                Slot canonical = canonicalize(slot);
                functionCalls.put(canonical, (FunctionCall) canonicalize(entry.getValue()));
                functionInfos.put(canonical, node.getFunctionInfos().get(slot));
            }

            return new AggregationNode(source, canonicalize(node.getGroupBy()), functionCalls.build(), functionInfos.build());
        }

        @Override
        public PlanNode visitTableScan(TableScan node, Void context)
        {
            return new TableScan(node.getCatalogName(), node.getSchemaName(), node.getTableName(), Maps.transformValues(node.getAttributes(), canonicalizeFunction()));
        }

        @Override
        public PlanNode visitFilter(FilterNode node, Void context)
        {
            PlanNode source = node.getSource().accept(this, context);

            return new FilterNode(source, canonicalize(node.getPredicate()), canonicalize(node.getOutputs()));
        }

        @Override
        public PlanNode visitProject(ProjectNode node, Void context)
        {
            PlanNode source = node.getSource().accept(this, context);

            Map<Slot, Expression> assignments = new HashMap<>();
            for (Map.Entry<Slot, Expression> entry : node.getOutputMap().entrySet()) {
                Expression expression = canonicalize(entry.getValue());

                if (entry.getValue() instanceof SlotReference) {
                    Slot slot = ((SlotReference) entry.getValue()).getSlot();
                    if (!slot.equals(entry.getKey())) {
                        map(entry.getKey(), slot);
                    }
                }

                Slot canonical = canonicalize(entry.getKey());

                if (!assignments.containsKey(canonical)) {
                    assignments.put(canonical, expression);
                }
            }

            return new ProjectNode(source, assignments);
        }

        @Override
        public PlanNode visitOutput(OutputPlan node, Void context)
        {
            PlanNode source = node.getSource().accept(this, context);

            Map<String, Slot> canonicalized = Maps.transformValues(node.getAssignments(), canonicalizeFunction());
            return new OutputPlan(source, node.getColumnNames(), canonicalized);
        }

        @Override
        public PlanNode visitLimit(LimitNode node, Void context)
        {
            PlanNode source = node.getSource().accept(this, context);
            return new LimitNode(source, node.getCount());
        }

        @Override
        public PlanNode visitTopN(TopNNode node, Void context)
        {
            PlanNode source = node.getSource().accept(this, context);

            ImmutableList.Builder<Slot> slots = ImmutableList.<Slot>builder();
            ImmutableMap.Builder<Slot, SortItem.Ordering> orderings = ImmutableMap.<Slot, SortItem.Ordering>builder();
            for (Slot slot : node.getOrderBy()) {
                Slot canonical = canonicalize(slot);
                slots.add(canonical);
                orderings.put(canonical, node.getOrderings().get(slot));
            }

            return new TopNNode(source, node.getCount(), slots.build(), orderings.build());
        }

        @Override
        protected PlanNode visitPlan(PlanNode node, Void context)
        {
            throw new UnsupportedOperationException("not yet implemented");
        }

        private void map(Slot slot, Slot canonical)
        {
            Preconditions.checkArgument(!slot.equals(canonical), "Can't map slot to itself: %s", slot);
            mapping.put(slot, canonical);
        }

        private Slot canonicalize(Slot slot)
        {
            Slot canonical = slot;
            while (mapping.containsKey(canonical)) {
                canonical = mapping.get(canonical);
            }
            return canonical;
        }

        private Expression canonicalize(Expression value)
        {
            return TreeRewriter.rewriteWith(new NodeRewriter<Void>()
            {
                @Override
                public Node rewriteSlotReference(SlotReference node, Void context, TreeRewriter<Void> treeRewriter)
                {
                    return new SlotReference(canonicalize(node.getSlot()));
                }
            }, value);
        }

        private List<Slot> canonicalize(List<Slot> outputs)
        {
            return Lists.transform(outputs, canonicalizeFunction());
        }

        private Function<Slot, Slot> canonicalizeFunction()
        {
            return new Function<Slot, Slot>()
            {
                @Override
                public Slot apply(Slot input)
                {
                    return canonicalize(input);
                }
            };
        }
    }
}
