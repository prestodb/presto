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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.Session;
import com.facebook.presto.execution.warnings.WarningCollector;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.SubfieldPath;
import com.facebook.presto.spi.SubfieldPath.NestedField;
import com.facebook.presto.spi.SubfieldPath.PathElement;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.SubfieldUtils;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.UnnestNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.isPushdownSubfields;
import static com.facebook.presto.spi.SubfieldPath.allSubscripts;
import static com.facebook.presto.sql.planner.SubfieldUtils.deferenceOrSubscriptExpressionToPath;
import static com.facebook.presto.sql.planner.SubfieldUtils.isDereferenceOrSubscriptExpression;
import static java.util.Objects.requireNonNull;

public class PushdownSubscripts
        implements PlanOptimizer
{
    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(types, "types is null");
        requireNonNull(symbolAllocator, "symbolAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");

        if (!isPushdownSubfields(session)) {
            return plan;
        }

        return SimplePlanRewriter.rewriteWith(new Rewriter(), plan, null);
    }

    private static class Rewriter
            extends SimplePlanRewriter<Void>
    {
        // TODO Move these into context
        private Set<Symbol> fullColumnUses = new HashSet<>();
        private Set<SubfieldPath> subfieldPaths = new HashSet<>();

        @Override
        public PlanNode visitJoin(JoinNode node, RewriteContext<Void> context)
        {
            if (node.getFilter().isPresent()) {
                collectSubfieldPaths(node.getFilter().get());
            }

            PlanNode left = context.rewrite(node.getLeft(), context.get());
            PlanNode right = context.rewrite(node.getRight(), context.get());
            return node.replaceChildren(ImmutableList.of(left, right));
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, RewriteContext<Void> context)
        {
            for (AggregationNode.Aggregation aggregation : node.getAggregations().values()) {
                collectSubfieldPaths(aggregation.getCall());
                if (aggregation.getMask().isPresent()) {
                    fullColumnUses.add(aggregation.getMask().get());
                }
            }

            PlanNode source = context.rewrite(node.getSource(), context.get());

            return node.replaceChildren(ImmutableList.of(source));
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<Void> context)
        {
            return new TableScanNode(
                    node.getId(),
                    node.getTable(),
                    node.getOutputSymbols(),
                    annotateColumnsWithSubfields(node.getAssignments()),
                    node.getCurrentConstraint(),
                    node.getEnforcedConstraint());
        }

        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<Void> context)
        {
            collectSubfieldPaths(node.getPredicate());

            PlanNode source = context.rewrite(node.getSource(), context.get());
            return node.replaceChildren(ImmutableList.of(source));
        }

        @Override
        public PlanNode visitUnnest(UnnestNode node, RewriteContext<Void> context)
        {
            processUnnestPaths(node.getUnnestSymbols());

            PlanNode source = context.rewrite(node.getSource(), context.get());
            return node.replaceChildren(ImmutableList.of(source));
        }

        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<Void> context)
        {
            processProjectionPaths(node.getAssignments());

            PlanNode source = context.rewrite(node.getSource(), context.get());
            return node.replaceChildren(ImmutableList.of(source));
        }

        @Override
        public PlanNode visitOutput(OutputNode node, RewriteContext<Void> context)
        {
            fullColumnUses.addAll(node.getOutputSymbols());

            PlanNode source = context.rewrite(node.getSource(), context.get());
            return node.replaceChildren(ImmutableList.of(source));
        }

        private Map<Symbol, ColumnHandle> annotateColumnsWithSubfields(Map<Symbol, ColumnHandle> assignments)
        {
            if (subfieldPaths.isEmpty()) {
                return assignments;
            }

            Map<Symbol, ColumnHandle> newAssignments = new HashMap<>();
            for (Map.Entry<Symbol, ColumnHandle> entry : assignments.entrySet()) {
                if (fullColumnUses.contains(entry.getKey())) {
                    newAssignments.put(entry.getKey(), entry.getValue());
                    continue;
                }

                List<SubfieldPath> subfields = new ArrayList();
                for (SubfieldPath path : subfieldPaths) {
                    if (path.getColumnName().equals(entry.getKey().getName())) {
                        subfields.add(path);
                    }
                }
                if (subfields.isEmpty()) {
                    newAssignments.put(entry.getKey(), entry.getValue());
                    continue;
                }
                // Compute the leaf subfields. If we have a.b.c and
                // a.b then a.b is the result. If a path is a prefix
                // of another path, then the longer is discarded.
                List<SubfieldPath> leafPaths = new ArrayList();
                for (SubfieldPath path : subfields) {
                    if (!prefixExists(path, subfields)) {
                        leafPaths.add(path);
                    }
                    newAssignments.put(entry.getKey(), entry.getValue().createSubfieldPruningColumnHandle(leafPaths));
                }
            }
            return ImmutableMap.copyOf(newAssignments);
        }

        private static boolean prefixExists(SubfieldPath subfieldPath, Collection<SubfieldPath> subfieldPaths)
        {
            return subfieldPaths.stream()
                    .filter(path -> path.isPrefix(subfieldPath))
                    .findAny()
                    .isPresent();
        }

        private void processProjectionPaths(Assignments assignments)
        {
            Set<SubfieldPath> newPaths = new HashSet();
            for (Map.Entry<Symbol, Expression> entry : assignments.getMap().entrySet()) {
                Symbol key = entry.getKey();
                Expression value = entry.getValue();
                if (value instanceof SymbolReference) {
                    SymbolReference valueRef = (SymbolReference) value;
                    if (fullColumnUses.contains(key)) {
                        fullColumnUses.add(new Symbol(valueRef.getName()));
                    }
                    for (SubfieldPath path : subfieldPaths) {
                        if (key.getName().equals(path.getColumnName())) {
                            newPaths.add(new SubfieldPath(ImmutableList.<PathElement>builder()
                                    .add(new NestedField(valueRef.getName()))
                                    .addAll(path.getPathElements().subList(1, path.getPathElements().size()))
                                    .build()));
                        }
                    }
                }
                else if (isDereferenceOrSubscriptExpression(value)) {
                    Expression base = SubfieldUtils.getDerefenceOrSubscriptBase(value);
                    if (base instanceof SymbolReference) {
                        if (fullColumnUses.contains(key)) {
                            subfieldPaths.add(deferenceOrSubscriptExpressionToPath(value));
                        }
                        for (SubfieldPath path : subfieldPaths) {
                            if (key.getName().equals(path.getColumnName())) {
                                newPaths.add(new SubfieldPath(ImmutableList.<PathElement>builder()
                                        .addAll(deferenceOrSubscriptExpressionToPath(value).getPathElements())
                                        .addAll(path.getPathElements())
                                        .build()));
                            }
                        }
                    }
                }
            }
            subfieldPaths.addAll(newPaths);
        }

        private void processUnnestPaths(Map<Symbol, List<Symbol>> unnestSymbols)
        {
            // If a result is referenced or is a start of a path, add
            // the unnest source + any subscript as the head of the
            // path.
            List<SubfieldPath> newPaths = new ArrayList();
            for (Map.Entry<Symbol, List<Symbol>> entry : unnestSymbols.entrySet()) {
                String source = entry.getKey().getName();
                for (Symbol member : entry.getValue()) {
                    if (fullColumnUses.contains(member)) {
                        newPaths.add(new SubfieldPath(ImmutableList.of(
                                new NestedField(source),
                                allSubscripts(),
                                new NestedField(member.getName()))));
                    }
                    else {
                        for (SubfieldPath path : subfieldPaths) {
                            if (member.getName().equals(path.getColumnName())) {
                                subfieldPaths.add(new SubfieldPath(ImmutableList.<PathElement>builder()
                                        .add(new NestedField(source))
                                        .add(allSubscripts())
                                        .add(new NestedField(member.getName()))
                                        .addAll(path.getPathElements())
                                        .build()));
                            }
                        }
                    }
                }
            }
            for (SubfieldPath newPath : newPaths) {
                subfieldPaths.add(newPath);
            }
        }

        private void collectSubfieldPaths(Node expression)
        {
            if (expression instanceof SymbolReference) {
                fullColumnUses.add(new Symbol(((SymbolReference) expression).getName()));
            }
            if (isDereferenceOrSubscriptExpression(expression)) {
                SubfieldPath path = deferenceOrSubscriptExpressionToPath(expression);
                if (path != null) {
                    subfieldPaths.add(path);
                    return;
                }
            }
            for (Node child : expression.getChildren()) {
                collectSubfieldPaths(child);
            }
        }
    }
}
