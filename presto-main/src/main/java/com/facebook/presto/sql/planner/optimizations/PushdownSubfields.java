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
import com.facebook.presto.spi.Subfield;
import com.facebook.presto.spi.Subfield.NestedField;
import com.facebook.presto.spi.Subfield.PathElement;
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
import com.facebook.presto.sql.tree.DefaultExpressionTraversalVisitor;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.SubscriptExpression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static com.facebook.presto.SystemSessionProperties.isPushdownSubfields;
import static com.facebook.presto.spi.Subfield.allSubscripts;
import static com.facebook.presto.sql.planner.SubfieldUtils.deferenceOrSubscriptExpressionToPath;
import static com.facebook.presto.sql.planner.SubfieldUtils.isDereferenceOrSubscriptExpression;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToExpression;
import static java.util.Objects.requireNonNull;

public class PushdownSubfields
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
        private final SubfieldExtractor subfieldExtractor = new SubfieldExtractor();

        // TODO Move these into context
        private Set<Symbol> fullColumnUses = new HashSet<>();
        private Set<Subfield> subfieldPaths = new HashSet<>();

        @Override
        public PlanNode visitJoin(JoinNode node, RewriteContext<Void> context)
        {
            if (node.getFilter().isPresent()) {
                collectSubfieldPaths(castToExpression(node.getFilter().get()));
            }

            PlanNode left = context.rewrite(node.getLeft(), context.get());
            PlanNode right = context.rewrite(node.getRight(), context.get());
            return node.replaceChildren(ImmutableList.of(left, right));
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, RewriteContext<Void> context)
        {
            for (AggregationNode.Aggregation aggregation : node.getAggregations().values()) {
                collectSubfieldPaths(aggregation);
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
            if (subfieldPaths.isEmpty()) {
                return node;
            }

            ImmutableMap.Builder<Symbol, List<Subfield>> leafSubfieldsBuilder = ImmutableMap.builder();

            for (Map.Entry<Symbol, ColumnHandle> entry : node.getAssignments().entrySet()) {
                if (fullColumnUses.contains(entry.getKey())) {
                    continue;
                }

                List<Subfield> subfields = new ArrayList();
                for (Subfield path : subfieldPaths) {
                    if (path.getRootName().equals(entry.getKey().getName())) {
                        subfields.add(path);
                    }
                }

                if (subfields.isEmpty()) {
                    continue;
                }

                // Compute the leaf subfields. If we have a.b.c and
                // a.b then a.b is the result. If a path is a prefix
                // of another path, then the longer is discarded.
                List<Subfield> leafPaths = new ArrayList();
                for (Subfield path : subfields) {
                    if (!prefixExists(path, subfields)) {
                        leafPaths.add(path);
                    }
                }

                leafSubfieldsBuilder.put(entry.getKey(), leafPaths);
            }

            Map<Symbol, List<Subfield>> leafSubfields = leafSubfieldsBuilder.build();
            if (leafSubfields.isEmpty()) {
                return node;
            }

            return new TableScanNode(
                    node.getId(),
                    node.getTable(),
                    node.getOutputSymbols(),
                    node.getAssignments(),
                    node.getCurrentConstraint(),
                    node.getEnforcedConstraint(),
                    node.isTemporaryTable(),
                    leafSubfields);
        }

        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<Void> context)
        {
            collectSubfieldPaths(castToExpression(node.getPredicate()));

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

        private static boolean prefixExists(Subfield subfieldPath, Collection<Subfield> subfieldPaths)
        {
            return subfieldPaths.stream()
                    .filter(path -> path.isPrefix(subfieldPath))
                    .findAny()
                    .isPresent();
        }

        private static final class Context
        {
            private final Consumer<Symbol> symbols;
            private final Consumer<Subfield> subfieldPaths;

            private Context(Consumer<Symbol> symbols, Consumer<Subfield> subfieldPaths)
            {
                this.symbols = requireNonNull(symbols, "symbols is null");
                this.subfieldPaths = requireNonNull(subfieldPaths, "subfieldPaths is null");
            }
        }

        private static final class SubfieldExtractor
                extends DefaultExpressionTraversalVisitor<Void, Context>
        {
            @Override
            protected Void visitSubscriptExpression(SubscriptExpression node, Context context)
            {
                if (processBaseExpression(node.getBase())) {
                    context.subfieldPaths.accept(deferenceOrSubscriptExpressionToPath(node));
                }
                return null;
            }

            @Override
            protected Void visitDereferenceExpression(DereferenceExpression node, Context context)
            {
                if (processBaseExpression(node.getBase())) {
                    context.subfieldPaths.accept(deferenceOrSubscriptExpressionToPath(node));
                }
                return null;
            }

            @Override
            protected Void visitSymbolReference(SymbolReference node, Context context)
            {
                context.symbols.accept(Symbol.from(node));
                return null;
            }

            private boolean processBaseExpression(Expression base)
            {
                while (true) {
                    if (base instanceof DereferenceExpression) {
                        base = ((DereferenceExpression) base).getBase();
                    }
                    else if (base instanceof SubscriptExpression) {
                        base = ((SubscriptExpression) base).getBase();
                    }
                    else if (base instanceof SymbolReference) {
                        return true;
                    }
                    else {
                        process(base);
                        return false;
                    }
                }
            }
        }

        private void processProjectionPaths(Assignments assignments)
        {
            ImmutableSet.Builder<Subfield> newPaths = ImmutableSet.builder();
            for (Map.Entry<Symbol, Expression> entry : assignments.entrySet()) {
                Symbol key = entry.getKey();
                Expression value = entry.getValue();
                if (value instanceof SymbolReference) {
                    SymbolReference valueRef = (SymbolReference) value;
                    if (fullColumnUses.contains(key)) {
                        fullColumnUses.add(Symbol.from(valueRef));
                    }
                    for (Subfield path : subfieldPaths) {
                        if (key.getName().equals(path.getRootName())) {
                            newPaths.add(new Subfield(valueRef.getName(), path.getPath()));
                        }
                    }
                }
                else if (isDereferenceOrSubscriptExpression(value)) {
                    Expression base = SubfieldUtils.getDerefenceOrSubscriptBase(value);
                    if (base instanceof SymbolReference) {
                        Subfield subfield = deferenceOrSubscriptExpressionToPath(value);
                        if (fullColumnUses.contains(key)) {
                            subfieldPaths.add(subfield);
                        }
                        for (Subfield path : subfieldPaths) {
                            if (key.getName().equals(path.getRootName())) {
                                newPaths.add(new Subfield(subfield.getRootName(), ImmutableList.<PathElement>builder()
                                        .addAll(subfield.getPath())
                                        .addAll(path.getPath())
                                        .build()));
                            }
                        }
                    }
                }
                else {
                    subfieldExtractor.process(value, new Context(fullColumnUses::add, newPaths::add));
                }
            }
            subfieldPaths.addAll(newPaths.build());
        }

        private void processUnnestPaths(Map<Symbol, List<Symbol>> unnestSymbols)
        {
            // If a result is referenced or is a start of a path, add
            // the unnest source + any subscript as the head of the
            // path.
            List<Subfield> newPaths = new ArrayList();
            for (Map.Entry<Symbol, List<Symbol>> entry : unnestSymbols.entrySet()) {
                String source = entry.getKey().getName();
                for (Symbol member : entry.getValue()) {
                    if (fullColumnUses.contains(member)) {
                        newPaths.add(new Subfield(source, ImmutableList.of(allSubscripts(), new NestedField(member.getName()))));
                    }
                    else {
                        for (Subfield path : subfieldPaths) {
                            if (member.getName().equals(path.getRootName())) {
                                subfieldPaths.add(new Subfield(source, ImmutableList.<PathElement>builder()
                                        .add(allSubscripts())
                                        .addAll(path.getPath())
                                        .build()));
                            }
                        }
                    }
                }
            }
            subfieldPaths.addAll(newPaths);
        }

        private void collectSubfieldPaths(AggregationNode.Aggregation expression)
        {
            // TODO Implement
        }

        private void collectSubfieldPaths(Node expression)
        {
            if (expression instanceof SymbolReference) {
                fullColumnUses.add(new Symbol(((SymbolReference) expression).getName()));
                return;
            }

            if (isDereferenceOrSubscriptExpression(expression)) {
                Subfield path = deferenceOrSubscriptExpressionToPath(expression);
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
