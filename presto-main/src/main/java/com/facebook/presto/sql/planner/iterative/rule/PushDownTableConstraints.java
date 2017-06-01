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

package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableLayoutResult;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.DomainTranslator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.optimizations.ExpressionEquivalence;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

import static com.facebook.presto.sql.ExpressionUtils.combineConjuncts;
import static com.facebook.presto.sql.ExpressionUtils.extractConjuncts;
import static com.facebook.presto.sql.ExpressionUtils.stripDeterministicConjuncts;
import static com.facebook.presto.sql.ExpressionUtils.stripNonDeterministicConjuncts;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;

public class PushDownTableConstraints
        implements Rule
{
    private final Metadata metadata;
    private final SqlParser sqlParser;

    public PushDownTableConstraints(Metadata metadata, SqlParser sqlParser)
    {
        this.metadata = metadata;
        this.sqlParser = sqlParser;
    }

    @Override
    public Optional<PlanNode> apply(PlanNode node, Context context)
    {
        if (!(node instanceof FilterNode)) {
            return Optional.empty();
        }

        FilterNode filter = (FilterNode) node;
        PlanNode source = context.getLookup().resolve(filter.getSource());
        if (!(source instanceof TableScanNode)) {
            return Optional.empty();
        }

        Expression predicate = filter.getPredicate();
        // don't include non-deterministic predicates
        Expression deterministicPredicate = stripNonDeterministicConjuncts(predicate);
        DomainTranslator.ExtractionResult decomposedPredicate = DomainTranslator.fromPredicate(
                metadata,
                context.getSession(),
                deterministicPredicate,
                context.getSymbolAllocator().getTypes());

        TableScanNode tableScan = (TableScanNode) source;
        TupleDomain<ColumnHandle> simplifiedConstraint = decomposedPredicate.getTupleDomain()
                .transform(tableScan.getAssignments()::get)
                .intersect(tableScan.getCurrentConstraint());

        Map<ColumnHandle, Symbol> assignments = ImmutableBiMap.copyOf(tableScan.getAssignments()).inverse();

        // Layouts will be returned in order of the connector's preference
        List<TableLayoutResult> layouts = metadata.getLayouts(
                context.getSession(), tableScan.getTable(),
                new Constraint<>(simplifiedConstraint, bindings -> true),
                Optional.of(tableScan.getOutputSymbols().stream()
                        .map(tableScan.getAssignments()::get)
                        .collect(toImmutableSet())));

        // Filter out layouts that cannot supply all the required columns
        layouts = layouts.stream()
                .filter(layoutHasAllNeededOutputs(tableScan))
                .collect(toImmutableList());

        if (layouts.isEmpty()) {
            return Optional.empty();
        }

        // At this point we have no way to choose between possible layouts, just take the first one
        TableLayoutResult layout = layouts.get(0);

        PlanNode rewrittenPlan = new TableScanNode(
                tableScan.getId(),
                tableScan.getTable(),
                tableScan.getOutputSymbols(),
                tableScan.getAssignments(),
                Optional.of(layout.getLayout().getHandle()),
                simplifiedConstraint.intersect(layout.getLayout().getPredicate()),
                Optional.ofNullable(tableScan.getOriginalConstraint()).orElse(predicate));

        Expression resultingPredicate = combineConjuncts(
                DomainTranslator.toPredicate(layout.getUnenforcedConstraint().transform(assignments::get)),
                stripDeterministicConjuncts(predicate),
                decomposedPredicate.getRemainingExpression());
        if (!BooleanLiteral.TRUE_LITERAL.equals(resultingPredicate)) {
            rewrittenPlan = new FilterNode(context.getIdAllocator().getNextId(), rewrittenPlan, resultingPredicate);
        }

        if (!planChanged(rewrittenPlan, filter, context.getLookup(), context.getSession(), context.getSymbolAllocator())) {
            return Optional.empty();
        }
        return Optional.of(rewrittenPlan);
    }

    private Predicate<TableLayoutResult> layoutHasAllNeededOutputs(TableScanNode node)
    {
        return layout -> {
            List<ColumnHandle> columnHandles = Lists.transform(node.getOutputSymbols(), node.getAssignments()::get);
            return !layout.getLayout().getColumns().isPresent()
                    || layout.getLayout().getColumns().get().containsAll(columnHandles);
        };
    }

    private boolean planChanged(PlanNode rewrittenPlan, FilterNode oldPlan, Lookup lookup, Session session, SymbolAllocator symbolAllocator)
    {
        if (!(rewrittenPlan instanceof FilterNode)) {
            return true;
        }

        FilterNode rewrittenFilter = (FilterNode) rewrittenPlan;
        if (!new ExpressionEquivalence(metadata, sqlParser).areExpressionsEquivalent(session, rewrittenFilter.getPredicate(), oldPlan.getPredicate(), symbolAllocator.getTypes())) {
            if (!ImmutableSet.copyOf(extractConjuncts(rewrittenFilter.getPredicate())).equals(ImmutableSet.copyOf(extractConjuncts(oldPlan.getPredicate())))) {
                return true;
            }
        }

        TableScanNode oldTableScan = (TableScanNode) lookup.resolve(oldPlan.getSource());
        TableScanNode rewrittenTableScan = (TableScanNode) lookup.resolve(rewrittenFilter.getSource());
        return !rewrittenTableScan.getCurrentConstraint().equals(oldTableScan.getCurrentConstraint());
    }
}
