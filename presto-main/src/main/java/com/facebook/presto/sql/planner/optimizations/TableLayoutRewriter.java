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
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableLayoutResult;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.sql.planner.DomainTranslator;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

import static com.facebook.presto.sql.ExpressionUtils.combineConjuncts;
import static com.facebook.presto.sql.ExpressionUtils.stripDeterministicConjuncts;
import static com.facebook.presto.sql.ExpressionUtils.stripNonDeterministicConjuncts;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class TableLayoutRewriter
{
    private final Metadata metadata;
    private final Session session;
    private final SymbolAllocator symbolAllocator;
    private final PlanNodeIdAllocator idAllocator;

    public TableLayoutRewriter(Metadata metadata, Session session, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        this.metadata = metadata;
        this.session = session;
        this.symbolAllocator = symbolAllocator;
        this.idAllocator = idAllocator;
    }

    public PlanNode planTableScan(TableScanNode node, Expression predicate)
    {
        Expression deterministicPredicate = stripNonDeterministicConjuncts(predicate);
        DomainTranslator.ExtractionResult decomposedPredicate = DomainTranslator.fromPredicate(
                metadata,
                session,
                deterministicPredicate,
                symbolAllocator.getTypes());

        TupleDomain<ColumnHandle> simplifiedConstraint = decomposedPredicate.getTupleDomain()
                .transform(node.getAssignments()::get)
                .intersect(node.getCurrentConstraint());

        List<TableLayoutResult> layouts = metadata.getLayouts(
                session, node.getTable(),
                new Constraint<>(simplifiedConstraint, bindings -> true),
                Optional.of(ImmutableSet.copyOf(node.getAssignments().values())));
        layouts = layouts.stream()
                .filter(layoutHasAllNeededOutputs(node))
                .collect(toImmutableList());
        if (layouts.isEmpty()) {
            return new ValuesNode(idAllocator.getNextId(), node.getOutputSymbols(), ImmutableList.of());
        }

        TableLayoutResult layout = layouts.get(0);

        TableScanNode rewrittenTableScan = new TableScanNode(
                node.getId(),
                node.getTable(),
                node.getOutputSymbols(),
                node.getAssignments(),
                Optional.of(layout.getLayout().getHandle()),
                simplifiedConstraint.intersect(layout.getLayout().getPredicate()),
                Optional.ofNullable(node.getOriginalConstraint()).orElse(predicate));

        Map<ColumnHandle, Symbol> assignments = ImmutableBiMap.copyOf(node.getAssignments()).inverse();
        Expression resultingPredicate = combineConjuncts(
                decomposedPredicate.getRemainingExpression(),
                stripDeterministicConjuncts(predicate),
                DomainTranslator.toPredicate(layout.getUnenforcedConstraint().transform(assignments::get)));

        PlanNode result = rewrittenTableScan;
        if (!BooleanLiteral.TRUE_LITERAL.equals(resultingPredicate)) {
            result = new FilterNode(idAllocator.getNextId(), rewrittenTableScan, resultingPredicate);
        }

        return result;
    }

    private Predicate<TableLayoutResult> layoutHasAllNeededOutputs(TableScanNode node)
    {
        return layout -> {
            List<ColumnHandle> columnHandles = Lists.transform(node.getOutputSymbols(), node.getAssignments()::get);
            return !layout.getLayout().getColumns().isPresent()
                    || layout.getLayout().getColumns().get().containsAll(columnHandles);
        };
    }
}
