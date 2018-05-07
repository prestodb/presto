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
import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableLayoutResult;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.sql.planner.DomainTranslator;
import com.facebook.presto.sql.planner.LiteralEncoder;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.isNewOptimizerEnabled;
import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.sql.ExpressionUtils.combineConjuncts;
import static com.facebook.presto.sql.ExpressionUtils.filterDeterministicConjuncts;
import static com.facebook.presto.sql.ExpressionUtils.filterNonDeterministicConjuncts;
import static com.facebook.presto.sql.planner.iterative.rule.PreconditionRules.checkRulesAreFiredBeforeAddExchangesRule;
import static com.facebook.presto.sql.planner.plan.Patterns.filter;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.facebook.presto.sql.planner.plan.Patterns.tableScan;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * These rules should not be run after AddExchanges so as not to overwrite the TableLayout
 * chosen by AddExchanges
 */
public class PickTableLayout
{
    private final Metadata metadata;
    private final DomainTranslator domainTranslator;

    public PickTableLayout(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.domainTranslator = new DomainTranslator(new LiteralEncoder(metadata.getBlockEncodingSerde()));
    }

    public Set<Rule<?>> rules()
    {
        return ImmutableSet.of(
                checkRulesAreFiredBeforeAddExchangesRule(),
                pickTableLayoutForPredicate(),
                pickTableLayoutWithoutPredicate());
    }

    public PickTableLayoutForPredicate pickTableLayoutForPredicate()
    {
        return new PickTableLayoutForPredicate(metadata, domainTranslator);
    }

    public PickTableLayoutWithoutPredicate pickTableLayoutWithoutPredicate()
    {
        return new PickTableLayoutWithoutPredicate(metadata, domainTranslator);
    }

    private static final class PickTableLayoutForPredicate
            implements Rule<FilterNode>
    {
        private final Metadata metadata;
        private final DomainTranslator domainTranslator;

        private PickTableLayoutForPredicate(Metadata metadata, DomainTranslator domainTranslator)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.domainTranslator = requireNonNull(domainTranslator, "domainTranslator is null");
        }

        private static final Capture<TableScanNode> TABLE_SCAN = newCapture();

        private static final Pattern<FilterNode> PATTERN = filter().with(source().matching(
                tableScan().capturedAs(TABLE_SCAN)));

        @Override
        public Pattern<FilterNode> getPattern()
        {
            return PATTERN;
        }

        @Override
        public boolean isEnabled(Session session)
        {
            return isNewOptimizerEnabled(session);
        }

        @Override
        public Result apply(FilterNode filterNode, Captures captures, Context context)
        {
            TableScanNode tableScan = captures.get(TABLE_SCAN);

            PlanNode rewritten = planTableScan(tableScan, filterNode.getPredicate(), context, metadata, domainTranslator);

            if (arePlansSame(filterNode, tableScan, rewritten)) {
                return Result.empty();
            }

            return Result.ofPlanNode(rewritten);
        }

        private boolean arePlansSame(FilterNode filter, TableScanNode tableScan, PlanNode rewritten)
        {
            if (!(rewritten instanceof FilterNode)) {
                return false;
            }

            FilterNode rewrittenFilter = (FilterNode) rewritten;
            if (!Objects.equals(filter.getPredicate(), rewrittenFilter.getPredicate())) {
                return false;
            }

            if (!(rewrittenFilter.getSource() instanceof TableScanNode)) {
                return false;
            }

            TableScanNode rewrittenTableScan = (TableScanNode) rewrittenFilter.getSource();
            return Objects.equals(tableScan.getCurrentConstraint(), rewrittenTableScan.getCurrentConstraint());
        }
    }

    private static final class PickTableLayoutWithoutPredicate
            implements Rule<TableScanNode>
    {
        private final Metadata metadata;
        private final DomainTranslator domainTranslator;

        private PickTableLayoutWithoutPredicate(Metadata metadata, DomainTranslator domainTranslator)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.domainTranslator = requireNonNull(domainTranslator, "domainTranslator is null");
        }

        private static final Pattern<TableScanNode> PATTERN = tableScan();

        @Override
        public Pattern<TableScanNode> getPattern()
        {
            return PATTERN;
        }

        @Override
        public boolean isEnabled(Session session)
        {
            return isNewOptimizerEnabled(session);
        }

        @Override
        public Result apply(TableScanNode tableScanNode, Captures captures, Context context)
        {
            if (tableScanNode.getLayout().isPresent()) {
                return Result.empty();
            }

            return Result.ofPlanNode(planTableScan(tableScanNode, TRUE_LITERAL, context, metadata, domainTranslator));
        }
    }

    private static PlanNode planTableScan(TableScanNode node, Expression predicate, Rule.Context context, Metadata metadata, DomainTranslator domainTranslator)
    {
        Expression deterministicPredicate = filterDeterministicConjuncts(predicate);
        DomainTranslator.ExtractionResult decomposedPredicate = DomainTranslator.fromPredicate(
                metadata,
                context.getSession(),
                deterministicPredicate,
                context.getSymbolAllocator().getTypes());

        TupleDomain<ColumnHandle> simplifiedConstraint = decomposedPredicate.getTupleDomain()
                .transform(node.getAssignments()::get)
                .intersect(node.getCurrentConstraint());

        List<TableLayoutResult> layouts = metadata.getLayouts(
                context.getSession(),
                node.getTable(),
                new Constraint<>(simplifiedConstraint, bindings -> true),
                Optional.of(ImmutableSet.copyOf(node.getAssignments().values())));
        if (layouts.isEmpty()) {
            return new ValuesNode(context.getIdAllocator().getNextId(), node.getOutputSymbols(), ImmutableList.of());
        }
        layouts = layouts.stream()
                .filter(layout -> layout.hasAllOutputs(node))
                .collect(toImmutableList());

        TableLayoutResult layout = layouts.get(0);

        TableScanNode result = new TableScanNode(
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
                filterNonDeterministicConjuncts(predicate),
                domainTranslator.toPredicate(layout.getUnenforcedConstraint().transform(assignments::get)));

        if (!TRUE_LITERAL.equals(resultingPredicate)) {
            return new FilterNode(context.getIdAllocator().getNextId(), result, resultingPredicate);
        }

        return result;
    }
}
