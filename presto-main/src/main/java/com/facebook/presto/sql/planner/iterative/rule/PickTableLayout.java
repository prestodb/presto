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
import com.facebook.presto.execution.warnings.WarningCollector;
import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableLayoutResult;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.predicate.NullableValue;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.DomainTranslator;
import com.facebook.presto.sql.planner.ExpressionInterpreter;
import com.facebook.presto.sql.planner.LiteralEncoder;
import com.facebook.presto.sql.planner.LookupSymbolResolver;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolsExtractor;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.NullLiteral;
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
import static com.facebook.presto.metadata.TableLayoutResult.computeEnforced;
import static com.facebook.presto.sql.ExpressionUtils.combineConjuncts;
import static com.facebook.presto.sql.ExpressionUtils.filterDeterministicConjuncts;
import static com.facebook.presto.sql.ExpressionUtils.filterNonDeterministicConjuncts;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypes;
import static com.facebook.presto.sql.planner.iterative.rule.PreconditionRules.checkRulesAreFiredBeforeAddExchangesRule;
import static com.facebook.presto.sql.planner.plan.Patterns.filter;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.facebook.presto.sql.planner.plan.Patterns.tableScan;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Sets.intersection;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * These rules should not be run after AddExchanges so as not to overwrite the TableLayout
 * chosen by AddExchanges
 */
public class PickTableLayout
{
    private final Metadata metadata;
    private final SqlParser parser;
    private final DomainTranslator domainTranslator;

    public PickTableLayout(Metadata metadata, SqlParser parser)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.parser = requireNonNull(parser, "parser is null");
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
        return new PickTableLayoutForPredicate(metadata, parser, domainTranslator);
    }

    public PickTableLayoutWithoutPredicate pickTableLayoutWithoutPredicate()
    {
        return new PickTableLayoutWithoutPredicate(metadata, parser, domainTranslator);
    }

    private static final class PickTableLayoutForPredicate
            implements Rule<FilterNode>
    {
        private final Metadata metadata;
        private final SqlParser parser;
        private final DomainTranslator domainTranslator;

        private PickTableLayoutForPredicate(Metadata metadata, SqlParser parser, DomainTranslator domainTranslator)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.parser = requireNonNull(parser, "parser is null");
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

            PlanNode rewritten = planTableScan(tableScan, filterNode.getPredicate(), context.getSession(), context.getSymbolAllocator().getTypes(), context.getIdAllocator(), metadata, parser, domainTranslator);

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

            if (!tableScan.getLayout().isPresent() && rewrittenTableScan.getLayout().isPresent()) {
                return false;
            }

            return Objects.equals(tableScan.getCurrentConstraint(), rewrittenTableScan.getCurrentConstraint())
                    && Objects.equals(tableScan.getEnforcedConstraint(), rewrittenTableScan.getEnforcedConstraint());
        }
    }

    private static final class PickTableLayoutWithoutPredicate
            implements Rule<TableScanNode>
    {
        private final Metadata metadata;
        private final SqlParser parser;
        private final DomainTranslator domainTranslator;

        private PickTableLayoutWithoutPredicate(Metadata metadata, SqlParser parser, DomainTranslator domainTranslator)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.parser = requireNonNull(parser, "parser is null");
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

            return Result.ofPlanNode(planTableScan(tableScanNode, TRUE_LITERAL, context.getSession(), context.getSymbolAllocator().getTypes(), context.getIdAllocator(), metadata, parser, domainTranslator));
        }
    }

    private static PlanNode planTableScan(
            TableScanNode node,
            Expression predicate,
            Session session,
            TypeProvider types,
            PlanNodeIdAllocator idAllocator,
            Metadata metadata,
            SqlParser parser,
            DomainTranslator domainTranslator)
    {
        return listTableLayouts(
                node,
                predicate,
                false,
                session,
                types,
                idAllocator,
                metadata,
                parser,
                domainTranslator)
                .get(0);
    }

    public static List<PlanNode> listTableLayouts(
            TableScanNode node,
            Expression predicate,
            boolean pruneWithPredicateExpression,
            Session session,
            TypeProvider types,
            PlanNodeIdAllocator idAllocator,
            Metadata metadata,
            SqlParser parser,
            DomainTranslator domainTranslator)
    {
        // don't include non-deterministic predicates
        Expression deterministicPredicate = filterDeterministicConjuncts(predicate);

        DomainTranslator.ExtractionResult decomposedPredicate = DomainTranslator.fromPredicate(
                metadata,
                session,
                deterministicPredicate,
                types);

        TupleDomain<ColumnHandle> newDomain = decomposedPredicate.getTupleDomain()
                .transform(node.getAssignments()::get)
                .intersect(node.getEnforcedConstraint());

        Map<ColumnHandle, Symbol> assignments = ImmutableBiMap.copyOf(node.getAssignments()).inverse();

        Constraint<ColumnHandle> constraint;
        if (pruneWithPredicateExpression) {
            LayoutConstraintEvaluator evaluator = new LayoutConstraintEvaluator(
                    metadata,
                    parser,
                    session,
                    types,
                    node.getAssignments(),
                    combineConjuncts(
                            deterministicPredicate,
                            // Simplify the tuple domain to avoid creating an expression with too many nodes,
                            // which would be expensive to evaluate in the call to isCandidate below.
                            domainTranslator.toPredicate(newDomain.simplify().transform(assignments::get))));
            constraint = new Constraint<>(newDomain, evaluator::isCandidate);
        }
        else {
            // Currently, invoking the expression interpreter is very expensive.
            // TODO invoke the interpreter unconditionally when the interpreter becomes cheap enough.
            constraint = new Constraint<>(newDomain);
        }

        // Layouts will be returned in order of the connector's preference
        List<TableLayoutResult> layouts = metadata.getLayouts(
                session,
                node.getTable(),
                constraint,
                Optional.of(node.getOutputSymbols().stream()
                        .map(node.getAssignments()::get)
                        .collect(toImmutableSet())));

        if (layouts.isEmpty()) {
            return ImmutableList.of(new ValuesNode(idAllocator.getNextId(), node.getOutputSymbols(), ImmutableList.of()));
        }

        // Filter out layouts that cannot supply all the required columns
        layouts = layouts.stream()
                .filter(layout -> layout.hasAllOutputs(node))
                .collect(toList());
        checkState(!layouts.isEmpty(), "No usable layouts for %s", node);

        if (layouts.stream().anyMatch(layout -> layout.getLayout().getPredicate().isNone())) {
            return ImmutableList.of(new ValuesNode(idAllocator.getNextId(), node.getOutputSymbols(), ImmutableList.of()));
        }

        return layouts.stream()
                .map(layout -> {
                    TableScanNode tableScan = new TableScanNode(
                            node.getId(),
                            node.getTable(),
                            node.getOutputSymbols(),
                            node.getAssignments(),
                            Optional.of(layout.getLayout().getHandle()),
                            layout.getLayout().getPredicate(),
                            computeEnforced(newDomain, layout.getUnenforcedConstraint()));

                    // The order of the arguments to combineConjuncts matters:
                    // * Unenforced constraints go first because they can only be simple column references,
                    //   which are not prone to logic errors such as out-of-bound access, div-by-zero, etc.
                    // * Conjuncts in non-deterministic expressions and non-TupleDomain-expressible expressions should
                    //   retain their original (maybe intermixed) order from the input predicate. However, this is not implemented yet.
                    // * Short of implementing the previous bullet point, the current order of non-deterministic expressions
                    //   and non-TupleDomain-expressible expressions should be retained. Changing the order can lead
                    //   to failures of previously successful queries.
                    Expression resultingPredicate = combineConjuncts(
                            domainTranslator.toPredicate(layout.getUnenforcedConstraint().transform(assignments::get)),
                            filterNonDeterministicConjuncts(predicate),
                            decomposedPredicate.getRemainingExpression());

                    if (!TRUE_LITERAL.equals(resultingPredicate)) {
                        return new FilterNode(idAllocator.getNextId(), tableScan, resultingPredicate);
                    }

                    return tableScan;
                })
                .collect(toImmutableList());
    }

    private static class LayoutConstraintEvaluator
    {
        private final Map<Symbol, ColumnHandle> assignments;
        private final ExpressionInterpreter evaluator;
        private final Set<ColumnHandle> arguments;

        public LayoutConstraintEvaluator(Metadata metadata, SqlParser parser, Session session, TypeProvider types, Map<Symbol, ColumnHandle> assignments, Expression expression)
        {
            this.assignments = assignments;

            Map<NodeRef<Expression>, Type> expressionTypes = getExpressionTypes(session, metadata, parser, types, expression, emptyList(), WarningCollector.NOOP);

            evaluator = ExpressionInterpreter.expressionOptimizer(expression, metadata, session, expressionTypes);
            arguments = SymbolsExtractor.extractUnique(expression).stream()
                    .map(assignments::get)
                    .collect(toImmutableSet());
        }

        private boolean isCandidate(Map<ColumnHandle, NullableValue> bindings)
        {
            if (intersection(bindings.keySet(), arguments).isEmpty()) {
                return true;
            }
            LookupSymbolResolver inputs = new LookupSymbolResolver(assignments, bindings);

            // If any conjuncts evaluate to FALSE or null, then the whole predicate will never be true and so the partition should be pruned
            Object optimized = evaluator.optimize(inputs);
            if (Boolean.FALSE.equals(optimized) || optimized == null || optimized instanceof NullLiteral) {
                return false;
            }

            return true;
        }
    }
}
