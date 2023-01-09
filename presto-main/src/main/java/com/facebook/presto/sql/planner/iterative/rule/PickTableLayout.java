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
import com.facebook.presto.common.predicate.NullableValue;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.expressions.LogicalRowExpressions;
import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableLayoutResult;
import com.facebook.presto.operator.scalar.TryFunction;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.DomainTranslator;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.RowExpressionInterpreter;
import com.facebook.presto.sql.planner.VariableResolver;
import com.facebook.presto.sql.planner.VariablesExtractor;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.facebook.presto.sql.relational.RowExpressionDomainTranslator;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.facebook.presto.SystemSessionProperties.isNewOptimizerEnabled;
import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.metadata.TableLayoutResult.computeEnforced;
import static com.facebook.presto.spi.relation.DomainTranslator.BASIC_COLUMN_EXTRACTOR;
import static com.facebook.presto.spi.relation.ExpressionOptimizer.Level.OPTIMIZED;
import static com.facebook.presto.sql.planner.iterative.rule.PreconditionRules.checkRulesAreFiredBeforeAddExchangesRule;
import static com.facebook.presto.sql.planner.plan.Patterns.filter;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.facebook.presto.sql.planner.plan.Patterns.tableScan;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Sets.intersection;
import static java.util.Objects.requireNonNull;

/**
 * These rules should not be run after AddExchanges so as not to overwrite the TableLayout
 * chosen by AddExchanges
 */
public class PickTableLayout
{
    private final Metadata metadata;

    public PickTableLayout(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
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
        return new PickTableLayoutForPredicate(metadata);
    }

    public PickTableLayoutWithoutPredicate pickTableLayoutWithoutPredicate()
    {
        return new PickTableLayoutWithoutPredicate(metadata);
    }

    private static final class PickTableLayoutForPredicate
            implements Rule<FilterNode>
    {
        private final Metadata metadata;

        private PickTableLayoutForPredicate(Metadata metadata)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
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
            if (!metadata.isLegacyGetLayoutSupported(context.getSession(), tableScan.getTable())) {
                return Result.empty();
            }

            PlanNode rewritten = pushPredicateIntoTableScan(
                    tableScan,
                    filterNode.getPredicate(),
                    false,
                    context.getSession(),
                    context.getIdAllocator(),
                    metadata);

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

            if (!tableScan.getTable().equals(rewrittenTableScan.getTable())) {
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

        private PickTableLayoutWithoutPredicate(Metadata metadata)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
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
            TableHandle tableHandle = tableScanNode.getTable();
            Session session = context.getSession();
            if (tableHandle.getLayout().isPresent() || !metadata.isLegacyGetLayoutSupported(session, tableHandle)) {
                return Result.empty();
            }

            TableLayoutResult layout = metadata.getLayout(
                    session,
                    tableHandle,
                    Constraint.alwaysTrue(),
                    Optional.of(tableScanNode.getOutputVariables().stream()
                            .map(variable -> tableScanNode.getAssignments().get(variable))
                            .collect(toImmutableSet())));

            if (layout.getLayout().getPredicate().isNone()) {
                return Result.ofPlanNode(new ValuesNode(tableScanNode.getSourceLocation(), context.getIdAllocator().getNextId(), tableScanNode.getOutputVariables(), ImmutableList.of(), Optional.empty()));
            }

            return Result.ofPlanNode(new TableScanNode(
                    tableScanNode.getSourceLocation(),
                    tableScanNode.getId(),
                    layout.getLayout().getNewTableHandle(),
                    tableScanNode.getOutputVariables(),
                    tableScanNode.getAssignments(),
                    tableScanNode.getTableConstraints(),
                    layout.getLayout().getPredicate(),
                    TupleDomain.all()));
        }
    }

    /**
     * @param predicate can be a RowExpression or an OriginalExpression. The method will handle both cases.
     * Once Expression is migrated to RowExpression in PickTableLayout, the method should only support RowExpression.
     */
    public static PlanNode pushPredicateIntoTableScan(
            TableScanNode node,
            RowExpression predicate,
            boolean pruneWithPredicateExpression,
            Session session,
            PlanNodeIdAllocator idAllocator,
            Metadata metadata)
    {
        if (!metadata.isLegacyGetLayoutSupported(session, node.getTable())) {
            return node;
        }

        DomainTranslator translator = new RowExpressionDomainTranslator(metadata);
        return pushPredicateIntoTableScan(node, predicate, pruneWithPredicateExpression, session, idAllocator, metadata, translator);
    }

    /**
     * For RowExpression {@param predicate}
     */
    private static PlanNode pushPredicateIntoTableScan(
            TableScanNode node,
            RowExpression predicate,
            boolean pruneWithPredicateExpression,
            Session session,
            PlanNodeIdAllocator idAllocator,
            Metadata metadata,
            DomainTranslator domainTranslator)
    {
        // don't include non-deterministic predicates
        LogicalRowExpressions logicalRowExpressions = new LogicalRowExpressions(
                new RowExpressionDeterminismEvaluator(metadata.getFunctionAndTypeManager()),
                new FunctionResolution(metadata.getFunctionAndTypeManager()),
                metadata.getFunctionAndTypeManager());
        RowExpression deterministicPredicate = logicalRowExpressions.filterDeterministicConjuncts(predicate);
        DomainTranslator.ExtractionResult<VariableReferenceExpression> decomposedPredicate = domainTranslator.fromPredicate(
                session.toConnectorSession(),
                deterministicPredicate,
                BASIC_COLUMN_EXTRACTOR);

        TupleDomain<ColumnHandle> newDomain = decomposedPredicate.getTupleDomain()
                .transform(variableName -> node.getAssignments().get(variableName))
                .intersect(node.getEnforcedConstraint());

        Map<ColumnHandle, VariableReferenceExpression> assignments = ImmutableBiMap.copyOf(node.getAssignments()).inverse();

        Constraint<ColumnHandle> constraint;
        if (pruneWithPredicateExpression) {
            LayoutConstraintEvaluatorForRowExpression evaluator = new LayoutConstraintEvaluatorForRowExpression(
                    metadata,
                    session,
                    node.getAssignments(),
                    logicalRowExpressions.combineConjuncts(
                            deterministicPredicate,
                            // Simplify the tuple domain to avoid creating an expression with too many nodes,
                            // which would be expensive to evaluate in the call to isCandidate below.
                            domainTranslator.toPredicate(newDomain.simplify().transform(column -> assignments.getOrDefault(column, null)))));
            constraint = new Constraint<>(newDomain, evaluator::isCandidate);
        }
        else {
            // Currently, invoking the expression interpreter is very expensive.
            // TODO invoke the interpreter unconditionally when the interpreter becomes cheap enough.
            constraint = new Constraint<>(newDomain);
        }
        if (constraint.getSummary().isNone()) {
            return new ValuesNode(node.getSourceLocation(), idAllocator.getNextId(), node.getOutputVariables(), ImmutableList.of(), Optional.empty());
        }

        // Layouts will be returned in order of the connector's preference
        TableLayoutResult layout = metadata.getLayout(
                session,
                node.getTable(),
                constraint,
                Optional.of(node.getOutputVariables().stream()
                        .map(variable -> node.getAssignments().get(variable))
                        .collect(toImmutableSet())));

        if (layout.getLayout().getPredicate().isNone()) {
            return new ValuesNode(node.getSourceLocation(), idAllocator.getNextId(), node.getOutputVariables(), ImmutableList.of(), Optional.empty());
        }

        TableScanNode tableScan = new TableScanNode(
                node.getSourceLocation(),
                node.getId(),
                layout.getLayout().getNewTableHandle(),
                node.getOutputVariables(),
                node.getAssignments(),
                node.getTableConstraints(),
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
        RowExpression resultingPredicate = logicalRowExpressions.combineConjuncts(
                domainTranslator.toPredicate(layout.getUnenforcedConstraint().transform(assignments::get)),
                logicalRowExpressions.filterNonDeterministicConjuncts(predicate),
                decomposedPredicate.getRemainingExpression());

        if (!TRUE_CONSTANT.equals(resultingPredicate)) {
            return new FilterNode(node.getSourceLocation(), idAllocator.getNextId(), tableScan, resultingPredicate);
        }
        return tableScan;
    }

    private static class LayoutConstraintEvaluatorForRowExpression
    {
        private final Map<VariableReferenceExpression, ColumnHandle> assignments;
        private final RowExpressionInterpreter evaluator;
        private final Set<ColumnHandle> arguments;

        public LayoutConstraintEvaluatorForRowExpression(Metadata metadata, Session session, Map<VariableReferenceExpression, ColumnHandle> assignments, RowExpression expression)
        {
            this.assignments = assignments;

            evaluator = new RowExpressionInterpreter(expression, metadata, session.toConnectorSession(), OPTIMIZED);
            arguments = VariablesExtractor.extractUnique(expression).stream()
                    .map(assignments::get)
                    .collect(toImmutableSet());
        }

        private boolean isCandidate(Map<ColumnHandle, NullableValue> bindings)
        {
            if (intersection(bindings.keySet(), arguments).isEmpty()) {
                return true;
            }
            LookupVariableResolver inputs = new LookupVariableResolver(assignments, bindings, variable -> variable);

            // Skip pruning if evaluation fails in a recoverable way. Failing here can cause
            // spurious query failures for partitions that would otherwise be filtered out.
            Object optimized = TryFunction.evaluate(() -> evaluator.optimize(inputs), true);

            // If any conjuncts evaluate to FALSE or null, then the whole predicate will never be true and so the partition should be pruned
            return !Boolean.FALSE.equals(optimized) && optimized != null && (!(optimized instanceof ConstantExpression) || !((ConstantExpression) optimized).isNull());
        }
    }

    private static class LookupVariableResolver
            implements VariableResolver
    {
        private final Map<VariableReferenceExpression, ColumnHandle> assignments;
        private final Map<ColumnHandle, NullableValue> bindings;
        // Use Object type to let interpreters consume the result
        // TODO: use RowExpression once the Expression-to-RowExpression is done
        private final Function<VariableReferenceExpression, Object> missingBindingSupplier;

        public LookupVariableResolver(
                Map<VariableReferenceExpression, ColumnHandle> assignments,
                Map<ColumnHandle, NullableValue> bindings,
                Function<VariableReferenceExpression, Object> missingBindingSupplier)
        {
            this.assignments = requireNonNull(assignments, "assignments is null");
            this.bindings = ImmutableMap.copyOf(requireNonNull(bindings, "bindings is null"));
            this.missingBindingSupplier = requireNonNull(missingBindingSupplier, "missingBindingSupplier is null");
        }

        @Override
        public Object getValue(VariableReferenceExpression variable)
        {
            ColumnHandle column = assignments.get(variable);
            checkArgument(column != null, "Missing column assignment for %s", variable);

            if (!bindings.containsKey(column)) {
                return missingBindingSupplier.apply(variable);
            }

            return bindings.get(column).getValue();
        }
    }
}
