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
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.iterative.RuleSet;
import com.facebook.presto.sql.planner.optimizations.ExpressionEquivalence;
import com.facebook.presto.sql.planner.optimizations.TableLayoutRewriter;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableSet;

import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.sql.ExpressionUtils.extractConjuncts;
import static com.facebook.presto.sql.planner.plan.Patterns.filter;
import static com.facebook.presto.sql.planner.plan.Patterns.tableScan;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * These rules should not be run after AddExchanges so as not to overwrite the TableLayout
 * chosen by AddExchanges
 */
public class PickTableLayout
        implements RuleSet
{
    private final ImmutableSet<Rule<?>> rules;

    public PickTableLayout(Metadata metadata, SqlParser sqlParser)
    {
        rules = ImmutableSet.of(
                new PickTableLayoutForPredicate(metadata, sqlParser),
                new PickTableLayoutWithoutPredicate(metadata));
    }

    @Override
    public Set<Rule<?>> rules()
    {
        return rules;
    }

    private static final class PickTableLayoutForPredicate
            implements Rule<FilterNode>
    {
        private final Metadata metadata;
        private final SqlParser sqlParser;

        private PickTableLayoutForPredicate(Metadata metadata, SqlParser sqlParser)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.sqlParser = requireNonNull(sqlParser);
        }

        private static final Pattern<FilterNode> PATTERN = filter();

        @Override
        public Pattern<FilterNode> getPattern()
        {
            return PATTERN;
        }

        @Override
        public Optional<PlanNode> apply(FilterNode filterNode, Captures captures, Context context)
        {
            PlanNode source = context.getLookup().resolve(filterNode.getSource());

            if (!((source instanceof TableScanNode))) {
                return Optional.empty();
            }

            TableScanNode tableScanNode = (TableScanNode) source;
            TableLayoutRewriter tableLayoutRewriter = new TableLayoutRewriter(metadata, context.getSession(), context.getSymbolAllocator(), context.getIdAllocator());
            PlanNode result = tableLayoutRewriter.planTableScan(tableScanNode, filterNode.getPredicate());

            if (!(result instanceof FilterNode)) {
                return Optional.of(result);
            }

            FilterNode resultFilterNode = (FilterNode) result;
            checkState(resultFilterNode.getSource() instanceof TableScanNode, "expected source to be a TableScanNode, but it was a %s", resultFilterNode.getSource().getClass().getName());
            if (!planChanged(tableScanNode, (TableScanNode) resultFilterNode.getSource(), filterNode.getPredicate(), resultFilterNode.getPredicate(), context.getSession(), context.getSymbolAllocator())) {
                return Optional.empty();
            }

            return Optional.of(result);
        }

        private boolean planChanged(TableScanNode oldTableScan, TableScanNode rewrittenTableScan, Expression oldPredicate, Expression rewrittenPredicate, Session session, SymbolAllocator symbolAllocator)
        {
            if (!new ExpressionEquivalence(metadata, sqlParser).areExpressionsEquivalent(session, oldPredicate, rewrittenPredicate, symbolAllocator.getTypes())) {
                if (!ImmutableSet.copyOf(extractConjuncts(oldPredicate)).equals(ImmutableSet.copyOf(extractConjuncts(rewrittenPredicate)))) {
                    return true;
                }
            }
            return !rewrittenTableScan.getCurrentConstraint().equals(oldTableScan.getCurrentConstraint());
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
        public Optional<PlanNode> apply(TableScanNode tableScanNode, Captures captures, Context context)
        {
            if (tableScanNode.getLayout().isPresent()) {
                return Optional.empty();
            }

            TableLayoutRewriter tableLayoutRewriter = new TableLayoutRewriter(metadata, context.getSession(), context.getSymbolAllocator(), context.getIdAllocator());
            return Optional.of(tableLayoutRewriter.planTableScan(tableScanNode, BooleanLiteral.TRUE_LITERAL));
        }
    }
}
