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
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.Domain;
import com.facebook.presto.spi.Range;
import com.facebook.presto.spi.SortedRangeSet;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.DependencyExtractor;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanRewriter;
import com.facebook.presto.sql.planner.plan.RowNumberNode;
import com.facebook.presto.sql.planner.plan.TopNRowNumberNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.sql.planner.DomainTranslator.fromPredicate;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.getOnlyElement;

public class WindowFilterPushDown
        extends PlanOptimizer
{
    private static final Signature ROW_NUMBER_SIGNATURE = new Signature("row_number", StandardTypes.BIGINT, ImmutableList.<String>of());

    private final Metadata metadata;

    public WindowFilterPushDown(Metadata metadata)
    {
        this.metadata = checkNotNull(metadata, "metadata is null");
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        checkNotNull(plan, "plan is null");
        checkNotNull(session, "session is null");
        checkNotNull(types, "types is null");
        checkNotNull(symbolAllocator, "symbolAllocator is null");
        checkNotNull(idAllocator, "idAllocator is null");

        return PlanRewriter.rewriteWith(new Rewriter(idAllocator, metadata, session, types), plan, null);
    }

    private static class Rewriter
            extends PlanRewriter<Constraint>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final Metadata metadata;
        private final Session session;
        private final Map<Symbol, Type> types;

        private Rewriter(PlanNodeIdAllocator idAllocator, Metadata metadata, Session session, Map<Symbol, Type> types)
        {
            this.idAllocator = checkNotNull(idAllocator, "idAllocator is null");
            this.metadata = checkNotNull(metadata, "metadata is null");
            this.session = checkNotNull(session, "session is null");
            this.types = ImmutableMap.copyOf(checkNotNull(types, "types is null"));
        }

        @Override
        public PlanNode visitWindow(WindowNode node, RewriteContext<Constraint> context)
        {
            if (canOptimizeWindowFunction(node)) {
                PlanNode rewrittenSource = context.rewrite(node.getSource(), null);
                Optional<Integer> limit = getLimit(node, context.get());
                if (node.getOrderBy().isEmpty()) {
                    return new RowNumberNode(idAllocator.getNextId(),
                            rewrittenSource,
                            node.getPartitionBy(),
                            getOnlyElement(node.getWindowFunctions().keySet()),
                            limit,
                            Optional.empty());
                }
                if (limit.isPresent()) {
                    return new TopNRowNumberNode(idAllocator.getNextId(),
                            rewrittenSource,
                            node.getPartitionBy(),
                            node.getOrderBy(),
                            node.getOrderings(),
                            getOnlyElement(node.getWindowFunctions().keySet()),
                            limit.get(),
                            false,
                            Optional.empty());
                }
            }
            return context.defaultRewrite(node);
        }

        private Optional<Integer> getLimit(WindowNode node, Constraint filter)
        {
            if (filter == null || (!filter.getLimit().isPresent() && !filter.getFilterExpression().isPresent())) {
                return Optional.empty();
            }
            if (filter.getLimit().isPresent()) {
                return filter.getLimit();
            }
            if (filterContainsWindowFunctions(node, filter.getFilterExpression().get())) {
                Symbol rowNumberSymbol = getOnlyElement(node.getWindowFunctions().entrySet()).getKey();
                TupleDomain<Symbol> tupleDomain = fromPredicate(metadata, session, filter.getFilterExpression().get(), types).getTupleDomain();
                if (tupleDomain.isNone()) {
                    return Optional.empty();
                }
                Domain domain = tupleDomain.getDomains().get(rowNumberSymbol);
                if (domain != null) {
                    return getRowNumberUpperBound(domain.getRanges());
                }
            }
            return Optional.empty();
        }

        private static Optional<Integer> getRowNumberUpperBound(SortedRangeSet rowNumberRangeSet)
        {
            if (rowNumberRangeSet.isAll() || rowNumberRangeSet.isNone() || rowNumberRangeSet.getRangeCount() <= 0) {
                return Optional.empty();
            }

            // try to get the upper bound of row number
            Optional<Long> upperBound = Optional.empty();
            Range span = rowNumberRangeSet.getSpan();

            if (!span.getHigh().isUpperUnbounded()) {
                switch (span.getHigh().getBound()) {
                    case EXACTLY:
                        upperBound = Optional.of((Long) span.getHigh().getValue());
                        break;
                    case BELOW:
                        upperBound = Optional.of((Long) span.getHigh().getValue() - 1);
                        break;
                    default:
                        break;
                }
            }

            if (upperBound.isPresent() && upperBound.get() <= Integer.MAX_VALUE) {
                return Optional.of(upperBound.get().intValue());
            }
            return Optional.empty();
        }

        private static boolean canOptimizeWindowFunction(WindowNode node)
        {
            if (node.getWindowFunctions().size() != 1) {
                return false;
            }
            Symbol rowNumberSymbol = getOnlyElement(node.getWindowFunctions().entrySet()).getKey();
            return isRowNumberSignature(node.getSignatures().get(rowNumberSymbol));
        }

        private static boolean filterContainsWindowFunctions(WindowNode node, Expression filterPredicate)
        {
            Set<Symbol> windowFunctionSymbols = node.getWindowFunctions().keySet();
            Sets.SetView<Symbol> commonSymbols = Sets.intersection(DependencyExtractor.extractUnique(filterPredicate), windowFunctionSymbols);
            return !commonSymbols.isEmpty();
        }

        @Override
        public PlanNode visitLimit(LimitNode node, RewriteContext<Constraint> context)
        {
            // Operators can handle MAX_VALUE rows per page, so do not optimize if count is greater than this value
            if (node.getCount() >= Integer.MAX_VALUE) {
                return context.defaultRewrite(node);
            }
            Constraint constraint = new Constraint(Optional.of((int) node.getCount()), Optional.empty());
            PlanNode rewrittenSource = context.rewrite(node.getSource(), constraint);

            if (rewrittenSource != node.getSource()) {
                return new LimitNode(idAllocator.getNextId(), rewrittenSource, node.getCount());
            }

            return context.defaultRewrite(node);
        }

        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<Constraint> context)
        {
            PlanNode rewrittenSource = context.rewrite(node.getSource(), new Constraint(Optional.empty(), Optional.of(node.getPredicate())));
            return context.replaceChildren(node, ImmutableList.of(rewrittenSource));
        }
    }

    private static boolean isRowNumberSignature(Signature signature)
    {
        return signature.equals(ROW_NUMBER_SIGNATURE);
    }

    private static class Constraint
    {
        private final Optional<Integer> limit;
        private final Optional<Expression> filterExpression;

        private Constraint(Optional<Integer> limit, Optional<Expression> filterExpression)
        {
            this.limit = limit;
            this.filterExpression = filterExpression;
        }

        public Optional<Integer> getLimit()
        {
            return limit;
        }

        public Optional<Expression> getFilterExpression()
        {
            return filterExpression;
        }
    }
}
