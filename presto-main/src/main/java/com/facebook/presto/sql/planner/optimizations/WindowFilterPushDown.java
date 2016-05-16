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
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.predicate.ValueSet;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.ExpressionUtils;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.RowNumberNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.TopNRowNumberNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Ints;

import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import static com.facebook.presto.metadata.FunctionKind.WINDOW;
import static com.facebook.presto.spi.predicate.Marker.Bound.BELOW;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.sql.planner.DomainTranslator.ExtractionResult;
import static com.facebook.presto.sql.planner.DomainTranslator.fromPredicate;
import static com.facebook.presto.sql.planner.DomainTranslator.toPredicate;
import static com.facebook.presto.sql.planner.plan.ChildReplacer.replaceChildren;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

public class WindowFilterPushDown
        implements PlanOptimizer
{
    private static final Signature ROW_NUMBER_SIGNATURE = new Signature("row_number", WINDOW, parseTypeSignature(StandardTypes.BIGINT), ImmutableList.of());

    private final Metadata metadata;

    public WindowFilterPushDown(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(types, "types is null");
        requireNonNull(symbolAllocator, "symbolAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");

        return SimplePlanRewriter.rewriteWith(new Rewriter(idAllocator, metadata, session, types), plan, null);
    }

    private static class Rewriter
            extends SimplePlanRewriter<Void>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final Metadata metadata;
        private final Session session;
        private final Map<Symbol, Type> types;

        private Rewriter(PlanNodeIdAllocator idAllocator, Metadata metadata, Session session, Map<Symbol, Type> types)
        {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.session = requireNonNull(session, "session is null");
            this.types = ImmutableMap.copyOf(requireNonNull(types, "types is null"));
        }

        @Override
        public PlanNode visitWindow(WindowNode node, RewriteContext<Void> context)
        {
            PlanNode rewrittenSource = context.rewrite(node.getSource());

            if (canReplaceWithRowNumber(node)) {
                return new RowNumberNode(idAllocator.getNextId(),
                        rewrittenSource,
                        node.getPartitionBy(),
                        getOnlyElement(node.getWindowFunctions().keySet()),
                        Optional.empty(),
                        Optional.empty());
            }
            return replaceChildren(node, ImmutableList.of(rewrittenSource));
        }

        @Override
        public PlanNode visitLimit(LimitNode node, RewriteContext<Void> context)
        {
            // Operators can handle MAX_VALUE rows per page, so do not optimize if count is greater than this value
            if (node.getCount() > Integer.MAX_VALUE) {
                return context.defaultRewrite(node);
            }

            PlanNode source = context.rewrite(node.getSource());
            int limit = Ints.checkedCast(node.getCount());
            if (source instanceof RowNumberNode) {
                RowNumberNode rowNumberNode = mergeLimit(((RowNumberNode) source), limit);
                if (rowNumberNode.getPartitionBy().isEmpty()) {
                    return rowNumberNode;
                }
                source = rowNumberNode;
            }
            else if (source instanceof WindowNode && canOptimizeWindowFunction((WindowNode) source)) {
                WindowNode windowNode = (WindowNode) source;
                // verify that unordered row_number window functions are replaced by RowNumberNode
                verify(!windowNode.getOrderBy().isEmpty());
                TopNRowNumberNode topNRowNumberNode = convertToTopNRowNumber(windowNode, limit);
                if (windowNode.getPartitionBy().isEmpty()) {
                    return topNRowNumberNode;
                }
                source = topNRowNumberNode;
            }
            return replaceChildren(node, ImmutableList.of(source));
        }

        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<Void> context)
        {
            PlanNode source = context.rewrite(node.getSource());

            TupleDomain<Symbol> tupleDomain = fromPredicate(metadata, session, node.getPredicate(), types).getTupleDomain();

            if (source instanceof RowNumberNode) {
                Symbol rowNumberSymbol = ((RowNumberNode) source).getRowNumberSymbol();
                OptionalInt upperBound = extractUpperBound(tupleDomain, rowNumberSymbol);

                if (upperBound.isPresent()) {
                    source = mergeLimit(((RowNumberNode) source), upperBound.getAsInt());
                    return rewriteFilterSource(node, source, rowNumberSymbol, upperBound.getAsInt());
                }
            }
            else if (source instanceof WindowNode && canOptimizeWindowFunction((WindowNode) source)) {
                WindowNode windowNode = (WindowNode) source;
                Symbol rowNumberSymbol = getOnlyElement(windowNode.getWindowFunctions().entrySet()).getKey();
                OptionalInt upperBound = extractUpperBound(tupleDomain, rowNumberSymbol);

                if (upperBound.isPresent()) {
                    source = convertToTopNRowNumber(windowNode, upperBound.getAsInt());
                    return rewriteFilterSource(node, source, rowNumberSymbol, upperBound.getAsInt());
                }
            }
            return replaceChildren(node, ImmutableList.of(source));
        }

        private PlanNode rewriteFilterSource(FilterNode filterNode, PlanNode source, Symbol rowNumberSymbol, int upperBound)
        {
            ExtractionResult extractionResult = fromPredicate(metadata, session, filterNode.getPredicate(), types);
            TupleDomain<Symbol> tupleDomain = extractionResult.getTupleDomain();

            if (!isEqualRange(tupleDomain, rowNumberSymbol, upperBound)) {
                return new FilterNode(filterNode.getId(), source, filterNode.getPredicate());
            }

            // Remove the row number domain because it is absorbed into the node
            Map<Symbol, Domain> newDomains = tupleDomain.getDomains().get().entrySet().stream()
                    .filter(entry -> !entry.getKey().equals(rowNumberSymbol))
                    .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));

            // Construct a new predicate
            TupleDomain<Symbol> newTupleDomain = TupleDomain.withColumnDomains(newDomains);
            Expression newPredicate = ExpressionUtils.combineConjuncts(
                    extractionResult.getRemainingExpression(),
                    toPredicate(newTupleDomain));

            if (newPredicate.equals(BooleanLiteral.TRUE_LITERAL)) {
                return source;
            }
            return new FilterNode(filterNode.getId(), source, newPredicate);
        }

        private static boolean isEqualRange(TupleDomain<Symbol> tupleDomain, Symbol symbol, long upperBound)
        {
            if (tupleDomain.isNone()) {
                return false;
            }
            Domain domain = tupleDomain.getDomains().get().get(symbol);
            return domain.getValues().equals(ValueSet.ofRanges(Range.lessThanOrEqual(domain.getType(), upperBound)));
        }

        private static OptionalInt extractUpperBound(TupleDomain<Symbol> tupleDomain, Symbol symbol)
        {
            if (tupleDomain.isNone()) {
                return OptionalInt.empty();
            }

            Domain rowNumberDomain = tupleDomain.getDomains().get().get(symbol);
            if (rowNumberDomain == null) {
                return OptionalInt.empty();
            }
            ValueSet values = rowNumberDomain.getValues();
            if (values.isAll() || values.isNone() || values.getRanges().getRangeCount() <= 0) {
                return OptionalInt.empty();
            }

            Range span = values.getRanges().getSpan();

            if (span.getHigh().isUpperUnbounded()) {
                return OptionalInt.empty();
            }

            verify(rowNumberDomain.getType().equals(BIGINT));
            long upperBound = (Long) span.getHigh().getValue();
            if (span.getHigh().getBound() == BELOW) {
                upperBound--;
            }

            if (upperBound > Integer.MAX_VALUE) {
                return OptionalInt.empty();
            }
            return OptionalInt.of(Ints.checkedCast(upperBound));
        }

        private static RowNumberNode mergeLimit(RowNumberNode node, int newRowCountPerPartition)
        {
            if (node.getMaxRowCountPerPartition().isPresent()) {
                newRowCountPerPartition = Math.min(node.getMaxRowCountPerPartition().get(), newRowCountPerPartition);
            }
            return new RowNumberNode(node.getId(), node.getSource(), node.getPartitionBy(), node.getRowNumberSymbol(), Optional.of(newRowCountPerPartition), node.getHashSymbol());
        }

        private TopNRowNumberNode convertToTopNRowNumber(WindowNode windowNode, int limit)
        {
            return new TopNRowNumberNode(idAllocator.getNextId(),
                    windowNode.getSource(),
                    windowNode.getPartitionBy(),
                    windowNode.getOrderBy(),
                    windowNode.getOrderings(),
                    getOnlyElement(windowNode.getWindowFunctions().keySet()),
                    limit,
                    false,
                    Optional.empty());
        }

        private static boolean canReplaceWithRowNumber(WindowNode node)
        {
            return canOptimizeWindowFunction(node) && node.getOrderBy().isEmpty();
        }

        private static boolean canOptimizeWindowFunction(WindowNode node)
        {
            if (node.getWindowFunctions().size() != 1) {
                return false;
            }
            Symbol rowNumberSymbol = getOnlyElement(node.getWindowFunctions().entrySet()).getKey();
            return isRowNumberSignature(node.getSignatures().get(rowNumberSymbol));
        }

        private static boolean isRowNumberSignature(Signature signature)
        {
            return signature.equals(ROW_NUMBER_SIGNATURE);
        }
    }
}
