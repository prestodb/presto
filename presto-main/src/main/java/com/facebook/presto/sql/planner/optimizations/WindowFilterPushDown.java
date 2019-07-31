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
import com.facebook.presto.metadata.FunctionManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.predicate.ValueSet;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.sql.ExpressionUtils;
import com.facebook.presto.sql.planner.ExpressionDomainTranslator;
import com.facebook.presto.sql.planner.LiteralEncoder;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.RowNumberNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.TopNRowNumberNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableList;

import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import static com.facebook.presto.SystemSessionProperties.isOptimizeTopNRowNumber;
import static com.facebook.presto.spi.function.FunctionKind.WINDOW;
import static com.facebook.presto.spi.predicate.Marker.Bound.BELOW;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.sql.planner.ExpressionDomainTranslator.ExtractionResult;
import static com.facebook.presto.sql.planner.ExpressionDomainTranslator.fromPredicate;
import static com.facebook.presto.sql.planner.plan.ChildReplacer.replaceChildren;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToExpression;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToRowExpression;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

public class WindowFilterPushDown
        implements PlanOptimizer
{
    private static final Signature ROW_NUMBER_SIGNATURE = new Signature("row_number", WINDOW, parseTypeSignature(StandardTypes.BIGINT), ImmutableList.of());

    private final Metadata metadata;
    private final ExpressionDomainTranslator domainTranslator;

    public WindowFilterPushDown(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.domainTranslator = new ExpressionDomainTranslator(new LiteralEncoder(metadata.getBlockEncodingSerde()));
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, PlanVariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(types, "types is null");
        requireNonNull(variableAllocator, "variableAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");

        return SimplePlanRewriter.rewriteWith(new Rewriter(idAllocator, metadata, domainTranslator, session, types), plan, null);
    }

    private static class Rewriter
            extends SimplePlanRewriter<Void>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final Metadata metadata;
        private final ExpressionDomainTranslator domainTranslator;
        private final Session session;
        private final TypeProvider types;

        private Rewriter(PlanNodeIdAllocator idAllocator, Metadata metadata, ExpressionDomainTranslator domainTranslator, Session session, TypeProvider types)
        {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.domainTranslator = requireNonNull(domainTranslator, "domainTranslator is null");
            this.session = requireNonNull(session, "session is null");
            this.types = requireNonNull(types, "types is null");
        }

        @Override
        public PlanNode visitWindow(WindowNode node, RewriteContext<Void> context)
        {
            checkState(node.getWindowFunctions().size() == 1, "WindowFilterPushdown requires that WindowNodes contain exactly one window function");
            PlanNode rewrittenSource = context.rewrite(node.getSource());

            if (canReplaceWithRowNumber(node, metadata.getFunctionManager())) {
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
            int limit = toIntExact(node.getCount());
            if (source instanceof RowNumberNode) {
                RowNumberNode rowNumberNode = mergeLimit(((RowNumberNode) source), limit);
                if (rowNumberNode.getPartitionBy().isEmpty()) {
                    return rowNumberNode;
                }
                source = rowNumberNode;
            }
            else if (source instanceof WindowNode && canOptimizeWindowFunction((WindowNode) source, metadata.getFunctionManager()) && isOptimizeTopNRowNumber(session)) {
                WindowNode windowNode = (WindowNode) source;
                // verify that unordered row_number window functions are replaced by RowNumberNode
                verify(windowNode.getOrderingScheme().isPresent());
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

            TupleDomain<String> tupleDomain = fromPredicate(metadata, session, castToExpression(node.getPredicate()), types).getTupleDomain();

            if (source instanceof RowNumberNode) {
                VariableReferenceExpression rowNumberVariable = ((RowNumberNode) source).getRowNumberVariable();
                OptionalInt upperBound = extractUpperBound(tupleDomain, rowNumberVariable);

                if (upperBound.isPresent()) {
                    source = mergeLimit(((RowNumberNode) source), upperBound.getAsInt());
                    return rewriteFilterSource(node, source, rowNumberVariable, upperBound.getAsInt());
                }
            }
            else if (source instanceof WindowNode && canOptimizeWindowFunction((WindowNode) source, metadata.getFunctionManager()) && isOptimizeTopNRowNumber(session)) {
                WindowNode windowNode = (WindowNode) source;
                VariableReferenceExpression rowNumberVariable = getOnlyElement(windowNode.getCreatedVariable());
                OptionalInt upperBound = extractUpperBound(tupleDomain, rowNumberVariable);

                if (upperBound.isPresent()) {
                    source = convertToTopNRowNumber(windowNode, upperBound.getAsInt());
                    return rewriteFilterSource(node, source, rowNumberVariable, upperBound.getAsInt());
                }
            }
            return replaceChildren(node, ImmutableList.of(source));
        }

        private PlanNode rewriteFilterSource(FilterNode filterNode, PlanNode source, VariableReferenceExpression rowNumberVariable, int upperBound)
        {
            ExtractionResult extractionResult = fromPredicate(metadata, session, castToExpression(filterNode.getPredicate()), types);
            TupleDomain<String> tupleDomain = extractionResult.getTupleDomain();

            if (!isEqualRange(tupleDomain, rowNumberVariable, upperBound)) {
                return new FilterNode(filterNode.getId(), source, filterNode.getPredicate());
            }

            // Remove the row number domain because it is absorbed into the node
            Map<String, Domain> newDomains = tupleDomain.getDomains().get().entrySet().stream()
                    .filter(entry -> !entry.getKey().equals(rowNumberVariable))
                    .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));

            // Construct a new predicate
            TupleDomain<String> newTupleDomain = TupleDomain.withColumnDomains(newDomains);
            Expression newPredicate = ExpressionUtils.combineConjuncts(
                    extractionResult.getRemainingExpression(),
                    domainTranslator.toPredicate(newTupleDomain));

            if (newPredicate.equals(BooleanLiteral.TRUE_LITERAL)) {
                return source;
            }
            return new FilterNode(filterNode.getId(), source, castToRowExpression(newPredicate));
        }

        private static boolean isEqualRange(TupleDomain<String> tupleDomain, VariableReferenceExpression variable, long upperBound)
        {
            if (tupleDomain.isNone()) {
                return false;
            }
            Domain domain = tupleDomain.getDomains().get().get(variable.getName());
            return domain.getValues().equals(ValueSet.ofRanges(Range.lessThanOrEqual(domain.getType(), upperBound)));
        }

        private static OptionalInt extractUpperBound(TupleDomain<String> tupleDomain, VariableReferenceExpression variable)
        {
            if (tupleDomain.isNone()) {
                return OptionalInt.empty();
            }

            Domain rowNumberDomain = tupleDomain.getDomains().get().get(variable.getName());
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

            if (upperBound > 0 && upperBound <= Integer.MAX_VALUE) {
                return OptionalInt.of(toIntExact(upperBound));
            }
            return OptionalInt.empty();
        }

        private static RowNumberNode mergeLimit(RowNumberNode node, int newRowCountPerPartition)
        {
            if (node.getMaxRowCountPerPartition().isPresent()) {
                newRowCountPerPartition = Math.min(node.getMaxRowCountPerPartition().get(), newRowCountPerPartition);
            }
            return new RowNumberNode(node.getId(), node.getSource(), node.getPartitionBy(), node.getRowNumberVariable(), Optional.of(newRowCountPerPartition), node.getHashVariable());
        }

        private TopNRowNumberNode convertToTopNRowNumber(WindowNode windowNode, int limit)
        {
            return new TopNRowNumberNode(idAllocator.getNextId(),
                    windowNode.getSource(),
                    windowNode.getSpecification(),
                    getOnlyElement(windowNode.getCreatedVariable()),
                    limit,
                    false,
                    Optional.empty());
        }

        private static boolean canReplaceWithRowNumber(WindowNode node, FunctionManager functionManager)
        {
            return canOptimizeWindowFunction(node, functionManager) && !node.getOrderingScheme().isPresent();
        }

        private static boolean canOptimizeWindowFunction(WindowNode node, FunctionManager functionManager)
        {
            if (node.getWindowFunctions().size() != 1) {
                return false;
            }
            VariableReferenceExpression rowNumberVariable = getOnlyElement(node.getWindowFunctions().keySet());
            return isRowNumberMetadata(functionManager.getFunctionMetadata(node.getWindowFunctions().get(rowNumberVariable).getFunctionHandle()));
        }

        private static boolean isRowNumberMetadata(FunctionMetadata functionMetadata)
        {
            return functionMetadata.getName().equals(ROW_NUMBER_SIGNATURE.getName())
                    && functionMetadata.getFunctionKind().equals(ROW_NUMBER_SIGNATURE.getKind())
                    && functionMetadata.getArgumentTypes().equals(ROW_NUMBER_SIGNATURE.getArgumentTypes())
                    && functionMetadata.getReturnType().equals(ROW_NUMBER_SIGNATURE.getReturnType());
        }
    }
}
