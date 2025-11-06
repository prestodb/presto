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
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.predicate.ValueSet;
import com.facebook.presto.expressions.LogicalRowExpressions;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.WindowNode;
import com.facebook.presto.spi.relation.DomainTranslator.ExtractionResult;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.RowNumberNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.TopNRowNumberNode;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.facebook.presto.sql.relational.RowExpressionDomainTranslator;
import com.google.common.collect.ImmutableList;

import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import static com.facebook.presto.SystemSessionProperties.isOptimizeTopNRowNumber;
import static com.facebook.presto.common.predicate.Marker.Bound.BELOW;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.sql.planner.plan.ChildReplacer.replaceChildren;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

public class WindowFilterPushDown
        implements PlanOptimizer
{
    private final Metadata metadata;
    private final RowExpressionDomainTranslator domainTranslator;
    private final LogicalRowExpressions logicalRowExpressions;

    public WindowFilterPushDown(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.domainTranslator = new RowExpressionDomainTranslator(metadata);
        this.logicalRowExpressions = new LogicalRowExpressions(
                new RowExpressionDeterminismEvaluator(metadata.getFunctionAndTypeManager()),
                new FunctionResolution(metadata.getFunctionAndTypeManager().getFunctionAndTypeResolver()),
                metadata.getFunctionAndTypeManager());
    }

    @Override
    public PlanOptimizerResult optimize(PlanNode plan, Session session, TypeProvider types, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(types, "types is null");
        requireNonNull(variableAllocator, "variableAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");

        Rewriter rewriter = new Rewriter(idAllocator, metadata, domainTranslator, logicalRowExpressions, session);
        PlanNode rewrittenPlan = SimplePlanRewriter.rewriteWith(rewriter, plan, null);
        return PlanOptimizerResult.optimizerResult(rewrittenPlan, rewriter.isPlanChanged());
    }

    private static class Rewriter
            extends SimplePlanRewriter<Void>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final Metadata metadata;
        private final RowExpressionDomainTranslator domainTranslator;
        private final LogicalRowExpressions logicalRowExpressions;
        private final Session session;
        private boolean planChanged;

        private Rewriter(PlanNodeIdAllocator idAllocator, Metadata metadata, RowExpressionDomainTranslator domainTranslator, LogicalRowExpressions logicalRowExpressions, Session session)
        {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.domainTranslator = requireNonNull(domainTranslator, "domainTranslator is null");
            this.logicalRowExpressions = logicalRowExpressions;
            this.session = requireNonNull(session, "session is null");
        }

        public boolean isPlanChanged()
        {
            return planChanged;
        }

        @Override
        public PlanNode visitWindow(WindowNode node, RewriteContext<Void> context)
        {
            checkState(node.getWindowFunctions().size() == 1, "WindowFilterPushdown requires that WindowNodes contain exactly one window function");
            PlanNode rewrittenSource = context.rewrite(node.getSource());

            if (canReplaceWithRowNumber(node, metadata.getFunctionAndTypeManager())) {
                planChanged = true;
                return new RowNumberNode(
                        rewrittenSource.getSourceLocation(),
                        idAllocator.getNextId(),
                        rewrittenSource,
                        node.getPartitionBy(),
                        getOnlyElement(node.getWindowFunctions().keySet()),
                        Optional.empty(),
                        false,
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
                planChanged = true;
                source = rowNumberNode;
            }
            else if (source instanceof WindowNode && canOptimizeWindowFunction((WindowNode) source, metadata.getFunctionAndTypeManager()) && isOptimizeTopNRowNumber(session)) {
                WindowNode windowNode = (WindowNode) source;
                // verify that unordered row_number window functions are replaced by RowNumberNode
                verify(windowNode.getOrderingScheme().isPresent());
                TopNRowNumberNode topNRowNumberNode = convertToTopNRowNumber(windowNode, limit);
                if (windowNode.getPartitionBy().isEmpty()) {
                    return topNRowNumberNode;
                }
                planChanged = true;
                source = topNRowNumberNode;
            }
            return replaceChildren(node, ImmutableList.of(source));
        }

        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<Void> context)
        {
            PlanNode source = context.rewrite(node.getSource());

            TupleDomain<VariableReferenceExpression> tupleDomain = domainTranslator.fromPredicate(session.toConnectorSession(), node.getPredicate()).getTupleDomain();

            if (source instanceof RowNumberNode) {
                VariableReferenceExpression rowNumberVariable = ((RowNumberNode) source).getRowNumberVariable();
                OptionalInt upperBound = extractUpperBound(tupleDomain, rowNumberVariable);

                if (upperBound.isPresent()) {
                    source = mergeLimit(((RowNumberNode) source), upperBound.getAsInt());
                    planChanged = true;
                    return rewriteFilterSource(node, source, rowNumberVariable, upperBound.getAsInt());
                }
            }
            else if (source instanceof WindowNode && canOptimizeWindowFunction((WindowNode) source, metadata.getFunctionAndTypeManager()) && isOptimizeTopNRowNumber(session)) {
                WindowNode windowNode = (WindowNode) source;
                VariableReferenceExpression rowNumberVariable = getOnlyElement(windowNode.getCreatedVariable());
                OptionalInt upperBound = extractUpperBound(tupleDomain, rowNumberVariable);

                if (upperBound.isPresent()) {
                    source = convertToTopNRowNumber(windowNode, upperBound.getAsInt());
                    planChanged = true;
                    return rewriteFilterSource(node, source, rowNumberVariable, upperBound.getAsInt());
                }
            }
            return replaceChildren(node, ImmutableList.of(source));
        }

        private PlanNode rewriteFilterSource(FilterNode filterNode, PlanNode source, VariableReferenceExpression rowNumberVariable, int upperBound)
        {
            ExtractionResult<VariableReferenceExpression> extractionResult = domainTranslator.fromPredicate(session.toConnectorSession(), filterNode.getPredicate());
            TupleDomain<VariableReferenceExpression> tupleDomain = extractionResult.getTupleDomain();

            if (!isEqualRange(tupleDomain, rowNumberVariable, upperBound)) {
                return new FilterNode(filterNode.getSourceLocation(), filterNode.getId(), source, filterNode.getPredicate());
            }

            // Remove the row number domain because it is absorbed into the node
            Map<VariableReferenceExpression, Domain> newDomains = tupleDomain.getDomains().get().entrySet().stream()
                    .filter(entry -> !entry.getKey().equals(rowNumberVariable))
                    .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));

            // Construct a new predicate
            TupleDomain<VariableReferenceExpression> newTupleDomain = TupleDomain.withColumnDomains(newDomains);
            RowExpression newPredicate = logicalRowExpressions.combineConjuncts(
                    extractionResult.getRemainingExpression(),
                    domainTranslator.toPredicate(newTupleDomain));

            if (newPredicate.equals(TRUE_CONSTANT)) {
                return source;
            }
            return new FilterNode(filterNode.getSourceLocation(), filterNode.getId(), source, newPredicate);
        }

        private static boolean isEqualRange(TupleDomain<VariableReferenceExpression> tupleDomain, VariableReferenceExpression variable, long upperBound)
        {
            if (tupleDomain.isNone()) {
                return false;
            }
            Domain domain = tupleDomain.getDomains().get().get(variable);
            return domain.getValues().equals(ValueSet.ofRanges(Range.lessThanOrEqual(domain.getType(), upperBound)));
        }

        private static OptionalInt extractUpperBound(TupleDomain<VariableReferenceExpression> tupleDomain, VariableReferenceExpression variable)
        {
            if (tupleDomain.isNone()) {
                return OptionalInt.empty();
            }

            Domain rowNumberDomain = tupleDomain.getDomains().get().get(variable);
            if (rowNumberDomain == null) {
                return OptionalInt.empty();
            }
            ValueSet values = rowNumberDomain.getValues();
            if (values.isAll() || values.isNone() || values.getRanges().getRangeCount() <= 0) {
                return OptionalInt.empty();
            }

            Range span = values.getRanges().getSpan();

            if (span.isHighUnbounded()) {
                return OptionalInt.empty();
            }

            verify(rowNumberDomain.getType().equals(BIGINT));
            long upperBound = (Long) span.getHighBoundedValue();
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
            return new RowNumberNode(node.getSourceLocation(), node.getId(), node.getSource(), node.getPartitionBy(), node.getRowNumberVariable(), Optional.of(newRowCountPerPartition), false, node.getHashVariable());
        }

        private TopNRowNumberNode convertToTopNRowNumber(WindowNode windowNode, int limit)
        {
            return new TopNRowNumberNode(
                    windowNode.getSourceLocation(),
                    idAllocator.getNextId(),
                    windowNode.getSource(),
                    windowNode.getSpecification(),
                    TopNRowNumberNode.RankingFunction.ROW_NUMBER,
                    getOnlyElement(windowNode.getCreatedVariable()),
                    limit,
                    false,
                    Optional.empty());
        }

        private static boolean canReplaceWithRowNumber(WindowNode node, FunctionAndTypeManager functionAndTypeManager)
        {
            return canOptimizeWindowFunction(node, functionAndTypeManager) && !node.getOrderingScheme().isPresent();
        }

        private static boolean canOptimizeWindowFunction(WindowNode node, FunctionAndTypeManager functionAndTypeManager)
        {
            if (node.getWindowFunctions().size() != 1) {
                return false;
            }
            VariableReferenceExpression rowNumberVariable = getOnlyElement(node.getWindowFunctions().keySet());
            return isRowNumberMetadata(functionAndTypeManager, functionAndTypeManager.getFunctionMetadata(node.getWindowFunctions().get(rowNumberVariable).getFunctionHandle()));
        }

        private static boolean isRowNumberMetadata(FunctionAndTypeManager functionAndTypeManager, FunctionMetadata functionMetadata)
        {
            FunctionHandle rowNumberFunction = functionAndTypeManager.lookupFunction("row_number", ImmutableList.of());
            return functionMetadata.equals(functionAndTypeManager.getFunctionMetadata(rowNumberFunction));
        }
    }
}
