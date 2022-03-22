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
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.expressions.LogicalRowExpressions;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.ResolvedIndex;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.DomainTranslator.ExtractionResult;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.IndexJoinNode;
import com.facebook.presto.sql.planner.plan.IndexSourceNode;
import com.facebook.presto.sql.planner.plan.InternalPlanVisitor;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.facebook.presto.sql.relational.RowExpressionDomainTranslator;
import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.spi.function.FunctionKind.AGGREGATE;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.identityAssignments;
import static com.facebook.presto.sql.planner.plan.WindowNode.Frame.WindowType.RANGE;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.in;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class IndexJoinOptimizer
        implements PlanOptimizer
{
    private final Metadata metadata;

    public IndexJoinOptimizer(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider type, PlanVariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(variableAllocator, "variableAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");

        return SimplePlanRewriter.rewriteWith(new Rewriter(idAllocator, metadata, session), plan, null);
    }

    private static class Rewriter
            extends SimplePlanRewriter<Void>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final Metadata metadata;
        private final Session session;

        private Rewriter(PlanNodeIdAllocator idAllocator, Metadata metadata, Session session)
        {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.session = requireNonNull(session, "session is null");
        }

        @Override
        public PlanNode visitJoin(JoinNode node, RewriteContext<Void> context)
        {
            PlanNode leftRewritten = context.rewrite(node.getLeft());
            PlanNode rightRewritten = context.rewrite(node.getRight());

            if (!node.getCriteria().isEmpty()) { // Index join only possible with JOIN criteria
                List<VariableReferenceExpression> leftJoinVariables = Lists.transform(node.getCriteria(), JoinNode.EquiJoinClause::getLeft);
                List<VariableReferenceExpression> rightJoinVariables = Lists.transform(node.getCriteria(), JoinNode.EquiJoinClause::getRight);

                Optional<PlanNode> leftIndexCandidate = IndexSourceRewriter.rewriteWithIndex(
                        leftRewritten,
                        ImmutableSet.copyOf(leftJoinVariables),
                        idAllocator,
                        metadata,
                        session);
                if (leftIndexCandidate.isPresent()) {
                    // Sanity check that we can trace the path for the index lookup key
                    Map<VariableReferenceExpression, VariableReferenceExpression> trace = IndexKeyTracer.trace(leftIndexCandidate.get(), ImmutableSet.copyOf(leftJoinVariables));
                    checkState(!trace.isEmpty() && leftJoinVariables.containsAll(trace.keySet()));
                }

                Optional<PlanNode> rightIndexCandidate = IndexSourceRewriter.rewriteWithIndex(
                        rightRewritten,
                        ImmutableSet.copyOf(rightJoinVariables),
                        idAllocator,
                        metadata,
                        session);
                if (rightIndexCandidate.isPresent()) {
                    // Sanity check that we can trace the path for the index lookup key
                    Map<VariableReferenceExpression, VariableReferenceExpression> trace = IndexKeyTracer.trace(rightIndexCandidate.get(), ImmutableSet.copyOf(rightJoinVariables));
                    checkState(!trace.isEmpty() && rightJoinVariables.containsAll(trace.keySet()));
                }

                switch (node.getType()) {
                    case INNER:
                        // Prefer the right candidate over the left candidate
                        PlanNode indexJoinNode = null;
                        if (rightIndexCandidate.isPresent()) {
                            indexJoinNode = new IndexJoinNode(idAllocator.getNextId(), IndexJoinNode.Type.INNER, leftRewritten, rightIndexCandidate.get(), createEquiJoinClause(leftJoinVariables, rightJoinVariables), Optional.empty(), Optional.empty());
                        }
                        else if (leftIndexCandidate.isPresent()) {
                            indexJoinNode = new IndexJoinNode(idAllocator.getNextId(), IndexJoinNode.Type.INNER, rightRewritten, leftIndexCandidate.get(), createEquiJoinClause(rightJoinVariables, leftJoinVariables), Optional.empty(), Optional.empty());
                        }

                        if (indexJoinNode != null) {
                            if (node.getFilter().isPresent()) {
                                indexJoinNode = new FilterNode(idAllocator.getNextId(), indexJoinNode, node.getFilter().get());
                            }

                            if (!indexJoinNode.getOutputVariables().equals(node.getOutputVariables())) {
                                indexJoinNode = new ProjectNode(
                                        idAllocator.getNextId(),
                                        indexJoinNode,
                                        identityAssignments(node.getOutputVariables()));
                            }

                            return indexJoinNode;
                        }
                        break;

                    case LEFT:
                        // We cannot use indices for outer joins until index join supports in-line filtering
                        if (!node.getFilter().isPresent() && rightIndexCandidate.isPresent()) {
                            return createIndexJoinWithExpectedOutputs(node.getOutputVariables(), IndexJoinNode.Type.SOURCE_OUTER, leftRewritten, rightIndexCandidate.get(), createEquiJoinClause(leftJoinVariables, rightJoinVariables), idAllocator);
                        }
                        break;

                    case RIGHT:
                        // We cannot use indices for outer joins until index join supports in-line filtering
                        if (!node.getFilter().isPresent() && leftIndexCandidate.isPresent()) {
                            return createIndexJoinWithExpectedOutputs(node.getOutputVariables(), IndexJoinNode.Type.SOURCE_OUTER, rightRewritten, leftIndexCandidate.get(), createEquiJoinClause(rightJoinVariables, leftJoinVariables), idAllocator);
                        }
                        break;

                    case FULL:
                        break;

                    default:
                        throw new IllegalArgumentException("Unknown type: " + node.getType());
                }
            }

            if (leftRewritten != node.getLeft() || rightRewritten != node.getRight()) {
                return new JoinNode(
                        node.getId(),
                        node.getType(),
                        leftRewritten,
                        rightRewritten,
                        node.getCriteria(),
                        node.getOutputVariables(),
                        node.getFilter(),
                        node.getLeftHashVariable(),
                        node.getRightHashVariable(),
                        node.getDistributionType(),
                        node.getDynamicFilters());
            }
            return node;
        }

        private static PlanNode createIndexJoinWithExpectedOutputs(
                List<VariableReferenceExpression> expectedOutputs,
                IndexJoinNode.Type type,
                PlanNode probe,
                PlanNode index,
                List<IndexJoinNode.EquiJoinClause> equiJoinClause,
                PlanNodeIdAllocator idAllocator)
        {
            PlanNode result = new IndexJoinNode(idAllocator.getNextId(), type, probe, index, equiJoinClause, Optional.empty(), Optional.empty());
            if (!result.getOutputVariables().equals(expectedOutputs)) {
                result = new ProjectNode(
                        idAllocator.getNextId(),
                        result,
                        identityAssignments(expectedOutputs));
            }
            return result;
        }

        private static List<IndexJoinNode.EquiJoinClause> createEquiJoinClause(List<VariableReferenceExpression> probeVariables, List<VariableReferenceExpression> indexVariables)
        {
            checkArgument(probeVariables.size() == indexVariables.size());
            ImmutableList.Builder<IndexJoinNode.EquiJoinClause> builder = ImmutableList.builder();
            for (int i = 0; i < probeVariables.size(); i++) {
                builder.add(new IndexJoinNode.EquiJoinClause(probeVariables.get(i), indexVariables.get(i)));
            }
            return builder.build();
        }
    }

    /**
     * Tries to rewrite a PlanNode tree with an IndexSource instead of a TableScan
     */
    private static class IndexSourceRewriter
            extends SimplePlanRewriter<IndexSourceRewriter.Context>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final Metadata metadata;
        private final RowExpressionDomainTranslator domainTranslator;
        private final LogicalRowExpressions logicalRowExpressions;
        private final Session session;

        private IndexSourceRewriter(PlanNodeIdAllocator idAllocator, Metadata metadata, Session session)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.domainTranslator = new RowExpressionDomainTranslator(metadata);
            this.logicalRowExpressions = new LogicalRowExpressions(
                    new RowExpressionDeterminismEvaluator(metadata.getFunctionAndTypeManager()),
                    new FunctionResolution(metadata.getFunctionAndTypeManager()),
                    metadata.getFunctionAndTypeManager());
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.session = requireNonNull(session, "session is null");
        }

        public static Optional<PlanNode> rewriteWithIndex(
                PlanNode planNode,
                Set<VariableReferenceExpression> lookupVariables,
                PlanNodeIdAllocator idAllocator,
                Metadata metadata,
                Session session)
        {
            AtomicBoolean success = new AtomicBoolean();
            IndexSourceRewriter indexSourceRewriter = new IndexSourceRewriter(idAllocator, metadata, session);
            PlanNode rewritten = SimplePlanRewriter.rewriteWith(indexSourceRewriter, planNode, new Context(lookupVariables, success));
            if (success.get()) {
                return Optional.of(rewritten);
            }
            return Optional.empty();
        }

        @Override
        public PlanNode visitPlan(PlanNode node, RewriteContext<Context> context)
        {
            // We don't know how to process this PlanNode in the context of an IndexJoin, so just give up by returning something
            return node;
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<Context> context)
        {
            return planTableScan(node, TRUE_CONSTANT, context.get());
        }

        private PlanNode planTableScan(TableScanNode node, RowExpression predicate, Context context)
        {
            ExtractionResult<VariableReferenceExpression> decomposedPredicate = domainTranslator.fromPredicate(session.toConnectorSession(), predicate);

            TupleDomain<ColumnHandle> simplifiedConstraint = decomposedPredicate.getTupleDomain()
                    .transform(variableName -> node.getAssignments().get(variableName))
                    .intersect(node.getEnforcedConstraint());

            checkState(node.getOutputVariables().containsAll(context.getLookupVariables()));

            Set<ColumnHandle> lookupColumns = context.getLookupVariables().stream()
                    .map(variable -> node.getAssignments().get(variable))
                    .collect(toImmutableSet());

            Set<ColumnHandle> outputColumns = node.getOutputVariables().stream().map(node.getAssignments()::get).collect(toImmutableSet());

            Optional<ResolvedIndex> optionalResolvedIndex = metadata.resolveIndex(session, node.getTable(), lookupColumns, outputColumns, simplifiedConstraint);
            if (!optionalResolvedIndex.isPresent()) {
                // No index available, so give up by returning something
                return node;
            }
            ResolvedIndex resolvedIndex = optionalResolvedIndex.get();

            Map<ColumnHandle, VariableReferenceExpression> inverseAssignments = node.getAssignments().entrySet().stream()
                    .collect(toImmutableMap(Map.Entry::getValue, Map.Entry::getKey));

            PlanNode source = new IndexSourceNode(
                    idAllocator.getNextId(),
                    resolvedIndex.getIndexHandle(),
                    node.getTable(),
                    context.getLookupVariables(),
                    node.getOutputVariables(),
                    node.getAssignments(),
                    simplifiedConstraint);

            RowExpression resultingPredicate = logicalRowExpressions.combineConjuncts(
                    domainTranslator.toPredicate(resolvedIndex.getUnresolvedTupleDomain().transform(inverseAssignments::get)),
                    decomposedPredicate.getRemainingExpression());

            if (!resultingPredicate.equals(TRUE_CONSTANT)) {
                // todo it is likely we end up with redundant filters here because the predicate push down has already been run... the fix is to run predicate push down again
                source = new FilterNode(idAllocator.getNextId(), source, resultingPredicate);
            }
            context.markSuccess();
            return source;
        }

        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<Context> context)
        {
            // Rewrite the lookup variables in terms of only the pre-projected variables that have direct translations
            ImmutableSet.Builder<VariableReferenceExpression> newLookupVariablesBuilder = ImmutableSet.builder();
            for (VariableReferenceExpression variable : context.get().getLookupVariables()) {
                RowExpression expression = node.getAssignments().get(variable);
                if (expression instanceof VariableReferenceExpression) {
                    newLookupVariablesBuilder.add((VariableReferenceExpression) expression);
                }
            }
            ImmutableSet<VariableReferenceExpression> newLookupVariables = newLookupVariablesBuilder.build();

            if (newLookupVariables.isEmpty()) {
                return node;
            }

            return context.defaultRewrite(node, new Context(newLookupVariables, context.get().getSuccess()));
        }

        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<Context> context)
        {
            if (node.getSource() instanceof TableScanNode) {
                return planTableScan((TableScanNode) node.getSource(), node.getPredicate(), context.get());
            }

            return context.defaultRewrite(node, new Context(context.get().getLookupVariables(), context.get().getSuccess()));
        }

        @Override
        public PlanNode visitWindow(WindowNode node, RewriteContext<Context> context)
        {
            if (!node.getWindowFunctions().values().stream()
                    .allMatch(function -> metadata.getFunctionAndTypeManager().getFunctionMetadata(function.getFunctionHandle()).getFunctionKind() == AGGREGATE)) {
                return node;
            }

            // Don't need this restriction if we can prove that all order by variables are deterministically produced
            if (node.getOrderingScheme().isPresent()) {
                return node;
            }

            // Only RANGE frame type currently supported for aggregation functions because it guarantees the
            // same value for each peer group.
            // ROWS frame type requires the ordering to be fully deterministic (e.g. deterministically sorted on all columns)
            if (node.getFrames().stream().map(WindowNode.Frame::getType).anyMatch(type -> type != RANGE)) { // TODO: extract frames of type RANGE and allow optimization on them
                return node;
            }

            // Lookup variables can only be passed through if they are part of the partitioning
            Set<VariableReferenceExpression> partitionByLookupVariables = context.get().getLookupVariables().stream()
                    .filter(node.getPartitionBy()::contains)
                    .collect(toImmutableSet());

            if (partitionByLookupVariables.isEmpty()) {
                return node;
            }

            return context.defaultRewrite(node, new Context(partitionByLookupVariables, context.get().getSuccess()));
        }

        @Override
        public PlanNode visitIndexSource(IndexSourceNode node, RewriteContext<Context> context)
        {
            throw new IllegalStateException("Should not be trying to generate an Index on something that has already been determined to use an Index");
        }

        @Override
        public PlanNode visitIndexJoin(IndexJoinNode node, RewriteContext<Context> context)
        {
            // Lookup variables can only be passed through the probe side of an index join
            Set<VariableReferenceExpression> probeLookupVariables = context.get().getLookupVariables().stream()
                    .filter(node.getProbeSource().getOutputVariables()::contains)
                    .collect(toImmutableSet());

            if (probeLookupVariables.isEmpty()) {
                return node;
            }

            PlanNode rewrittenProbeSource = context.rewrite(node.getProbeSource(), new Context(probeLookupVariables, context.get().getSuccess()));

            PlanNode source = node;
            if (rewrittenProbeSource != node.getProbeSource()) {
                source = new IndexJoinNode(node.getId(), node.getType(), rewrittenProbeSource, node.getIndexSource(), node.getCriteria(), node.getProbeHashVariable(), node.getIndexHashVariable());
            }

            return source;
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, RewriteContext<Context> context)
        {
            // Lookup variables can only be passed through if they are part of the group by columns
            Set<VariableReferenceExpression> groupByLookupVariables = context.get().getLookupVariables().stream()
                    .filter(node.getGroupingKeys()::contains)
                    .collect(toImmutableSet());

            if (groupByLookupVariables.isEmpty()) {
                return node;
            }

            return context.defaultRewrite(node, new Context(groupByLookupVariables, context.get().getSuccess()));
        }

        @Override
        public PlanNode visitSort(SortNode node, RewriteContext<Context> context)
        {
            // Sort has no bearing when building an index, so just ignore the sort
            return context.rewrite(node.getSource(), context.get());
        }

        public static class Context
        {
            private final Set<VariableReferenceExpression> lookupVariables;
            private final AtomicBoolean success;

            public Context(Set<VariableReferenceExpression> lookupVariables, AtomicBoolean success)
            {
                requireNonNull(lookupVariables, "lookupVariables is null");
                checkArgument(!lookupVariables.isEmpty(), "lookupVariables can not be empty");
                this.lookupVariables = ImmutableSet.copyOf(lookupVariables);
                this.success = requireNonNull(success, "success is null");
            }

            public Set<VariableReferenceExpression> getLookupVariables()
            {
                return lookupVariables;
            }

            public AtomicBoolean getSuccess()
            {
                return success;
            }

            public void markSuccess()
            {
                checkState(success.compareAndSet(false, true), "Can only have one success per context");
            }
        }
    }

    /**
     * Identify the mapping from the lookup variables used at the top of the index plan to
     * the actual variables produced by the IndexSource. Note that multiple top-level lookup variables may share the same
     * underlying IndexSource symbol. Also note that lookup variables that do not correspond to underlying index source variables
     * will be omitted from the returned Map.
     */
    public static class IndexKeyTracer
    {
        public static Map<VariableReferenceExpression, VariableReferenceExpression> trace(PlanNode node, Set<VariableReferenceExpression> lookupVariables)
        {
            return node.accept(new Visitor(), lookupVariables);
        }

        private static class Visitor
                extends InternalPlanVisitor<Map<VariableReferenceExpression, VariableReferenceExpression>, Set<VariableReferenceExpression>>
        {
            @Override
            public Map<VariableReferenceExpression, VariableReferenceExpression> visitPlan(PlanNode node, Set<VariableReferenceExpression> lookupVariables)
            {
                throw new UnsupportedOperationException("Node not expected to be part of Index pipeline: " + node);
            }

            @Override
            public Map<VariableReferenceExpression, VariableReferenceExpression> visitProject(ProjectNode node, Set<VariableReferenceExpression> lookupVariables)
            {
                // Map from output Variables to source Variables
                Map<VariableReferenceExpression, VariableReferenceExpression> directVariableTranslationOutputMap = Maps.transformValues(
                        Maps.filterValues(node.getAssignments().getMap(), IndexKeyTracer::isVariable),
                        VariableReferenceExpression.class::cast);
                Map<VariableReferenceExpression, VariableReferenceExpression> outputToSourceMap = lookupVariables.stream()
                        .filter(directVariableTranslationOutputMap.keySet()::contains)
                        .collect(toImmutableMap(identity(), directVariableTranslationOutputMap::get));

                checkState(!outputToSourceMap.isEmpty(), "No lookup variables were able to pass through the projection");

                // Map from source variables to underlying index source variables
                Map<VariableReferenceExpression, VariableReferenceExpression> sourceToIndexMap = node.getSource().accept(this, ImmutableSet.copyOf(outputToSourceMap.values()));

                // Generate the Map the connects lookup variables to underlying index source variables
                Map<VariableReferenceExpression, VariableReferenceExpression> outputToIndexMap = Maps.transformValues(Maps.filterValues(outputToSourceMap, in(sourceToIndexMap.keySet())), Functions.forMap(sourceToIndexMap));
                return ImmutableMap.copyOf(outputToIndexMap);
            }

            @Override
            public Map<VariableReferenceExpression, VariableReferenceExpression> visitFilter(FilterNode node, Set<VariableReferenceExpression> lookupVariables)
            {
                return node.getSource().accept(this, lookupVariables);
            }

            @Override
            public Map<VariableReferenceExpression, VariableReferenceExpression> visitWindow(WindowNode node, Set<VariableReferenceExpression> lookupVariables)
            {
                Set<VariableReferenceExpression> partitionByLookupVariables = lookupVariables.stream()
                        .filter(node.getPartitionBy()::contains)
                        .collect(toImmutableSet());
                checkState(!partitionByLookupVariables.isEmpty(), "No lookup variables were able to pass through the aggregation group by");
                return node.getSource().accept(this, partitionByLookupVariables);
            }

            @Override
            public Map<VariableReferenceExpression, VariableReferenceExpression> visitIndexJoin(IndexJoinNode node, Set<VariableReferenceExpression> lookupVariables)
            {
                Set<VariableReferenceExpression> probeLookupVariables = lookupVariables.stream()
                        .filter(node.getProbeSource().getOutputVariables()::contains)
                        .collect(toImmutableSet());
                checkState(!probeLookupVariables.isEmpty(), "No lookup variables were able to pass through the index join probe source");
                return node.getProbeSource().accept(this, probeLookupVariables);
            }

            @Override
            public Map<VariableReferenceExpression, VariableReferenceExpression> visitAggregation(AggregationNode node, Set<VariableReferenceExpression> lookupVariables)
            {
                Set<VariableReferenceExpression> groupByLookupVariables = lookupVariables.stream()
                        .filter(node.getGroupingKeys()::contains)
                        .collect(toImmutableSet());
                checkState(!groupByLookupVariables.isEmpty(), "No lookup variables were able to pass through the aggregation group by");
                return node.getSource().accept(this, groupByLookupVariables);
            }

            @Override
            public Map<VariableReferenceExpression, VariableReferenceExpression> visitSort(SortNode node, Set<VariableReferenceExpression> lookupVariables)
            {
                return node.getSource().accept(this, lookupVariables);
            }

            @Override
            public Map<VariableReferenceExpression, VariableReferenceExpression> visitIndexSource(IndexSourceNode node, Set<VariableReferenceExpression> lookupVariables)
            {
                checkState(node.getLookupVariables().equals(lookupVariables), "lookupVariables must be the same as IndexSource lookup variables");
                return lookupVariables.stream().collect(toImmutableMap(identity(), identity()));
            }
        }

        private static boolean isVariable(RowExpression expression)
        {
            return expression instanceof VariableReferenceExpression;
        }
    }
}
