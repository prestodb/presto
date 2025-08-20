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
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.expressions.LogicalRowExpressions;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.ResolvedIndex;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.IndexSourceNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.SortNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.WindowNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.DomainTranslator.ExtractionResult;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.SimplePlanVisitor;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.IndexJoinNode;
import com.facebook.presto.sql.planner.plan.InternalPlanVisitor;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.facebook.presto.sql.relational.RowExpressionDomainTranslator;
import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.expressions.LogicalRowExpressions.extractConjuncts;
import static com.facebook.presto.spi.function.FunctionKind.AGGREGATE;
import static com.facebook.presto.spi.plan.WindowNode.Frame.WindowType.RANGE;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.identityAssignments;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.in;
import static com.google.common.collect.ImmutableList.toImmutableList;
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
    public PlanOptimizerResult optimize(
            PlanNode plan,
            Session session,
            TypeProvider type,
            VariableAllocator variableAllocator,
            PlanNodeIdAllocator idAllocator,
            WarningCollector warningCollector)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(variableAllocator, "variableAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");

        IndexJoinRewriter rewriter;
        if (SystemSessionProperties.isNativeExecutionEnabled(session)) {
            rewriter = new NativeIndexJoinRewriter(idAllocator, metadata, session);
        }
        else {
            rewriter = new DefaultIndexJoinRewriter(idAllocator, metadata, session);
        }
        PlanNode rewrittenPlan = SimplePlanRewriter.rewriteWith(rewriter, plan, null);
        return PlanOptimizerResult.optimizerResult(rewrittenPlan, rewriter.isPlanChanged());
    }

    private abstract static class IndexJoinRewriter
            extends SimplePlanRewriter<Void>
    {
        protected final PlanNodeIdAllocator idAllocator;
        protected final Metadata metadata;
        protected final Session session;
        protected boolean planChanged;

        protected IndexJoinRewriter(PlanNodeIdAllocator idAllocator, Metadata metadata, Session session)
        {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.session = requireNonNull(session, "session is null");
        }

        public boolean isPlanChanged()
        {
            return planChanged;
        }

        protected static List<IndexJoinNode.EquiJoinClause> createEquiJoinClause(
                List<VariableReferenceExpression> probeVariables,
                List<VariableReferenceExpression> indexVariables)
        {
            checkArgument(probeVariables.size() == indexVariables.size());
            ImmutableList.Builder<IndexJoinNode.EquiJoinClause> builder = ImmutableList.builder();
            for (int i = 0; i < probeVariables.size(); i++) {
                builder.add(new IndexJoinNode.EquiJoinClause(probeVariables.get(i), indexVariables.get(i)));
            }
            return builder.build();
        }

        protected static PlanNode createIndexJoinWithExpectedOutputs(
                List<VariableReferenceExpression> expectedOutputs,
                PlanNode probe,
                PlanNode index,
                List<IndexJoinNode.EquiJoinClause> equiJoinClause,
                Optional<RowExpression> filter,
                List<VariableReferenceExpression> lookupVariables,
                PlanNodeIdAllocator idAllocator)
        {
            PlanNode result = new IndexJoinNode(
                    index.getSourceLocation(),
                    idAllocator.getNextId(),
                    JoinType.SOURCE_OUTER,
                    probe,
                    index,
                    equiJoinClause,
                    filter,
                    Optional.empty(),
                    Optional.empty(),
                    lookupVariables);
            if (!result.getOutputVariables().equals(expectedOutputs)) {
                result = new ProjectNode(
                        idAllocator.getNextId(),
                        result,
                        identityAssignments(expectedOutputs));
            }
            return result;
        }
    }

    private static class DefaultIndexJoinRewriter
            extends IndexJoinRewriter
    {
        private DefaultIndexJoinRewriter(PlanNodeIdAllocator idAllocator, Metadata metadata, Session session)
        {
            super(idAllocator, metadata, session);
        }

        @Override
        public PlanNode visitJoin(JoinNode node, RewriteContext<Void> context)
        {
            PlanNode leftRewritten = context.rewrite(node.getLeft());
            PlanNode rightRewritten = context.rewrite(node.getRight());

            if (!node.getCriteria().isEmpty()) { // Index join only possible with JOIN criteria
                List<VariableReferenceExpression> leftJoinVariables = Lists.transform(node.getCriteria(), EquiJoinClause::getLeft);
                List<VariableReferenceExpression> rightJoinVariables = Lists.transform(node.getCriteria(), EquiJoinClause::getRight);

                Optional<PlanNode> leftIndexCandidate = IndexSourceRewriter.rewriteWithIndex(
                        leftRewritten,
                        ImmutableSet.copyOf(leftJoinVariables),
                        idAllocator,
                        metadata,
                        session);
                if (leftIndexCandidate.isPresent()) {
                    // Sanity check that we can trace the path for the index lookup key
                    Map<VariableReferenceExpression, VariableReferenceExpression> trace
                            = IndexKeyTracer.trace(leftIndexCandidate.get(), ImmutableSet.copyOf(leftJoinVariables));
                    checkState(!trace.isEmpty() && ImmutableSet.copyOf(leftJoinVariables).containsAll(trace.keySet()));
                }

                Optional<PlanNode> rightIndexCandidate = IndexSourceRewriter.rewriteWithIndex(
                        rightRewritten,
                        ImmutableSet.copyOf(rightJoinVariables),
                        idAllocator,
                        metadata,
                        session);
                if (rightIndexCandidate.isPresent()) {
                    // Sanity check that we can trace the path for the index lookup key
                    Map<VariableReferenceExpression, VariableReferenceExpression> trace
                            = IndexKeyTracer.trace(rightIndexCandidate.get(), ImmutableSet.copyOf(rightJoinVariables));
                    checkState(!trace.isEmpty() && ImmutableSet.copyOf(rightJoinVariables).containsAll(trace.keySet()));
                }

                switch (node.getType()) {
                    case INNER:
                        // Prefer the right candidate over the left candidate
                        PlanNode indexJoinNode = null;
                        if (rightIndexCandidate.isPresent()) {
                            indexJoinNode = new IndexJoinNode(
                                    leftRewritten.getSourceLocation(),
                                    idAllocator.getNextId(),
                                    JoinType.INNER,
                                    leftRewritten,
                                    rightIndexCandidate.get(),
                                    createEquiJoinClause(leftJoinVariables, rightJoinVariables),
                                    node.getFilter(),
                                    Optional.empty(),
                                    Optional.empty(),
                                    ImmutableList.copyOf(rightJoinVariables));
                        }
                        else if (leftIndexCandidate.isPresent()) {
                            indexJoinNode = new IndexJoinNode(
                                    rightRewritten.getSourceLocation(),
                                    idAllocator.getNextId(),
                                    JoinType.INNER,
                                    rightRewritten,
                                    leftIndexCandidate.get(),
                                    createEquiJoinClause(rightJoinVariables, leftJoinVariables),
                                    node.getFilter(),
                                    Optional.empty(),
                                    Optional.empty(),
                                    ImmutableList.copyOf(leftJoinVariables));
                        }

                        if (indexJoinNode != null) {
                            if (node.getFilter().isPresent()) {
                                indexJoinNode = new FilterNode(node.getSourceLocation(), idAllocator.getNextId(), indexJoinNode, node.getFilter().get());
                            }

                            if (!indexJoinNode.getOutputVariables().equals(node.getOutputVariables())) {
                                indexJoinNode = new ProjectNode(
                                        idAllocator.getNextId(),
                                        indexJoinNode,
                                        identityAssignments(node.getOutputVariables()));
                            }

                            planChanged = true;
                            return indexJoinNode;
                        }
                        break;

                    case LEFT:
                        // We cannot use indices for outer joins until index join supports in-line filtering
                        if (!node.getFilter().isPresent() && rightIndexCandidate.isPresent()) {
                            planChanged = true;
                            return createIndexJoinWithExpectedOutputs(
                                    node.getOutputVariables(),
                                    leftRewritten,
                                    rightIndexCandidate.get(),
                                    createEquiJoinClause(leftJoinVariables, rightJoinVariables),
                                    node.getFilter(),
                                    ImmutableList.copyOf(rightJoinVariables),
                                    idAllocator);
                        }
                        break;

                    case RIGHT:
                        // We cannot use indices for outer joins until index join supports in-line filtering
                        if (!node.getFilter().isPresent() && leftIndexCandidate.isPresent()) {
                            planChanged = true;
                            return createIndexJoinWithExpectedOutputs(
                                    node.getOutputVariables(),
                                    rightRewritten,
                                    leftIndexCandidate.get(),
                                    createEquiJoinClause(rightJoinVariables, leftJoinVariables),
                                    node.getFilter(),
                                    ImmutableList.copyOf(leftJoinVariables),
                                    idAllocator);
                        }
                        break;

                    case FULL:
                        break;

                    default:
                        throw new IllegalArgumentException("Unknown type: " + node.getType());
                }
            }

            if (leftRewritten != node.getLeft() || rightRewritten != node.getRight()) {
                planChanged = true;
                return new JoinNode(
                        node.getSourceLocation(),
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
    }

    private static class NativeIndexJoinRewriter
            extends IndexJoinRewriter
    {
        private NativeIndexJoinRewriter(PlanNodeIdAllocator idAllocator, Metadata metadata, Session session)
        {
            super(idAllocator, metadata, session);
        }

        @Override
        public PlanNode visitJoin(JoinNode node, RewriteContext<Void> context)
        {
            PlanNode leftRewritten = context.rewrite(node.getLeft());
            PlanNode rightRewritten = context.rewrite(node.getRight());

            StandardFunctionResolution functionResolution = new FunctionResolution(metadata.getFunctionAndTypeManager().getFunctionAndTypeResolver());
            LookupVariableExtractor.Context leftExtractorContext = new LookupVariableExtractor.Context(new HashSet<>(), functionResolution);
            LookupVariableExtractor.Context rightExtractorContext = new LookupVariableExtractor.Context(new HashSet<>(), functionResolution);

            Set<VariableReferenceExpression> leftLookupVariables = leftExtractorContext.getLookupVariables();
            Set<VariableReferenceExpression> rightLookupVariables = rightExtractorContext.getLookupVariables();

            // Extract non-equal join keys.
            if (node.getFilter().isPresent()) {
                LookupVariableExtractor.Context commonExtractorContext = new LookupVariableExtractor.Context(new HashSet<>(), functionResolution);
                LookupVariableExtractor.extractFromFilter(node.getFilter().get(), commonExtractorContext);
                if (commonExtractorContext.isEligible()) {
                    leftLookupVariables.addAll(commonExtractorContext.getLookupVariables());
                    rightLookupVariables.addAll(commonExtractorContext.getLookupVariables());
                }
                else {
                    return node;
                }
            }

            // Extract equal Join keys.
            List<VariableReferenceExpression> leftEqualJoinVariables;
            List<VariableReferenceExpression> rightEqualJoinVariables;
            if (node.getCriteria().isEmpty()) {
                leftEqualJoinVariables = ImmutableList.of();
                rightEqualJoinVariables = ImmutableList.of();
            }
            else {
                leftEqualJoinVariables = node.getCriteria().stream().map(EquiJoinClause::getLeft).collect(toImmutableList());
                rightEqualJoinVariables = node.getCriteria().stream().map(EquiJoinClause::getRight).collect(toImmutableList());
                leftLookupVariables.addAll(leftEqualJoinVariables);
                rightLookupVariables.addAll(rightEqualJoinVariables);
            }

            // Extract Join keys from pushed-down filters.
            LookupVariableExtractor.extractFromSubPlan(node.getLeft(), leftExtractorContext);
            LookupVariableExtractor.extractFromSubPlan(node.getRight(), rightExtractorContext);

            Optional<PlanNode> leftIndexCandidate;
            if (leftExtractorContext.isEligible() && !leftExtractorContext.getLookupVariables().isEmpty()) {
                leftIndexCandidate = IndexSourceRewriter.rewriteWithIndex(
                        leftRewritten,
                        leftLookupVariables,
                        idAllocator,
                        metadata,
                        session);
            }
            else {
                leftIndexCandidate = Optional.empty();
            }

            if (leftIndexCandidate.isPresent()) {
                // Sanity check that we can trace the path for the index lookup key
                Map<VariableReferenceExpression, VariableReferenceExpression> trace = IndexKeyTracer.trace(leftIndexCandidate.get(), leftLookupVariables);
                checkState(!trace.isEmpty() && leftLookupVariables.containsAll(trace.keySet()));
            }

            Optional<PlanNode> rightIndexCandidate;
            if (rightExtractorContext.isEligible() && !rightExtractorContext.getLookupVariables().isEmpty()) {
                rightIndexCandidate = IndexSourceRewriter.rewriteWithIndex(
                        rightRewritten,
                        rightLookupVariables,
                        idAllocator,
                        metadata,
                        session);
            }
            else {
                rightIndexCandidate = Optional.empty();
            }

            if (rightIndexCandidate.isPresent()) {
                // Sanity check that we can trace the path for the index lookup key
                Map<VariableReferenceExpression, VariableReferenceExpression> trace = IndexKeyTracer.trace(rightIndexCandidate.get(), rightLookupVariables);
                checkState(!trace.isEmpty() && rightLookupVariables.containsAll(trace.keySet()));
            }

            switch (node.getType()) {
                // Only INNER and LEFT joins are supported on native.
                case INNER:
                    // Prefer the right candidate over the left candidate
                    PlanNode indexJoinNode = null;
                    if (rightIndexCandidate.isPresent()) {
                        indexJoinNode = new IndexJoinNode(
                                leftRewritten.getSourceLocation(),
                                idAllocator.getNextId(),
                                JoinType.INNER,
                                leftRewritten,
                                rightIndexCandidate.get(),
                                createEquiJoinClause(leftEqualJoinVariables, rightEqualJoinVariables),
                                node.getFilter(),
                                Optional.empty(),
                                Optional.empty(),
                                ImmutableList.copyOf(rightLookupVariables));
                    }
                    else if (leftIndexCandidate.isPresent()) {
                        indexJoinNode = new IndexJoinNode(
                                rightRewritten.getSourceLocation(),
                                idAllocator.getNextId(),
                                JoinType.INNER,
                                rightRewritten,
                                leftIndexCandidate.get(),
                                createEquiJoinClause(rightEqualJoinVariables, leftEqualJoinVariables),
                                node.getFilter(),
                                Optional.empty(),
                                Optional.empty(),
                                ImmutableList.copyOf(leftLookupVariables));
                    }

                    if (indexJoinNode != null) {
                        if (!indexJoinNode.getOutputVariables().equals(node.getOutputVariables())) {
                            indexJoinNode = new ProjectNode(
                                    idAllocator.getNextId(),
                                    indexJoinNode,
                                    identityAssignments(node.getOutputVariables()));
                        }

                        planChanged = true;
                        return indexJoinNode;
                    }
                    break;

                case LEFT:
                    if (rightIndexCandidate.isPresent()) {
                        planChanged = true;
                        return createIndexJoinWithExpectedOutputs(
                                node.getOutputVariables(),
                                leftRewritten,
                                rightIndexCandidate.get(),
                                createEquiJoinClause(leftEqualJoinVariables, rightEqualJoinVariables),
                                node.getFilter(),
                                ImmutableList.copyOf(rightLookupVariables),
                                idAllocator);
                    }
                    break;

                case RIGHT:
                    if (leftIndexCandidate.isPresent()) {
                        planChanged = true;
                        return createIndexJoinWithExpectedOutputs(
                                node.getOutputVariables(),
                                rightRewritten,
                                leftIndexCandidate.get(),
                                createEquiJoinClause(rightEqualJoinVariables, leftEqualJoinVariables),
                                node.getFilter(),
                                ImmutableList.copyOf(leftLookupVariables),
                                idAllocator);
                    }
                    break;

                case FULL:
                    break;

                default:
                    throw new IllegalArgumentException("Unknown type: " + node.getType());
            }

            if (leftRewritten != node.getLeft() || rightRewritten != node.getRight()) {
                planChanged = true;
                return new JoinNode(
                        node.getSourceLocation(),
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
                    new FunctionResolution(metadata.getFunctionAndTypeManager().getFunctionAndTypeResolver()),
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

            if (!ImmutableSet.copyOf(node.getOutputVariables()).containsAll(context.getLookupVariables())) {
                // Lookup variable is not present in the plan. Index join is not possible.
                return node;
            }

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
                    node.getSourceLocation(),
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
                // TODO: it is likely we end up with redundant filters here because the predicate push down has already been run... the fix is to run predicate push down again
                source = new FilterNode(source.getSourceLocation(), idAllocator.getNextId(), source, resultingPredicate);
            }
            context.markSuccess();
            return source;
        }

        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<Context> context)
        {
            if (SystemSessionProperties.isNativeExecutionEnabled(session)) {
                // Preserve the lookup variables for native execution.
                ProjectNode rewrittenNode = (ProjectNode) context.defaultRewrite(node, context.get());
                Set<VariableReferenceExpression> directVariables = Maps.filterValues(node.getAssignments().getMap(), IndexJoinOptimizer::isVariable).keySet();
                if (!directVariables.containsAll(context.get().getLookupVariables())) {
                    Assignments.Builder newAssignments = Assignments.builder();
                    for (VariableReferenceExpression variable : context.get().getLookupVariables()) {
                        newAssignments.put(variable, variable);
                    }
                    for (Map.Entry<VariableReferenceExpression, RowExpression> entry : node.getAssignments().entrySet()) {
                        if (!context.get().lookupVariables.contains(entry.getKey())) {
                            newAssignments.put(entry);
                        }
                    }
                    return new ProjectNode(rewrittenNode.getSourceLocation(),
                            rewrittenNode.getId(),
                            rewrittenNode.getStatsEquivalentPlanNode(),
                            rewrittenNode.getSource(),
                            newAssignments.build(),
                            rewrittenNode.getLocality());
                }
                return rewrittenNode;
            }
            // Rewrite the lookup variables in terms of only the pre-projected variables that have direct translations
            ImmutableSet.Builder<VariableReferenceExpression> newLookupVariablesBuilder = ImmutableSet.builder();
            for (VariableReferenceExpression variable : context.get().getLookupVariables()) {
                RowExpression expression = node.getAssignments().get(variable);
                if (isVariable(expression)) {
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
                source = new IndexJoinNode(
                        rewrittenProbeSource.getSourceLocation(),
                        node.getId(),
                        node.getType(),
                        rewrittenProbeSource,
                        node.getIndexSource(),
                        node.getCriteria(),
                        node.getFilter(),
                        node.getProbeHashVariable(),
                        node.getIndexHashVariable(),
                        node.getLookupVariables());
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

    public static class LookupVariableExtractor
            extends SimplePlanVisitor<LookupVariableExtractor.Context>
    {
        public static class Context
        {
            private final Set<VariableReferenceExpression> lookupVariables;
            private final StandardFunctionResolution standardFunctionResolution;
            private boolean eligible = true;

            public Context(Set<VariableReferenceExpression> lookupVariables, StandardFunctionResolution standardFunctionResolution)
            {
                this.lookupVariables = requireNonNull(lookupVariables, "lookupVariables is null");
                this.standardFunctionResolution = requireNonNull(standardFunctionResolution, "standardFunctionResolution is null");
            }

            public Set<VariableReferenceExpression> getLookupVariables()
            {
                return lookupVariables;
            }

            public StandardFunctionResolution getStandardFunctionResolution()
            {
                return standardFunctionResolution;
            }

            public boolean isEligible()
            {
                return eligible;
            }

            public void markIneligible()
            {
                eligible = false;
            }

            @Override
            public String toString()
            {
                return "Context{" +
                        "lookupVariables=" + lookupVariables +
                        ", eligible=" + eligible +
                        '}';
            }
        }

        // Traverse the non-equal join condition and extract the lookup variables.
        private static void extractFromFilter(RowExpression expression, Context context)
        {
            List<RowExpression> conjuncts = extractConjuncts(expression);
            for (RowExpression conjunct : conjuncts) {
                // Index lookup condition only supports Equal, BETWEEN and CONTAINS.
                if (!(conjunct instanceof CallExpression)) {
                    continue;
                }

                CallExpression callExpression = (CallExpression) conjunct;
                if (context.getStandardFunctionResolution().isEqualsFunction(callExpression.getFunctionHandle())
                        && callExpression.getArguments().size() == 2) {
                    RowExpression leftArg = callExpression.getArguments().get(0);
                    RowExpression rightArg = callExpression.getArguments().get(1);

                    VariableReferenceExpression variable = null;
                    // Check for pattern: constant = variable or variable = constant.
                    if (isConstant(leftArg) && isVariable(rightArg)) {
                        variable = (VariableReferenceExpression) rightArg;
                    }
                    else if (isVariable(leftArg) && isConstant(rightArg)) {
                        variable = (VariableReferenceExpression) leftArg;
                    }

                    if (variable != null) {
                        // It is a lookup equal condition only when it's variable=constant.
                        context.getLookupVariables().add(variable);
                    }
                }
                else if (context.getStandardFunctionResolution().isBetweenFunction(callExpression.getFunctionHandle())
                        && isVariable(callExpression.getArguments().get(0))) {
                    context.getLookupVariables().add((VariableReferenceExpression) callExpression.getArguments().get(0));
                }
                else if (callExpression.getDisplayName().equalsIgnoreCase("CONTAINS")
                        && callExpression.getArguments().size() == 2
                        && isVariable(callExpression.getArguments().get(1))) {
                    context.getLookupVariables().add((VariableReferenceExpression) callExpression.getArguments().get(1));
                }
            }
        }

        public static void extractFromSubPlan(PlanNode node, Context context)
        {
            node.accept(new LookupVariableExtractor(), context);
        }

        @Override
        public Void visitPlan(PlanNode node, Context context)
        {
            // Node isn't expected to be part of the index pipeline.
            context.markIneligible();
            return null;
        }

        @Override
        public Void visitProject(ProjectNode node, Context context)
        {
            node.getSource().accept(this, context);
            return null;
        }

        @Override
        public Void visitFilter(FilterNode node, Context context)
        {
            if (node.getSource() instanceof TableScanNode) {
                extractFromFilter(node.getPredicate(), context);
                return null;
            }
            return node.getSource().accept(this, context);
        }

        @Override
        public Void visitTableScan(TableScanNode node, Context context)
        {
            return null;
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
                        Maps.filterValues(node.getAssignments().getMap(), IndexJoinOptimizer::isVariable),
                        VariableReferenceExpression.class::cast);
                Map<VariableReferenceExpression, VariableReferenceExpression> outputToSourceMap = lookupVariables.stream()
                        .filter(directVariableTranslationOutputMap.keySet()::contains)
                        .collect(toImmutableMap(identity(), directVariableTranslationOutputMap::get));

                checkState(!outputToSourceMap.isEmpty(), "No lookup variables were able to pass through the projection");

                // Map from source variables to underlying index source variables
                Map<VariableReferenceExpression, VariableReferenceExpression> sourceToIndexMap = node.getSource().accept(this, ImmutableSet.copyOf(outputToSourceMap.values()));

                // Generate the Map the connects lookup variables to underlying index source variables
                Map<VariableReferenceExpression, VariableReferenceExpression> outputToIndexMap = Maps.transformValues(
                        Maps.filterValues(outputToSourceMap, in(sourceToIndexMap.keySet())), Functions.forMap(sourceToIndexMap));
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
    }

    private static boolean isVariable(RowExpression expression)
    {
        return expression instanceof VariableReferenceExpression;
    }

    private static boolean isConstant(RowExpression expression)
    {
        return expression instanceof ConstantExpression;
    }
}
