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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.expressions.DefaultRowExpressionTraversalVisitor;
import com.facebook.presto.expressions.LogicalRowExpressions;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.JoinTableInfo;
import com.facebook.presto.spi.JoinTableSet;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.DeterminismEvaluator;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.EqualityInference;
import com.facebook.presto.sql.planner.NullabilityAnalyzer;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.AssignmentUtils;
import com.facebook.presto.sql.planner.plan.MultiJoinNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.facebook.presto.SystemSessionProperties.INEQUALITY_JOIN_PUSHDOWN_ENABLED;
import static com.facebook.presto.SystemSessionProperties.isInnerJoinPushdownEnabled;
import static com.facebook.presto.common.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.common.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.common.function.OperatorType.LESS_THAN;
import static com.facebook.presto.common.function.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.common.function.OperatorType.NOT_EQUAL;
import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.expressions.LogicalRowExpressions.and;
import static com.facebook.presto.expressions.LogicalRowExpressions.extractConjuncts;
import static com.facebook.presto.spi.connector.ConnectorCapabilities.SUPPORTS_JOIN_PUSHDOWN;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.sql.planner.VariablesExtractor.extractUnique;
import static com.facebook.presto.sql.planner.optimizations.JoinNodeUtils.toRowExpression;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.filter;
import static java.util.Objects.requireNonNull;

/**
 * GroupInnerJoinsByConnector Optimizer:
 * This optimizer attempts to group TableScanNode's of an inner-join graph that belong to the same connector
 * This allows those connectors that can participate in join pushdown, to rewrite these sources to a new TableScanNode that represents the result of the pushed down join
 *
 * Example:
 * Before Transformation:
 *   --OutputNode
 *      `-- InnerJoin1
 *          |-- InnerJoin2 (Left)
 *          |   |-- TableScanNode1 (Left)
 *          |   |   `-- TableHandle1
 *          |   `-- TableScanNode2 (Right)
 *          |       `-- TableHandle2
 *          `-- TableScanNode3 (Right)
 *              `-- TableHandle3
 *
 * Suppose that TableScanNode1, TableScanNode2 and TableScanNode3 are from the same catalog.
 *
 * After Transformation:
 *   --OutputNode
 *   `-- TableScanNode (with all the details of the three TableScanNodes)
 *       `-- Set<ConnectorTableHandle> (ConnectorHandleSet)
 *          `-- TableHandle1
 *          `-- TableHandle2
 *          `-- TableHandle3
 */
public class GroupInnerJoinsByConnector
        implements PlanOptimizer
{
    private static final Logger logger = Logger.get(GroupInnerJoinsByConnector.class);
    private final FunctionResolution functionResolution;
    private final DeterminismEvaluator determinismEvaluator;
    private final Metadata metadata;
    private boolean isEnabledForTesting;

    public GroupInnerJoinsByConnector(Metadata metadata)
    {
        this.functionResolution = new FunctionResolution(metadata.getFunctionAndTypeManager().getFunctionAndTypeResolver());
        this.determinismEvaluator = new RowExpressionDeterminismEvaluator(metadata.getFunctionAndTypeManager());
        this.metadata = metadata;
    }

    @Override
    public PlanOptimizerResult optimize(PlanNode plan, Session session, TypeProvider types, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        if (isEnabled(session)) {
            PlanNode rewrittenPlan = SimplePlanRewriter.rewriteWith(new Rewriter(functionResolution, determinismEvaluator, idAllocator, metadata, session), plan);
            return PlanOptimizerResult.optimizerResult(rewrittenPlan, !rewrittenPlan.equals(plan));
        }
        return PlanOptimizerResult.optimizerResult(plan, false);
    }

    @Override
    public void setEnabledForTesting(boolean isSet)
    {
        isEnabledForTesting = isSet;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isEnabledForTesting || isInnerJoinPushdownEnabled(session);
    }

    private static class Rewriter
            extends SimplePlanRewriter<Void>
    {
        private final FunctionResolution functionResolution;
        private final DeterminismEvaluator determinismEvaluator;
        private final NullabilityAnalyzer nullabilityAnalyzer;
        private final PlanNodeIdAllocator idAllocator;
        private final Metadata metadata;
        private final LogicalRowExpressions logicalRowExpressions;
        private final Session session;
        private final FunctionAndTypeManager functionAndTypeManager;

        private Rewriter(FunctionResolution functionResolution, DeterminismEvaluator determinismEvaluator, PlanNodeIdAllocator idAllocator, Metadata metadata, Session session)
        {
            this.functionResolution = requireNonNull(functionResolution, "functionResolution is null");
            this.determinismEvaluator = requireNonNull(determinismEvaluator, "determinismEvaluator is null");
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.functionAndTypeManager = metadata.getFunctionAndTypeManager();
            this.logicalRowExpressions = new LogicalRowExpressions(
                    determinismEvaluator,
                    functionResolution,
                    functionAndTypeManager);
            this.nullabilityAnalyzer = new NullabilityAnalyzer(functionAndTypeManager);
            this.session = requireNonNull(session, "session is null");
        }

        private static List<RowExpression> getExpressionsWithinVariableScope(Set<RowExpression> rowExpressions, Set<VariableReferenceExpression> variableScope)
        {
            return rowExpressions.stream()
                    .filter(rowExpression -> Sets.difference(extractUnique(rowExpression), variableScope).isEmpty())
                    .collect(toImmutableList());
        }

        private static boolean isOperation(RowExpression expression, OperatorType type, FunctionAndTypeManager functionAndTypeManager)
        {
            if (expression instanceof CallExpression) {
                CallExpression call = (CallExpression) expression;
                Optional<OperatorType> expressionOperatorType = functionAndTypeManager.getFunctionMetadata(call.getFunctionHandle()).getOperatorType();
                if (expressionOperatorType.isPresent()) {
                    return expressionOperatorType.get() == type;
                }
            }
            return false;
        }

        private static RowExpression getLeft(RowExpression expression)
        {
            checkArgument(expression instanceof CallExpression && ((CallExpression) expression).getArguments().size() == 2, "must be binary call expression");
            return ((CallExpression) expression).getArguments().get(0);
        }

        private static RowExpression getRight(RowExpression expression)
        {
            checkArgument(expression instanceof CallExpression && ((CallExpression) expression).getArguments().size() == 2, "must be binary call expression");
            return ((CallExpression) expression).getArguments().get(1);
        }

        private static Set<VariableReferenceExpression> extractVariableExpressions(RowExpression expression)
        {
            ImmutableSet.Builder<VariableReferenceExpression> builder = ImmutableSet.builder();
            expression.accept(new VariableReferenceBuilderVisitor(), builder);
            return builder.build();
        }

        private static class VariableReferenceBuilderVisitor
                extends DefaultRowExpressionTraversalVisitor<ImmutableSet.Builder<VariableReferenceExpression>>
        {
            @Override
            public Void visitVariableReference(
                    VariableReferenceExpression variable,
                    ImmutableSet.Builder<VariableReferenceExpression> builder)
            {
                builder.add(variable);
                return null;
            }
        }

        @Override
        public PlanNode visitJoin(JoinNode node, RewriteContext<Void> context)
        {
            return getCombinedJoin(node, functionResolution, determinismEvaluator, metadata, session);
        }

        /**
         * This method is only for the top most filter on top of join node.
         * The JoinNodeFlattener#flattenNode should take care of handling intermediate Filter nodes for the rest of the tree
         *
         * If presto gets non equijoin criteria like ( < , >, <=, >=, !=, etc.) for a JoinNode,
         * then this criteria is converted as a filter on top of JoinNode and the JoinNode should not have joincriteria or the joinfilter inside the JoinNode.
         * This kind of filter on top JoinNode is handled here to perform join pushdown
         *
         * @param node
         * @param context
         * @return PlanNode
         */
        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<Void> context)
        {
            if ((node.getSource() instanceof JoinNode)) {
                JoinNode oldJoinNode = (JoinNode) node.getSource();
                ImmutableList.Builder<RowExpression> predicates = ImmutableList.builder();
                predicates.add(node.getPredicate());
                if (oldJoinNode.getFilter().isPresent()) {
                    predicates.add(oldJoinNode.getFilter().get());
                }
                RowExpression expression = logicalRowExpressions.combineConjuncts(predicates.build());
                JoinNode newJoin = new JoinNode(oldJoinNode.getSourceLocation(),
                                                idAllocator.getNextId(),
                                                oldJoinNode.getStatsEquivalentPlanNode(),
                                                oldJoinNode.getType(),
                                                oldJoinNode.getLeft(),
                                                oldJoinNode.getRight(),
                                                oldJoinNode.getCriteria(),
                                                oldJoinNode.getOutputVariables(),
                                                Optional.ofNullable(expression),
                                                oldJoinNode.getLeftHashVariable(),
                                                oldJoinNode.getRightHashVariable(),
                                                oldJoinNode.getDistributionType(),
                                                oldJoinNode.getDynamicFilters());
                PlanNode source = getCombinedJoin(newJoin, functionResolution, determinismEvaluator, metadata, session);
                if (!source.getId().equals(newJoin.getId())) {
                    return source;
                }
            }
            return node;
        }

        private PlanNode getCombinedJoin(JoinNode node, FunctionResolution functionResolution, DeterminismEvaluator determinismEvaluator, Metadata metadata, Session session)
        {
            if (node.getType() == INNER) {
                MultiJoinNode groupInnerJoinsMultiJoinNode = new JoinNodeFlattener(node, functionResolution, determinismEvaluator).toMultiJoinNode();
                MultiJoinNode rewrittenMultiJoinNode = joinPushdownCombineSources(groupInnerJoinsMultiJoinNode, idAllocator, metadata, session);
                if (rewrittenMultiJoinNode.getContainsCombinedSources()) {
                    return createLeftDeepJoinTree(rewrittenMultiJoinNode, idAllocator);
                }
            }
            return node;
        }

        /**
         * Creates a left deep Join tree of CrossJoins, with a FilterNode at the top
         * The final result then needs a predicate pushdown / EliminateCrossJoins pass for the equality criteria to be set
         *
         * @param multiJoinNode
         * @param idAllocator
         * @return
         */
        private PlanNode createLeftDeepJoinTree(MultiJoinNode multiJoinNode, PlanNodeIdAllocator idAllocator)
        {
            PlanNode joinNode = createJoin(0, ImmutableList.copyOf(multiJoinNode.getSources()), idAllocator);
            RowExpression combinedFilters = and(multiJoinNode.getJoinFilter().get(), multiJoinNode.getFilter());
            return new FilterNode(Optional.empty(), idAllocator.getNextId(), joinNode, combinedFilters);
        }

        private PlanNode createJoin(int index, List<PlanNode> sources, PlanNodeIdAllocator idAllocator)
        {
            if (index == sources.size() - 1) {
                return sources.get(index);
            }

            PlanNode leftNode = createJoin(index + 1, sources, idAllocator);
            PlanNode rightNode = sources.get(index);
            return new JoinNode(
                    Optional.empty(),
                    idAllocator.getNextId(),
                    INNER,
                    leftNode,
                    rightNode,
                    ImmutableList.of(),
                    ImmutableList.<VariableReferenceExpression>builder()
                            .addAll(leftNode.getOutputVariables())
                            .addAll(rightNode.getOutputVariables())
                            .build(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    ImmutableMap.of());
        }

        private MultiJoinNode joinPushdownCombineSources(MultiJoinNode multiJoinNode, PlanNodeIdAllocator idAllocator,
                                                         Metadata metadata, Session session)
        {
            LinkedHashSet<PlanNode> rewrittenSources = new LinkedHashSet<>();
            List<RowExpression> overallTableFilter = extractConjuncts(multiJoinNode.getFilter());
            Map<String, List<PlanNode>> sourcesByConnector = new HashMap<>();
            final boolean isInEqualityPushDownEnabled = session.getSystemProperty(INEQUALITY_JOIN_PUSHDOWN_ENABLED, Boolean.class);

             /*
              Here the join push down is happening based on  multiJoinNode.getJoinFilter() criteria.
              JoinQueries that Inference presto to remove join criteria are not able to  push down.
              Join push down should happen only for the tables which have valid join criteria in Presto flow [presto PlanNode]
              For all join where no join criteria is treated as cross join, detailed discussion is available here https://github.ibm.com/lakehouse/tracker/issues/16482
             */

            EqualityInference filterEqualityInference = new EqualityInference.Builder(metadata)
                    .addEqualityInference(multiJoinNode.getJoinFilter().get())
                    .build();
            Iterable<RowExpression> inequalityPredicates = isInEqualityPushDownEnabled ? filter(extractConjuncts(multiJoinNode.getJoinFilter().get()), isInequalityInferenceCandidate()) : ImmutableSet.of();
            AtomicReference<Boolean> wereSourcesRewritten = new AtomicReference<>(false);
            for (PlanNode source : multiJoinNode.getSources()) {
                Optional<String> connectorId = getConnectorIdFromSource(source, session);
                if (connectorId.isPresent()) {
                    // This source can be combined with other 'sources' of the same connector to produce a single TableScanNode
                    sourcesByConnector.computeIfAbsent(connectorId.get(), k -> new ArrayList<>());
                    sourcesByConnector.get(connectorId.get()).add(source);
                }
                else {
                    rewrittenSources.add(source);
                }
            }

            sourcesByConnector.forEach(((connectorId, planNodes) -> {
                PlanNode newSource = getNewTableScanNode(planNodes, filterEqualityInference, inequalityPredicates, rewrittenSources, idAllocator, session);
                if (null != newSource) {
                    wereSourcesRewritten.set(true);
                    rewrittenSources.add(newSource);
                }
            }));

            return new MultiJoinNode(
                    rewrittenSources,
                    and(overallTableFilter),
                    multiJoinNode.getOutputVariables(),
                    multiJoinNode.getAssignments(), wereSourcesRewritten.get(),
                    multiJoinNode.getJoinFilter());
        }

        private Predicate<RowExpression> isInequalityInferenceCandidate()
        {
            return expression -> (isOperation(expression, GREATER_THAN_OR_EQUAL, functionAndTypeManager) ||
                    isOperation(expression, GREATER_THAN, functionAndTypeManager) ||
                    isOperation(expression, LESS_THAN_OR_EQUAL, functionAndTypeManager) ||
                    isOperation(expression, LESS_THAN, functionAndTypeManager) ||
                    isOperation(expression, NOT_EQUAL, functionAndTypeManager)) &&
                    determinismEvaluator.isDeterministic(expression) &&
                    !nullabilityAnalyzer.mayReturnNullOnNonNullInput(expression) &&
                    !getLeft(expression).equals(getRight(expression));
        }

        private PlanNode getNewTableScanNode(List<PlanNode> groupedSources,
                                             EqualityInference filterEqualityInference,
                                             Iterable<RowExpression> inequalityPredicates, LinkedHashSet<PlanNode> rewrittenSources,
                                             PlanNodeIdAllocator idAllocator, Session session)
        {
            /*
             At present, we are not pushing down the ProjectNode if it is not an identity projection.
             All FilterNode is already handled and pushed into overall predicate and no FilterNode will reach here.
             All nodes except TableScanNode that resolved here (FilterNode, ProjectNode, JoinNode, etc.) need not for grouping and join push down
            */
            ImmutableList.Builder<PlanNode> nodesToCombineBuilder = ImmutableList.builder();
            ImmutableList.Builder<PlanNode> joinPushdownSourcesBuilder = ImmutableList.builder();

            groupedSources.forEach(planNode -> {
                if (planNode instanceof TableScanNode) {
                    nodesToCombineBuilder.add(planNode);
                }
                else {
                    rewrittenSources.add(planNode);
                }
            });
            List<PlanNode> nodesToCombine = nodesToCombineBuilder.build();
            if (nodesToCombine.isEmpty()) {
                return null;
            }
            // Build combined output variables
            Set<VariableReferenceExpression> combinedOutputVariables = nodesToCombine.stream()
                    .flatMap(o -> o.getOutputVariables().stream())
                    .collect(Collectors.toSet());

            List<RowExpression> equiJoinFilters = filterEqualityInference.generateEqualitiesPartitionedBy(combinedOutputVariables::contains)
                    .getScopeEqualities();
            Set<RowExpression> inequalityPredicateSet = StreamSupport.stream(inequalityPredicates.spliterator(), false)
                    .collect(Collectors.toSet());
            Map<VariableReferenceExpression, ColumnHandle> groupAssignments = nodesToCombine.stream().map(this::getTableScanNode).map(TableScanNode::getAssignments)
                    .flatMap(map -> map.entrySet().stream()) // Flatten the maps into a stream of entries
                    .collect(Collectors.toMap(
                            entry -> entry.getKey(),
                            entry -> entry.getValue()));

            Set<ConnectorTableHandle> nodeHandles = new HashSet<>();
            TableHandle firstResolvedTableHandle = null;
            Set<JoinTableInfo> joinTableInfos = new HashSet<>();
            for (PlanNode planNode : nodesToCombine) {
                TableHandle resolvedTableHandle = getTableScanNode(planNode).getTable();
                if (firstResolvedTableHandle == null) {
                    firstResolvedTableHandle = resolvedTableHandle;
                }
                ConnectorTableHandle connectorHandle = resolvedTableHandle.getConnectorHandle();
                nodeHandles.add(connectorHandle);
                joinTableInfos.add(new JoinTableInfo(connectorHandle, getTableScanNode(planNode).getAssignments(), planNode.getOutputVariables()));
            }
            JoinTableSet combinedTableHandles = new JoinTableSet(joinTableInfos);
            TableHandle combinedTableHandle = new TableHandle(firstResolvedTableHandle.getConnectorId(),
                    combinedTableHandles,
                    firstResolvedTableHandle.getTransaction(),
                    firstResolvedTableHandle.getLayout());

            List<RowExpression> translatableJoinFilters = new ArrayList<>();
            for (RowExpression filter : equiJoinFilters) {
                if (metadata.isPushdownSupportedForFilter(session, combinedTableHandle, filter, groupAssignments)) {
                    translatableJoinFilters.add(filter);
                }
            }

            List<RowExpression> scopedInequalities = getExpressionsWithinVariableScope(inequalityPredicateSet, combinedOutputVariables);
            for (RowExpression nonEquiFilter : scopedInequalities) {
                if (metadata.isPushdownSupportedForFilter(session, combinedTableHandle, nonEquiFilter, groupAssignments)) {
                    translatableJoinFilters.add(nonEquiFilter);
                }
            }

            RowExpression joinFilters = and(translatableJoinFilters);
            Set<VariableReferenceExpression> referredVariables = extractVariableExpressions(joinFilters);

            if (referredVariables.isEmpty()) {
                rewrittenSources.addAll(nodesToCombine);
            }

            nodesToCombine.forEach(node -> {
                if (node.getOutputVariables().stream().anyMatch(referredVariables::contains)) {
                    // At least one of the output variables of this node was involved in join with another source
                    // So there is a valid JOIN with one of the other sources
                    joinPushdownSourcesBuilder.add(node);
                }
                else {
                    rewrittenSources.add(node);
                }
            });
            List<PlanNode> joinPushdownSources = joinPushdownSourcesBuilder.build();

            // At least two source required for Join
            if (joinPushdownSources.isEmpty()) {
                return null;
            }
            else if (joinPushdownSources.size() == 1) {
                rewrittenSources.add(joinPushdownSources.get(0));
                return null;
            }

            /*
             At this point we should have
             1. All the table references that belong to the same connector that need to be combined
             2. All the predicates that refer to these tables
             3. A list of overall output variables
             We can now build our new TableScanNode which represents the join pushed down tables and the final TableScanNode could create at connector level
            */
            return buildSingleTableScan(joinPushdownSources, idAllocator);
        }

        private PlanNode buildSingleTableScan(List<PlanNode> groupNodes, PlanNodeIdAllocator idAllocator)
        {
            // Build a set of individual TableHandles that need to be combined
            TableHandle firstResolvedTableHandle = null;
            TableScanNode firstResolvedTableScanNode = null;
            ImmutableSet.Builder<JoinTableInfo> builder = ImmutableSet.builder();
            // Get over all output variables and assignments from grouped TableScanNode
            List<VariableReferenceExpression> outputVariables = new ArrayList<>();
            Map<VariableReferenceExpression, ColumnHandle> assignments = new HashMap<>();
            for (PlanNode groupNode : groupNodes) {
                TableScanNode tableScanNode = getTableScanNode(groupNode);
                TableHandle tableHandle = tableScanNode.getTable();
                if (firstResolvedTableHandle == null) {
                    firstResolvedTableHandle = tableHandle;
                    firstResolvedTableScanNode = tableScanNode;
                }

                outputVariables.addAll(tableScanNode.getOutputVariables());
                assignments.putAll(tableScanNode.getAssignments());
                builder.add(new JoinTableInfo(tableHandle.getConnectorHandle(), tableScanNode.getAssignments(), tableScanNode.getOutputVariables()));
            }

            // Build an new TableHandle that represents the combined set of TableHandles
            TableHandle updatedTableHandle = new TableHandle(firstResolvedTableHandle.getConnectorId(),
                    new JoinTableSet(builder.build()),
                    firstResolvedTableHandle.getTransaction(),
                    firstResolvedTableHandle.getLayout(),
                    firstResolvedTableHandle.getDynamicFilter());

            return new TableScanNode(Optional.empty(),
                    idAllocator.getNextId(),
                    updatedTableHandle,
                    outputVariables,
                    assignments,
                    firstResolvedTableScanNode.getCurrentConstraint(),
                    firstResolvedTableScanNode.getEnforcedConstraint(),
                    firstResolvedTableScanNode.getCteMaterializationInfo());
        }

        private TableScanNode getTableScanNode(PlanNode planNode)
        {
            while (!(planNode instanceof TableScanNode)) {
                planNode = planNode.getSources().get(0);
            }
            return (TableScanNode) planNode;
        }

        /**
         * For a join source, see if we can resolve it to TableScanNode and if it resolves to TableScanNode
         * then get it connector and check connector capabilities for join push down.
         * This will only happen iff the parent hierarchy only contains {Project, Filter, TableScanNode}'s as the parent's
         *
         * @param resolved
         * @param session
         * @return Optional<String>
         */
        private Optional<String> getConnectorIdFromSource(PlanNode resolved, Session session)
        {
            if (resolved instanceof ProjectNode) {
                return getConnectorIdFromSource(((ProjectNode) resolved).getSource(), session);
            }
            else if (resolved instanceof FilterNode) {
                return getConnectorIdFromSource(((FilterNode) resolved).getSource(), session);
            }
            else if (resolved instanceof TableScanNode) {
                TableScanNode ts = (TableScanNode) resolved;
                ConnectorId connectorId = ts.getTable().getConnectorId();
                boolean supportsJoinPushDown = metadata.getConnectorCapabilities(session, connectorId).contains(SUPPORTS_JOIN_PUSHDOWN);
                if (supportsJoinPushDown) {
                    return Optional.of(connectorId.toString());
                }
            }
            return Optional.empty();
        }
    }

    @VisibleForTesting
    private static class JoinNodeFlattener
    {
        private final LinkedHashSet<PlanNode> sources = new LinkedHashSet<>();
        private List<RowExpression> joinCriteriaFilters = new ArrayList<>();
        private List<RowExpression> filters = new ArrayList<>();
        private final List<VariableReferenceExpression> outputVariables;
        private final FunctionResolution functionResolution;
        private final DeterminismEvaluator determinismEvaluator;
        private final boolean connectorJoinNode = false;

        JoinNodeFlattener(PlanNode node, FunctionResolution functionResolution, DeterminismEvaluator determinismEvaluator)
        {
            requireNonNull(node, "node is null");
            this.outputVariables = node.getOutputVariables();
            this.functionResolution = requireNonNull(functionResolution, "functionResolution is null");
            this.determinismEvaluator = requireNonNull(determinismEvaluator, "determinismEvaluator is null");

            flattenNode(node);
        }

        private void flattenNode(PlanNode resolved)
        {
            if (resolved instanceof FilterNode) {
            /*
                We pull up all Filters to the top of the join graph, these will be pushed down again by predicate pushdown
                We do this in hope of surfacing any TableScan nodes that can be combined
            */
                FilterNode filterNode = (FilterNode) resolved;
                filters.add(filterNode.getPredicate());
                flattenNode(filterNode.getSource());
                return;
            }

            if (!(resolved instanceof JoinNode)) {
                if (resolved instanceof ProjectNode) {
                    /*
                        Certain ProjectNodes can be 'inlined' into the parent TableScan, e.g a CAST expression
                        We will do this here while flattening the JoinNode if possible
                        For now, we log the fact that we saw a ProjectNode and if identity projection, will continue
                    */
                    //Only identity projections can be handled.
                    if (AssignmentUtils.isIdentity(((ProjectNode) resolved).getAssignments())) {
                        flattenNode(((ProjectNode) resolved).getSource());
                        return;
                    }
                }
                sources.add(resolved);
                return;
            }
            JoinNode joinNode = (JoinNode) resolved;
            if (joinNode.getType() != INNER || !determinismEvaluator.isDeterministic(joinNode.getFilter().orElse(TRUE_CONSTANT))) {
                sources.add(resolved);
                return;
            }

            flattenNode(joinNode.getLeft());
            flattenNode(joinNode.getRight());
            joinNode.getCriteria().stream()
                    .map(criteria -> toRowExpression(criteria, functionResolution))
                    .forEach(joinCriteriaFilters::add);
            joinNode.getFilter().ifPresent(joinCriteriaFilters::add);
        }

        MultiJoinNode toMultiJoinNode()
        {
            ImmutableSet<VariableReferenceExpression> inputVariables = sources.stream().flatMap(source -> source.getOutputVariables().stream()).collect(toImmutableSet());
            /*
                We do this to satisfy the invariant that the rewritten Join node must produce the same output variables as the input Join node
            */
            ImmutableSet.Builder<VariableReferenceExpression> updatedOutputVariables = ImmutableSet.builder();

            for (VariableReferenceExpression outputVariable : outputVariables) {
                if (inputVariables.contains(outputVariable)) {
                    updatedOutputVariables.add(outputVariable);
                }
            }

            return new MultiJoinNode(sources,
                    and(filters),
                    updatedOutputVariables.build().asList(), Assignments.of(), connectorJoinNode, Optional.of(and(joinCriteriaFilters)));
        }
    }
}
