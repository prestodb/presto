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
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.expressions.LogicalRowExpressions;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.VariablesExtractor;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slices;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.facebook.presto.SystemSessionProperties.isOptimizePayloadJoins;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.TypeUtils.isNumericType;
import static com.facebook.presto.spi.plan.AggregationNode.Step.SINGLE;
import static com.facebook.presto.spi.plan.AggregationNode.singleGroupingSet;
import static com.facebook.presto.spi.plan.JoinType.LEFT;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IS_NULL;
import static com.facebook.presto.sql.planner.PlannerUtils.addProjections;
import static com.facebook.presto.sql.planner.PlannerUtils.clonePlanNode;
import static com.facebook.presto.sql.planner.PlannerUtils.coalesce;
import static com.facebook.presto.sql.planner.PlannerUtils.equalityPredicate;
import static com.facebook.presto.sql.planner.PlannerUtils.isScanFilterProject;
import static com.facebook.presto.sql.planner.PlannerUtils.restrictOutput;
import static com.facebook.presto.sql.planner.plan.ChildReplacer.replaceChildren;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.Expressions.specialForm;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Sets.intersection;
import static java.util.Objects.requireNonNull;

/*
This optimization targets long chains of LOJ, where a large base table is extended with columns from medium-sized (not broadcastable) tables.
We rewrite a query of the form:

SELECT T.*, S1.A, S2.B...
FROM
T
LOJ S1 ON T.k1 = S1.k
LOJ S2 ON T.k2 = S2.k
LOJ S3 ON T.k3 = S3.k
...
LOJ Sn ON T.kn = Sn.k

into  something like this:

SELECT T.*, S1.A, S2.B, ...
FROM
(
(SELECT DISTINCT k1, k2, .. kn,
k1 IS NULL as k1_null,
k2 IS NULL as k2_null,
k3 IS NULL as k3_null,
…
kn IS NULL as kn_null
FROM T1 ) AS T_keys

LOJ S1 ON T_keys.k1 = S1.k
LOJ S2 ON T_keys.k2 = S2.k
LOJ S3 ON T_keys.k3 = S3.k
...
LOJ Sn ON T_keys.kn = Sn.k)
ROJ  T
ON
T.k1 IS NULL = k1_null AND
T.k2 IS NULL = k2_null AND
T.k3 IS NULL = k3_null AND
...
T.kn IS NULL = kn_null AND
COALESCE(T.k1, ‘’)  = COALESCE(T_keys.k1, ‘’) AND
COALESCE(T.k2, ‘’)  = COALESCE(T_keys.k2, ‘’) AND
COALESCE(T.k3, ‘’)  = COALESCE(T_keys.k3, ‘’) AND
...
COALESCE(T.kn, ‘’)  = COALESCE(T_keys.kn, ‘’)

 */
public class PayloadJoinOptimizer
        implements PlanOptimizer
{
    private final Metadata metadata;
    private boolean isEnabledForTesting;

    public PayloadJoinOptimizer(Metadata metadata)
    {
        requireNonNull(metadata, "metadata is null");

        this.metadata = metadata;
    }

    @Override
    public PlanOptimizerResult optimize(PlanNode plan, Session session, TypeProvider types, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        FunctionAndTypeManager functionAndTypeManager = metadata.getFunctionAndTypeManager();
        if (isEnabled(session)) {
            PlanNode flattenedPlan = flattenJoinChains(plan, idAllocator);
            Rewriter rewriter = new PayloadJoinOptimizer.Rewriter(session, this.metadata, types, functionAndTypeManager, idAllocator, variableAllocator);
            PlanNode rewrittenPlan = SimplePlanRewriter.rewriteWith(rewriter, flattenedPlan, new JoinContext());
            if (rewriter.isPlanChanged()) {
                return PlanOptimizerResult.optimizerResult(rewrittenPlan, true);
            }
            // Pre-pass may have restructured the plan, but if the main rewrite
            // didn't fire, return the original plan to avoid plan changes that
            // don't produce the payload join optimization.
            return PlanOptimizerResult.optimizerResult(plan, false);
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
        return isEnabledForTesting || isOptimizePayloadJoins(session);
    }

    private static class Rewriter
            extends SimplePlanRewriter<JoinContext>
    {
        private final Session session;
        Metadata metadata;
        private final TypeProvider types;
        private final FunctionAndTypeManager functionAndTypeManager;
        private final PlanNodeIdAllocator planNodeIdAllocator;
        private final VariableAllocator variableAllocator;
        private boolean planChanged;

        private Rewriter(Session session, Metadata metadata, TypeProvider types, FunctionAndTypeManager functionAndTypeManager, PlanNodeIdAllocator planNodeIdAllocator, VariableAllocator variableAllocator)
        {
            this.session = requireNonNull(session, "session is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.types = requireNonNull(types, "types is null");
            this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
            this.planNodeIdAllocator = requireNonNull(planNodeIdAllocator, "planNodeIdAllocator is null");
            this.variableAllocator = requireNonNull(variableAllocator, "variableAllocator is null");
        }

        public boolean isPlanChanged()
        {
            return planChanged;
        }

        @Override
        public PlanNode visitPlan(PlanNode planNode, RewriteContext<JoinContext> context)
        {
            // do a default rewrite with a new context for each child to avoid lateral propagation of context information
            List<PlanNode> newChildren = planNode.getSources().stream().map(childNode -> context.rewrite(childNode, new JoinContext())).collect(Collectors.toList());
            return replaceChildren(planNode, newChildren);
        }

        @Override
        public PlanNode visitJoin(JoinNode joinNode, RewriteContext<JoinContext> context)
        {
            final JoinContext joinContext = context.get();
            Set<VariableReferenceExpression> inputJoinKeys = joinContext.getJoinKeys();
            PlanNode leftNode = joinNode.getLeft();
            PlanNode rightNode = joinNode.getRight();

            boolean isTopJoin = joinContext.getJoinKeys().size() == 0;

            ImmutableSet<VariableReferenceExpression> leftColumns = leftNode.getOutputVariables().stream().collect(toImmutableSet());

            // abort rewrite if some of the collected join keys are in the RHS of the current join
            ImmutableSet<VariableReferenceExpression> rightJoinKeys = inputJoinKeys.stream().filter(key -> rightNode.getOutputVariables().contains(key)).collect(toImmutableSet());
            Set<VariableReferenceExpression> joinKeys = extractJoinKeys(joinNode.getFilter(), joinNode.getCriteria());

            ImmutableSet<VariableReferenceExpression> leftJoinKeys = intersection(joinKeys, leftColumns).immutableCopy();
            if (!rightJoinKeys.isEmpty() || !needsRewrite(joinNode.getType(), leftColumns, leftJoinKeys)) {
                List<PlanNode> newChildren = joinNode.getSources().stream()
                        .map(child -> defaultRewriteJoinChild(child, context, joinNode.isCrossJoin()))
                        .collect(toImmutableList());
                return replaceChildren(joinNode, newChildren);
            }

            joinContext.addKeys(leftJoinKeys);
            joinContext.incrementNumJoins();

            PlanNode newLeftNode = context.rewrite(leftNode, joinContext);
            if (leftNode.equals(newLeftNode)) {
                newLeftNode = context.rewrite(leftNode, new JoinContext());
                return replaceChildren(joinNode, ImmutableList.of(newLeftNode, rightNode));
            }

            List<VariableReferenceExpression> leftCols = newLeftNode.getOutputVariables();
            List<VariableReferenceExpression> rightCols = rightNode.getOutputVariables();
            List<VariableReferenceExpression> allCols = Stream.concat(leftCols.stream(), rightCols.stream()).collect(toImmutableList());

            JoinNode newJoinNode = new JoinNode(
                    joinNode.getSourceLocation(),
                    planNodeIdAllocator.getNextId(),
                    joinNode.getType(),
                    newLeftNode,
                    rightNode,
                    joinNode.getCriteria(),
                    allCols,
                    joinNode.getFilter(),
                    joinNode.getLeftHashVariable(),
                    joinNode.getRightHashVariable(),
                    joinNode.getDistributionType(),
                    joinNode.getDynamicFilters());

            if (isTopJoin && context.get().needsPayloadRejoin()) {
                PlanNode payloadJoin = transformJoin(newJoinNode, joinContext);
                // reset payload node as it has been reattached to the plan node
                context.get().setPayloadNode(null);

                // do a final check that the rewrite didn't lose any columns (can happen if there are intermediate projections on non-join keys that get hidden because of the DISTINCT keys computation)
                List<VariableReferenceExpression> outputVariables = joinNode.getOutputVariables();
                if (!payloadJoin.getOutputVariables().containsAll(outputVariables)) {
                    return joinNode;
                }
                return restrictOutput(payloadJoin, planNodeIdAllocator, outputVariables);
            }

            planChanged = true;
            return newJoinNode;
        }

        private PlanNode defaultRewriteJoinChild(PlanNode child, RewriteContext<JoinContext> context, boolean isCrossJoin)
        {
            PlanNode newChild = context.rewrite(child, new JoinContext());
            if (isCrossJoin && child.getOutputVariables() != newChild.getOutputVariables()) {
                return restrictOutput(newChild, planNodeIdAllocator, child.getOutputVariables());
            }
            return newChild;
        }

        private boolean needsRewrite(JoinType joinType, ImmutableSet<VariableReferenceExpression> leftColumns, Set<VariableReferenceExpression> joinKeys)
        {
            return joinType == LEFT && supportedJoinKeyTypes(joinKeys) && leftColumns.stream().anyMatch(var -> !joinKeys.contains(var));
        }

        @Override
        public PlanNode visitProject(ProjectNode projectNode, RewriteContext<JoinContext> context)
        {
            if (isScanFilterProject(projectNode)) {
                return rewriteScanFilterProject(projectNode, context);
            }

            PlanNode child = projectNode.getSource();

            Set<VariableReferenceExpression> inputJoinKeys = context.get().getJoinKeys();
            if (!child.getOutputVariables().containsAll(inputJoinKeys)) {
                Map<VariableReferenceExpression, RowExpression> pushableExpressions = new HashMap<>();

                projectNode.getAssignments().forEach((var, expr) -> {
                    if (inputJoinKeys.contains(var) && !var.equals(expr)) {
                        // join key computed in this projection: need to push down
                        pushableExpressions.put(var, expr);
                    }
                });

                context.get().addProjectionsToPush(pushableExpressions);
            }

            PlanNode newChild = context.rewrite(child, context.get());

            if (child.equals(newChild)) {
                return projectNode;
            }

            // remove assignments that were pushed down
            Set<VariableReferenceExpression> joinKeys = context.get().getJoinKeys();

            Assignments newAssignments = projectNode.getAssignments();
            if (context.get().needsPayloadRejoin() && !child.getOutputVariables().containsAll(joinKeys)) {
                Assignments.Builder assignments = Assignments.builder();

                projectNode.getAssignments().forEach((var, expr) -> {
                    if (joinKeys.contains(var) && !var.equals(expr)) {
                        // join key computed in this projection: need to push down
                        assignments.put(var, var);
                    }
                    else {
                        assignments.put(var, expr);
                    }
                });

                newAssignments = assignments.build();
            }

            Set<VariableReferenceExpression> newChildOutputVarSet = newChild.getOutputVariables().stream().collect(toImmutableSet());
            Assignments newProjectAssighments = removeHiddenColumns(newAssignments, newChildOutputVarSet, context.get().getJoinKeys());

            ProjectNode newProjectNode = new ProjectNode(projectNode.getId(), newChild, newProjectAssighments);

            // cancel rewrite when some columns needed for the project were hidden by the rewrite
            return validateProjectAssignments(newProjectNode) ? newProjectNode : projectNode;
        }

        @Override
        public PlanNode visitFilter(FilterNode filterNode, RewriteContext<JoinContext> context)
        {
            if (isScanFilterProject(filterNode)) {
                return rewriteScanFilterProject(filterNode, context);
            }

            return context.defaultRewrite(filterNode, new JoinContext());
        }

        @Override
        public PlanNode visitTableScan(TableScanNode scanNode, RewriteContext<JoinContext> context)
        {
            return rewriteScanFilterProject(scanNode, context);
        }

        private PlanNode rewriteScanFilterProject(PlanNode planNode, RewriteContext<JoinContext> context)
        {
            Set<VariableReferenceExpression> joinKeys = context.get().getJoinKeys();

            if (joinKeys.size() == 0 || context.get().getNumJoins() < 2) {
                return planNode;
            }

            // Detect IS NOT NULL predicates on join keys to skip null checks in the rejoin
            Set<VariableReferenceExpression> nonNullVars = extractNonNullVariablesFromScanFilterProject(planNode, joinKeys);
            context.get().addNonNullKeys(nonNullVars);

            List<VariableReferenceExpression> outputCols = planNode.getOutputVariables();
            if (!ImmutableSet.copyOf(planNode.getOutputVariables()).containsAll(joinKeys)) {
                // not all join keys are in the plan node: check if there are any pushable projections
                Map<VariableReferenceExpression, RowExpression> projectionsToPush = context.get().getProjectionsToPush();
                if (!outputCols.containsAll(VariablesExtractor.extractUnique(projectionsToPush.values()))) {
                    // abort rewrite
                    return planNode;
                }

                PlanNode newProjectNode = addProjections(planNode, planNodeIdAllocator, context.get().getProjectionsToPush());
                return constructDistinctKeysPlan(newProjectNode, context, joinKeys);
            }

            return constructDistinctKeysPlan(planNode, context, joinKeys);
        }

        private AggregationNode constructDistinctKeysPlan(PlanNode planNode, RewriteContext<JoinContext> context, Set<VariableReferenceExpression> joinKeys)
        {
            List<VariableReferenceExpression> groupingKeys = joinKeys.stream().collect(toImmutableList());
            AggregationNode agg = new AggregationNode(
                    planNode.getSourceLocation(),
                    planNodeIdAllocator.getNextId(),
                    planNode,
                    ImmutableMap.of(),
                    singleGroupingSet(groupingKeys),
                    ImmutableList.of(),
                    SINGLE,
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty());

            Map<VariableReferenceExpression, VariableReferenceExpression> varMap = new HashMap<>();
            for (VariableReferenceExpression var : joinKeys) {
                VariableReferenceExpression newVar = variableAllocator.newVariable(var.getName(), var.getType());
                varMap.put(var, newVar);
            }

            context.get().setJoinKeyMap(new HashMap<>(varMap));
            PlanNode planNodeCopy = clonePlanNode(planNode, session, metadata, planNodeIdAllocator, planNode.getOutputVariables(), varMap);
            context.get().setPayloadNode(planNodeCopy);

            return agg;
        }

        private PlanNode transformJoin(JoinNode keysNode, JoinContext context)
        {
            PlanNode payloadPlanNode = context.getPayloadNode();

            Set<VariableReferenceExpression> joinKeys = context.getJoinKeys();
            Map<VariableReferenceExpression, VariableReferenceExpression> joinKeyMap = context.getJoinKeyMap();
            Set<VariableReferenceExpression> nonNullKeys = context.getNonNullKeys();

            checkState(null != payloadPlanNode, "Payload plannode not initialized");
            checkState(null != joinKeyMap, "joinkey map not initialized");

            FunctionResolution functionResolution = new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver());

            // build new assignments of the form "jk IS NULL as jk_NULL"
            Assignments.Builder assignments = Assignments.builder();

            ImmutableList.Builder<RowExpression> joinPredicateBuilder = ImmutableList.builder();

            List<VariableReferenceExpression> joinOutputCols = keysNode.getOutputVariables();

            for (VariableReferenceExpression var : joinOutputCols) {
                assignments.put(var, var);
            }

            for (VariableReferenceExpression var : joinKeys) {
                VariableReferenceExpression newVar = joinKeyMap.get(var);

                if (nonNullKeys.contains(var)) {
                    // Key is guaranteed non-null: use direct equality
                    joinPredicateBuilder.add(equalityPredicate(functionResolution, newVar, var));
                }
                else {
                    // Key may be null: use IS_NULL comparison + COALESCE comparison
                    VariableReferenceExpression isNullVar = variableAllocator.newVariable(var.getName() + "_NULL", BOOLEAN);
                    assignments.put(isNullVar, specialForm(IS_NULL, BOOLEAN, ImmutableList.of(var)));

                    RowExpression coalesceComp = equalityPredicate(functionResolution, coalesceToZero(newVar), coalesceToZero(var));
                    RowExpression nullComp = equalityPredicate(functionResolution, specialForm(IS_NULL, BOOLEAN, ImmutableList.of(newVar)), isNullVar);
                    joinPredicateBuilder.add(nullComp);
                    joinPredicateBuilder.add(coalesceComp);
                }
            }

            ProjectNode projectNode = new ProjectNode(planNodeIdAllocator.getNextId(), keysNode, assignments.build());
            List<VariableReferenceExpression> resultOutputCols = Stream.concat(payloadPlanNode.getOutputVariables().stream(), projectNode.getOutputVariables().stream()).collect(toImmutableList());

            List<RowExpression> joinCriteria = joinPredicateBuilder.build();

            // If all keys are non-null and all key expressions are deterministic,
            // use INNER join (every payload row matches exactly one distinct key).
            // Non-deterministic keys (e.g., random()) are computed separately in the
            // cloned payload and distinct-keys subtrees, so values may differ and
            // an INNER join could incorrectly drop rows.
            RowExpressionDeterminismEvaluator determinismEvaluator = new RowExpressionDeterminismEvaluator(functionAndTypeManager);
            Map<VariableReferenceExpression, RowExpression> projectionsToPush = context.getProjectionsToPush();
            boolean allKeysDeterministic = joinKeys.stream()
                    .allMatch(key -> !projectionsToPush.containsKey(key) || determinismEvaluator.isDeterministic(projectionsToPush.get(key)));
            boolean allKeysNonNull = nonNullKeys.containsAll(joinKeys);
            JoinType rejoinType = (allKeysNonNull && allKeysDeterministic) ? JoinType.INNER : JoinType.LEFT;

            return new JoinNode(
                    keysNode.getSourceLocation(),
                    planNodeIdAllocator.getNextId(),
                    rejoinType,
                    payloadPlanNode,
                    projectNode,
                    ImmutableList.of(),
                    resultOutputCols,
                    Optional.of(LogicalRowExpressions.and(joinCriteria)),
                    keysNode.getLeftHashVariable(),
                    keysNode.getRightHashVariable(),
                    keysNode.getDistributionType(),
                    keysNode.getDynamicFilters());
        }

        private Assignments removeHiddenColumns(Assignments newAssignments, Set<VariableReferenceExpression> newChildOutputVarSet, Set<VariableReferenceExpression> joinKeys)
        {
            Map<VariableReferenceExpression, RowExpression> newAssignmentsMap =
                    newAssignments.entrySet().stream().filter(assignment ->
                            newChildOutputVarSet.containsAll(VariablesExtractor.extractUnique(assignment.getValue()))).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            Set<VariableReferenceExpression> outputKeys = newAssignmentsMap.keySet();
            Map<VariableReferenceExpression, RowExpression> joinKeyMap = joinKeys.stream().filter(key -> !outputKeys.contains(key) && newChildOutputVarSet.contains(key)).collect(Collectors.toMap(Function.identity(), Function.identity()));
            newAssignmentsMap.putAll(joinKeyMap);
            return new Assignments(newAssignmentsMap);
        }

        private boolean validateProjectAssignments(ProjectNode projectNode)
        {
            Assignments assignments = projectNode.getAssignments();
            PlanNode input = projectNode.getSource();
            ImmutableSet<VariableReferenceExpression> inputColsSet = input.getOutputVariables().stream().collect(toImmutableSet());

            for (Map.Entry<VariableReferenceExpression, RowExpression> assignment : assignments.entrySet()) {
                RowExpression expr = assignment.getValue();
                if (!inputColsSet.containsAll(VariablesExtractor.extractUnique(expr))) {
                    return false;
                }
            }
            return true;
        }

        private RowExpression coalesceToZero(RowExpression var)
        {
            RowExpression zero = zeroForType(var.getType());
            return coalesce(ImmutableList.of(var, zero));
        }

        private Set<VariableReferenceExpression> extractJoinKeys(Optional<RowExpression> filter, List<EquiJoinClause> criteria)
        {
            ImmutableSet.Builder<VariableReferenceExpression> builder = ImmutableSet.builder();

            criteria.forEach((v) -> {
                builder.add(v.getLeft());
                builder.add(v.getRight());
            });

            if (filter.isPresent()) {
                builder.addAll(VariablesExtractor.extractAll(filter.get()));
            }

            return builder.build();
        }

        private boolean supportedJoinKeyTypes(Set<VariableReferenceExpression> joinKeys)
        {
            return joinKeys.stream().allMatch(key -> key.getType() instanceof VarcharType || isNumericType(key.getType()));
        }

        private Set<VariableReferenceExpression> extractNonNullVariablesFromScanFilterProject(PlanNode node, Set<VariableReferenceExpression> joinKeys)
        {
            FunctionResolution functionResolution = new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver());
            ImmutableSet.Builder<VariableReferenceExpression> nonNullVars = ImmutableSet.builder();
            extractNonNullVariablesRecursive(node, joinKeys, functionResolution, nonNullVars);
            return nonNullVars.build();
        }

        private void extractNonNullVariablesRecursive(PlanNode node, Set<VariableReferenceExpression> joinKeys,
                FunctionResolution functionResolution, ImmutableSet.Builder<VariableReferenceExpression> result)
        {
            if (node instanceof FilterNode) {
                FilterNode filterNode = (FilterNode) node;
                RowExpression predicate = filterNode.getPredicate();
                for (RowExpression conjunct : LogicalRowExpressions.extractConjuncts(predicate)) {
                    if (conjunct instanceof CallExpression) {
                        CallExpression call = (CallExpression) conjunct;
                        if (functionResolution.isNotFunction(call.getFunctionHandle())
                                && call.getArguments().size() == 1
                                && call.getArguments().get(0) instanceof SpecialFormExpression
                                && ((SpecialFormExpression) call.getArguments().get(0)).getForm() == IS_NULL
                                && ((SpecialFormExpression) call.getArguments().get(0)).getArguments().size() == 1
                                && ((SpecialFormExpression) call.getArguments().get(0)).getArguments().get(0) instanceof VariableReferenceExpression) {
                            VariableReferenceExpression variable = (VariableReferenceExpression) ((SpecialFormExpression) call.getArguments().get(0)).getArguments().get(0);
                            if (joinKeys.contains(variable)) {
                                result.add(variable);
                            }
                        }
                    }
                }
                extractNonNullVariablesRecursive(filterNode.getSource(), joinKeys, functionResolution, result);
            }
            else if (node instanceof ProjectNode) {
                extractNonNullVariablesRecursive(((ProjectNode) node).getSource(), joinKeys, functionResolution, result);
            }
            // TableScanNode is a leaf — nothing to do
        }
    }

    private static RowExpression zeroForType(Type type)
    {
        checkArgument(isNumericType(type) || type instanceof VarcharType, "join key should be of numeric or varchar type");

        if (isNumericType(type)) {
            return constant(0L, BIGINT);
        }
        return constant(Slices.utf8Slice(""), VarcharType.VARCHAR);
    }

    /**
     * Pre-pass: flatten LOJ chains by removing identity projections and hoisting cross joins
     * above the LOJ chain. This allows the payload join optimization to handle more join patterns.
     */
    private static PlanNode flattenJoinChains(PlanNode node, PlanNodeIdAllocator idAllocator)
    {
        List<PlanNode> children = node.getSources();
        ImmutableList.Builder<PlanNode> newChildrenBuilder = ImmutableList.builder();
        boolean childChanged = false;
        for (PlanNode child : children) {
            PlanNode newChild = flattenJoinChains(child, idAllocator);
            if (newChild != child) {
                childChanged = true;
            }
            newChildrenBuilder.add(newChild);
        }

        PlanNode current = childChanged ? replaceChildren(node, newChildrenBuilder.build()) : node;

        if (current instanceof JoinNode && ((JoinNode) current).getType() == LEFT) {
            PlanNode flattened = flattenLeftChain((JoinNode) current, idAllocator);
            if (flattened instanceof JoinNode && ((JoinNode) flattened).getType() == LEFT) {
                return reorderLeftJoinChain((JoinNode) flattened, idAllocator);
            }
            return flattened;
        }

        return current;
    }

    private static PlanNode flattenLeftChain(JoinNode joinNode, PlanNodeIdAllocator idAllocator)
    {
        PlanNode left = joinNode.getLeft();

        // Case 1: Left child is an identity projection - remove it
        if (left instanceof ProjectNode && isIdentityProjection((ProjectNode) left)) {
            PlanNode projectSource = ((ProjectNode) left).getSource();
            List<VariableReferenceExpression> newOutput = Stream.concat(
                    projectSource.getOutputVariables().stream(),
                    joinNode.getRight().getOutputVariables().stream())
                    .collect(toImmutableList());

            JoinNode newJoin = new JoinNode(
                    joinNode.getSourceLocation(),
                    idAllocator.getNextId(),
                    joinNode.getType(),
                    projectSource,
                    joinNode.getRight(),
                    joinNode.getCriteria(),
                    newOutput,
                    joinNode.getFilter(),
                    joinNode.getLeftHashVariable(),
                    joinNode.getRightHashVariable(),
                    joinNode.getDistributionType(),
                    joinNode.getDynamicFilters());

            return flattenLeftChain(newJoin, idAllocator);
        }

        // Case 2: Left child is a cross join - hoist it above the LOJ
        if (left instanceof JoinNode && ((JoinNode) left).isCrossJoin()) {
            JoinNode crossJoin = (JoinNode) left;

            Set<VariableReferenceExpression> lojLeftKeys = extractLeftJoinKeys(joinNode);
            Set<VariableReferenceExpression> crossLeftCols = ImmutableSet.copyOf(crossJoin.getLeft().getOutputVariables());
            Set<VariableReferenceExpression> crossRightCols = ImmutableSet.copyOf(crossJoin.getRight().getOutputVariables());

            PlanNode chainSide = null;
            PlanNode crossSide = null;

            if (crossLeftCols.containsAll(lojLeftKeys)) {
                chainSide = crossJoin.getLeft();
                crossSide = crossJoin.getRight();
            }
            else if (crossRightCols.containsAll(lojLeftKeys)) {
                chainSide = crossJoin.getRight();
                crossSide = crossJoin.getLeft();
            }

            if (chainSide != null) {
                List<VariableReferenceExpression> lojOutput = Stream.concat(
                        chainSide.getOutputVariables().stream(),
                        joinNode.getRight().getOutputVariables().stream())
                        .collect(toImmutableList());

                JoinNode newLOJ = new JoinNode(
                        joinNode.getSourceLocation(),
                        idAllocator.getNextId(),
                        joinNode.getType(),
                        chainSide,
                        joinNode.getRight(),
                        joinNode.getCriteria(),
                        lojOutput,
                        joinNode.getFilter(),
                        joinNode.getLeftHashVariable(),
                        joinNode.getRightHashVariable(),
                        joinNode.getDistributionType(),
                        joinNode.getDynamicFilters());

                PlanNode flattenedLOJ = flattenLeftChain(newLOJ, idAllocator);

                List<VariableReferenceExpression> crossOutput = Stream.concat(
                        flattenedLOJ.getOutputVariables().stream(),
                        crossSide.getOutputVariables().stream())
                        .collect(toImmutableList());

                return new JoinNode(
                        crossJoin.getSourceLocation(),
                        idAllocator.getNextId(),
                        crossJoin.getType(),
                        flattenedLOJ,
                        crossSide,
                        crossJoin.getCriteria(),
                        crossOutput,
                        crossJoin.getFilter(),
                        crossJoin.getLeftHashVariable(),
                        crossJoin.getRightHashVariable(),
                        crossJoin.getDistributionType(),
                        crossJoin.getDynamicFilters());
            }
        }

        return joinNode;
    }

    private static Set<VariableReferenceExpression> extractLeftJoinKeys(JoinNode joinNode)
    {
        ImmutableSet.Builder<VariableReferenceExpression> builder = ImmutableSet.builder();

        for (EquiJoinClause clause : joinNode.getCriteria()) {
            builder.add(clause.getLeft());
        }

        if (joinNode.getFilter().isPresent()) {
            Set<VariableReferenceExpression> rightCols = ImmutableSet.copyOf(joinNode.getRight().getOutputVariables());
            for (VariableReferenceExpression var : VariablesExtractor.extractAll(joinNode.getFilter().get())) {
                if (!rightCols.contains(var)) {
                    builder.add(var);
                }
            }
        }

        return builder.build();
    }

    private static boolean isIdentityProjection(ProjectNode project)
    {
        return project.getAssignments().entrySet().stream()
                .allMatch(entry -> entry.getValue().equals(entry.getKey()));
    }

    /**
     * Reorder LOJs in a chain so that base-keyed LOJs (keys from the base table) come first,
     * and dependent LOJs (keys from other LOJ results) are pushed to the top. This maximizes
     * the number of LOJs that the payload join optimization can handle.
     */
    private static PlanNode reorderLeftJoinChain(JoinNode topJoin, PlanNodeIdAllocator idAllocator)
    {
        // Collect all LOJs in the chain (top to bottom order)
        List<JoinNode> joins = new ArrayList<>();
        PlanNode current = topJoin;
        while (current instanceof JoinNode && ((JoinNode) current).getType() == LEFT) {
            joins.add((JoinNode) current);
            current = ((JoinNode) current).getLeft();
        }
        PlanNode baseNode = current;

        if (joins.size() < 3) {
            return topJoin;
        }

        Set<VariableReferenceExpression> baseColumns = ImmutableSet.copyOf(baseNode.getOutputVariables());

        // Classify in bottom-to-top order: base-keyed vs dependent
        List<JoinNode> baseKeyed = new ArrayList<>();
        List<JoinNode> dependent = new ArrayList<>();
        boolean needsReorder = false;
        boolean seenDependent = false;

        for (int i = joins.size() - 1; i >= 0; i--) {
            JoinNode j = joins.get(i);
            Set<VariableReferenceExpression> leftKeys = extractLeftJoinKeys(j);
            if (baseColumns.containsAll(leftKeys)) {
                baseKeyed.add(j);
                if (seenDependent) {
                    needsReorder = true;
                }
            }
            else {
                dependent.add(j);
                seenDependent = true;
            }
        }

        if (!needsReorder || baseKeyed.size() < 2) {
            return topJoin;
        }

        // Rebuild: base -> baseKeyed LOJs -> dependent LOJs
        PlanNode result = baseNode;
        for (JoinNode j : baseKeyed) {
            List<VariableReferenceExpression> newOutput = Stream.concat(
                    result.getOutputVariables().stream(),
                    j.getRight().getOutputVariables().stream())
                    .collect(toImmutableList());

            result = new JoinNode(
                    j.getSourceLocation(),
                    idAllocator.getNextId(),
                    j.getType(),
                    result,
                    j.getRight(),
                    j.getCriteria(),
                    newOutput,
                    j.getFilter(),
                    j.getLeftHashVariable(),
                    j.getRightHashVariable(),
                    j.getDistributionType(),
                    j.getDynamicFilters());
        }
        for (JoinNode j : dependent) {
            List<VariableReferenceExpression> newOutput = Stream.concat(
                    result.getOutputVariables().stream(),
                    j.getRight().getOutputVariables().stream())
                    .collect(toImmutableList());

            result = new JoinNode(
                    j.getSourceLocation(),
                    idAllocator.getNextId(),
                    j.getType(),
                    result,
                    j.getRight(),
                    j.getCriteria(),
                    newOutput,
                    j.getFilter(),
                    j.getLeftHashVariable(),
                    j.getRightHashVariable(),
                    j.getDistributionType(),
                    j.getDynamicFilters());
        }

        return result;
    }

    private static class JoinContext
    {
        private Set<VariableReferenceExpression> joinKeys = new HashSet<>();
        private Map<VariableReferenceExpression, VariableReferenceExpression> joinKeyMap;
        private Map<VariableReferenceExpression, RowExpression> projectionsToPush = new HashMap<>();
        private Set<VariableReferenceExpression> nonNullKeys = new HashSet<>();

        int numJoins;
        PlanNode payloadNode;

        public JoinContext() {}

        public Set<VariableReferenceExpression> getJoinKeys()
        {
            return joinKeys;
        }

        public void addKeys(ImmutableSet<VariableReferenceExpression> keys)
        {
            joinKeys.addAll(keys);
        }

        public Map<VariableReferenceExpression, RowExpression> getProjectionsToPush()
        {
            return projectionsToPush;
        }

        public void addProjectionsToPush(Map<VariableReferenceExpression, RowExpression> map)
        {
            projectionsToPush.putAll(map);
        }

        public Map<VariableReferenceExpression, VariableReferenceExpression> getJoinKeyMap()
        {
            return joinKeyMap;
        }

        public void setJoinKeyMap(Map<VariableReferenceExpression, VariableReferenceExpression> map)
        {
            joinKeyMap = map;
        }

        public PlanNode getPayloadNode()
        {
            return payloadNode;
        }

        public void setPayloadNode(PlanNode payloadNode)
        {
            this.payloadNode = payloadNode;
        }

        public void reset()
        {
            joinKeys = new HashSet<>();
            projectionsToPush = new HashMap<>();
            joinKeyMap = null;
            numJoins = 0;
            payloadNode = null;
            nonNullKeys = new HashSet<>();
        }

        public int getNumJoins()
        {
            return numJoins;
        }

        public void incrementNumJoins()
        {
            numJoins++;
        }

        public boolean needsPayloadRejoin()
        {
            return payloadNode != null;
        }

        public Set<VariableReferenceExpression> getNonNullKeys()
        {
            return nonNullKeys;
        }

        public void addNonNullKeys(Set<VariableReferenceExpression> keys)
        {
            nonNullKeys.addAll(keys);
        }
    }
}
