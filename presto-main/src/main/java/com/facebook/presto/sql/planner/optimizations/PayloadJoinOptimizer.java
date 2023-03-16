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
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.expressions.LogicalRowExpressions;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.VariablesExtractor;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slices;

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
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IS_NULL;
import static com.facebook.presto.sql.planner.PlannerUtils.addProjections;
import static com.facebook.presto.sql.planner.PlannerUtils.clonePlanNode;
import static com.facebook.presto.sql.planner.PlannerUtils.coalesce;
import static com.facebook.presto.sql.planner.PlannerUtils.equalityPredicate;
import static com.facebook.presto.sql.planner.PlannerUtils.isScanFilterProject;
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

    public PayloadJoinOptimizer(Metadata metadata)
    {
        requireNonNull(metadata, "metadata is null");

        this.metadata = metadata;
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        FunctionAndTypeManager functionAndTypeManager = metadata.getFunctionAndTypeManager();
        if (isEnabled(session)) {
            PlanNode result = SimplePlanRewriter.rewriteWith(new PayloadJoinOptimizer.Rewriter(session, this.metadata, types, functionAndTypeManager, idAllocator, variableAllocator), plan, new JoinContext());
            return result;
        }
        return plan;
    }

    private boolean isEnabled(Session session)
    {
        return isOptimizePayloadJoins(session);
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

        private Rewriter(Session session, Metadata metadata, TypeProvider types, FunctionAndTypeManager functionAndTypeManager, PlanNodeIdAllocator planNodeIdAllocator, VariableAllocator variableAllocator)
        {
            this.session = requireNonNull(session, "session is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.types = requireNonNull(types, "types is null");
            this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
            this.planNodeIdAllocator = requireNonNull(planNodeIdAllocator, "planNodeIdAllocator is null");
            this.variableAllocator = requireNonNull(variableAllocator, "variableAllocator is null");
        }

        private Set<VariableReferenceExpression> extractPayloadColumns(ImmutableSet<VariableReferenceExpression> columns)
        {
            return columns.stream().filter(var -> var.getType() instanceof MapType || var.getType() instanceof ArrayType).collect(toImmutableSet());
        }

        @Override
        public PlanNode visitPlan(PlanNode node, RewriteContext<JoinContext> context)
        {
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitJoin(JoinNode joinNode, RewriteContext<JoinContext> context)
        {
            final JoinContext joinContext = context.get();
            int numJoinKeys = joinContext.getJoinKeys().size();
            PlanNode leftNode = joinNode.getLeft();
            PlanNode rightNode = joinNode.getRight();
            boolean isTopJoin = numJoinKeys == 0;

            ImmutableSet<VariableReferenceExpression> leftColumns = leftNode.getOutputVariables().stream().collect(toImmutableSet());
            Set<VariableReferenceExpression> joinKeys = extractJoinKeys(joinNode.getFilter(), joinNode.getCriteria());

            ImmutableSet<VariableReferenceExpression> leftJoinKeys = intersection(joinKeys, leftColumns).immutableCopy();
            if (!needsRewrite(joinNode.getType(), leftColumns, leftJoinKeys)) {
                return context.defaultRewrite(joinNode, joinContext);
            }

            joinContext.addKeys(leftJoinKeys);
            joinContext.incrementNumJoins();

            PlanNode newLeftNode = context.rewrite(leftNode, joinContext);
            if (leftNode == newLeftNode) {
                newLeftNode = context.rewrite(leftNode, new JoinContext());
                return joinNode.replaceChildren(ImmutableList.of(newLeftNode, rightNode));
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
                // do a final check that the rewrite didn't lose any columns (can happen if there are intermediate projections on non-join keys that get hidden because of the DISTINCT keys computation)
                if (!payloadJoin.getOutputVariables().containsAll(joinNode.getOutputVariables())) {
                    return joinNode;
                }
                return payloadJoin;
            }

            return newJoinNode;
        }

        private boolean needsRewrite(JoinNode.Type joinType, ImmutableSet<VariableReferenceExpression> leftColumns, Set<VariableReferenceExpression> joinKeys)
        {
            if (joinType == JoinNode.Type.LEFT && supportedJoinKeyTypes(joinKeys)) {
                Set<VariableReferenceExpression> leftPayloadColumns = extractPayloadColumns(leftColumns);

                return !leftPayloadColumns.isEmpty() && !leftPayloadColumns.containsAll(joinKeys);
            }

            return false;
        }

        @Override
        public PlanNode visitProject(ProjectNode projectNode, RewriteContext<JoinContext> context)
        {
            if (isScanFilterProject(projectNode)) {
                return rewriteScanFilterProject(projectNode, context);
            }

            PlanNode child = projectNode.getSource();

            Set<VariableReferenceExpression> joinKeys = context.get().getJoinKeys();
            Assignments newAssignments = projectNode.getAssignments();
            if (!child.getOutputVariables().containsAll(joinKeys)) {
                Assignments.Builder assignments = Assignments.builder();
                Map<VariableReferenceExpression, RowExpression> pushableExpressions = new HashMap<>();

                projectNode.getAssignments().forEach((var, expr) -> {
                    if (joinKeys.contains(var) && var.equals(expr)) {
                        // join key computed in this projection: need to push down
                        assignments.put(var, var);
                        pushableExpressions.put(var, expr);
                    }
                    else {
                        assignments.put(var, expr);
                    }
                });

                newAssignments = assignments.build();
                context.get().addProjectionsToPush(pushableExpressions);
            }

            PlanNode newChild = context.rewrite(child, context.get());

            if (child == newChild) {
                return projectNode;
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
            Set<VariableReferenceExpression> joinKeys = context.get().getJoinKeys();
            List<VariableReferenceExpression> predicateVars = VariablesExtractor.extractAll(filterNode.getPredicate());

            if (isScanFilterProject(filterNode)) {
                return rewriteScanFilterProject(filterNode, context);
            }

            if (joinKeys.isEmpty() || joinKeys.containsAll(predicateVars)) {
                return context.defaultRewrite(filterNode, context.get());
            }

            return filterNode;
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

            checkState(null != payloadPlanNode, "Payload plannode not initialized");
            checkState(null != joinKeyMap, "joinkey map not initialized");

            FunctionResolution functionResolution = new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver());

            // build new assignments of the form "jk IS NULL as jk_NULL"
            Assignments.Builder assignments = Assignments.builder();

            ImmutableList.Builder<RowExpression> coalesceComparisonBuilder = ImmutableList.builder();
            ImmutableList.Builder<RowExpression> nullComparisonBuilder = ImmutableList.builder();

            List<VariableReferenceExpression> joinOutputCols = keysNode.getOutputVariables();

            for (VariableReferenceExpression var : joinOutputCols) {
                assignments.put(var, var);
            }

            for (VariableReferenceExpression var : joinKeys) {
                VariableReferenceExpression newVar = joinKeyMap.get(var);

                VariableReferenceExpression isNullVar = variableAllocator.newVariable(var.getName() + "_NULL", BOOLEAN);
                assignments.put(isNullVar, specialForm(IS_NULL, BOOLEAN, ImmutableList.of(var)));

                // construct predicate of the form "coalesce(newVar, 0) = coalesce(var, 0)"
                RowExpression coalesceComp = equalityPredicate(functionResolution, coalesceToZero(newVar), coalesceToZero(var));
                RowExpression nullComp = equalityPredicate(functionResolution, specialForm(IS_NULL, BOOLEAN, ImmutableList.of(newVar)), isNullVar);
                nullComparisonBuilder.add(nullComp);
                coalesceComparisonBuilder.add(coalesceComp);
            }

            ProjectNode projectNode = new ProjectNode(planNodeIdAllocator.getNextId(), keysNode, assignments.build());
            List<VariableReferenceExpression> resultOutputCols = Stream.concat(payloadPlanNode.getOutputVariables().stream(), projectNode.getOutputVariables().stream()).collect(toImmutableList());

            List<RowExpression> joinCriteria = Stream.concat(nullComparisonBuilder.build().stream(), coalesceComparisonBuilder.build().stream()).collect(toImmutableList());

            return new JoinNode(
                    keysNode.getSourceLocation(),
                    planNodeIdAllocator.getNextId(),
                    JoinNode.Type.LEFT,
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

        private Set<VariableReferenceExpression> extractJoinKeys(Optional<RowExpression> filter, List<JoinNode.EquiJoinClause> criteria)
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
    }

    private static RowExpression zeroForType(Type type)
    {
        checkArgument(isNumericType(type) || type instanceof VarcharType, "join key should be of numeric or varchar type");

        if (isNumericType(type)) {
            return constant(0L, BIGINT);
        }
        return constant(Slices.utf8Slice(""), VarcharType.VARCHAR);
    }

    private static class JoinContext
    {
        private final Set<VariableReferenceExpression> joinKeys = new HashSet<>();
        private Map<VariableReferenceExpression, VariableReferenceExpression> joinKeyMap;
        private Map<VariableReferenceExpression, RowExpression> projectionsToPush = new HashMap<>();

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
    }
}
