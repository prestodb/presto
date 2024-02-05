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
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.isRemoveRedundantCastToVarcharInJoinEnabled;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.sql.planner.PlannerUtils.addProjections;
import static com.facebook.presto.sql.planner.plan.Patterns.join;
import static com.facebook.presto.sql.relational.Expressions.castToBigInt;

/**
 * Remove redundant cast to varchar in join condition for queries like `select select * from orders o join customer c on cast(o.custkey as varchar) = cast(c.custkey as varchar)`
 * Transform from
 * <pre>
 *     - Join
 *          left_cast = right_cast
 *          - Project
 *              left_cast := cast(lkey as varchar)
 *              - TableScan
 *                  lkey BIGINT
 *          - Project
 *              right_cast := cast(rkey as varchar)
 *              - TableScan
 *                  rkey BIGINT
 *
 * </pre>
 * into
 * <pre>
 *     - Join
 *          new_lkey = new_rkey
 *          - Project
 *              left_cast := cast(lkey as varchar)
 *              new_lkey := lkey
 *              - TableScan
 *                  lkey BIGINT
 *          - Project
 *              right_cast := cast(rkey as varchar)
 *              new_rkey := rkey
 *              - TableScan
 *                  rkey BIGINT
 * </pre>
 * We will rely on optimizations later to remove unnecessary cast (if not used) and identity projection here.
 * <p>
 * Notice that we do not apply similar optimizations to queries with similar join condition like `cast(bigint as varchar) = varchar`. In general it can be converted to
 * `bigint = try_cast(varchar as bigint)` as if the varchar here cannot be converted to bigint, try_cast will return null and will not match anyway. However, a special case is
 * varchar begins with 0. `select cast(92 as varchar) = '092'` is false, but `select 92 = try_cast('092' as bigint)` returns true.
 */
public class RemoveRedundantCastToVarcharInJoinClause
        implements Rule<JoinNode>
{
    private static final List<Type> TYPE_SUPPORTED = ImmutableList.of(INTEGER, BIGINT);
    private final FunctionAndTypeManager functionAndTypeManager;
    private final FunctionResolution functionResolution;

    public RemoveRedundantCastToVarcharInJoinClause(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionAndTypeManager = functionAndTypeManager;
        this.functionResolution = new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver());
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isRemoveRedundantCastToVarcharInJoinEnabled(session);
    }

    @Override
    public Pattern<JoinNode> getPattern()
    {
        return join();
    }

    @Override
    public Result apply(JoinNode node, Captures captures, Context context)
    {
        PlanNode leftInput = context.getLookup().resolve(node.getLeft());
        PlanNode rightInput = context.getLookup().resolve(node.getRight());
        if (!(leftInput instanceof ProjectNode) || !(rightInput instanceof ProjectNode)) {
            return Result.empty();
        }
        ProjectNode leftProject = (ProjectNode) leftInput;
        ProjectNode rightProject = (ProjectNode) rightInput;

        ImmutableList.Builder<EquiJoinClause> joinClauseBuilder = ImmutableList.builder();
        ImmutableMap.Builder<VariableReferenceExpression, RowExpression> newLeftAssignmentsBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<VariableReferenceExpression, RowExpression> newRightAssignmentsBuilder = ImmutableMap.builder();
        boolean isChanged = false;
        for (EquiJoinClause equiJoinClause : node.getCriteria()) {
            RowExpression leftProjectAssignment = leftProject.getAssignments().getMap().get(equiJoinClause.getLeft());
            RowExpression rightProjectAssignment = rightProject.getAssignments().getMap().get(equiJoinClause.getRight());
            if (!isSupportedCast(leftProjectAssignment) || !isSupportedCast(rightProjectAssignment)) {
                joinClauseBuilder.add(equiJoinClause);
                continue;
            }

            RowExpression leftAssignment = ((CallExpression) leftProjectAssignment).getArguments().get(0);
            RowExpression rightAssignment = ((CallExpression) rightProjectAssignment).getArguments().get(0);

            if (!leftAssignment.getType().equals(rightAssignment.getType())) {
                leftAssignment = castToBigInt(functionAndTypeManager, leftAssignment);
                rightAssignment = castToBigInt(functionAndTypeManager, rightAssignment);
            }

            VariableReferenceExpression newLeft = context.getVariableAllocator().newVariable(leftAssignment);
            newLeftAssignmentsBuilder.put(newLeft, leftAssignment);

            VariableReferenceExpression newRight = context.getVariableAllocator().newVariable(rightAssignment);
            newRightAssignmentsBuilder.put(newRight, rightAssignment);

            joinClauseBuilder.add(new EquiJoinClause(newLeft, newRight));
            isChanged = true;
        }

        if (!isChanged) {
            return Result.empty();
        }

        newLeftAssignmentsBuilder.putAll(leftProject.getAssignments().getMap());
        Map<VariableReferenceExpression, RowExpression> newLeftAssignments = newLeftAssignmentsBuilder.build();
        newRightAssignmentsBuilder.putAll(rightProject.getAssignments().getMap());
        Map<VariableReferenceExpression, RowExpression> newRightAssignments = newRightAssignmentsBuilder.build();

        PlanNode newLeftProject = addProjections(leftProject.getSource(), context.getIdAllocator(), newLeftAssignments);
        PlanNode newRightProject = addProjections(rightProject.getSource(), context.getIdAllocator(), newRightAssignments);

        return Result.ofPlanNode(new JoinNode(node.getSourceLocation(), context.getIdAllocator().getNextId(), node.getType(), newLeftProject, newRightProject, joinClauseBuilder.build(), node.getOutputVariables(), node.getFilter(), Optional.empty(), Optional.empty(), node.getDistributionType(), node.getDynamicFilters()));
    }

    private boolean isSupportedCast(RowExpression rowExpression)
    {
        if (rowExpression instanceof CallExpression && functionResolution.isCastFunction(((CallExpression) rowExpression).getFunctionHandle())) {
            CallExpression cast = (CallExpression) rowExpression;
            return TYPE_SUPPORTED.contains(cast.getArguments().get(0).getType()) && cast.getType() instanceof VarcharType;
        }
        return false;
    }
}
