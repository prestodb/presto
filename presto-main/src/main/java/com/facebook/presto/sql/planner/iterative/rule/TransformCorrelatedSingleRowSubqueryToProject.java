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

import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.LateralJoinNode;

import java.util.List;

import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.identitiesAsSymbolReferences;
import static com.facebook.presto.sql.planner.plan.Patterns.lateralJoin;

/**
 * This optimizer can rewrite correlated single row subquery to projection in a way described here:
 * From:
 * <pre>
 * - Lateral(with correlation list: [A, C])
 *   - (input) plan which produces symbols: [A, B, C]
 *   - (subquery)
 *     - Project (A + C)
 *       - single row VALUES()
 * </pre>
 * to:
 * <pre>
 *   - Project(A, B, C, A + C)
 *       - (input) plan which produces symbols: [A, B, C]
 * </pre>
 */

public class TransformCorrelatedSingleRowSubqueryToProject
        implements Rule<LateralJoinNode>
{
    private static final Pattern<LateralJoinNode> PATTERN = lateralJoin();

    @Override
    public Pattern<LateralJoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(LateralJoinNode parent, Captures captures, Context context)
    {
        List<ValuesNode> values = searchFrom(parent.getSubquery(), context.getLookup())
                .recurseOnlyWhen(ProjectNode.class::isInstance)
                .where(ValuesNode.class::isInstance)
                .findAll();

        if (values.size() != 1 || !isSingleRowValuesWithNoColumns(values.get(0))) {
            return Result.empty();
        }

        List<ProjectNode> subqueryProjections = searchFrom(parent.getSubquery(), context.getLookup())
                .where(node -> node instanceof ProjectNode && !node.getOutputVariables().equals(parent.getCorrelation()))
                .findAll();

        if (subqueryProjections.size() == 0) {
            return Result.ofPlanNode(parent.getInput());
        }
        else if (subqueryProjections.size() == 1) {
            Assignments assignments = Assignments.builder()
                    .putAll(identitiesAsSymbolReferences(parent.getInput().getOutputVariables()))
                    .putAll(subqueryProjections.get(0).getAssignments())
                    .build();
            return Result.ofPlanNode(projectNode(parent.getInput(), assignments, context));
        }

        return Result.empty();
    }

    private ProjectNode projectNode(PlanNode source, Assignments assignments, Context context)
    {
        return new ProjectNode(context.getIdAllocator().getNextId(), source, assignments);
    }

    private static boolean isSingleRowValuesWithNoColumns(ValuesNode values)
    {
        return values.getRows().size() == 1 && values.getRows().get(0).size() == 0;
    }
}
