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
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.LateralJoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.ValuesNode;

import java.util.List;

import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.facebook.presto.sql.planner.plan.SimplePlanRewriter.rewriteWith;
import static java.util.Objects.requireNonNull;

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
        implements PlanOptimizer
{
    @Override
    public PlanNode optimize(
            PlanNode plan,
            Session session,
            TypeProvider types,
            SymbolAllocator symbolAllocator,
            PlanNodeIdAllocator idAllocator)
    {
        return rewriteWith(new Rewriter(idAllocator), plan, null);
    }

    private static class Rewriter
            extends SimplePlanRewriter<PlanNode>
    {
        private final PlanNodeIdAllocator idAllocator;

        public Rewriter(PlanNodeIdAllocator idAllocator)
        {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
        }

        @Override
        public PlanNode visitLateralJoin(LateralJoinNode lateral, RewriteContext<PlanNode> context)
        {
            LateralJoinNode rewrittenLateral = (LateralJoinNode) context.defaultRewrite(lateral, context.get());
            if (rewrittenLateral.getCorrelation().isEmpty()) {
                return rewrittenLateral;
            }

            List<ValuesNode> values = searchFrom(lateral.getSubquery())
                    .recurseOnlyWhen(ProjectNode.class::isInstance)
                    .where(ValuesNode.class::isInstance)
                    .findAll();

            if (values.size() != 1 || !isSingleRowValuesWithNoColumns(values.get(0))) {
                return rewrittenLateral;
            }

            List<ProjectNode> subqueryProjections = searchFrom(lateral.getSubquery())
                    .where(ProjectNode.class::isInstance)
                    .findAll();

            if (subqueryProjections.size() == 0) {
                return rewrittenLateral.getInput();
            }
            else if (subqueryProjections.size() == 1) {
                Assignments assignments = Assignments.builder()
                        .putIdentities(rewrittenLateral.getInput().getOutputSymbols())
                        .putAll(subqueryProjections.get(0).getAssignments())
                        .build();
                return projectNode(rewrittenLateral.getInput(), assignments);
            }
            return rewrittenLateral;
        }

        private ProjectNode projectNode(PlanNode source, Assignments assignments)
        {
            return new ProjectNode(idAllocator.getNextId(), source, assignments);
        }

        private static boolean isSingleRowValuesWithNoColumns(ValuesNode values)
        {
            return values.getRows().size() == 1 && values.getRows().get(0).size() == 0;
        }
    }
}
