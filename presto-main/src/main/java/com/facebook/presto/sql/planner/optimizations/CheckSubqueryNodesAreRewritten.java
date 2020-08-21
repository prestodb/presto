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
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.LateralJoinNode;

import java.util.List;

import static com.facebook.presto.spi.StandardErrorCode.UNSUPPORTED_SUBQUERY;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;

public class CheckSubqueryNodesAreRewritten
        implements PlanOptimizer
{
    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, PlanVariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        searchFrom(plan).where(ApplyNode.class::isInstance)
                .findFirst()
                .ifPresent(node -> {
                    ApplyNode applyNode = (ApplyNode) node;
                    error(applyNode.getCorrelation(), applyNode.getOriginSubqueryError());
                });

        searchFrom(plan).where(LateralJoinNode.class::isInstance)
                .findFirst()
                .ifPresent(node -> {
                    LateralJoinNode lateralJoinNode = (LateralJoinNode) node;
                    error(lateralJoinNode.getCorrelation(), lateralJoinNode.getOriginSubqueryError());
                });

        return plan;
    }

    private void error(List<VariableReferenceExpression> correlation, String originSubqueryError)
    {
        checkState(!correlation.isEmpty(), "All the non correlated subqueries should be rewritten at this point");
        throw new PrestoException(UNSUPPORTED_SUBQUERY, format(originSubqueryError, "Given correlated subquery is not supported"));
    }
}
