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

package io.prestosql.sql.planner.optimizations;

import io.prestosql.Session;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.sql.analyzer.SemanticException;
import io.prestosql.sql.planner.PlanNodeIdAllocator;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.SymbolAllocator;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.plan.ApplyNode;
import io.prestosql.sql.planner.plan.LateralJoinNode;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.tree.Node;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.sql.analyzer.SemanticExceptions.notSupportedException;
import static io.prestosql.sql.planner.optimizations.PlanNodeSearcher.searchFrom;

public class CheckSubqueryNodesAreRewritten
        implements PlanOptimizer
{
    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        searchFrom(plan).where(ApplyNode.class::isInstance)
                .findFirst()
                .ifPresent(node -> {
                    ApplyNode applyNode = (ApplyNode) node;
                    throw error(applyNode.getCorrelation(), applyNode.getOriginSubquery());
                });

        searchFrom(plan).where(LateralJoinNode.class::isInstance)
                .findFirst()
                .ifPresent(node -> {
                    LateralJoinNode lateralJoinNode = (LateralJoinNode) node;
                    throw error(lateralJoinNode.getCorrelation(), lateralJoinNode.getOriginSubquery());
                });

        return plan;
    }

    private SemanticException error(List<Symbol> correlation, Node originSubquery)
    {
        checkState(!correlation.isEmpty(), "All the non correlated subqueries should be rewritten at this point");
        throw notSupportedException(originSubquery, "Given correlated subquery");
    }
}
