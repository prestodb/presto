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
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.DistinctLimitNode;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;

import static com.facebook.presto.SystemSessionProperties.getHashBasedDistinctLimitThreshold;
import static com.facebook.presto.SystemSessionProperties.isHashBasedDistinctLimitEnabled;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.plan.SimplePlanRewriter.rewriteWith;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;

public class HashBasedPartialDistinctLimit
        implements PlanOptimizer
{
    private final FunctionAndTypeManager functionAndTypeManager;

    public HashBasedPartialDistinctLimit(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionAndTypeManager = functionAndTypeManager;
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, PlanVariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        if (isHashBasedDistinctLimitEnabled(session)) {
            return rewriteWith(new Rewriter(session, idAllocator, variableAllocator, functionAndTypeManager), plan, null);
        }

        return plan;
    }

    private static class Rewriter
            extends SimplePlanRewriter<Void>
    {
        private final Session session;
        private final PlanNodeIdAllocator idAllocator;
        private final PlanVariableAllocator variableAllocator;
        private final FunctionAndTypeManager functionAndTypeManager;

        private Rewriter(Session session, PlanNodeIdAllocator idAllocator, PlanVariableAllocator variableAllocator, FunctionAndTypeManager functionAndTypeManager)
        {
            this.session = session;
            this.idAllocator = idAllocator;
            this.variableAllocator = variableAllocator;
            this.functionAndTypeManager = functionAndTypeManager;
        }

        @Override
        public PlanNode visitDistinctLimit(DistinctLimitNode node, RewriteContext<Void> context)
        {
            if (node.isPartial() && node.getLimit() <= getHashBasedDistinctLimitThreshold(session)) {
                VariableReferenceExpression hashVariable;

                if (node.getHashVariable().isPresent()) {
                    hashVariable = node.getHashVariable().get();
                }
                else if (node.getDistinctVariables().size() == 1 && node.getDistinctVariables().get(0).getType() == BIGINT) {
                    // see if this is a distinct on single long variable for which we don't optimize hash
                    hashVariable = node.getDistinctVariables().get(0);
                }
                else {
                    return node;
                }

                return new FilterNode(
                        node.getSourceLocation(),
                        idAllocator.getNextId(),
                        node.getSource(),
                        call(functionAndTypeManager, "k_distinct", BIGINT, hashVariable, constant(node.getLimit(), BIGINT)));
            }

            return context.defaultRewrite(node);
        }
    }
}
