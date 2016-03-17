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

import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;

import static com.google.common.collect.Iterables.getOnlyElement;

public final class ScalarQueryUtil
{
    private ScalarQueryUtil() {}

    public static boolean isScalar(PlanNode node)
    {
        return node.accept(new IsScalarPlanVisitor(), null);
    }

    private static final class IsScalarPlanVisitor
            extends PlanVisitor<Void, Boolean>
    {
        @Override
        protected Boolean visitPlan(PlanNode node, Void context)
        {
            return false;
        }

        @Override
        public Boolean visitEnforceSingleRow(EnforceSingleRowNode node, Void context)
        {
            return true;
        }

        @Override
        public Boolean visitExchange(ExchangeNode node, Void context)
        {
            return (node.getSources().size() == 1) &&
                    getOnlyElement(node.getSources()).accept(this, null);
        }

        @Override
        public Boolean visitProject(ProjectNode node, Void context)
        {
            return node.getSource().accept(this, null);
        }

        @Override
        public Boolean visitFilter(FilterNode node, Void context)
        {
            return node.getSource().accept(this, null);
        }
    }
}
