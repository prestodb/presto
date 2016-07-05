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
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;

import java.util.Optional;

import static com.google.common.collect.Iterables.getOnlyElement;

public final class PlanCardinalityExtractor
{
    private PlanCardinalityExtractor() {}

    public static Optional<Long> extractCardinality(PlanNode node)
    {
        return node.accept(new IsScalarPlanVisitor(), null);
    }

    private static final class IsScalarPlanVisitor
            extends PlanVisitor<Void, Optional<Long>>
    {
        @Override
        protected Optional<Long> visitPlan(PlanNode node, Void context)
        {
            return Optional.empty();
        }

        @Override
        public Optional<Long> visitEnforceSingleRow(EnforceSingleRowNode node, Void context)
        {
            return Optional.of(1L);
        }

        @Override
        public Optional<Long> visitLimit(LimitNode node, Void context)
        {
            return Optional.of(node.getCount());
        }

        @Override
        public Optional<Long> visitExchange(ExchangeNode node, Void context)
        {
            if (node.getSources().size() == 1) {
                return getOnlyElement(node.getSources()).accept(this, null);
            }
            return Optional.empty();
        }

        @Override
        public Optional<Long> visitProject(ProjectNode node, Void context)
        {
            return node.getSource().accept(this, null);
        }

        @Override
        public Optional<Long> visitFilter(FilterNode node, Void context)
        {
            return node.getSource().accept(this, null);
        }
    }
}
