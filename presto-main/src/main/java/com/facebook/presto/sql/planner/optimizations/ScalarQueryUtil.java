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

import com.facebook.presto.sql.planner.iterative.GroupReference;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.google.common.collect.ImmutableList;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

public final class ScalarQueryUtil
{
    private ScalarQueryUtil() {}

    public static boolean isScalar(PlanNode node, Lookup lookup)
    {
        return node.accept(new IsScalarPlanVisitor(Optional.ofNullable(lookup)), null);
    }

    public static boolean isScalar(PlanNode node)
    {
        return isScalar(node, null);
    }

    private static final class IsScalarPlanVisitor
            extends PlanVisitor<Void, Boolean>
    {
        private final Optional<Lookup> lookup;

        public IsScalarPlanVisitor(Optional<Lookup> lookup)
        {
            this.lookup = requireNonNull(lookup, "lookup is null");
        }

        @Override
        protected Boolean visitPlan(PlanNode node, Void context)
        {
            if (node instanceof GroupReference) {
                checkState(lookup.isPresent(), "Lookup has to be provided to test plans with GroupReference nodes");
                return lookup.get().resolve(node).accept(this, null);
            }

            return false;
        }

        @Override
        public Boolean visitEnforceSingleRow(EnforceSingleRowNode node, Void context)
        {
            return true;
        }

        @Override
        public Boolean visitAggregation(AggregationNode node, Void context)
        {
            return node.getGroupingSets().equals(ImmutableList.of(ImmutableList.of()));
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

        public Boolean visitValues(ValuesNode node, Void context)
        {
            return node.getRows().size() == 1;
        }
    }
}
