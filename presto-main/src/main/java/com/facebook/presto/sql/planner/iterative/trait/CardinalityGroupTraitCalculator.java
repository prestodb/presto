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

package com.facebook.presto.sql.planner.iterative.trait;

import com.facebook.presto.sql.planner.iterative.GroupReference;
import com.facebook.presto.sql.planner.iterative.GroupTraitCalculator;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.MarkDistinctNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.google.common.collect.Range;

import java.util.List;

import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.Math.min;

public final class CardinalityGroupTraitCalculator
        implements GroupTraitCalculator<CardinalityGroupTrait>
{
    private final Visitor visitor = new Visitor();

    @Override
    public CardinalityGroupTrait calculate(PlanNode node, List<CardinalityGroupTrait> sourceTraits)
    {
        return node.accept(visitor, sourceTraits);
    }

    private static final class Visitor
            extends PlanVisitor<CardinalityGroupTrait, List<CardinalityGroupTrait>>
    {
        @Override
        protected CardinalityGroupTrait visitPlan(PlanNode node, List<CardinalityGroupTrait> sourceRanges)
        {
            return CardinalityGroupTrait.unknown();
        }

        @Override
        public CardinalityGroupTrait visitGroupReference(GroupReference node, List<CardinalityGroupTrait> sourceRanges)
        {
            throw new IllegalStateException("GroupReference node is not supported");
        }

        @Override
        public CardinalityGroupTrait visitEnforceSingleRow(EnforceSingleRowNode node, List<CardinalityGroupTrait> sourceRanges)
        {
            return CardinalityGroupTrait.scalar();
        }

        @Override
        public CardinalityGroupTrait visitAggregation(AggregationNode node, List<CardinalityGroupTrait> sourceRanges)
        {
            if (node.hasEmptyGroupingSet()) {
                return CardinalityGroupTrait.scalar();
            }
            return CardinalityGroupTrait.unknown();
        }

        @Override
        public CardinalityGroupTrait visitExchange(ExchangeNode node, List<CardinalityGroupTrait> sourceRanges)
        {
            if (sourceRanges.size() == 1) {
                return getOnlyElement(sourceRanges);
            }
            return CardinalityGroupTrait.unknown();
        }

        @Override
        public CardinalityGroupTrait visitProject(ProjectNode node, List<CardinalityGroupTrait> sourceRanges)
        {
            return getOnlyElement(sourceRanges);
        }

        @Override
        public CardinalityGroupTrait visitFilter(FilterNode node, List<CardinalityGroupTrait> sourceRanges)
        {
            return getOnlyElement(sourceRanges);
        }

        @Override
        public CardinalityGroupTrait visitMarkDistinct(MarkDistinctNode node, List<CardinalityGroupTrait> sourceRanges)
        {
            return getOnlyElement(sourceRanges);
        }

        public CardinalityGroupTrait visitValues(ValuesNode node, List<CardinalityGroupTrait> sourceRanges)
        {
            return CardinalityGroupTrait.exactly(node.getRows().size());
        }

        @Override
        public CardinalityGroupTrait visitLimit(LimitNode node, List<CardinalityGroupTrait> sourceRanges)
        {
            CardinalityGroupTrait sourceCardinalityRange = getOnlyElement(sourceRanges);
            long upper = node.getCount();
            if (sourceCardinalityRange.getCardinalityRange().hasUpperBound()) {
                upper = min(sourceCardinalityRange.getCardinalityRange().upperEndpoint(), upper);
            }
            long lower = min(upper, sourceCardinalityRange.getCardinalityRange().lowerEndpoint());
            return new CardinalityGroupTrait(Range.closed(lower, upper));
        }
    }
}
