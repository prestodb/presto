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

import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.VariablesExtractor;
import com.google.common.collect.Streams;

import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.sql.planner.iterative.rule.Util.restrictChildOutputs;
import static com.facebook.presto.sql.planner.plan.Patterns.filter;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToExpression;
import static com.google.common.collect.ImmutableSet.toImmutableSet;

public class PruneFilterColumns
        extends ProjectOffPushDownRule<FilterNode>
{
    public PruneFilterColumns()
    {
        super(filter());
    }

    @Override
    protected Optional<PlanNode> pushDownProjectOff(PlanNodeIdAllocator idAllocator, PlanVariableAllocator variableAllocator, FilterNode filterNode, Set<VariableReferenceExpression> referencedOutputs)
    {
        Set<VariableReferenceExpression> prunedFilterInputs = Streams.concat(
                referencedOutputs.stream(),
                VariablesExtractor.extractUnique(castToExpression(filterNode.getPredicate()), variableAllocator.getTypes()).stream())
                .collect(toImmutableSet());

        return restrictChildOutputs(idAllocator, filterNode, prunedFilterInputs);
    }
}
