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

import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.facebook.presto.sql.planner.iterative.rule.Util.restrictOutputs;
import static com.facebook.presto.sql.planner.plan.Patterns.semiJoin;
import static com.google.common.collect.ImmutableSet.toImmutableSet;

public class PruneSemiJoinColumns
        extends ProjectOffPushDownRule<SemiJoinNode>
{
    public PruneSemiJoinColumns()
    {
        super(semiJoin());
    }

    @Override
    protected Optional<PlanNode> pushDownProjectOff(PlanNodeIdAllocator idAllocator, PlanVariableAllocator variableAllocator, SemiJoinNode semiJoinNode, Set<VariableReferenceExpression> referencedOutputs)
    {
        if (!referencedOutputs.contains(semiJoinNode.getSemiJoinOutput())) {
            return Optional.of(semiJoinNode.getSource());
        }

        Set<VariableReferenceExpression> requiredSourceInputs = Streams.concat(
                referencedOutputs.stream()
                        .filter(variable -> !variable.equals(semiJoinNode.getSemiJoinOutput())),
                Stream.of(semiJoinNode.getSourceJoinVariable()),
                semiJoinNode.getSourceHashVariable().map(Stream::of).orElse(Stream.empty()))
                .collect(toImmutableSet());

        return restrictOutputs(idAllocator, semiJoinNode.getSource(), requiredSourceInputs, false)
                .map(newSource ->
                        semiJoinNode.replaceChildren(ImmutableList.of(
                                newSource, semiJoinNode.getFilteringSource())));
    }
}
