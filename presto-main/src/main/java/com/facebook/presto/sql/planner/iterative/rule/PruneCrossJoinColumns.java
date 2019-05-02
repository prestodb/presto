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

import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.google.common.collect.ImmutableList;

import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.sql.planner.iterative.rule.Util.restrictOutputs;
import static com.facebook.presto.sql.planner.plan.Patterns.join;

/**
 * Cross joins don't support output symbol selection, so push the project-off through the node.
 */
public class PruneCrossJoinColumns
        extends ProjectOffPushDownRule<JoinNode>
{
    public PruneCrossJoinColumns()
    {
        super(join().matching(JoinNode::isCrossJoin));
    }

    @Override
    protected Optional<PlanNode> pushDownProjectOff(PlanNodeIdAllocator idAllocator, JoinNode joinNode, Set<Symbol> referencedOutputs)
    {
        Optional<PlanNode> newLeft = restrictOutputs(idAllocator, joinNode.getLeft(), referencedOutputs);
        Optional<PlanNode> newRight = restrictOutputs(idAllocator, joinNode.getRight(), referencedOutputs);

        if (!newLeft.isPresent() && !newRight.isPresent()) {
            return Optional.empty();
        }

        ImmutableList.Builder<Symbol> outputSymbolBuilder = ImmutableList.builder();
        outputSymbolBuilder.addAll(newLeft.orElse(joinNode.getLeft()).getOutputSymbols());
        outputSymbolBuilder.addAll(newRight.orElse(joinNode.getRight()).getOutputSymbols());

        return Optional.of(new JoinNode(
                idAllocator.getNextId(),
                joinNode.getType(),
                newLeft.orElse(joinNode.getLeft()),
                newRight.orElse(joinNode.getRight()),
                joinNode.getCriteria(),
                outputSymbolBuilder.build(),
                joinNode.getFilter(),
                joinNode.getLeftHashSymbol(),
                joinNode.getRightHashSymbol(),
                joinNode.getDistributionType()));
    }
}
