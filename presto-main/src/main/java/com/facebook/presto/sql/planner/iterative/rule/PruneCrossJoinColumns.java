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
import com.facebook.presto.sql.planner.plan.JoinNode;
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
    protected Optional<PlanNode> pushDownProjectOff(PlanNodeIdAllocator idAllocator, PlanVariableAllocator variableAllocator, JoinNode joinNode, Set<VariableReferenceExpression> referencedOutputs)
    {
        Optional<PlanNode> newLeft = restrictOutputs(idAllocator, joinNode.getLeft(), referencedOutputs, false);
        Optional<PlanNode> newRight = restrictOutputs(idAllocator, joinNode.getRight(), referencedOutputs, false);

        if (!newLeft.isPresent() && !newRight.isPresent()) {
            return Optional.empty();
        }

        ImmutableList.Builder<VariableReferenceExpression> outputVariableBuilder = ImmutableList.builder();
        outputVariableBuilder.addAll(newLeft.orElse(joinNode.getLeft()).getOutputVariables());
        outputVariableBuilder.addAll(newRight.orElse(joinNode.getRight()).getOutputVariables());

        return Optional.of(new JoinNode(
                idAllocator.getNextId(),
                joinNode.getType(),
                newLeft.orElse(joinNode.getLeft()),
                newRight.orElse(joinNode.getRight()),
                joinNode.getCriteria(),
                outputVariableBuilder.build(),
                joinNode.getFilter(),
                joinNode.getLeftHashVariable(),
                joinNode.getRightHashVariable(),
                joinNode.getDistributionType(),
                joinNode.getDynamicFilters()));
    }
}
