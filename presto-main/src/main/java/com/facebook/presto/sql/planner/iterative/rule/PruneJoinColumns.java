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

import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.sql.planner.plan.Patterns.join;
import static com.facebook.presto.util.MoreLists.filteredCopy;
import static com.google.common.base.Predicates.not;

/**
 * Non-cross joins support output symbol selection, so absorb any project-off into the node.
 */
public class PruneJoinColumns
        extends ProjectOffPushDownRule<JoinNode>
{
    public PruneJoinColumns()
    {
        super(join().matching(not(JoinNode::isCrossJoin)));
    }

    @Override
    protected Optional<PlanNode> pushDownProjectOff(PlanNodeIdAllocator idAllocator, PlanVariableAllocator variableAllocator, JoinNode joinNode, Set<VariableReferenceExpression> referencedOutputs)
    {
        return Optional.of(
                new JoinNode(
                        joinNode.getId(),
                        joinNode.getType(),
                        joinNode.getLeft(),
                        joinNode.getRight(),
                        joinNode.getCriteria(),
                        filteredCopy(joinNode.getOutputVariables(), referencedOutputs::contains),
                        joinNode.getFilter(),
                        joinNode.getLeftHashVariable(),
                        joinNode.getRightHashVariable(),
                        joinNode.getDistributionType(),
                        joinNode.getDynamicFilters()));
    }
}
