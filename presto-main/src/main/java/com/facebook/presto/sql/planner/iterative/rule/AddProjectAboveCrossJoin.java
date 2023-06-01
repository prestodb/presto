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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.Set;

import static com.facebook.presto.spi.plan.ProjectNode.Locality.LOCAL;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.identityAssignments;
import static com.facebook.presto.sql.planner.plan.Patterns.join;

/**
 * Cross joins don't support output symbol selection, so add a ProjectNode to limit it's output
 */
public class AddProjectAboveCrossJoin
        implements Rule<JoinNode>
{

    private static final Pattern<JoinNode> PATTERN = join().matching(JoinNode::isCrossJoin);
    private static final Logger log = Logger.get(AddProjectAboveCrossJoin.class);

    @Override
    public Pattern<JoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(JoinNode node, Captures captures, Context context)
    {
        Set<VariableReferenceExpression> inputVariables = ImmutableSet.<VariableReferenceExpression>builder()
                .addAll(node.getLeft().getOutputVariables())
                .addAll(node.getRight().getOutputVariables())
                .build();
        if (inputVariables.size() == node.getOutputVariables().size()) {
            // CrossJoin already has input and output variables that match
            return Result.empty();
        }

        log.info("CrossJoin(%s) found with left vars (%s), right vars (%s), outputs (%s):",
                node.getType(), node.getLeft().getOutputVariables(), node.getRight().getOutputVariables(), node.getOutputVariables());

        return Result.ofPlanNode(new ProjectNode(node.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                new JoinNode(
                        node.getSourceLocation(),
                        node.getId(),
                        node.getType(),
                        node.getLeft(),
                        node.getRight(),
                        node.getCriteria(),
                        // Set new output variables to match the input variables
                        new ArrayList<>(inputVariables),
                        node.getFilter(),
                        node.getLeftHashVariable(),
                        node.getRightHashVariable(),
                        node.getDistributionType(),
                        node.getDynamicFilters()),
                // Set the Projection to identity assignment of the original output variables
                identityAssignments(node.getOutputVariables()),
                LOCAL));
    }
}
