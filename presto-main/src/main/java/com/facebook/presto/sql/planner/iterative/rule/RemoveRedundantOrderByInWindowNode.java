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

import com.facebook.presto.Session;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.planner.plan.WindowNode.Specification;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.isRemoveRedundantOrderByInWindowEnabled;
import static com.facebook.presto.sql.planner.plan.Patterns.window;

public class RemoveRedundantOrderByInWindowNode
        implements Rule<WindowNode>
{
    private static final Pattern<WindowNode> PATTERN = window()
            .matching(RemoveRedundantOrderByInWindowNode::orderBySubsetOfPartition);

    private static boolean orderBySubsetOfPartition(WindowNode windowNode)
    {
        return windowNode.getOrderingScheme().isPresent() && windowNode.getPartitionBy().containsAll(windowNode.getOrderingScheme().get().getOrderByVariables());
    }

    @Override
    public Pattern<WindowNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isRemoveRedundantOrderByInWindowEnabled(session);
    }

    @Override
    public Result apply(WindowNode windowNode, Captures captures, Context context)
    {
        Specification specification = new Specification(windowNode.getPartitionBy(), Optional.empty());
        return Result.ofPlanNode(new WindowNode(
                windowNode.getSourceLocation(),
                windowNode.getId(),
                windowNode.getStatsEquivalentPlanNode(),
                windowNode.getSource(),
                specification,
                windowNode.getWindowFunctions(),
                windowNode.getHashVariable(),
                windowNode.getPrePartitionedInputs(),
                windowNode.getPreSortedOrderPrefix()));
    }
}
