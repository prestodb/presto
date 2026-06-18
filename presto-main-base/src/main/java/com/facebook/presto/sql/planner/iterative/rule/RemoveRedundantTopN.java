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

import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.sql.planner.iterative.GroupReference;
import com.facebook.presto.sql.planner.iterative.Rule;

import static com.facebook.presto.sql.planner.plan.Patterns.topN;

/**
 * Removes top N operations where the input is provably at most one row.
 */
public class RemoveRedundantTopN
        implements Rule<TopNNode>
{
    private static final Pattern<TopNNode> PATTERN = topN()
            .matching(RemoveRedundantTopN::singleRowInput);

    private static boolean singleRowInput(TopNNode node)
    {
        return (node.getStep() == TopNNode.Step.SINGLE &&
                ((GroupReference) node.getSource()).getLogicalProperties().isPresent() &&
                ((GroupReference) node.getSource()).getLogicalProperties().get().isAtMostSingleRow());
    }

    @Override
    public Pattern<TopNNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(TopNNode node, Captures captures, Context context)
    {
        return Result.ofPlanNode(node.getSource());
    }
}
