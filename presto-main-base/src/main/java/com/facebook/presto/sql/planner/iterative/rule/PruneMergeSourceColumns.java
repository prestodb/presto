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
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.MergeWriterNode;
import com.google.common.collect.ImmutableSet;

import static com.facebook.presto.sql.planner.iterative.rule.Util.restrictChildOutputs;
import static com.facebook.presto.sql.planner.plan.Patterns.mergeWriter;

public class PruneMergeSourceColumns
        implements Rule<MergeWriterNode>
{
    private static final Pattern<MergeWriterNode> PATTERN = mergeWriter();

    @Override
    public Pattern<MergeWriterNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(MergeWriterNode mergeNode, Captures captures, Context context)
    {
        return restrictChildOutputs(context.getIdAllocator(), mergeNode, ImmutableSet.copyOf(mergeNode.getProjectedSymbols()))
                .map(Result::ofPlanNode)
                .orElse(Result.empty());
    }
}
