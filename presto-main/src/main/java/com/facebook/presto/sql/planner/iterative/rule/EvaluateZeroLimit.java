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
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.google.common.collect.ImmutableList;

import static com.facebook.presto.sql.planner.plan.Patterns.Limit.count;
import static com.facebook.presto.sql.planner.plan.Patterns.limit;

public class EvaluateZeroLimit
        implements Rule<LimitNode>
{
    private static final Pattern<LimitNode> PATTERN = limit()
            .with(count().equalTo(0L));

    @Override
    public Pattern<LimitNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(LimitNode limit, Captures captures, Context context)
    {
        return Result.ofPlanNode(new ValuesNode(limit.getId(), limit.getOutputVariables(), ImmutableList.of()));
    }
}
