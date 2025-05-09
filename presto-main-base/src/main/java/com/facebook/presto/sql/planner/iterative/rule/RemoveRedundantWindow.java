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
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.plan.WindowNode;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.google.common.collect.ImmutableList;

import java.util.Optional;

import static com.facebook.presto.sql.planner.optimizations.QueryCardinalityUtil.isEmpty;
import static com.facebook.presto.sql.planner.plan.Patterns.window;

public class RemoveRedundantWindow
        implements Rule<WindowNode>
{
    private static final Pattern<WindowNode> PATTERN = window();

    @Override
    public Pattern<WindowNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(WindowNode window, Captures captures, Context context)
    {
        if (isEmpty(window.getSource(), context.getLookup())) {
            return Result.ofPlanNode(new ValuesNode(window.getSource().getSourceLocation(), window.getId(), window.getOutputVariables(), ImmutableList.of(), Optional.empty()));
        }
        return Result.empty();
    }
}
