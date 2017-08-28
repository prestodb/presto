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
import com.facebook.presto.sql.planner.plan.PlanNode;

import static java.util.Objects.requireNonNull;

public class SimpleRule<T extends PlanNode>
        implements Rule<T>
{
    public static <T extends PlanNode> Rule<T> simpleRule(Pattern<T> pattern, ApplyFunction<T> applyFunction)
    {
        return new SimpleRule<>(pattern, applyFunction);
    }

    private final Pattern<T> pattern;
    private final ApplyFunction<T> applyFunction;

    private SimpleRule(Pattern<T> pattern, ApplyFunction<T> applyFunction)
    {
        this.pattern = requireNonNull(pattern, "pattern is null");
        this.applyFunction = requireNonNull(applyFunction, "applyFunction is null");
    }

    @Override
    public Pattern<T> getPattern()
    {
        return pattern;
    }

    @Override
    public Result apply(T node, Captures captures, Context context)
    {
        return applyFunction.apply(node, captures, context);
    }

    @FunctionalInterface
    public interface ApplyFunction<T extends PlanNode>
    {
        Result apply(T node, Captures captures, Context context);
    }
}
