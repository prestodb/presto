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
package com.facebook.presto.sql.planner.assertions;

import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

final class PlanMatchingState
{
    private final List<PlanMatchPattern> patterns;
    private final ExpressionAliases expressionAliases;

    PlanMatchingState(List<PlanMatchPattern> patterns, ExpressionAliases expressionAliases)
    {
        requireNonNull(expressionAliases, "expressionAliases is null");
        requireNonNull(patterns, "matchers is null");
        this.expressionAliases = new ExpressionAliases(expressionAliases);
        this.patterns = ImmutableList.copyOf(patterns);
    }

    boolean isTerminated()
    {
        return patterns.isEmpty() || patterns.stream().allMatch(PlanMatchPattern::isTerminated);
    }

    PlanMatchingContext createContext(int matcherId)
    {
        checkArgument(matcherId < patterns.size(), "mactcherId out of scope");
        return new PlanMatchingContext(expressionAliases, patterns.get(matcherId));
    }

    List<PlanMatchPattern> getPatterns()
    {
        return patterns;
    }
}
