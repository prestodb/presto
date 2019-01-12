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
package io.prestosql.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.SemiJoinNode;

import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.prestosql.sql.planner.iterative.rule.Util.restrictOutputs;
import static io.prestosql.sql.planner.plan.Patterns.semiJoin;

public class PruneSemiJoinFilteringSourceColumns
        implements Rule<SemiJoinNode>
{
    private static final Pattern<SemiJoinNode> PATTERN = semiJoin();

    @Override
    public Pattern<SemiJoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(SemiJoinNode semiJoinNode, Captures captures, Context context)
    {
        Set<Symbol> requiredFilteringSourceInputs = Streams.concat(
                Stream.of(semiJoinNode.getFilteringSourceJoinSymbol()),
                semiJoinNode.getFilteringSourceHashSymbol().map(Stream::of).orElse(Stream.empty()))
                .collect(toImmutableSet());

        return restrictOutputs(context.getIdAllocator(), semiJoinNode.getFilteringSource(), requiredFilteringSourceInputs)
                .map(newFilteringSource ->
                        semiJoinNode.replaceChildren(ImmutableList.of(semiJoinNode.getSource(), newFilteringSource)))
                .map(Result::ofPlanNode)
                .orElse(Result.empty());
    }
}
