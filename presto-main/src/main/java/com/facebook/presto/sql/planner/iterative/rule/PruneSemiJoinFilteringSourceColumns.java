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
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Pattern;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.facebook.presto.sql.planner.iterative.rule.Util.restrictOutputs;
import static com.google.common.collect.ImmutableSet.toImmutableSet;

public class PruneSemiJoinFilteringSourceColumns
        implements Rule
{
    private static final Pattern PATTERN = Pattern.node(SemiJoinNode.class);

    @Override
    public Pattern getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<PlanNode> apply(PlanNode node, Lookup lookup, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, Session session)
    {
        SemiJoinNode semiJoinNode = (SemiJoinNode) node;

        Set<Symbol> requiredFilteringSourceInputs = Streams.concat(
                Stream.of(semiJoinNode.getFilteringSourceJoinSymbol()),
                semiJoinNode.getFilteringSourceHashSymbol().map(Stream::of).orElse(Stream.empty()))
                .collect(toImmutableSet());

        return restrictOutputs(idAllocator, semiJoinNode.getFilteringSource(), requiredFilteringSourceInputs)
                .map(newFilteringSource ->
                        semiJoinNode.replaceChildren(ImmutableList.of(
                                semiJoinNode.getSource(), newFilteringSource)));
    }
}
