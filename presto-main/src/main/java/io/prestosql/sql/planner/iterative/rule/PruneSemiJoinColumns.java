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
import io.prestosql.sql.planner.PlanNodeIdAllocator;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.SemiJoinNode;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.prestosql.sql.planner.iterative.rule.Util.restrictOutputs;
import static io.prestosql.sql.planner.plan.Patterns.semiJoin;

public class PruneSemiJoinColumns
        extends ProjectOffPushDownRule<SemiJoinNode>
{
    public PruneSemiJoinColumns()
    {
        super(semiJoin());
    }

    @Override
    protected Optional<PlanNode> pushDownProjectOff(PlanNodeIdAllocator idAllocator, SemiJoinNode semiJoinNode, Set<Symbol> referencedOutputs)
    {
        if (!referencedOutputs.contains(semiJoinNode.getSemiJoinOutput())) {
            return Optional.of(semiJoinNode.getSource());
        }

        Set<Symbol> requiredSourceInputs = Streams.concat(
                referencedOutputs.stream()
                        .filter(symbol -> !symbol.equals(semiJoinNode.getSemiJoinOutput())),
                Stream.of(semiJoinNode.getSourceJoinSymbol()),
                semiJoinNode.getSourceHashSymbol().map(Stream::of).orElse(Stream.empty()))
                .collect(toImmutableSet());

        return restrictOutputs(idAllocator, semiJoinNode.getSource(), requiredSourceInputs)
                .map(newSource ->
                        semiJoinNode.replaceChildren(ImmutableList.of(
                                newSource, semiJoinNode.getFilteringSource())));
    }
}
