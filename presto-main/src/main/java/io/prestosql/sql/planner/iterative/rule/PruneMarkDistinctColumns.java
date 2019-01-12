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

import com.google.common.collect.Streams;
import io.prestosql.sql.planner.PlanNodeIdAllocator;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.plan.MarkDistinctNode;
import io.prestosql.sql.planner.plan.PlanNode;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.prestosql.sql.planner.iterative.rule.Util.restrictChildOutputs;
import static io.prestosql.sql.planner.plan.Patterns.markDistinct;

public class PruneMarkDistinctColumns
        extends ProjectOffPushDownRule<MarkDistinctNode>
{
    public PruneMarkDistinctColumns()
    {
        super(markDistinct());
    }

    @Override
    protected Optional<PlanNode> pushDownProjectOff(PlanNodeIdAllocator idAllocator, MarkDistinctNode markDistinctNode, Set<Symbol> referencedOutputs)
    {
        if (!referencedOutputs.contains(markDistinctNode.getMarkerSymbol())) {
            return Optional.of(markDistinctNode.getSource());
        }

        Set<Symbol> requiredInputs = Streams.concat(
                referencedOutputs.stream()
                        .filter(symbol -> !symbol.equals(markDistinctNode.getMarkerSymbol())),
                markDistinctNode.getDistinctSymbols().stream(),
                markDistinctNode.getHashSymbol().map(Stream::of).orElse(Stream.empty()))
                .collect(toImmutableSet());

        return restrictChildOutputs(idAllocator, markDistinctNode, requiredInputs);
    }
}
