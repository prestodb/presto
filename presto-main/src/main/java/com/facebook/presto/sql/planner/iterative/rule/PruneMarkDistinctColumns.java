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

import com.facebook.presto.matching.Pattern;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.MarkDistinctNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.google.common.collect.Streams;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.facebook.presto.sql.planner.iterative.rule.Util.pushDownProjectOff;
import static com.facebook.presto.sql.planner.iterative.rule.Util.restrictChildOutputs;
import static com.google.common.collect.ImmutableSet.toImmutableSet;

public class PruneMarkDistinctColumns
        implements Rule
{
    private static final Pattern PATTERN = Pattern.typeOf(ProjectNode.class);

    @Override
    public Pattern getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<PlanNode> apply(PlanNode node, Context context)
    {
        return pushDownProjectOff(
                context.getLookup(),
                MarkDistinctNode.class,
                (ProjectNode) node,
                (markDistinctNode, referencedOutputs) -> {
                    if (!referencedOutputs.contains(markDistinctNode.getMarkerSymbol())) {
                        return Optional.of(markDistinctNode.getSource());
                    }

                    Set<Symbol> requiredInputs = Streams.concat(
                            referencedOutputs.stream()
                                    .filter(symbol -> !symbol.equals(markDistinctNode.getMarkerSymbol())),
                            markDistinctNode.getDistinctSymbols().stream(),
                            markDistinctNode.getHashSymbol().map(Stream::of).orElse(Stream.empty()))
                            .collect(toImmutableSet());

                    return restrictChildOutputs(context.getIdAllocator(), markDistinctNode, requiredInputs);
                });
    }
}
