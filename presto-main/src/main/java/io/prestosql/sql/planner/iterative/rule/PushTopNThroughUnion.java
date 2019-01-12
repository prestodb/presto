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
import com.google.common.collect.ImmutableSet;
import io.prestosql.matching.Capture;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.optimizations.SymbolMapper;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.TopNNode;
import io.prestosql.sql.planner.plan.UnionNode;

import java.util.Set;

import static com.google.common.collect.Iterables.getLast;
import static com.google.common.collect.Sets.intersection;
import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.sql.planner.plan.Patterns.TopN.step;
import static io.prestosql.sql.planner.plan.Patterns.source;
import static io.prestosql.sql.planner.plan.Patterns.topN;
import static io.prestosql.sql.planner.plan.Patterns.union;
import static io.prestosql.sql.planner.plan.TopNNode.Step.PARTIAL;

public class PushTopNThroughUnion
        implements Rule<TopNNode>
{
    private static final Capture<UnionNode> CHILD = newCapture();

    private static final Pattern<TopNNode> PATTERN = topN()
            .with(step().equalTo(PARTIAL))
            .with(source().matching(union().capturedAs(CHILD)));

    @Override
    public Pattern<TopNNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(TopNNode topNNode, Captures captures, Context context)
    {
        UnionNode unionNode = captures.get(CHILD);

        ImmutableList.Builder<PlanNode> sources = ImmutableList.builder();

        for (PlanNode source : unionNode.getSources()) {
            SymbolMapper.Builder symbolMapper = SymbolMapper.builder();
            Set<Symbol> sourceOutputSymbols = ImmutableSet.copyOf(source.getOutputSymbols());

            for (Symbol unionOutput : unionNode.getOutputSymbols()) {
                Set<Symbol> inputSymbols = ImmutableSet.copyOf(unionNode.getSymbolMapping().get(unionOutput));
                Symbol unionInput = getLast(intersection(inputSymbols, sourceOutputSymbols));
                symbolMapper.put(unionOutput, unionInput);
            }
            sources.add(symbolMapper.build().map(topNNode, source, context.getIdAllocator().getNextId()));
        }

        return Result.ofPlanNode(new UnionNode(
                unionNode.getId(),
                sources.build(),
                unionNode.getSymbolMapping(),
                unionNode.getOutputSymbols()));
    }
}
