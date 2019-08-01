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

import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.plan.UnionNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.optimizations.SymbolMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.spi.plan.TopNNode.Step.PARTIAL;
import static com.facebook.presto.sql.planner.plan.Patterns.TopN.step;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.facebook.presto.sql.planner.plan.Patterns.topN;
import static com.facebook.presto.sql.planner.plan.Patterns.union;
import static com.google.common.collect.Iterables.getLast;
import static com.google.common.collect.Sets.intersection;

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

            Set<VariableReferenceExpression> sourceOutputVariables = ImmutableSet.copyOf(source.getOutputVariables());

            for (VariableReferenceExpression unionOutput : unionNode.getOutputVariables()) {
                Set<VariableReferenceExpression> inputVariables = ImmutableSet.copyOf(unionNode.getVariableMapping().get(unionOutput));
                VariableReferenceExpression unionInput = getLast(intersection(inputVariables, sourceOutputVariables));
                symbolMapper.put(unionOutput, unionInput);
            }
            sources.add(symbolMapper.build().map(topNNode, source, context.getIdAllocator().getNextId()));
        }

        return Result.ofPlanNode(new UnionNode(
                unionNode.getId(),
                sources.build(),
                unionNode.getOutputVariables(),
                unionNode.getVariableMapping()));
    }
}
