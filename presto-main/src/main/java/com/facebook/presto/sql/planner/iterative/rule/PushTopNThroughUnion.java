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
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.optimizations.SymbolMapper;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.TopNNode;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.sql.planner.plan.TopNNode.Step.PARTIAL;
import static com.google.common.collect.Iterables.getLast;
import static com.google.common.collect.Sets.intersection;

public class PushTopNThroughUnion
        implements Rule
{
    private static final Pattern PATTERN = Pattern.typeOf(TopNNode.class);

    @Override
    public Pattern getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<PlanNode> apply(PlanNode node, Lookup lookup, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, Session session)
    {
        if (!(node instanceof TopNNode)) {
            return Optional.empty();
        }

        TopNNode topNNode = (TopNNode) node;

        if (!topNNode.getStep().equals(PARTIAL)) {
            return Optional.empty();
        }

        PlanNode child = lookup.resolve(topNNode.getSource());
        if (!(child instanceof UnionNode)) {
            return Optional.empty();
        }
        UnionNode unionNode = (UnionNode) child;

        ImmutableList.Builder<PlanNode> sources = ImmutableList.builder();

        for (PlanNode source : unionNode.getSources()) {
            SymbolMapper.Builder symbolMapper = SymbolMapper.builder();
            Set<Symbol> sourceOutputSymbols = ImmutableSet.copyOf(source.getOutputSymbols());

            for (Symbol unionOutput : unionNode.getOutputSymbols()) {
                Set<Symbol> inputSymbols = ImmutableSet.copyOf(unionNode.getMultiSourceSymbolMapping().getInput(unionOutput));
                Symbol unionInput = getLast(intersection(inputSymbols, sourceOutputSymbols));
                symbolMapper.put(unionOutput, unionInput);
            }
            sources.add(symbolMapper.build().map(topNNode, source, idAllocator.getNextId()));
        }

        return Optional.of(new UnionNode(unionNode.getId(), unionNode.getMultiSourceSymbolMapping().replaceSources(sources.build())));
    }
}
