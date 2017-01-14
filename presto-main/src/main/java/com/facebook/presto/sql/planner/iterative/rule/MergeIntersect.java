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

import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.IntersectNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class MergeIntersect
    implements Rule
{
    @Override
    public Optional<PlanNode> apply(PlanNode node, Lookup lookup, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator)
    {
        if (!(node instanceof IntersectNode)) {
            return Optional.empty();
        }

        List<PlanNode> children = node.getSources().stream()
                .map(lookup::resolve)
                .collect(Collectors.toList());

        if (children.stream().noneMatch(IntersectNode.class::isInstance)) {
            return Optional.empty();
        }

        IntersectNode parent = (IntersectNode) node;

        List<PlanNode> newChildren = new ArrayList<>();
        ImmutableListMultimap.Builder<Symbol, Symbol> newMappings = ImmutableListMultimap.builder();
        for (int i = 0; i < children.size(); i++) {
            PlanNode source = children.get(i);

            if (source instanceof IntersectNode) {
                IntersectNode child = (IntersectNode) source;
                newChildren.addAll(child.getSources());
                for (Map.Entry<Symbol, Collection<Symbol>> entry : parent.getSymbolMapping().asMap().entrySet()) {
                    Symbol inputSymbol = Iterables.get(entry.getValue(), i);
                    newMappings.putAll(entry.getKey(), child.getSymbolMapping().get(inputSymbol));
                }
            }
            else {
                newChildren.add(source);
                for (Map.Entry<Symbol, Collection<Symbol>> entry : parent.getSymbolMapping().asMap().entrySet()) {
                    newMappings.put(entry.getKey(), Iterables.get(entry.getValue(), i));
                }
            }
        }

        return Optional.of(
                new IntersectNode(
                        parent.getId(),
                        newChildren,
                        newMappings.build(),
                        parent.getOutputSymbols()));
    }
}
