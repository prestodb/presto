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
import com.facebook.presto.sql.planner.plan.ExceptNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class MergeExcept
    implements Rule
{
    @Override
    public Optional<PlanNode> apply(PlanNode node, Lookup lookup, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator)
    {
        if (!(node instanceof ExceptNode)) {
            return Optional.empty();
        }

        ExceptNode parent = (ExceptNode) node;

        PlanNode source = lookup.resolve(parent.getSources().get(0));
        if (!(source instanceof ExceptNode)) {
            return Optional.empty();
        }

        ExceptNode child = (ExceptNode) source;

        List<PlanNode> newChildren = new ArrayList<>();
        ImmutableListMultimap.Builder<Symbol, Symbol> newMappings = ImmutableListMultimap.builder();

        // we can only inline the first child because EXCEPT is not associative:
        // e.g., (a-b)-(c-d)
        // is equivalent to a-b-(c-d)
        // but not to a-b-c-d
        newChildren.addAll(child.getSources());
        for (Map.Entry<Symbol, Collection<Symbol>> entry : parent.getSymbolMapping().asMap().entrySet()) {
            Symbol inputSymbol = Iterables.get(entry.getValue(), 0);
            newMappings.putAll(entry.getKey(), child.getSymbolMapping().get(inputSymbol));
        }

        for (int i = 1; i < parent.getSources().size(); i++) {
            newChildren.add(parent.getSources().get(i));
            for (Map.Entry<Symbol, Collection<Symbol>> entry : parent.getSymbolMapping().asMap().entrySet()) {
                newMappings.put(entry.getKey(), Iterables.get(entry.getValue(), i));
            }
        }

        return Optional.of(
                new ExceptNode(
                        parent.getId(),
                        newChildren,
                        newMappings.build(),
                        parent.getOutputSymbols()));
    }
}
