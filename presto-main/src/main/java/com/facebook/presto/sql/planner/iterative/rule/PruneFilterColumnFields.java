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
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.sql.planner.SymbolsExtractor.extractUniqueWithFields;
import static com.facebook.presto.sql.planner.iterative.rule.Util.restrictChildOutputs;
import static com.facebook.presto.sql.planner.plan.Patterns.filter;

public class PruneFilterColumnFields
        extends ProjectOffPushDownFieldRule<FilterNode>
{
    public PruneFilterColumnFields()
    {
        super(filter());
    }

    @Override
    protected Optional<PlanNode> pushDownProjectOff(PlanNodeIdAllocator idAllocator, FilterNode filterNode, Set<Symbol> referencedOutputs)
    {
        Map<String, Symbol> filterInputsMap = new HashMap<>(extractUniqueWithFields(ImmutableSet.of(filterNode.getPredicate())));

        referencedOutputs.forEach(symbol -> {
            String name = symbol.getName();
            if (filterInputsMap.containsKey(name)) {
                filterInputsMap.put(name, Symbol.merge(filterInputsMap.get(name), symbol));
            }
            else {
                filterInputsMap.put(name, symbol);
            }
        });
        Set<Symbol> prunedFilterInputs = ImmutableSet.copyOf(filterInputsMap.values());

        return restrictChildOutputs(idAllocator, filterNode, prunedFilterInputs);
    }
}
