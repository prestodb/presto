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
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.Maps;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.facebook.presto.sql.planner.plan.Patterns.project;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

public class PruneProjectColumnFields
        extends ProjectOffPushDownFieldRule<ProjectNode>
{
    public PruneProjectColumnFields()
    {
        super(project());
    }

    @Override
    protected Optional<PlanNode> pushDownProjectOff(
            PlanNodeIdAllocator idAllocator,
            ProjectNode childProjectNode,
            Set<Symbol> referencedOutputs)
    {
        Map<String, Symbol> symbolMap = referencedOutputs.stream().collect(toImmutableMap(Symbol::getName, Function.identity()));
        Function<Map.Entry<Symbol, Expression>, Map.Entry<Symbol, Expression>> rewrite = entry -> {
            Symbol newSymbol = requireNonNull(symbolMap.get(entry.getKey().getName()));
            if (entry.getValue() instanceof SymbolReference) {
                return Maps.immutableEntry(newSymbol, (Expression) newSymbol.toSymbolReference());
            }
            return Maps.immutableEntry(newSymbol, entry.getValue());
        };

        return Optional.of(
                new ProjectNode(
                        childProjectNode.getId(),
                        childProjectNode.getSource(),
                        childProjectNode.getAssignments().filter(symbol -> symbolMap.containsKey(symbol.getName())).rewriteEntry(rewrite)));
    }
}
