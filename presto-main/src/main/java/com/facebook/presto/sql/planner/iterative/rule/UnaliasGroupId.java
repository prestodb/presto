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
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.GroupIdNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public class UnaliasGroupId
        implements Rule
{
    @Override
    public Optional<PlanNode> apply(PlanNode node, Lookup lookup, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator)
    {
        if (!(node instanceof GroupIdNode)) {
            return Optional.empty();
        }

        GroupIdNode parent = (GroupIdNode) node;

        PlanNode source = lookup.resolve(parent.getSource());
        if (!(source instanceof ProjectNode)) {
            return Optional.empty();
        }

        ProjectNode child = (ProjectNode) source;

        Renamer renamer = Renamer.from(child);

        if (!renamer.hasRenames()) {
            return Optional.empty();
        }

        List<List<Symbol>> rewrittenGroupingSets = parent.getGroupingSets().stream()
                .map(s -> s.stream()
                        .map(renamer::rename)
                        .collect(Collectors.toList()))
                .collect(Collectors.toList());

        Map<Symbol, Symbol> rewrittenArgumentMappings = parent.getArgumentMappings()
                .entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> renamer.rename(e.getValue())));

        Map<Symbol, Symbol> rewrittenGroupingSetMappings = parent.getGroupingSetMappings()
                .entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> renamer.rename(e.getValue())));

        GroupIdNode rewrittenNode = new GroupIdNode(
                parent.getId(),
                renamer.renameOutputs(child),
                rewrittenGroupingSets,
                rewrittenGroupingSetMappings,
                rewrittenArgumentMappings,
                parent.getGroupIdSymbol());

        return Optional.of(
                new ProjectNode(
                        idAllocator.getNextId(),
                        rewrittenNode,
                        // compute assignments to preserve original output names
                        Assignments.copyOf(
                                parent.getOutputSymbols().stream()
                                        .collect(Collectors.toMap(
                                                Function.identity(),
                                                symbol -> renamer.rename(symbol).toSymbolReference())))));
    }
}
