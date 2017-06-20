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
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Pattern;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.sql.planner.iterative.rule.Util.pruneInputs;
import static com.google.common.collect.ImmutableList.toImmutableList;

/**
 * Non-cross joins support output symbol selection, so absorb any project-off into the node.
 */
public class PruneJoinColumns
        implements Rule
{
    private static final Pattern PATTERN = Pattern.node(ProjectNode.class);

    @Override
    public Pattern getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<PlanNode> apply(PlanNode node, Lookup lookup, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, Session session)
    {
        ProjectNode parent = (ProjectNode) node;

        PlanNode child = lookup.resolve(parent.getSource());
        if (!(child instanceof JoinNode)) {
            return Optional.empty();
        }

        JoinNode joinNode = (JoinNode) child;
        if (joinNode.isCrossJoin()) {
            return Optional.empty();
        }

        Optional<Set<Symbol>> dependencies = pruneInputs(child.getOutputSymbols(), parent.getAssignments().getExpressions())
                .map(ImmutableSet::copyOf);
        if (!dependencies.isPresent()) {
            return Optional.empty();
        }

        return Optional.of(
                parent.replaceChildren(ImmutableList.of(
                        new JoinNode(
                                joinNode.getId(),
                                joinNode.getType(),
                                joinNode.getLeft(),
                                joinNode.getRight(),
                                joinNode.getCriteria(),
                                joinNode.getOutputSymbols().stream()
                                        .filter(dependencies.get()::contains)
                                        .collect(toImmutableList()),
                                joinNode.getFilter(),
                                joinNode.getLeftHashSymbol(),
                                joinNode.getRightHashSymbol(),
                                joinNode.getDistributionType()))));
    }
}
