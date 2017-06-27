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
import com.facebook.presto.sql.planner.SymbolsExtractor;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Pattern;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.google.common.collect.ImmutableSet;

import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.sql.planner.iterative.rule.Util.restrictChildOutputs;

/**
 * Non-Cross joins support output symbol selection, so make any project-off of child columns explicit in project nodes.
 */
public class PruneJoinChildrenColumns
        implements Rule
{
    private static final Pattern PATTERN = Pattern.node(JoinNode.class);

    @Override
    public Pattern getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<PlanNode> apply(PlanNode node, Lookup lookup, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, Session session)
    {
        JoinNode joinNode = (JoinNode) node;
        if (joinNode.isCrossJoin()) {
            return Optional.empty();
        }

        Set<Symbol> globallyUsableInputs = ImmutableSet.<Symbol>builder()
                .addAll(joinNode.getOutputSymbols())
                .addAll(
                        joinNode.getFilter()
                                .map(SymbolsExtractor::extractUnique)
                                .orElse(ImmutableSet.of()))
                .build();

        Set<Symbol> leftUsableInputs = ImmutableSet.<Symbol>builder()
                .addAll(globallyUsableInputs)
                .addAll(
                        joinNode.getCriteria().stream()
                                .map(JoinNode.EquiJoinClause::getLeft)
                                .iterator())
                .addAll(joinNode.getLeftHashSymbol().map(ImmutableSet::of).orElse(ImmutableSet.of()))
                .build();

        Set<Symbol> rightUsableInputs = ImmutableSet.<Symbol>builder()
                .addAll(globallyUsableInputs)
                .addAll(
                        joinNode.getCriteria().stream()
                                .map(JoinNode.EquiJoinClause::getRight)
                                .iterator())
                .addAll(joinNode.getRightHashSymbol().map(ImmutableSet::of).orElse(ImmutableSet.of()))
                .build();

        return restrictChildOutputs(idAllocator, joinNode, leftUsableInputs, rightUsableInputs);
    }
}
