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
package com.facebook.presto.sql.planner.assertions;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableMap;

import java.util.List;

import static com.facebook.presto.sql.planner.assertions.DetailMatchResult.NO_MATCH;
import static com.facebook.presto.sql.planner.assertions.DetailMatchResult.match;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

final class PlanMatchingVisitor
        extends PlanVisitor<PlanMatchPattern, DetailMatchResult>
{
    private final Metadata metadata;
    private final Session session;

    PlanMatchingVisitor(Session session, Metadata metadata)
    {
        this.session = requireNonNull(session, "session is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public DetailMatchResult visitExchange(ExchangeNode node, PlanMatchPattern pattern)
    {
        checkState(node.getType() == ExchangeNode.Type.GATHER, "Only GATHER is supported");
        List<List<Symbol>> allInputs = node.getInputs();
        checkState(allInputs.size() == 1, "Multiple lists of inputs are not supported yet");

        List<Symbol> inputs = allInputs.get(0);
        List<Symbol> outputs = node.getOutputSymbols();

        DetailMatchResult result = super.visitExchange(node, pattern);

        if (!result.getMatches()) {
            return result;
        }

        ImmutableMap.Builder<Symbol, Expression> assignments = ImmutableMap.builder();
        for (int i = 0; i < inputs.size(); ++i) {
            assignments.put(outputs.get(i), inputs.get(i).toSymbolReference());
        }

        return match(result.getNewAliases().updateAssignments(assignments.build()));
    }

    @Override
    public DetailMatchResult visitProject(ProjectNode node, PlanMatchPattern pattern)
    {
        DetailMatchResult result = super.visitProject(node, pattern);

        if (!result.getMatches()) {
            return result;
        }

        return match(result.getNewAliases().replaceAssignments(node.getAssignments()));
    }

    @Override
    protected DetailMatchResult visitPlan(PlanNode node, PlanMatchPattern pattern)
    {
        List<PlanMatchingState> states = pattern.downMatches(node);

        // No shape match; don't need to check the internals of any of the nodes.
        if (states.isEmpty()) {
            return NO_MATCH;
        }

        // Leaf node in the plan.
        if (node.getSources().isEmpty()) {
            return matchLeaf(node, pattern, states);
        }

        DetailMatchResult result = NO_MATCH;
        for (PlanMatchingState state : states) {
            // Traverse down the tree, checking to see if the sources match the source patterns in state.
            DetailMatchResult sourcesMatch = matchSources(node, state);

            if (!sourcesMatch.getMatches()) {
                continue;
            }

            // Try upMatching this node with the the aliases gathered from the source nodes.
            SymbolAliases allSourceAliases = sourcesMatch.getNewAliases();
            DetailMatchResult matchResult = pattern.upMatches(node, session, metadata, allSourceAliases);
            if (matchResult.getMatches()) {
                checkState(result == NO_MATCH, format("Ambiguous match on node %s", node));
                result = match(allSourceAliases.withAliases(matchResult.getNewAliases()));
            }
        }
        return result;
    }

    private DetailMatchResult matchLeaf(PlanNode node, PlanMatchPattern pattern, List<PlanMatchingState> states)
    {
        DetailMatchResult result = NO_MATCH;

        for (PlanMatchingState state : states) {
            // Don't consider un-terminated PlanMatchingStates.
            if (!state.isTerminated()) {
                continue;
            }

                /*
                 * We have to call upMatches for two reasons:
                 * 1) Make sure there aren't any mismatches checking the internals of a leaf node.
                 * 2) Collect the aliases from the source nodes so we can add them to
                 *    SymbolAliases. They'll be needed further up.
                 */
            DetailMatchResult matchResult = pattern.upMatches(node, session, metadata, new SymbolAliases());
            if (matchResult.getMatches()) {
                checkState(result == NO_MATCH, format("Ambiguous match on leaf node %s", node));
                result = matchResult;
            }
        }

        return result;
    }

    private DetailMatchResult matchSources(PlanNode node, PlanMatchingState state)
    {
        List<PlanMatchPattern> sourcePatterns = state.getPatterns();
        checkState(node.getSources().size() == sourcePatterns.size(), "Matchers count does not match count of sources");

        int i = 0;
        SymbolAliases.Builder allSourceAliases = SymbolAliases.builder();
        for (PlanNode source : node.getSources()) {
                /*
                 * Create a context for each source individually. Aliases from one source
                 * shouldn't be visible in the context of other sources.
                 */
            DetailMatchResult matchResult = source.accept(this, sourcePatterns.get(i++));
            if (!matchResult.getMatches()) {
                return NO_MATCH;
            }

            // Add the per-source aliases to the per-state aliases.
            allSourceAliases.putAll(matchResult.getNewAliases());
        }

        return match(allSourceAliases.build());
    }
}
