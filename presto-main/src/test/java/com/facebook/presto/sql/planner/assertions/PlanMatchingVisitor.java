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
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.GroupReference;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.InternalPlanVisitor;

import java.util.List;

import static com.facebook.presto.sql.planner.assertions.MatchResult.NO_MATCH;
import static com.facebook.presto.sql.planner.assertions.MatchResult.match;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.asSymbolReference;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToRowExpression;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

final class PlanMatchingVisitor
        extends InternalPlanVisitor<MatchResult, PlanMatchPattern>
{
    private final Metadata metadata;
    private final Session session;
    private final StatsProvider statsProvider;
    private final Lookup lookup;

    PlanMatchingVisitor(Session session, Metadata metadata, StatsProvider statsProvider, Lookup lookup)
    {
        this.session = requireNonNull(session, "session is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.statsProvider = requireNonNull(statsProvider, "statsProvider is null");
        this.lookup = requireNonNull(lookup, "lookup is null");
    }

    @Override
    public MatchResult visitExchange(ExchangeNode node, PlanMatchPattern pattern)
    {
        List<List<VariableReferenceExpression>> allInputs = node.getInputs();
        List<VariableReferenceExpression> outputs = node.getOutputVariables();

        MatchResult result = super.visitExchange(node, pattern);

        if (!result.isMatch()) {
            return result;
        }

        SymbolAliases newAliases = result.getAliases();
        for (List<VariableReferenceExpression> inputs : allInputs) {
            Assignments.Builder assignments = Assignments.builder();
            for (int i = 0; i < inputs.size(); ++i) {
                assignments.put(outputs.get(i), castToRowExpression(asSymbolReference(inputs.get(i))));
            }
            newAliases = newAliases.updateAssignments(assignments.build());
        }

        return match(newAliases);
    }

    @Override
    public MatchResult visitProject(ProjectNode node, PlanMatchPattern pattern)
    {
        MatchResult result = super.visitProject(node, pattern);

        if (!result.isMatch()) {
            return result;
        }

        return match(result.getAliases().replaceAssignments(node.getAssignments()));
    }

    @Override
    public MatchResult visitGroupReference(GroupReference node, PlanMatchPattern pattern)
    {
        MatchResult match = lookup.resolve(node).accept(this, pattern);
        if (match.isMatch()) {
            return match;
        }
        return matchLeaf(node, pattern, pattern.shapeMatches(node));
    }

    @Override
    public MatchResult visitPlan(PlanNode node, PlanMatchPattern pattern)
    {
        List<PlanMatchingState> states = pattern.shapeMatches(node);

        // No shape match; don't need to check the internals of any of the nodes.
        if (states.isEmpty()) {
            return NO_MATCH;
        }

        // Leaf node in the plan.
        if (node.getSources().isEmpty()) {
            return matchLeaf(node, pattern, states);
        }

        MatchResult result = NO_MATCH;
        for (PlanMatchingState state : states) {
            // Traverse down the tree, checking to see if the sources match the source patterns in state.
            MatchResult sourcesMatch = matchSources(node, state);

            if (!sourcesMatch.isMatch()) {
                continue;
            }

            // Try upMatching this node with the aliases gathered from the source nodes.
            SymbolAliases allSourceAliases = sourcesMatch.getAliases();
            MatchResult matchResult = pattern.detailMatches(node, statsProvider, session, metadata, allSourceAliases);
            if (matchResult.isMatch()) {
                checkState(result == NO_MATCH, format("Ambiguous match on node %s", node));
                result = match(allSourceAliases.withNewAliases(matchResult.getAliases()));
            }
        }
        return result;
    }

    private MatchResult matchLeaf(PlanNode node, PlanMatchPattern pattern, List<PlanMatchingState> states)
    {
        MatchResult result = NO_MATCH;

        for (PlanMatchingState state : states) {
            // Don't consider un-terminated PlanMatchingStates.
            if (!state.isTerminated()) {
                continue;
            }

            /*
             * We have to call detailMatches for two reasons:
             * 1) Make sure there aren't any mismatches checking the internals of a leaf node.
             * 2) Collect the aliases from the source nodes so we can add them to
             *    SymbolAliases. They'll be needed further up.
             */
            MatchResult matchResult = pattern.detailMatches(node, statsProvider, session, metadata, new SymbolAliases());
            if (matchResult.isMatch()) {
                checkState(result == NO_MATCH, format("Ambiguous match on leaf node %s", node));
                result = matchResult;
            }
        }

        return result;
    }

    /*
     * This is a little counter-intuitive. Calling matchSources calls
     * source.accept, which (eventually) ends up calling into visitPlan
     * recursively. Assuming the plan and pattern currently being matched
     * actually match each other, eventually you hit the leaf nodes. At that
     * point, visitPlan starts by returning the match result for the leaf nodes
     * containing the symbol aliases needed by further up.
     *
     * For the non-leaf nodes, an invocation of matchSources returns a match
     * result for a successful match containing the union of all of the symbol
     * aliases added by the sources of the node currently being visited.
     *
     * Visiting that node proceeds by trying to apply the current pattern's
     * detailMatches() method to the node being visited. When a match is found,
     * visitPlan returns a match result containing the aliases for all of the
     * current node's sources, and the aliases for the current node.
     */
    private MatchResult matchSources(PlanNode node, PlanMatchingState state)
    {
        List<PlanMatchPattern> sourcePatterns = state.getPatterns();
        checkState(node.getSources().size() == sourcePatterns.size(), "Matchers count does not match count of sources");

        int i = 0;
        SymbolAliases.Builder allSourceAliases = SymbolAliases.builder();
        for (PlanNode source : node.getSources()) {
            // Match sources to patterns 1:1
            MatchResult matchResult = source.accept(this, sourcePatterns.get(i++));
            if (!matchResult.isMatch()) {
                return NO_MATCH;
            }

            // Add the per-source aliases to the per-state aliases.
            allSourceAliases.putAll(matchResult.getAliases());
        }

        return match(allSourceAliases.build());
    }
}
