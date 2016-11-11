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

import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

final class PlanMatchingVisitor
        extends PlanVisitor<PlanMatchingContext, Boolean>
{
    private final Metadata metadata;
    private final Session session;

    PlanMatchingVisitor(Session session, Metadata metadata)
    {
        this.session = requireNonNull(session, "session is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public Boolean visitExchange(ExchangeNode node, PlanMatchingContext context)
    {
        checkState(node.getType() == ExchangeNode.Type.GATHER, "Only GATHER is supported");
        List<List<Symbol>> allInputs = node.getInputs();
        checkState(allInputs.size() == 1, "Multiple lists of inputs are not supported yet");

        List<Symbol> inputs = allInputs.get(0);
        List<Symbol> outputs = node.getOutputSymbols();

        boolean result = super.visitExchange(node, context);

        if (result) {
            ImmutableMap.Builder<Symbol, Expression> assignments = ImmutableMap.builder();
            for (int i = 0; i < inputs.size(); ++i) {
                assignments.put(outputs.get(i), inputs.get(i).toSymbolReference());
            }

            context.getExpressionAliases().updateAssignments(assignments.build());
        }
        return result;
    }

    @Override
    public Boolean visitProject(ProjectNode node, PlanMatchingContext context)
    {
        boolean result = super.visitProject(node, context);
        if (result) {
            context.getExpressionAliases().replaceAssignments(node.getAssignments());
        }
        return result;
    }

    @Override
    protected Boolean visitPlan(PlanNode node, PlanMatchingContext context)
    {
        List<PlanMatchingState> states = context.getPattern().downMatches(node, context.getExpressionAliases());

        // No shape match; don't need to check the internals of any of the nodes.
        if (states.isEmpty()) {
            return false;
        }

        // Leaf node in the plan.
        if (node.getSources().isEmpty()) {
            int terminatedUpMatchCount = 0;
            for (PlanMatchingState state : states) {
                // Don't consider un-terminated PlanMatchingStates.
                if (!state.isTerminated()) {
                    continue;
                }

                /*
                 * We have to call upMatches for two reasons:
                 * 1) Make sure there aren't any mismatches checking the internals of a leaf node.
                 * 2) Calling upMatches has the side-effect of adding aliases to the context's
                 *    ExpressionAliases. They'll be needed further up.
                 */
                if (context.getPattern().upMatches(node, session, metadata, context.getExpressionAliases())) {
                    ++terminatedUpMatchCount;
                }
            }

            checkState(terminatedUpMatchCount < 2, format("Ambiguous shape match on leaf node %s", node));
            return terminatedUpMatchCount == 1;
        }

        int upMatchCount = 0;
        for (PlanMatchingState state : states) {
            checkState(node.getSources().size() == state.getPatterns().size(), "Matchers count does not match count of sources");
            int i = 0;
            boolean sourcesMatch = true;

            /*
             * For every state, start with a clean set of aliases. Aliases from a different state
             * are not in scope.
             */
            ExpressionAliases stateAliases = new ExpressionAliases();
            for (PlanNode source : node.getSources()) {
                /*
                 * Create a context for each source individually. Aliases from one source
                 * shouldn't be visible in the context of other sources.
                 */
                PlanMatchingContext sourceContext = state.createContext(i++);
                sourcesMatch = sourcesMatch && source.accept(this, sourceContext);
                if (!sourcesMatch) {
                    break;
                }

                // Add the per-source aliases to the per-state aliases.
                stateAliases.putSourceAliases(sourceContext.getExpressionAliases());
            }

            /*
             * Try upMatching this node with the union of the aliases gathered from the
             * source nodes.
             */
            if (sourcesMatch && context.getPattern().upMatches(node, session, metadata, stateAliases)) {
                context.getExpressionAliases().putSourceAliases(stateAliases);
                ++upMatchCount;
            }
        }
        checkState(upMatchCount < 2, format("Ambiguous detail match on node %s", node));
        return upMatchCount == 1;
    }
}
