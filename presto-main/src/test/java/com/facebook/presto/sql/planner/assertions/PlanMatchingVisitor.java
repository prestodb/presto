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
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;

import java.util.List;

import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkState;
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
    protected Boolean visitPlan(PlanNode node, PlanMatchingContext context)
    {
        List<PlanMatchingState> states = context.getPattern().matches(node, session, metadata, context.getSymbolAliases());

        if (states.isEmpty()) {
            return false;
        }

        if (node.getSources().isEmpty()) {
            return !filterTerminated(states).isEmpty();
        }

        for (PlanMatchingState state : states) {
            checkState(node.getSources().size() == state.getPatterns().size(), "Matchers count does not match count of sources");
            int i = 0;
            boolean sourcesMatch = true;
            for (PlanNode source : node.getSources()) {
                sourcesMatch = sourcesMatch && source.accept(this, state.createContext(i++));
            }
            if (sourcesMatch) {
                return true;
            }
        }
        return false;
    }

    private List<PlanMatchingState> filterTerminated(List<PlanMatchingState> states)
    {
        return states.stream()
                .filter(PlanMatchingState::isTerminated)
                .collect(toImmutableList());
    }
}
