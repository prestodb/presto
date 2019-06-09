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
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.LateralJoinNode;
import com.facebook.presto.sql.tree.SymbolReference;

import java.util.List;

import static com.facebook.presto.sql.planner.assertions.MatchResult.NO_MATCH;
import static com.facebook.presto.sql.planner.assertions.MatchResult.match;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class CorrelationMatcher
        implements Matcher
{
    private final List<String> correlation;

    CorrelationMatcher(List<String> correlation)
    {
        this.correlation = requireNonNull(correlation, "correlation is null");
    }

    @Override
    public boolean shapeMatches(PlanNode node)
    {
        return node instanceof ApplyNode || node instanceof LateralJoinNode;
    }

    @Override
    public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        checkState(
                shapeMatches(node),
                "Plan testing framework error: shapeMatches returned false in detailMatches in %s",
                this.getClass().getName());

        List<VariableReferenceExpression> actualCorrelation = getCorrelation(node);
        if (this.correlation.size() != actualCorrelation.size()) {
            return NO_MATCH;
        }

        int i = 0;
        for (String alias : this.correlation) {
            if (!symbolAliases.get(alias).equals(new SymbolReference(actualCorrelation.get(i++).getName()))) {
                return NO_MATCH;
            }
        }
        return match();
    }

    private List<VariableReferenceExpression> getCorrelation(PlanNode node)
    {
        if (node instanceof ApplyNode) {
            return ((ApplyNode) node).getCorrelation();
        }
        else if (node instanceof LateralJoinNode) {
            return ((LateralJoinNode) node).getCorrelation();
        }
        else {
            throw new IllegalStateException("Unexpected plan node: " + node);
        }
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("correlation", correlation)
                .toString();
    }
}
