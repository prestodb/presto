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
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern.Ordering;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.PlanNode;

import java.util.List;

import static com.facebook.presto.sql.planner.assertions.MatchResult.NO_MATCH;
import static com.facebook.presto.sql.planner.assertions.Util.orderingSchemeMatches;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

final class ExchangeMatcher
        implements Matcher
{
    private final ExchangeNode.Scope scope;
    private final ExchangeNode.Type type;
    private final List<Ordering> orderBy;

    public ExchangeMatcher(ExchangeNode.Scope scope, ExchangeNode.Type type, List<Ordering> orderBy)
    {
        this.scope = scope;
        this.type = type;
        this.orderBy = requireNonNull(orderBy, "orderBy is null");
    }

    @Override
    public boolean shapeMatches(PlanNode node)
    {
        if (!(node instanceof ExchangeNode)) {
            return false;
        }

        ExchangeNode exchangeNode = (ExchangeNode) node;
        return exchangeNode.getScope() == scope && exchangeNode.getType() == type;
    }

    @Override
    public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        checkState(shapeMatches(node), "Plan testing framework error: shapeMatches returned false in detailMatches in %s", this.getClass().getName());
        ExchangeNode exchangeNode = (ExchangeNode) node;

        if (!orderBy.isEmpty()) {
            if (!exchangeNode.getOrderingScheme().isPresent()) {
                return NO_MATCH;
            }

            if (!orderingSchemeMatches(orderBy, exchangeNode.getOrderingScheme().get(), symbolAliases)) {
                return NO_MATCH;
            }
        }

        return MatchResult.match();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("scope", scope)
                .add("type", type)
                .add("orderBy", orderBy)
                .toString();
    }
}
