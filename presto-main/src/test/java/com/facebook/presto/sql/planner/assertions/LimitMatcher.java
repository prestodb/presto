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
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.sql.planner.assertions.MatchResult.NO_MATCH;
import static com.facebook.presto.sql.planner.assertions.MatchResult.match;
import static com.facebook.presto.sql.planner.assertions.Util.orderingSchemeMatches;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class LimitMatcher
        implements Matcher
{
    private final long limit;
    private final List<PlanMatchPattern.Ordering> tiesResolvers;
    private final boolean partial;

    public LimitMatcher(long limit, List<PlanMatchPattern.Ordering> tiesResolvers, boolean partial)
    {
        this.limit = limit;
        this.tiesResolvers = ImmutableList.copyOf(requireNonNull(tiesResolvers, "tiesResolvers is null"));
        this.partial = partial;
    }

    @Override
    public boolean shapeMatches(PlanNode node)
    {
        if (!(node instanceof LimitNode)) {
            return false;
        }

        LimitNode limitNode = (LimitNode) node;
        return limitNode.getCount() == limit
                && limitNode.isWithTies() == !tiesResolvers.isEmpty()
                && limitNode.isPartial() == partial;
    }

    @Override
    public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        checkState(shapeMatches(node));
        if (!((LimitNode) node).isWithTies()) {
            return match();
        }
        OrderingScheme tiesResolvingScheme = ((LimitNode) node).getTiesResolvingScheme().get();
        if (orderingSchemeMatches(tiesResolvers, tiesResolvingScheme, symbolAliases)) {
            return match();
        }
        return NO_MATCH;
    }
}
