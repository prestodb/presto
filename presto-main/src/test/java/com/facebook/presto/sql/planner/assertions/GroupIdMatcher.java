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
import com.facebook.presto.sql.planner.plan.GroupIdNode;
import com.facebook.presto.sql.planner.plan.PlanNode;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.sql.planner.assertions.MatchResult.NO_MATCH;
import static com.facebook.presto.sql.planner.assertions.MatchResult.match;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;

public class GroupIdMatcher
        implements Matcher
{
    private final List<List<String>> groups;
    private final Map<String, String> identityMappings;
    private final String groupIdAlias;

    public GroupIdMatcher(List<List<String>> groups, Map<String, String> identityMappings, String groupIdAlias)
    {
        this.groups = groups;
        this.identityMappings = identityMappings;
        this.groupIdAlias = groupIdAlias;
    }

    @Override
    public boolean shapeMatches(PlanNode node)
    {
        return node instanceof GroupIdNode;
    }

    @Override
    public MatchResult detailMatches(PlanNode node, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        checkState(shapeMatches(node), "Plan testing framework error: shapeMatches returned false in detailMatches in %s", this.getClass().getName());

        GroupIdNode groudIdNode = (GroupIdNode) node;
        List<List<Symbol>> actualGroups = groudIdNode.getGroupingSets();
        Map<Symbol, Symbol> actualArgumentMappings = groudIdNode.getArgumentMappings();

        if (actualGroups.size() != groups.size()) {
            return NO_MATCH;
        }

        for (int i = 0; i < actualGroups.size(); i++) {
            if (!AggregationMatcher.matches(groups.get(i), actualGroups.get(i), symbolAliases)) {
                return NO_MATCH;
            }
        }

        if (!AggregationMatcher.matches(identityMappings.keySet(), actualArgumentMappings.keySet(), symbolAliases)) {
            return NO_MATCH;
        }

        return match(groupIdAlias, groudIdNode.getGroupIdSymbol().toSymbolReference());
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("groups", groups)
                .toString();
    }
}
