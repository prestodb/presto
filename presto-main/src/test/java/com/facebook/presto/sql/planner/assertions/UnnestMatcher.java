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
import com.facebook.presto.sql.planner.plan.UnnestNode;
import com.google.common.collect.ImmutableMap;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.sql.analyzer.ExpressionTreeUtils.createSymbolReference;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class UnnestMatcher
        implements Matcher
{
    private final Map<String, List<String>> unnestMap;

    public UnnestMatcher(Map<String, List<String>> unnestMap)
    {
        this.unnestMap = ImmutableMap.copyOf(unnestMap);
    }

    @Override
    public boolean shapeMatches(PlanNode node)
    {
        return node instanceof UnnestNode;
    }

    @Override
    public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        checkState(shapeMatches(node), "Plan testing framework error: shapeMatches returned false in detailMatches in %s", this.getClass().getName());
        UnnestNode unnestNode = (UnnestNode) node;
        if (unnestMap.size() != unnestNode.getUnnestVariables().size()) {
            return MatchResult.NO_MATCH;
        }

        if (!AggregationMatcher.matches(unnestMap.keySet(), unnestNode.getUnnestVariables().keySet(), symbolAliases)) {
            return MatchResult.NO_MATCH;
        }

        List<String> expectedUnnestVariables = unnestMap.values().stream().flatMap(Collection::stream).collect(toImmutableList());
        List<VariableReferenceExpression> actualUnnestVariables = unnestNode.getUnnestVariables().values().stream().flatMap(Collection::stream).collect(toImmutableList());
        if (expectedUnnestVariables.size() != actualUnnestVariables.size()) {
            return MatchResult.NO_MATCH;
        }

        SymbolAliases.Builder builder = SymbolAliases.builder();
        for (int i = 0; i < expectedUnnestVariables.size(); ++i) {
            builder.put(expectedUnnestVariables.get(i), createSymbolReference(actualUnnestVariables.get(i)));
        }
        return MatchResult.match(builder.build());
    }
}
