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
package io.prestosql.sql.planner.assertions;

import io.prestosql.Session;
import io.prestosql.cost.StatsProvider;
import io.prestosql.metadata.Metadata;
import io.prestosql.sql.planner.plan.FilterNode;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.tree.Expression;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

final class FilterMatcher
        implements Matcher
{
    private final Expression predicate;

    FilterMatcher(Expression predicate)
    {
        this.predicate = requireNonNull(predicate, "predicate is null");
    }

    @Override
    public boolean shapeMatches(PlanNode node)
    {
        return node instanceof FilterNode;
    }

    @Override
    public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        checkState(shapeMatches(node), "Plan testing framework error: shapeMatches returned false in detailMatches in %s", this.getClass().getName());

        FilterNode filterNode = (FilterNode) node;
        ExpressionVerifier verifier = new ExpressionVerifier(symbolAliases);
        return new MatchResult(verifier.process(filterNode.getPredicate(), predicate));
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("predicate", predicate)
                .toString();
    }
}
