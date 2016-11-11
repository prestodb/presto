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
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.tree.Expression;

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
    public boolean downMatches(PlanNode node)
    {
        return node instanceof FilterNode;
    }

    @Override
    public boolean upMatches(PlanNode node, Session session, Metadata metadata, ExpressionAliases expressionAliases)
    {
        checkState(downMatches(node), "Plan testing framework error: downMatches returned false in upMatches in %s", this.getClass().getName());

        FilterNode filterNode = (FilterNode) node;
        return new ExpressionVerifier(expressionAliases).process(filterNode.getPredicate(), predicate);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("predicate", predicate)
                .toString();
    }
}
