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
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Set;

import static com.facebook.presto.util.ImmutableCollectors.toImmutableSet;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

final class JoinMatcher
        implements Matcher
{
    private final JoinNode.Type joinType;
    private final List<ExpectedValueProvider<JoinNode.EquiJoinClause>> equiCriteria;

    JoinMatcher(JoinNode.Type joinType, List<ExpectedValueProvider<JoinNode.EquiJoinClause>> equiCriteria)
    {
        this.joinType = requireNonNull(joinType, "joinType is null");
        this.equiCriteria = requireNonNull(equiCriteria, "equiCriteria is null");
    }

    @Override
    public boolean downMatches(PlanNode node)
    {
        if (!(node instanceof JoinNode)) {
            return false;
        }

        JoinNode joinNode = (JoinNode) node;
        return joinNode.getType() == joinType;
    }

    @Override
    public boolean upMatches(PlanNode node, Session session, Metadata metadata, ExpressionAliases expressionAliases)
    {
        checkState(downMatches(node), "Plan testing framework error: downMatches returned false in upMatches in %s", this.getClass().getName());

        JoinNode joinNode = (JoinNode) node;

        if (joinNode.getCriteria().size() != equiCriteria.size()) {
            return false;
        }

        /*
         * Have to use order-independent comparison; there are no guarantees what order
         * the equi criteria will have after planning and optimizing.
         */
        Set<JoinNode.EquiJoinClause> actual = ImmutableSet.copyOf(joinNode.getCriteria());
        Set<JoinNode.EquiJoinClause> expected =
                equiCriteria.stream()
                .map(maker -> maker.getExpectedValue(expressionAliases))
                .collect(toImmutableSet());

        return expected.equals(actual);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("equiCriteria", equiCriteria)
                .toString();
    }
}
