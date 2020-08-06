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
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.JoinNode.DistributionType;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.sql.planner.assertions.MatchResult.NO_MATCH;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToExpression;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.isExpression;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

final class JoinMatcher
        implements Matcher
{
    private final JoinNode.Type joinType;
    private final List<ExpectedValueProvider<JoinNode.EquiJoinClause>> equiCriteria;
    private final Optional<Expression> filter;
    private final Optional<DistributionType> distributionType;
    private final Optional<DynamicFilterMatcher> dynamicFilter;

    JoinMatcher(
            JoinNode.Type joinType,
            List<ExpectedValueProvider<JoinNode.EquiJoinClause>> equiCriteria,
            Optional<Expression> filter,
            Optional<DistributionType> distributionType,
            Optional<DynamicFilterMatcher> dynamicFilter)
    {
        this.joinType = requireNonNull(joinType, "joinType is null");
        this.equiCriteria = requireNonNull(equiCriteria, "equiCriteria is null");
        this.filter = requireNonNull(filter, "filter can not be null");
        this.distributionType = requireNonNull(distributionType, "distributionType is null");
        this.dynamicFilter = requireNonNull(dynamicFilter, "dynamicFilter is null");
    }

    @Override
    public boolean shapeMatches(PlanNode node)
    {
        if (!(node instanceof JoinNode)) {
            return false;
        }

        JoinNode joinNode = (JoinNode) node;
        return joinNode.getType() == joinType;
    }

    @Override
    public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        checkState(shapeMatches(node), "Plan testing framework error: shapeMatches returned false in detailMatches in %s", this.getClass().getName());

        JoinNode joinNode = (JoinNode) node;

        if (joinNode.getCriteria().size() != equiCriteria.size()) {
            return NO_MATCH;
        }

        if (filter.isPresent()) {
            if (!joinNode.getFilter().isPresent()) {
                return NO_MATCH;
            }
            RowExpression expression = joinNode.getFilter().get();
            if (isExpression(expression)) {
                if (!new ExpressionVerifier(symbolAliases).process(castToExpression(expression), filter.get())) {
                    return NO_MATCH;
                }
            }
            else {
                if (!new RowExpressionVerifier(symbolAliases, metadata, session).process(filter.get(), expression)) {
                    return NO_MATCH;
                }
            }
        }
        else {
            if (joinNode.getFilter().isPresent()) {
                return NO_MATCH;
            }
        }

        if (distributionType.isPresent() && !distributionType.equals(joinNode.getDistributionType())) {
            return NO_MATCH;
        }

        /*
         * Have to use order-independent comparison; there are no guarantees what order
         * the equi criteria will have after planning and optimizing.
         */
        Set<List<String>> actual = joinNode.getCriteria().stream().map(criteria -> ImmutableList.of(criteria.getLeft().getName(), criteria.getRight().getName())).collect(toImmutableSet());
        Set<List<String>> expected =
                equiCriteria.stream()
                        .map(maker -> maker.getExpectedValue(symbolAliases))
                        .map(criteria -> ImmutableList.of(criteria.getLeft().getName(), criteria.getRight().getName()))
                        .collect(toImmutableSet());

        if (!expected.equals(actual)) {
            return NO_MATCH;
        }

        if (dynamicFilter.isPresent() && !dynamicFilter.get().match(joinNode, symbolAliases).isMatch()) {
            return NO_MATCH;
        }

        return MatchResult.match();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .omitNullValues()
                .add("type", joinType)
                .add("equiCriteria", equiCriteria)
                .add("filter", filter.orElse(null))
                .add("distributionType", distributionType)
                .add("dynamicFilter", dynamicFilter.map(DynamicFilterMatcher::getJoinExpectedMappings))
                .toString();
    }
}
