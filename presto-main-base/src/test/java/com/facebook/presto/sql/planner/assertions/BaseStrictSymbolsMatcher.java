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

import java.util.Set;
import java.util.function.Function;

import static com.facebook.presto.common.type.UnknownType.UNKNOWN;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

public abstract class BaseStrictSymbolsMatcher
        implements Matcher
{
    private final Function<PlanNode, Set<VariableReferenceExpression>> getActual;

    public BaseStrictSymbolsMatcher(Function<PlanNode, Set<VariableReferenceExpression>> getActual)
    {
        this.getActual = requireNonNull(getActual, "getActual is null");
    }

    @Override
    public boolean shapeMatches(PlanNode node)
    {
        try {
            getActual.apply(node);
            return true;
        }
        catch (ClassCastException e) {
            return false;
        }
    }

    @Override
    public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        checkState(shapeMatches(node), "Plan testing framework error: shapeMatches returned false in detailMatches in %s", this.getClass().getName());
        return new MatchResult(match(getActual.apply(node), getExpectedVariables(node, session, metadata, symbolAliases)));
    }

    protected abstract Set<VariableReferenceExpression> getExpectedVariables(PlanNode node, Session session, Metadata metadata, SymbolAliases symbolAliases);

    boolean match(Set<VariableReferenceExpression> actual, Set<VariableReferenceExpression> expected)
    {
        if (expected.stream().anyMatch(variable -> variable.getType().equals(UNKNOWN))) {
            return actual.stream().map(VariableReferenceExpression::getName).collect(toImmutableSet()).equals(expected.stream().map(VariableReferenceExpression::getName).collect(toImmutableSet()));
        }
        return actual.equals(expected);
    }
}
