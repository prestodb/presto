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
import com.facebook.presto.sql.planner.plan.PlanNode;

import java.util.Set;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public abstract class BaseStrictSymbolsMatcher
        implements Matcher
{
    private final Function<PlanNode, Set<Symbol>> getActual;

    public BaseStrictSymbolsMatcher(Function<PlanNode, Set<Symbol>> getActual)
    {
        this.getActual = requireNonNull(getActual, "getActual is null");
    }

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

    public MatchResult detailMatches(PlanNode node, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        checkState(shapeMatches(node), "Plan testing framework error: shapeMatches returned false in detailMatches in %s", this.getClass().getName());

        Set<Symbol> expected = getExpectedSymbols(node, session, metadata, symbolAliases);

        return new MatchResult(getActual.apply(node).equals(getExpectedSymbols(node, session, metadata, symbolAliases)));
    }

    protected abstract Set<Symbol> getExpectedSymbols(PlanNode node, Session session, Metadata metadata, SymbolAliases symbolAliases);
}
