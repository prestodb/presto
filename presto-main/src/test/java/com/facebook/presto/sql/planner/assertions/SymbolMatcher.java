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
import com.google.common.base.MoreObjects;

import static com.google.common.base.Preconditions.checkState;

final class SymbolMatcher
        implements Matcher
{
    private final Symbol expectedSymbol;
    private final String alias;

    SymbolMatcher(Symbol expectedSymbol, String alias)
    {
        this.expectedSymbol = expectedSymbol;
        this.alias = alias;
    }

    @Override
    public boolean matches(PlanNode node, Session session, Metadata metadata, ExpressionAliases expressionAliases)
    {
        Symbol symbol = null;
        for (Symbol outputSymbol : node.getOutputSymbols()) {
            if (expectedSymbol.equals(outputSymbol)) {
                checkState(symbol == null, "%s symbol was found multiple times in %s", expectedSymbol.getName(), node.getOutputSymbols());
                symbol = outputSymbol;
            }
        }
        if (symbol != null) {
            expressionAliases.put(alias, symbol.toSymbolReference());
            return true;
        }
        return false;
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                .add("alias", alias)
                .add("name", expectedSymbol)
                .toString();
    }
}
