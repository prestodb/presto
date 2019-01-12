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

import io.prestosql.sql.planner.Symbol;

import static java.util.Objects.requireNonNull;

class SymbolAlias
        implements PlanTestSymbol
{
    private final String alias;

    SymbolAlias(String alias)
    {
        this.alias = requireNonNull(alias, "alias is null");
    }

    public Symbol toSymbol(SymbolAliases aliases)
    {
        return Symbol.from(aliases.get(alias));
    }

    @Override
    public String toString()
    {
        return alias;
    }
}
