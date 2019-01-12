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

import io.prestosql.sql.tree.SymbolReference;

import static java.util.Objects.requireNonNull;

public class MatchResult
{
    public static final MatchResult NO_MATCH = new MatchResult(false, new SymbolAliases());

    private final boolean matches;
    private final SymbolAliases newAliases;

    public static MatchResult match()
    {
        return new MatchResult(true, new SymbolAliases());
    }

    public static MatchResult match(String alias, SymbolReference symbolReference)
    {
        SymbolAliases newAliases = SymbolAliases.builder()
                .put(alias, symbolReference)
                .build();
        return new MatchResult(true, newAliases);
    }

    public static MatchResult match(SymbolAliases newAliases)
    {
        return new MatchResult(true, newAliases);
    }

    public MatchResult(boolean matches)
    {
        this(matches, new SymbolAliases());
    }

    private MatchResult(boolean matches, SymbolAliases newAliases)
    {
        this.matches = matches;
        this.newAliases = requireNonNull(newAliases, "newAliases is null");
    }

    public boolean isMatch()
    {
        return matches;
    }

    public SymbolAliases getAliases()
    {
        return newAliases;
    }

    @Override
    public String toString()
    {
        if (matches) {
            return "MATCH";
        }
        else {
            return "NO MATCH";
        }
    }
}
