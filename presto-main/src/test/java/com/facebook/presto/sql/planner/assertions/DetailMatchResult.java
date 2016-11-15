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

import com.facebook.presto.sql.tree.SymbolReference;

import static java.util.Objects.requireNonNull;

public class DetailMatchResult
{
    public static final DetailMatchResult NO_MATCH = new DetailMatchResult(false, new SymbolAliases());

    private final boolean matches;
    private final SymbolAliases newAliases;

    public static DetailMatchResult match()
    {
        return new DetailMatchResult(true, new SymbolAliases());
    }

    public static DetailMatchResult match(String alias, SymbolReference symbolReference)
    {
        SymbolAliases newAliases = SymbolAliases.builder()
                .put(alias, symbolReference)
                .build();
        return new DetailMatchResult(true, newAliases);
    }

    public static DetailMatchResult match(SymbolAliases newAliases)
    {
        return new DetailMatchResult(true, newAliases);
    }

    public DetailMatchResult(boolean matches)
    {
        this(matches, new SymbolAliases());
    }

    private DetailMatchResult(boolean matches, SymbolAliases newAliases)
    {
        this.matches = matches;
        this.newAliases = requireNonNull(newAliases, "newAliases is null");
    }

    public boolean getMatches()
    {
        return matches;
    }

    public SymbolAliases getNewAliases()
    {
        return newAliases;
    }
}
