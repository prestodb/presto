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

import com.facebook.presto.sql.planner.Symbol;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

final class SymbolAliases
{
    private final Multimap<String, Symbol> map;

    SymbolAliases()
    {
        this.map = ArrayListMultimap.create();
    }

    SymbolAliases(SymbolAliases symbolAliases)
    {
        requireNonNull(symbolAliases, "symbolAliases are null");
        this.map = ArrayListMultimap.create(symbolAliases.map);
    }

    public void put(String alias, Symbol symbol)
    {
        alias = alias.toLowerCase();
        if (map.containsKey(alias)) {
            checkState(map.get(alias).contains(symbol), "Alias %s points to different symbols %s and %s", alias, symbol, map.get(alias));
        }
        else {
            checkState(!map.values().contains(symbol), "Symbol %s is already pointed by different alias than %s, check mapping %s", symbol, alias, map);
            map.put(alias, symbol);
        }
    }
}
