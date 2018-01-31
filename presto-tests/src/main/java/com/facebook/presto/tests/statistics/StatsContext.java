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
package com.facebook.presto.tests.statistics;

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.Symbol;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

public class StatsContext
{
    private final Map<String, Symbol> columnSymbols;
    private final Map<Symbol, Type> symbolTypes;

    public StatsContext(Map<String, Symbol> columnSymbols, Map<Symbol, Type> symbolTypes)
    {
        this.columnSymbols = ImmutableMap.copyOf(columnSymbols);
        this.symbolTypes = ImmutableMap.copyOf(symbolTypes);
    }

    public Symbol getSymbolForColumn(String columnName)
    {
        checkArgument(columnSymbols.containsKey(columnName), "no symbol found for column '" + columnName + "'");
        return columnSymbols.get(columnName);
    }

    public Type getTypeForSymbol(Symbol symbol)
    {
        checkArgument(symbolTypes.containsKey(symbol), "no type found found for symbol '" + symbol + "'");
        return symbolTypes.get(symbol);
    }
}
