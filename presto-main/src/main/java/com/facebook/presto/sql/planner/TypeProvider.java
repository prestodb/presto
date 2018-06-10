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
package com.facebook.presto.sql.planner;

import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

public class TypeProvider
{
    private final Map<Symbol, Type> types;

    public TypeProvider(Map<Symbol, Type> types)
    {
        this.types = ImmutableMap.copyOf(types);
    }

    public static TypeProvider empty()
    {
        return new TypeProvider(ImmutableMap.of());
    }

    public Type get(Symbol symbol)
    {
        checkArgument(types.containsKey(symbol), "no type found found for symbol '" + symbol + "'");
        return types.get(symbol);
    }

    public Map<Symbol, Type> allTypes()
    {
        return types;
    }
}
