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

import java.util.Collections;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class TypeProvider
{
    private final Map<Symbol, Type> types;

    public static TypeProvider viewOf(Map<Symbol, Type> types)
    {
        return new TypeProvider(types);
    }

    public static TypeProvider copyOf(Map<Symbol, Type> types)
    {
        return new TypeProvider(ImmutableMap.copyOf(types));
    }

    public static TypeProvider empty()
    {
        return new TypeProvider(ImmutableMap.of());
    }

    private TypeProvider(Map<Symbol, Type> types)
    {
        this.types = types;
    }

    public Type get(Symbol symbol)
    {
        requireNonNull(symbol, "symbol is null");

        Type type = types.get(symbol);
        checkArgument(type != null, "no type found found for symbol '%s'", symbol);

        return type;
    }

    public Map<Symbol, Type> allTypes()
    {
        // types may be a HashMap, so creating an ImmutableMap here would add extra cost when allTypes gets called frequently
        return Collections.unmodifiableMap(types);
    }
}
