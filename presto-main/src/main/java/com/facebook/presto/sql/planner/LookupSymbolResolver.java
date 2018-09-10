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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.predicate.NullableValue;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

public class LookupSymbolResolver
        implements SymbolResolver
{
    private final Map<String, Symbol> assignmentSymbolLookup;
    private final Map<Symbol, ColumnHandle> assignments;
    private final Map<ColumnHandle, NullableValue> bindings;

    public LookupSymbolResolver(Map<Symbol, ColumnHandle> assignments, Map<ColumnHandle, NullableValue> bindings)
    {
        requireNonNull(assignments, "assignments is null");
        requireNonNull(bindings, "bindings is null");

        this.assignmentSymbolLookup = assignments.keySet().stream()
                .collect(toImmutableMap(Symbol::getName, Function.identity()));
        this.assignments = ImmutableMap.copyOf(assignments);
        this.bindings = ImmutableMap.copyOf(bindings);
    }

    @Override
    public Object getValue(Symbol symbol)
    {
        Symbol assignmentSymbol = assignmentSymbolLookup.get(symbol.getName());
        checkArgument(assignmentSymbol != null, "Missing column assignment for %s", symbol.getName());
        ColumnHandle column = assignments.get(assignmentSymbol);
        checkArgument(column != null, "Missing column assignment for %s", symbol);

        if (!bindings.containsKey(column)) {
            return assignmentSymbol.toSymbolReference();
        }

        return bindings.get(column).getValue();
    }
}
