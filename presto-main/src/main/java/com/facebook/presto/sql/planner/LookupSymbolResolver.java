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
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

public class LookupSymbolResolver
        implements SymbolResolver
{
    private final Map<Symbol, ColumnHandle> assignments;
    private final Map<ColumnHandle, NullableValue> bindings;

    public LookupSymbolResolver(Map<VariableReferenceExpression, ColumnHandle> assignments, Map<ColumnHandle, NullableValue> bindings)
    {
        requireNonNull(assignments, "assignments is null");
        requireNonNull(bindings, "bindings is null");

        this.assignments = assignments.entrySet().stream().collect(toImmutableMap(entry -> new Symbol(entry.getKey().getName()), Map.Entry::getValue));
        this.bindings = ImmutableMap.copyOf(bindings);
    }

    @Override
    public Object getValue(Symbol symbol)
    {
        ColumnHandle column = assignments.get(symbol);
        checkArgument(column != null, "Missing column assignment for %s", symbol);

        if (!bindings.containsKey(column)) {
            return symbol.toSymbolReference();
        }

        return bindings.get(column).getValue();
    }
}
