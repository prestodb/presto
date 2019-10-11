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
import static java.util.Objects.requireNonNull;

public class LookupVariableResolver
        implements VariableResolver
{
    private final Map<VariableReferenceExpression, ColumnHandle> assignments;
    private final Map<ColumnHandle, NullableValue> bindings;

    public LookupVariableResolver(Map<VariableReferenceExpression, ColumnHandle> assignments, Map<ColumnHandle, NullableValue> bindings)
    {
        this.assignments = requireNonNull(assignments, "assignments is null");
        this.bindings = ImmutableMap.copyOf(requireNonNull(bindings, "bindings is null"));
    }

    @Override
    public Object getValue(VariableReferenceExpression variable)
    {
        ColumnHandle column = assignments.get(variable);
        checkArgument(column != null, "Missing column assignment for %s", variable);

        if (!bindings.containsKey(column)) {
            // still return SymbolReference that will be consumed by interpreters
            return new Symbol(variable.getName()).toSymbolReference();
        }

        return bindings.get(column).getValue();
    }
}
