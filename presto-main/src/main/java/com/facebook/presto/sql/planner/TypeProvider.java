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

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableMap;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

public class TypeProvider
{
    private final Map<String, Type> types;

    public static TypeProvider viewOf(Map<String, Type> types)
    {
        return new TypeProvider(types);
    }

    public static TypeProvider copyOf(Map<String, Type> types)
    {
        return new TypeProvider(ImmutableMap.copyOf(types));
    }

    public static TypeProvider empty()
    {
        return new TypeProvider(ImmutableMap.of());
    }

    public static TypeProvider fromVariables(Collection<VariableReferenceExpression> variables)
    {
        return new TypeProvider(variables.stream().collect(toImmutableMap(VariableReferenceExpression::getName, VariableReferenceExpression::getType)));
    }

    private TypeProvider(Map<String, Type> types)
    {
        this.types = types;
    }

    public Type get(Expression expression)
    {
        requireNonNull(expression, "expression is null");
        Symbol symbol = Symbol.from(expression);
        Type type = types.get(symbol.getName());
        checkArgument(type != null, "no type found found for symbol '%s'", symbol);

        return type;
    }

    public Set<VariableReferenceExpression> allVariables()
    {
        return types.entrySet().stream()
                .map(entry -> new VariableReferenceExpression(entry.getKey(), entry.getValue()))
                .collect(toImmutableSet());
    }

    public Map<String, Type> allTypes()
    {
        return types;
    }
}
