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
package com.facebook.presto.sql.analyzer;

import com.facebook.presto.common.type.TypeWithName;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableMap;

import java.util.Collection;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

public class SemanticTypeProvider
{
    private final Map<String, TypeWithName> types;

    public static SemanticTypeProvider copyOf(Map<String, TypeWithName> types)
    {
        return new SemanticTypeProvider(ImmutableMap.copyOf(types));
    }

    public static SemanticTypeProvider copyOf(TypeProvider types)
    {
        return new SemanticTypeProvider(ImmutableMap.copyOf(types.allTypes().entrySet().stream().collect(toImmutableMap(Map.Entry::getKey, entry -> new TypeWithName(entry.getValue())))));
    }

    public static SemanticTypeProvider viewOf(Map<String, TypeWithName> types)
    {
        return new SemanticTypeProvider(types);
    }

    public static SemanticTypeProvider fromVariables(Collection<VariableReferenceExpression> variables)
    {
        return new SemanticTypeProvider(variables.stream().collect(toImmutableMap(VariableReferenceExpression::getName, variable -> new TypeWithName(variable.getType()))));
    }

    public static SemanticTypeProvider empty()
    {
        return new SemanticTypeProvider(ImmutableMap.of());
    }

    private SemanticTypeProvider(Map<String, TypeWithName> types)
    {
        this.types = types;
    }

    public TypeWithName get(Expression expression)
    {
        requireNonNull(expression, "expression is null");
        Symbol symbol = Symbol.from(expression);
        TypeWithName type = types.get(symbol.getName());
        checkArgument(type != null, "no type found found for symbol '%s'", symbol);

        return type;
    }

    public Map<String, TypeWithName> allTypes()
    {
        return types;
    }
}
