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
package com.facebook.presto.sql.planner.plan;

import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.tree.Expression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.BiConsumer;

import static java.util.Objects.requireNonNull;

public class Assignments
{
    private final Map<Symbol, Expression> assignments;

    @JsonCreator
    public Assignments(@JsonProperty("assignments") Map<Symbol, Expression> assignments)
    {
        this.assignments = ImmutableMap.copyOf(requireNonNull(assignments, "assignments is null"));
    }

    public List<Symbol> getOutputs()
    {
        return ImmutableList.copyOf(assignments.keySet());
    }

    @JsonProperty("assignments")
    public Map<Symbol, Expression> getMap()
    {
        return assignments;
    }

    public Collection<Expression> getExpressions()
    {
        return assignments.values();
    }

    public Set<Symbol> getSymbols()
    {
        return assignments.keySet();
    }

    public Set<Entry<Symbol, Expression>> entrySet()
    {
        return assignments.entrySet();
    }

    public Expression get(Symbol symbol)
    {
        return assignments.get(symbol);
    }

    public int size()
    {
        return assignments.size();
    }

    public boolean isEmpty()
    {
        return size() == 0;
    }

    public void forEach(BiConsumer<Symbol, Expression> consumer)
    {
        assignments.forEach(consumer);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Assignments that = (Assignments) o;

        return assignments.equals(that.assignments);
    }

    @Override
    public int hashCode()
    {
        return assignments.hashCode();
    }
}
