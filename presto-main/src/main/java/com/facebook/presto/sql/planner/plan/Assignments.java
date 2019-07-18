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

import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.stream.Collector;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class Assignments
{
    public static Builder builder()
    {
        return new Builder();
    }

    public static Builder builder(Map<VariableReferenceExpression, RowExpression> assignments)
    {
        return new Builder().putAll(assignments);
    }

    public static Assignments copyOf(Map<VariableReferenceExpression, RowExpression> assignments)
    {
        return builder()
                .putAll(assignments)
                .build();
    }

    public static Assignments of()
    {
        return builder().build();
    }

    public static Assignments of(VariableReferenceExpression variable, RowExpression expression)
    {
        return builder().put(variable, expression).build();
    }

    public static Assignments of(VariableReferenceExpression variable1, RowExpression expression1, VariableReferenceExpression variable2, RowExpression expression2)
    {
        return builder().put(variable1, expression1).put(variable2, expression2).build();
    }

    private final Map<VariableReferenceExpression, RowExpression> assignments;

    @JsonCreator
    public Assignments(@JsonProperty("assignments") Map<VariableReferenceExpression, RowExpression> assignments)
    {
        this.assignments = ImmutableMap.copyOf(requireNonNull(assignments, "assignments is null"));
    }

    public List<VariableReferenceExpression> getOutputs()
    {
        return ImmutableList.copyOf(assignments.keySet());
    }

    @JsonProperty("assignments")
    public Map<VariableReferenceExpression, RowExpression> getMap()
    {
        return assignments;
    }

    public Assignments filter(Collection<VariableReferenceExpression> variables)
    {
        return filter(variables::contains);
    }

    public Assignments filter(Predicate<VariableReferenceExpression> predicate)
    {
        return assignments.entrySet().stream()
                .filter(entry -> predicate.test(entry.getKey()))
                .collect(toAssignments());
    }

    private Collector<Entry<VariableReferenceExpression, RowExpression>, Builder, Assignments> toAssignments()
    {
        return Collector.of(
                Assignments::builder,
                (builder, entry) -> builder.put(entry.getKey(), entry.getValue()),
                (left, right) -> {
                    left.putAll(right.build());
                    return left;
                },
                Builder::build);
    }

    public Collection<RowExpression> getExpressions()
    {
        return assignments.values();
    }

    public Set<VariableReferenceExpression> getVariables()
    {
        return assignments.keySet();
    }

    public Set<Entry<VariableReferenceExpression, RowExpression>> entrySet()
    {
        return assignments.entrySet();
    }

    public RowExpression get(VariableReferenceExpression variable)
    {
        return assignments.get(variable);
    }

    public int size()
    {
        return assignments.size();
    }

    public boolean isEmpty()
    {
        return size() == 0;
    }

    public void forEach(BiConsumer<VariableReferenceExpression, RowExpression> consumer)
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

    public static class Builder
    {
        private final Map<VariableReferenceExpression, RowExpression> assignments = new LinkedHashMap<>();

        public Builder putAll(Assignments assignments)
        {
            return putAll(assignments.getMap());
        }

        public Builder putAll(Map<VariableReferenceExpression, RowExpression> assignments)
        {
            for (Entry<VariableReferenceExpression, RowExpression> assignment : assignments.entrySet()) {
                put(assignment.getKey(), assignment.getValue());
            }
            return this;
        }

        public Builder put(VariableReferenceExpression variable, RowExpression expression)
        {
            if (assignments.containsKey(variable)) {
                RowExpression assignment = assignments.get(variable);
                checkState(
                        assignment.equals(expression),
                        "Variable %s already has assignment %s, while adding %s",
                        variable,
                        assignment,
                        expression);
            }
            assignments.put(variable, expression);
            return this;
        }

        public Builder put(Entry<VariableReferenceExpression, RowExpression> assignment)
        {
            put(assignment.getKey(), assignment.getValue());
            return this;
        }

        public Assignments build()
        {
            return new Assignments(assignments);
        }
    }
}
