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

import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.SymbolReference;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collector;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

public class Assignments
{
    public static Builder builder()
    {
        return new Builder();
    }

    public static Assignments identity(VariableReferenceExpression... variables)
    {
        return identity(asList(variables));
    }

    public static Assignments identity(Iterable<VariableReferenceExpression> variables)
    {
        return builder().putIdentities(variables).build();
    }

    public static Assignments copyOf(Map<VariableReferenceExpression, Expression> assignments)
    {
        return builder()
                .putAll(assignments)
                .build();
    }

    public static Assignments of()
    {
        return builder().build();
    }

    public static Assignments of(VariableReferenceExpression variable, Expression expression)
    {
        return builder().put(variable, expression).build();
    }

    public static Assignments of(VariableReferenceExpression variable1, Expression expression1, VariableReferenceExpression variable2, Expression expression2)
    {
        return builder().put(variable1, expression1).put(variable2, expression2).build();
    }

    private final Map<VariableReferenceExpression, Expression> assignments;

    @JsonCreator
    public Assignments(@JsonProperty("assignments") Map<VariableReferenceExpression, Expression> assignments)
    {
        this.assignments = ImmutableMap.copyOf(requireNonNull(assignments, "assignments is null"));
    }

    public List<Symbol> getOutputSymbols()
    {
        return assignments.keySet().stream().map(VariableReferenceExpression::getName).map(Symbol::new).collect(toImmutableList());
    }

    public List<VariableReferenceExpression> getOutputs()
    {
        return ImmutableList.copyOf(assignments.keySet());
    }

    @JsonProperty("assignments")
    public Map<VariableReferenceExpression, Expression> getMap()
    {
        return assignments;
    }

    public <C> Assignments rewrite(ExpressionRewriter<C> rewriter)
    {
        return rewrite(expression -> ExpressionTreeRewriter.rewriteWith(rewriter, expression));
    }

    public Assignments rewrite(Function<Expression, Expression> rewrite)
    {
        return assignments.entrySet().stream()
                .map(entry -> Maps.immutableEntry(entry.getKey(), rewrite.apply(entry.getValue())))
                .collect(toAssignments());
    }

    public Assignments filter(Collection<VariableReferenceExpression> variables)
    {
        return filter(variables::contains);
    }

    public Assignments filter(Predicate<VariableReferenceExpression> predicate)
    {
        return assignments.entrySet().stream()
                .filter(entry -> predicate.apply(entry.getKey()))
                .collect(toAssignments());
    }

    public boolean isIdentity(VariableReferenceExpression output)
    {
        Expression expression = assignments.get(output);

        return expression instanceof SymbolReference && ((SymbolReference) expression).getName().equals(output.getName());
    }

    private Collector<Entry<VariableReferenceExpression, Expression>, Builder, Assignments> toAssignments()
    {
        return Collector.of(
                Assignments::builder,
                (builder, entry) -> builder.put(entry.getKey(), entry.getValue()),
                (left, right) -> {
                    left.putAll(right.build());
                    return left;
                },
                Assignments.Builder::build);
    }

    public Collection<Expression> getExpressions()
    {
        return assignments.values();
    }

    public Set<Symbol> getSymbols()
    {
        return assignments.keySet().stream().map(VariableReferenceExpression::getName).map(Symbol::new).collect(toImmutableSet());
    }

    public Set<VariableReferenceExpression> getVariables()
    {
        return assignments.keySet();
    }

    public Set<Entry<VariableReferenceExpression, Expression>> entrySet()
    {
        return assignments.entrySet();
    }

    public Expression get(VariableReferenceExpression variable)
    {
        return assignments.get(variable);
    }

    public Expression get(Symbol symbol)
    {
        List<Expression> candidate = assignments.entrySet().stream()
                .filter(entry -> entry.getKey().getName().equals(symbol.getName()))
                .map(Entry::getValue)
                .collect(toImmutableList());
        if (candidate.isEmpty()) {
            return null;
        }
        return candidate.get(0);
    }

    public int size()
    {
        return assignments.size();
    }

    public boolean isEmpty()
    {
        return size() == 0;
    }

    public void forEach(BiConsumer<VariableReferenceExpression, Expression> consumer)
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
        private final Map<VariableReferenceExpression, Expression> assignments = new LinkedHashMap<>();

        public Builder putAll(Assignments assignments)
        {
            return putAll(assignments.getMap());
        }

        public Builder putAll(Map<VariableReferenceExpression, Expression> assignments)
        {
            for (Entry<VariableReferenceExpression, Expression> assignment : assignments.entrySet()) {
                put(assignment.getKey(), assignment.getValue());
            }
            return this;
        }

        public Builder put(VariableReferenceExpression variable, Expression expression)
        {
            if (assignments.containsKey(variable)) {
                Expression assignment = assignments.get(variable);
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

        public Builder put(Entry<VariableReferenceExpression, Expression> assignment)
        {
            put(assignment.getKey(), assignment.getValue());
            return this;
        }

        public Builder putIdentities(Iterable<VariableReferenceExpression> variables)
        {
            for (VariableReferenceExpression variable : variables) {
                putIdentity(variable);
            }
            return this;
        }

        public Builder putIdentity(VariableReferenceExpression variable)
        {
            put(variable, new SymbolReference(variable.getName()));
            return this;
        }

        public Assignments build()
        {
            return new Assignments(assignments);
        }
    }
}
