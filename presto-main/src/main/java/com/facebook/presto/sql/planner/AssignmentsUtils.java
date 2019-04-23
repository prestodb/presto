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

import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.Maps;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collector;

import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToExpression;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToRowExpression;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Arrays.asList;

/**
 * Everything in this should be moved back to Assignments
 */
public class AssignmentsUtils
{
    private AssignmentsUtils() {}

    // Originally, the following functions are also static
    public static Builder builder()
    {
        return new Builder();
    }

    public static Assignments identity(Symbol... symbols)
    {
        return identity(asList(symbols));
    }

    public static Assignments identity(Iterable<Symbol> symbols)
    {
        return builder().putIdentities(symbols).build();
    }

    public static Assignments copyOf(Map<Symbol, RowExpression> assignments)
    {
        return builder()
                .putAll(assignments)
                .build();
    }

    public static Assignments of()
    {
        return builder().build();
    }

    public static Assignments of(Symbol symbol, RowExpression expression)
    {
        return builder().put(symbol, expression).build();
    }

    public static Assignments of(Symbol symbol, Expression expression)
    {
        return builder().put(symbol, castToRowExpression(expression)).build();
    }

    public static Assignments of(Symbol symbol1, RowExpression expression1, Symbol symbol2, RowExpression expression2)
    {
        return builder().put(symbol1, expression1).put(symbol2, expression2).build();
    }

    public static Assignments of(Symbol symbol1, Expression expression1, Symbol symbol2, Expression expression2)
    {
        return builder().put(symbol1, castToRowExpression(expression1)).put(symbol2, castToRowExpression(expression2)).build();
    }

    // Originally, the following functions are not static move assignments as member variables
    public static <C> Assignments rewrite(Assignments assignments, ExpressionRewriter<C> rewriter)
    {
        return rewrite(assignments, expression -> ExpressionTreeRewriter.rewriteWith(rewriter, expression));
    }

    public static Assignments rewrite(Assignments assignments, Function<Expression, Expression> rewrite)
    {
        return assignments.entrySet().stream()
                .map(entry -> Maps.immutableEntry(entry.getKey(), castToRowExpression(rewrite.apply(castToExpression(entry.getValue())))))
                .collect(toAssignments());
    }

    public static Assignments filter(Assignments assignments, Collection<Symbol> symbols)
    {
        return filter(assignments, symbols::contains);
    }

    public static Assignments filter(Assignments assignments, Predicate<Symbol> predicate)
    {
        return assignments.entrySet().stream()
                .filter(entry -> predicate.test(entry.getKey()))
                .collect(toAssignments());
    }

    public static boolean isIdentity(Assignments assignments, Symbol output)
    {
        Expression expression = castToExpression(assignments.get(output));

        return expression instanceof SymbolReference && ((SymbolReference) expression).getName().equals(output.getName());
    }

    private static Collector<Map.Entry<Symbol, RowExpression>, Builder, Assignments> toAssignments()
    {
        return Collector.of(
                AssignmentsUtils::builder,
                (builder, entry) -> builder.put(entry.getKey(), entry.getValue()),
                (left, right) -> {
                    left.putAll(right.build());
                    return left;
                },
                Builder::build);
    }

    // Originally, the following class is also static
    public static class Builder
    {
        private final Map<Symbol, RowExpression> assignments = new LinkedHashMap<>();

        public Builder putAll(Assignments assignments)
        {
            return putAll(assignments.getMap());
        }

        public Builder putAll(Map<Symbol, RowExpression> assignments)
        {
            for (Map.Entry<Symbol, RowExpression> assignment : assignments.entrySet()) {
                put(assignment.getKey(), assignment.getValue());
            }
            return this;
        }

        public Builder put(Symbol symbol, RowExpression expression)
        {
            if (assignments.containsKey(symbol)) {
                RowExpression assignment = assignments.get(symbol);
                checkState(
                        assignment.equals(expression),
                        "Symbol %s already has assignment %s, while adding %s",
                        symbol,
                        assignment,
                        expression);
            }
            assignments.put(symbol, expression);
            return this;
        }

        public Builder put(Symbol symbol, Expression expression)
        {
            return put(symbol, castToRowExpression(expression));
        }

        public Builder put(Map.Entry<Symbol, RowExpression> assignment)
        {
            put(assignment.getKey(), assignment.getValue());
            return this;
        }

        public Builder putIdentities(Iterable<Symbol> symbols)
        {
            for (Symbol symbol : symbols) {
                putIdentity(symbol);
            }
            return this;
        }

        public Builder putIdentity(Symbol symbol)
        {
            put(symbol, castToRowExpression(symbol.toSymbolReference()));
            return this;
        }

        public Assignments build()
        {
            return new Assignments(assignments);
        }
    }
}
