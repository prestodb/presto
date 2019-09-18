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

import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.Maps;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collector;

import static com.facebook.presto.sql.relational.OriginalExpressionUtils.asSymbolReference;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToExpression;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToRowExpression;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.isExpression;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;

public class AssignmentUtils
{
    private AssignmentUtils() {}

    @Deprecated
    public static Map.Entry<VariableReferenceExpression, RowExpression> identityAsSymbolReference(VariableReferenceExpression variable)
    {
        return singletonMap(variable, castToRowExpression(asSymbolReference(variable)))
                .entrySet().iterator().next();
    }

    @Deprecated
    public static Map<VariableReferenceExpression, RowExpression> identitiesAsSymbolReferences(Collection<VariableReferenceExpression> variables)
    {
        Map<VariableReferenceExpression, RowExpression> map = new LinkedHashMap<>();
        for (VariableReferenceExpression variable : variables) {
            map.put(variable, castToRowExpression(asSymbolReference(variable)));
        }
        return map;
    }

    @Deprecated
    public static Assignments identityAssignmentsAsSymbolReferences(Collection<VariableReferenceExpression> variables)
    {
        return Assignments.builder().putAll(identitiesAsSymbolReferences(variables)).build();
    }

    public static Assignments identityAssignments(Collection<VariableReferenceExpression> variables)
    {
        Assignments.Builder builder = Assignments.builder();
        variables.forEach(variable -> builder.put(variable, variable));
        return builder.build();
    }

    public static Assignments identityAssignments(VariableReferenceExpression... variables)
    {
        return identityAssignments(asList(variables));
    }

    public static boolean isIdentity(Assignments assignments, VariableReferenceExpression output)
    {
        //TODO this will be checking against VariableExpression once getOutput returns VariableReferenceExpression
        RowExpression value = assignments.get(output);
        if (isExpression(value)) {
            Expression expression = castToExpression(value);
            return expression instanceof SymbolReference && ((SymbolReference) expression).getName().equals(output.getName());
        }
        return value instanceof VariableReferenceExpression && ((VariableReferenceExpression) value).getName().equals(output.getName());
    }

    @Deprecated
    public static Assignments identityAssignmentsAsSymbolReferences(VariableReferenceExpression... variables)
    {
        return identityAssignmentsAsSymbolReferences(asList(variables));
    }

    public static Assignments rewrite(Assignments assignments, Function<Expression, Expression> rewrite)
    {
        return assignments.entrySet().stream()
                .map(entry -> Maps.immutableEntry(entry.getKey(), castToRowExpression(rewrite.apply(castToExpression(entry.getValue())))))
                .collect(toAssignments());
    }

    private static Collector<Map.Entry<VariableReferenceExpression, RowExpression>, Assignments.Builder, Assignments> toAssignments()
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
}
