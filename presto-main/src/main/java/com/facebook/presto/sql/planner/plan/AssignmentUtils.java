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
import com.google.common.collect.Maps;

import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collector;

import static java.util.Arrays.asList;

public class AssignmentUtils
{
    private AssignmentUtils() {}

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
        RowExpression value = assignments.get(output);
        return value instanceof VariableReferenceExpression && ((VariableReferenceExpression) value).getName().equals(output.getName());
    }

    public static Assignments rewrite(Assignments assignments, Function<RowExpression, RowExpression> rewrite)
    {
        return assignments.entrySet().stream()
                .map(entry -> Maps.immutableEntry(entry.getKey(), rewrite.apply(entry.getValue())))
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
