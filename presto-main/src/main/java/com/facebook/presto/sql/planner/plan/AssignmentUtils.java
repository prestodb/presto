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
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.SymbolReference;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.facebook.presto.sql.relational.OriginalExpressionUtils.asSymbolReference;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;

public class AssignmentUtils
{
    private AssignmentUtils() {}

    @Deprecated
    public static Map.Entry<VariableReferenceExpression, Expression> identityAsSymbolReference(VariableReferenceExpression variable)
    {
        return singletonMap(variable, asSymbolReference(variable))
                .entrySet().iterator().next();
    }

    @Deprecated
    public static Map<VariableReferenceExpression, Expression> identitiesAsSymbolReferences(Collection<VariableReferenceExpression> variables)
    {
        Map<VariableReferenceExpression, Expression> map = new LinkedHashMap<>();
        for (VariableReferenceExpression variable : variables) {
            map.put(variable, asSymbolReference(variable));
        }
        return map;
    }

    @Deprecated
    public static Assignments identityAssignmentsAsSymbolReferences(Collection<VariableReferenceExpression> variables)
    {
        return Assignments.builder().putAll(identitiesAsSymbolReferences(variables)).build();
    }

    public static boolean isIdentity(Assignments assignments, VariableReferenceExpression output)
    {
        //TODO this will be checking against VariableExpression once getOutput returns VariableReferenceExpression
        Expression expression = assignments.get(output);
        return expression instanceof SymbolReference && ((SymbolReference) expression).getName().equals(output.getName());
    }

    @Deprecated
    public static Assignments identityAssignmentsAsSymbolReferences(VariableReferenceExpression... variables)
    {
        return identityAssignmentsAsSymbolReferences(asList(variables));
    }
}
