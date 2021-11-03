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

import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.analyzer.Field;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GroupingOperation;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.primitives.Ints;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class PlanVariableAllocator
        implements VariableAllocator
{
    private static final Pattern DISALLOWED_CHAR_PATTERN = Pattern.compile("[^a-zA-Z0-9_\\-$]+");

    private final Map<String, Type> variables;
    private int nextId;

    public PlanVariableAllocator()
    {
        variables = new HashMap<>();
    }

    public PlanVariableAllocator(Collection<VariableReferenceExpression> initial)
    {
        this.variables = requireNonNull(initial, "initial is null").stream()
                .collect(Collectors.toMap(VariableReferenceExpression::getName, VariableReferenceExpression::getType));
    }

    public VariableReferenceExpression newVariable(VariableReferenceExpression variableHint)
    {
        checkArgument(variables.containsKey(variableHint.getName()), "variableHint name not in variables map");
        return newVariable(variableHint.getName(), variableHint.getType());
    }

    public VariableReferenceExpression newVariable(QualifiedName nameHint, Type type)
    {
        return newVariable(nameHint.getSuffix(), type, null);
    }

    public VariableReferenceExpression newVariable(String nameHint, Type type)
    {
        return newVariable(nameHint, type, null);
    }

    public VariableReferenceExpression newHashVariable()
    {
        return newVariable("$hashValue", BigintType.BIGINT);
    }

    @Override
    public VariableReferenceExpression newVariable(String nameHint, Type type, String suffix)
    {
        requireNonNull(nameHint, "name is null");
        requireNonNull(type, "type is null");

        // TODO: workaround for the fact that QualifiedName lowercases parts
        nameHint = nameHint.toLowerCase(ENGLISH);

        // don't strip the tail if the only _ is the first character
        int index = nameHint.lastIndexOf("_");
        if (index > 0) {
            String tail = nameHint.substring(index + 1);

            // only strip if tail is numeric or _ is the last character
            if (Ints.tryParse(tail) != null || index == nameHint.length() - 1) {
                nameHint = nameHint.substring(0, index);
            }
        }

        String unique = nameHint;

        if (suffix != null) {
            unique = unique + "$" + suffix;
        }
        // remove special characters for other special serde
        unique = DISALLOWED_CHAR_PATTERN.matcher(unique).replaceAll("_");

        String attempt = unique;
        while (variables.containsKey(attempt)) {
            attempt = unique + "_" + nextId();
        }

        variables.put(attempt, type);
        return new VariableReferenceExpression(attempt, type);
    }

    public VariableReferenceExpression newVariable(Expression expression, Type type)
    {
        return newVariable(expression, type, null);
    }

    public VariableReferenceExpression newVariable(Expression expression, Type type, String suffix)
    {
        String nameHint = "expr";
        if (expression instanceof Identifier) {
            nameHint = ((Identifier) expression).getValue();
        }
        else if (expression instanceof FunctionCall) {
            nameHint = ((FunctionCall) expression).getName().getSuffix();
        }
        else if (expression instanceof SymbolReference) {
            nameHint = ((SymbolReference) expression).getName();
        }
        else if (expression instanceof GroupingOperation) {
            nameHint = "grouping";
        }
        return newVariable(nameHint, type, suffix);
    }

    public VariableReferenceExpression newVariable(Field field)
    {
        return newVariable(field.getName().orElse("field"), field.getType(), null);
    }

    public TypeProvider getTypes()
    {
        return TypeProvider.viewOf(variables);
    }

    private int nextId()
    {
        return nextId++;
    }

    public VariableReferenceExpression toVariableReference(Expression expression)
    {
        checkArgument(expression instanceof SymbolReference, "Unexpected expression: %s", expression);
        String name = ((SymbolReference) expression).getName();
        checkArgument(variables.containsKey(name), "variable map does not contain name %s", name);
        return new VariableReferenceExpression(name, variables.get(name));
    }

    public VariableReferenceExpression newVariable(RowExpression expression)
    {
        return newVariable(expression, null);
    }

    public VariableReferenceExpression newVariable(RowExpression expression, String suffix)
    {
        String nameHint = "expr";
        if (expression instanceof VariableReferenceExpression) {
            nameHint = ((VariableReferenceExpression) expression).getName();
        }
        else if (expression instanceof CallExpression) {
            nameHint = ((CallExpression) expression).getDisplayName();
        }
        return newVariable(nameHint, expression.getType(), suffix);
    }
}
