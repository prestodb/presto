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
package com.facebook.presto.spi;

import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableMap;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class VariableAllocator
{
    protected static final Pattern DISALLOWED_CHAR_PATTERN = Pattern.compile("[^a-zA-Z0-9_\\-$]+");

    protected final Map<String, Type> variables;
    protected int nextId;

    public VariableAllocator()
    {
        this.variables = new HashMap<>();
    }

    public VariableAllocator(Collection<VariableReferenceExpression> initial)
    {
        this.variables = requireNonNull(initial, "initial is null").stream()
                .collect(Collectors.toMap(VariableReferenceExpression::getName, VariableReferenceExpression::getType));
    }

    /*
     * Parses the string argument as a signed decimal integer.
     * This method returns null instead of throwing an exception if parsing fails
     */
    protected static Integer tryParse(String str)
    {
        Integer result = null;
        try {
            result = Integer.parseInt(str);
        }
        catch (NumberFormatException e) {
            // do nothing, result remains null
        }
        return result;
    }

    protected static void checkArgument(boolean expression, Object errorMessage)
    {
        if (!expression) {
            throw new IllegalArgumentException(String.valueOf(errorMessage));
        }
    }

    public VariableReferenceExpression newVariable(VariableReferenceExpression variableHint)
    {
        checkArgument(variables.containsKey(variableHint.getName()), "variableHint name not in variables map");
        return newVariable(variableHint.getSourceLocation(), variableHint.getName(), variableHint.getType());
    }

    public VariableReferenceExpression newVariable(String nameHint, Type type)
    {
        return newVariable(Optional.empty(), nameHint, type);
    }

    public VariableReferenceExpression newVariable(String nameHint, Type type, String suffix)
    {
        return newVariable(Optional.empty(), nameHint, type);
    }

    public VariableReferenceExpression newVariable(Optional<SourceLocation> sourceLocation, String nameHint, Type type)
    {
        return newVariable(sourceLocation, nameHint, type, null);
    }

    public VariableReferenceExpression newVariable(Optional<SourceLocation> sourceLocation, String nameHint, Type type, String suffix)
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
            if (tryParse(tail) != null || index == nameHint.length() - 1) {
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
        while (variables.putIfAbsent(attempt, type) != null) {
            attempt = unique + "_" + nextId();
        }

        return new VariableReferenceExpression(sourceLocation, attempt, type);
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
        return newVariable(expression.getSourceLocation(), nameHint, expression.getType(), suffix);
    }

    public VariableReferenceExpression getVariableReferenceExpression(Optional<SourceLocation> sourceLocation, String name)
    {
        checkArgument(variables.containsKey(name), "variable map does not contain name " + name);
        return new VariableReferenceExpression(sourceLocation, name, variables.get(name));
    }

    public VariableReferenceExpression newHashVariable()
    {
        return newVariable("$hashValue", BigintType.BIGINT);
    }

    protected int nextId()
    {
        return nextId++;
    }

    public Map<String, Type> getVariables()
    {
        return unmodifiableMap(variables);
    }
}
