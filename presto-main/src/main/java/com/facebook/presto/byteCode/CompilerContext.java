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
package com.facebook.presto.byteCode;

import com.facebook.presto.byteCode.instruction.LabelNode;
import com.google.common.collect.ImmutableList;
import org.objectweb.asm.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static com.facebook.presto.byteCode.ParameterizedType.type;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class CompilerContext
{
    private final Map<String, Variable> variables = new TreeMap<>();
    private final List<Variable> allVariables = new ArrayList<>();

    private final LabelNode variableStartLabel = new LabelNode("VariableStart");
    private final LabelNode variableEndLabel = new LabelNode("VariableEnd");

    private int nextTempVariableId;

    // This can only be constructed by a method definition
    CompilerContext()
    {
    }

    void declareThisVariable(ParameterizedType type)
    {
        if (variables.containsKey("this")) {
            return;
        }

        checkState(variables.isEmpty(), "The 'this' variable must be declared first");
        Variable variable = new Variable("this", type);

        variables.put("this", variable);
        allVariables.add(variable);
    }

    public List<Variable> getVariables()
    {
        return ImmutableList.copyOf(allVariables);
    }

    public Variable createTempVariable(Class<?> type)
    {
        // reserve a slot for this variable
        Variable variable = new Variable("temp_" + nextTempVariableId, type(type));
        nextTempVariableId += Type.getType(type(type).getType()).getSize();

        allVariables.add(variable);

        return variable;
    }

    public Variable getThis()
    {
        return getVariable("this");
    }

    public Variable getVariable(String name)
    {
        Variable variable = variables.get(name);
        checkArgument(variable != null, "Variable %s not defined", name);
        return variable;
    }

    public Variable declareVariable(Class<?> type, String variableName)
    {
        return declareVariable(type(type), variableName);
    }

    public Variable declareVariable(ParameterizedType type, String variableName)
    {
        requireNonNull(type, "type is null");
        requireNonNull(variableName, "variableName is null");
        checkArgument(!variables.containsKey(variableName), "There is already a variable named %s", variableName);
        checkArgument(!variableName.equals("this"), "The 'this' variable can not be declared");

        Variable variable = new Variable(variableName, type);

        variables.put(variableName, variable);
        allVariables.add(variable);

        return variable;
    }

    public LabelNode getVariableStartLabel()
    {
        return variableStartLabel;
    }

    public LabelNode getVariableEndLabel()
    {
        return variableEndLabel;
    }

    public void addLocalVariables(MethodDefinition methodDefinition)
    {
        for (Variable variable : variables.values()) {
            methodDefinition.addLocalVariable(variable, variableStartLabel, variableEndLabel);
        }
    }
}
