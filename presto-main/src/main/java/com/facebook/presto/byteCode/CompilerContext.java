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
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import org.objectweb.asm.Type;

import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static com.facebook.presto.byteCode.ParameterizedType.type;

public class CompilerContext
{
    private final Method defaultBootstrapMethod;
    private final Object[] defaultBootstrapArguments;

    private final VariableFactory variableFactory;
    private final VariableFactory parameterFactory;

    private final Map<String, Variable> variables = new TreeMap<>();
    private final List<Variable> allVariables = new ArrayList<>();

    private int nextSlot;

    private final Deque<IterationScope> iterationScopes = new ArrayDeque<>();

    private final LabelNode variableStartLabel = new LabelNode("VariableStart");
    private final LabelNode variableEndLabel = new LabelNode("VariableEnd");

    private Integer currentLine;

    public CompilerContext(Method defaultBootstrapMethod)
    {
        this(defaultBootstrapMethod, new Object[0], new LocalVariableFactory(), new LocalVariableFactory());
    }

    public CompilerContext(Method defaultBootstrapMethod, Object[] defaultBootstrapArguments, VariableFactory variableFactory, VariableFactory parameterFactory)
    {
        this.defaultBootstrapMethod = defaultBootstrapMethod;
        this.defaultBootstrapArguments = defaultBootstrapArguments;
        this.parameterFactory = parameterFactory;
        Preconditions.checkNotNull(variableFactory, "localVariableType is null");
        this.variableFactory = variableFactory;
    }

    public Variable createTempVariable(Class<?> type)
    {
        Variable variable;

        // reserve a slot for this variable
        LocalVariableDefinition variableDefinition = new LocalVariableDefinition("temp_" + nextSlot, nextSlot, type(type));
        nextSlot += Type.getType(type(type).getType()).getSize();

        variable = variableFactory.createVariable(this, variableDefinition.getName(), variableDefinition);

        return variable;
    }

    public Variable getVariable(String name)
    {
        Variable variable = variables.get(name);
        Preconditions.checkArgument(variable != null, "Variable %s not defined", name);
        return variable;
    }

    /**
     * BE VERY CAREFUL WITH THIS METHOD
     */
    public void setVariable(String name, Variable variable)
    {
        Preconditions.checkNotNull(name, "name is null");
        Preconditions.checkNotNull(variable, "variable is null");
        variables.put(name, variable);
    }

    public void declareThisVariable(ParameterizedType type)
    {
        if (variables.containsKey("this")) {
            return;
        }

        Preconditions.checkState(nextSlot == 0, "The 'this' variable must be declared before all other parameters and local variables");
        LocalVariableDefinition variableDefinition = new LocalVariableDefinition("this", 0, type);
        nextSlot = 1;

        Variable variable = variableFactory.createVariable(this, "this", variableDefinition);
        variables.put("this", variable);
    }

    public LocalVariableDefinition declareParameter(ParameterizedType type, String parameterName)
    {
        Preconditions.checkArgument(!variables.containsKey(parameterName), "There is already a parameter named %s", parameterName);

        LocalVariableDefinition variableDefinition = new LocalVariableDefinition(parameterName, nextSlot, type);
        nextSlot += Type.getType(type.getType()).getSize();

        Variable variable = parameterFactory.createVariable(this, parameterName, variableDefinition);

        allVariables.add(variable);
        variables.put(parameterName, variable);

        return variableDefinition;
    }

    public LocalVariableDefinition declareVariable(Class<?> type, String variableName)
    {
        return declareVariable(type(type), variableName);
    }

    public LocalVariableDefinition declareVariable(ParameterizedType type, String variableName)
    {
        Preconditions.checkArgument(!variables.containsKey(variableName), "There is already a parameter named %s", variableName);

        LocalVariableDefinition variableDefinition = new LocalVariableDefinition(variableName, nextSlot, type);
        nextSlot += Type.getType(type.getType()).getSize();

        Variable variable = parameterFactory.createVariable(this, variableName, variableDefinition);

        allVariables.add(variable);
        variables.put(variableName, variable);

        return variableDefinition;
    }

    public void pushIterationScope(LabelNode begin, LabelNode end)
    {
        iterationScopes.push(new IterationScope(begin, end));
    }

    public void popIterationScope()
    {
        iterationScopes.pop();
    }

    // level 1 is the top of the stack
    public IterationScope peekIterationScope(int level)
    {
        return Iterators.get(iterationScopes.iterator(), level - 1, null);
    }

    public Method getDefaultBootstrapMethod()
    {
        return defaultBootstrapMethod;
    }

    public Object[] getDefaultBootstrapArguments()
    {
        return defaultBootstrapArguments;
    }

    public boolean hasVisitedLine(Integer line)
    {
        return this.currentLine != null && line.intValue() == this.currentLine.intValue();
    }

    public void cleanLineNumber()
    {
        this.currentLine = null;
    }

    public void visitLine(int currentLine)
    {
        this.currentLine = currentLine;
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
        for (Variable variable : allVariables) {
            LocalVariableDefinition localVariableDefinition = variable.getLocalVariableDefinition();
            methodDefinition.addLocalVariable(localVariableDefinition, variableStartLabel, variableEndLabel);
        }
    }
}
