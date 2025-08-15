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
package com.facebook.presto.bytecode.expression;

import com.facebook.presto.bytecode.BytecodeBlock;
import com.facebook.presto.bytecode.BytecodeNode;
import com.facebook.presto.bytecode.MethodGenerationContext;
import com.facebook.presto.bytecode.ParameterizedType;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

class InvokeBytecodeExpression
        extends BytecodeExpression
{
    public static InvokeBytecodeExpression createInvoke(
            BytecodeExpression instance,
            String methodName,
            ParameterizedType returnType,
            Iterable<ParameterizedType> parameterTypes,
            Iterable<? extends BytecodeExpression> parameters)
    {
        return new InvokeBytecodeExpression(
                requireNonNull(instance, "instance is null"),
                instance.getType(),
                requireNonNull(methodName, "methodName is null"),
                requireNonNull(returnType, "returnType is null"),
                requireNonNull(parameterTypes, "parameterTypes is null"),
                requireNonNull(parameters, "parameters is null"));
    }

    @Nullable
    private final BytecodeExpression instance;
    private final ParameterizedType methodTargetType;
    private final String methodName;
    private final ParameterizedType returnType;
    private final List<BytecodeExpression> parameters;
    private final ImmutableList<ParameterizedType> parameterTypes;

    public InvokeBytecodeExpression(
            @Nullable BytecodeExpression instance,
            ParameterizedType methodTargetType,
            String methodName,
            ParameterizedType returnType,
            Iterable<ParameterizedType> parameterTypes,
            Iterable<? extends BytecodeExpression> parameters)
    {
        super(requireNonNull(returnType, "returnType is null"));
        checkArgument(instance == null || !instance.getType().isPrimitive(), "Type %s does not have methods", getType());
        this.instance = instance;
        this.methodTargetType = requireNonNull(methodTargetType, "methodTargetType is null");
        this.methodName = requireNonNull(methodName, "methodName is null");
        this.returnType = returnType;
        this.parameterTypes = ImmutableList.copyOf(requireNonNull(parameterTypes, "parameterTypes is null"));
        this.parameters = ImmutableList.copyOf(requireNonNull(parameters, "parameters is null"));
    }

    @Override
    public BytecodeNode getBytecode(MethodGenerationContext generationContext)
    {
        BytecodeBlock block = new BytecodeBlock();
        if (instance != null) {
            block.append(instance);
        }

        for (BytecodeExpression parameter : parameters) {
            block.append(parameter);
        }

        if (instance == null) {
            return block.invokeStatic(methodTargetType, methodName, returnType, parameterTypes);
        }
        else if (instance.getType().isInterface()) {
            return block.invokeInterface(methodTargetType, methodName, returnType, parameterTypes);
        }
        else {
            return block.invokeVirtual(methodTargetType, methodName, returnType, parameterTypes);
        }
    }

    @Override
    protected String formatOneLine()
    {
        if (instance == null) {
            return methodTargetType.getSimpleName() + "." + methodName + "(" + Joiner.on(", ").join(parameters) + ")";
        }

        return instance + "." + methodName + "(" + Joiner.on(", ").join(parameters) + ")";
    }

    @Override
    public List<BytecodeNode> getChildNodes()
    {
        ImmutableList.Builder<BytecodeNode> children = ImmutableList.builder();
        if (instance != null) {
            children.add(instance);
        }
        children.addAll(parameters);
        return children.build();
    }
}
