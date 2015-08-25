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
package com.facebook.presto.byteCode.expression;

import com.facebook.presto.byteCode.ByteCodeBlock;
import com.facebook.presto.byteCode.ByteCodeNode;
import com.facebook.presto.byteCode.MethodGenerationContext;
import com.facebook.presto.byteCode.ParameterizedType;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import java.lang.reflect.Method;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;

class InvokeDynamicByteCodeExpression
        extends ByteCodeExpression
{
    private final Method bootstrapMethod;
    private final List<Object> bootstrapArgs;
    private final String methodName;
    private final ParameterizedType returnType;
    private final List<ByteCodeExpression> parameters;
    private final List<ParameterizedType> parameterTypes;

    InvokeDynamicByteCodeExpression(
            Method bootstrapMethod,
            Iterable<?> bootstrapArgs,
            String methodName,
            ParameterizedType returnType,
            Iterable<? extends ByteCodeExpression> parameters,
            Iterable<ParameterizedType> parameterTypes)
    {
        super(returnType);
        this.bootstrapMethod = checkNotNull(bootstrapMethod, "bootstrapMethod is null");
        this.bootstrapArgs = ImmutableList.copyOf(checkNotNull(bootstrapArgs, "bootstrapArgs is null"));
        this.methodName = checkNotNull(methodName, "methodName is null");
        this.returnType = checkNotNull(returnType, "returnType is null");
        this.parameters = ImmutableList.copyOf(checkNotNull(parameters, "parameters is null"));
        this.parameterTypes = ImmutableList.copyOf(checkNotNull(parameterTypes, "parameterTypes is null"));
    }

    @Override
    public ByteCodeNode getByteCode(MethodGenerationContext generationContext)
    {
        ByteCodeBlock block = new ByteCodeBlock();
        for (ByteCodeExpression parameter : parameters) {
            block.append(parameter);
        }
        return block.invokeDynamic(methodName, returnType, parameterTypes, bootstrapMethod, bootstrapArgs);
    }

    @Override
    public List<ByteCodeNode> getChildNodes()
    {
        return ImmutableList.of();
    }

    @Override
    protected String formatOneLine()
    {
        StringBuilder builder = new StringBuilder();
        builder.append("[").append(bootstrapMethod.getName());
        if (!bootstrapArgs.isEmpty()) {
            builder.append("(").append(Joiner.on(", ").join(transform(bootstrapArgs, ConstantByteCodeExpression::renderConstant))).append(")");
        }
        builder.append("]=>");

        builder.append(methodName).append("(").append(Joiner.on(", ").join(parameters)).append(")");

        return builder.toString();
    }
}
