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

import com.facebook.presto.byteCode.ByteCodeNode;
import com.facebook.presto.byteCode.instruction.InvokeInstruction;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodType;
import java.lang.reflect.Method;
import java.util.List;

import static com.facebook.presto.byteCode.ParameterizedType.type;
import static com.google.common.base.Preconditions.checkNotNull;

class InvokeDynamicByteCodeExpression
        extends ByteCodeExpression
{
    private final String methodName;
    private final MethodType methodType;
    private final Method bootstrapMethod;
    private final List<Object> bootstrapArgs;

    public InvokeDynamicByteCodeExpression(
            String methodName,
            MethodType methodType,
            Method bootstrapMethod,
            List<Object> bootstrapArgs)
    {
        super(type(checkNotNull(methodType, "methodType is null").returnType()));
        this.methodName = checkNotNull(methodName, "methodName is null");
        this.methodType = checkNotNull(methodType, "methodType is null");
        this.bootstrapMethod = checkNotNull(bootstrapMethod, "bootstrapMethod is null");
        this.bootstrapArgs = checkNotNull(bootstrapArgs, "bootstrapArgs is null");
    }

    @Override
    public ByteCodeNode getByteCode()
    {
        return InvokeInstruction.invokeDynamic(methodName, methodType, bootstrapMethod, bootstrapArgs);
    }

    @Override
    public List<ByteCodeNode> getChildNodes()
    {
        return ImmutableList.of();
    }
}
