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
package com.facebook.presto.sql.gen;

import com.facebook.presto.byteCode.ByteCodeNode;
import com.facebook.presto.byteCode.instruction.Constant;
import com.facebook.presto.sql.analyzer.Session;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import java.lang.invoke.CallSite;
import java.lang.invoke.ConstantCallSite;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class DefaultFunctionBinder
        implements FunctionBinder
{
    private final MethodHandle methodHandle;
    private final boolean nullable;

    public DefaultFunctionBinder(MethodHandle methodHandle, boolean nullable)
    {
        this.methodHandle = methodHandle;
        this.nullable = nullable;
    }

    public FunctionBinding bindFunction(long bindingId, String name, ByteCodeNode getSessionByteCode, List<TypedByteCodeNode> arguments)
    {
        return bindConstantArguments(bindingId, name, getSessionByteCode, arguments, this.methodHandle, nullable);
    }

    public static FunctionBinding bindConstantArguments(long bindingId, String name, ByteCodeNode getSessionByteCode, List<TypedByteCodeNode> arguments, MethodHandle methodHandle, boolean nullable)
    {
        Builder<TypedByteCodeNode> unboundArguments = ImmutableList.builder();

        int argIndex = 0;

        // bind session
        if (methodHandle.type().parameterCount() > 0 && methodHandle.type().parameterType(0) == Session.class) {
            unboundArguments.add(TypedByteCodeNode.typedByteCodeNode(getSessionByteCode, Session.class));
            argIndex++;
        }

        for (TypedByteCodeNode argument : arguments) {
            ByteCodeNode node = argument.getNode();
            if (node instanceof Constant) {
                Object value = ((Constant) node).getValue();
                if (argument.getType() == boolean.class) {
                    checkArgument(value instanceof Integer, "boolean should be represented as an integer");
                    value = (((Integer) value) != 0);
                }
                // bind constant argument
                methodHandle = MethodHandles.insertArguments(methodHandle, argIndex, value);
                // we bound an argument so don't increment the argIndex
            }
            else {
                unboundArguments.add(argument);
                argIndex++;
            }
        }

        CallSite callSite = new ConstantCallSite(methodHandle);
        return new FunctionBinding(bindingId, name, callSite, unboundArguments.build(), nullable);
    }
}
