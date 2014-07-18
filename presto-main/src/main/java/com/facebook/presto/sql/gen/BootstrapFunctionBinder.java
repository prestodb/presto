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
import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.OperatorType;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.CallSite;
import java.lang.invoke.ConstantCallSite;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class BootstrapFunctionBinder
{
    private static final AtomicLong NEXT_BINDING_ID = new AtomicLong();

    private final Metadata metadata;

    private final ConcurrentMap<Long, CallSite> bindings = new ConcurrentHashMap<>();

    public BootstrapFunctionBinder(Metadata metadata)
    {
        this.metadata = checkNotNull(metadata, "metadata is null");
    }

    public FunctionBinding bindFunction(FunctionInfo function, ByteCodeNode getSessionByteCode, List<ByteCodeNode> arguments)
    {
        FunctionBinding binding = bindConstantArguments(NEXT_BINDING_ID.getAndIncrement(), function.getSignature().getName(), getSessionByteCode, arguments, function.getMethodHandle(), function.isNullable());
        bindings.put(binding.getBindingId(), binding.getCallSite());
        return binding;
    }

    public FunctionBinding bindOperator(OperatorType operatorType, ByteCodeNode getSessionByteCode, List<ByteCodeNode> arguments, List<Type> argumentTypes)
    {
        FunctionInfo operatorInfo = metadata.resolveOperator(operatorType, argumentTypes);
        return bindFunction(operatorInfo, getSessionByteCode, arguments);
    }

    public FunctionBinding bindConstant(Object constant, Class<?> type)
    {
        long bindingId = NEXT_BINDING_ID.getAndIncrement();
        ConstantCallSite callsite = new ConstantCallSite(MethodHandles.constant(type, constant));
        bindings.put(bindingId, callsite);
        return new FunctionBinding(bindingId, "constant_" + bindingId, callsite, ImmutableList.<ByteCodeNode>of(), true);
    }

    public CallSite bootstrap(String name, MethodType type, long bindingId)
    {
        CallSite callSite = bindings.get(bindingId);
        checkArgument(callSite != null, "Binding %s for function %s%s not found", bindingId, name, type.parameterList());

        return callSite;
    }

    private static FunctionBinding bindConstantArguments(long bindingId, String name, ByteCodeNode getSessionByteCode, List<ByteCodeNode> arguments, MethodHandle methodHandle, boolean nullable)
    {
        ImmutableList.Builder<ByteCodeNode> unboundArguments = ImmutableList.builder();

        int argIndex = 0;

        // bind session
        if (methodHandle.type().parameterCount() > 0 && methodHandle.type().parameterType(0) == ConnectorSession.class) {
            unboundArguments.add(getSessionByteCode);
            argIndex++;
        }

        for (ByteCodeNode argument : arguments) {
            ByteCodeNode node = argument;
            if (node instanceof Constant) {
                Object value = ((Constant) node).getValue();
                if (methodHandle.type().parameterType(argIndex) == boolean.class) {
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
