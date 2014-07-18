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
import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.spi.ConnectorSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.lang.invoke.CallSite;
import java.lang.invoke.ConstantCallSite;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BootstrapFunctionBinder
{
    private int nextBindingId;

    private final Map<Long, CallSite> bindings = new HashMap<>();

    public FunctionBinding bindFunction(FunctionInfo function, ByteCodeNode getSessionByteCode, List<? extends ByteCodeNode> arguments)
    {
        MethodHandle method = function.getMethodHandle();

        // bind session if necessary
        ImmutableList.Builder<ByteCodeNode> actualArguments = ImmutableList.builder();

        int index = 0;
        for (Class<?> type : method.type().parameterArray()) {
            if (type == ConnectorSession.class) {
                actualArguments.add(getSessionByteCode);
            }
            else {
                actualArguments.add(arguments.get(index));
                index++;
            }
        }

        FunctionBinding binding = new FunctionBinding(
                nextBindingId++,
                function.getSignature().getName(),
                new ConstantCallSite(method),
                actualArguments.build(),
                function.isNullable());

        bindings.put(binding.getBindingId(), binding.getCallSite());
        return binding;
    }

    public FunctionBinding bindConstant(Object constant, Class<?> type)
    {
        long bindingId = nextBindingId++;
        ConstantCallSite callSite = new ConstantCallSite(MethodHandles.constant(type, constant));
        bindings.put(bindingId, callSite);
        return new FunctionBinding(bindingId, "constant_" + bindingId, callSite, ImmutableList.<ByteCodeNode>of(), true);
    }

    public Map<Long, CallSite> getCallSites()
    {
        return ImmutableMap.copyOf(bindings);
    }
}
