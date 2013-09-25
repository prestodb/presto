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
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import io.airlift.slice.Slice;

import java.lang.invoke.CallSite;
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

    private final ConcurrentMap<Long, FunctionBinding> functionBindings = new ConcurrentHashMap<>();

    public BootstrapFunctionBinder(Metadata metadata)
    {
        this.metadata = checkNotNull(metadata, "metadata is null");
    }

    public FunctionBinding bindFunction(QualifiedName name, ByteCodeNode getSessionByteCode, List<TypedByteCodeNode> arguments)
    {
        List<Type> argumentTypes = Lists.transform(arguments, toTupleType());
        FunctionInfo function = metadata.getFunction(name, argumentTypes);
        checkArgument(function != null, "Unknown function %s%s", name, argumentTypes);

        FunctionBinding functionBinding = bindFunction(name.toString(), getSessionByteCode, arguments, function.getFunctionBinder());

        return functionBinding;
    }

    public FunctionBinding bindFunction(String name, ByteCodeNode getSessionByteCode, List<TypedByteCodeNode> arguments, FunctionBinder defaultFunctionBinder)
    {
        // perform binding
        FunctionBinding functionBinding = defaultFunctionBinder.bindFunction(NEXT_BINDING_ID.getAndIncrement(), name, getSessionByteCode, arguments);

        // record binding
        functionBindings.put(functionBinding.getBindingId(), functionBinding);

        return functionBinding;
    }

    public CallSite bootstrap(String name, MethodType type, long bindingId)
    {
        FunctionBinding functionBinding = functionBindings.get(bindingId);
        checkArgument(functionBinding != null, "Binding %s for function %s%s not found", bindingId, name, type.parameterList());

        return functionBinding.getCallSite();
    }

    public static Function<TypedByteCodeNode, Type> toTupleType()
    {
        return new Function<TypedByteCodeNode, Type>()
        {
            @Override
            public Type apply(TypedByteCodeNode node)
            {
                Class<?> type = node.getType();
                if (type == boolean.class) {
                    return Type.BOOLEAN;
                }
                if (type == long.class) {
                    return Type.BIGINT;
                }
                if (type == double.class) {
                    return Type.DOUBLE;
                }
                if (type == String.class) {
                    return Type.VARCHAR;
                }
                if (type == Slice.class) {
                    return Type.VARCHAR;
                }
                throw new UnsupportedOperationException("Unsupported function type " + type);
            }
        };
    }
}
