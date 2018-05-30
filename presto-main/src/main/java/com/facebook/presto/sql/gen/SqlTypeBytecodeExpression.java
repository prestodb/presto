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

import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.bytecode.BytecodeNode;
import io.airlift.bytecode.MethodGenerationContext;
import io.airlift.bytecode.expression.BytecodeExpression;
import io.airlift.bytecode.instruction.InvokeInstruction;
import io.airlift.slice.Slice;

import java.lang.reflect.Method;
import java.util.List;

import static com.facebook.presto.sql.gen.Bootstrap.BOOTSTRAP_METHOD;
import static io.airlift.bytecode.ParameterizedType.type;
import static java.util.Objects.requireNonNull;

public class SqlTypeBytecodeExpression
        extends BytecodeExpression
{
    public static SqlTypeBytecodeExpression constantType(CallSiteBinder callSiteBinder, Type type)
    {
        requireNonNull(callSiteBinder, "callSiteBinder is null");
        requireNonNull(type, "type is null");

        Binding binding = callSiteBinder.bind(type, Type.class);
        return new SqlTypeBytecodeExpression(type, binding, BOOTSTRAP_METHOD);
    }

    private final Type type;
    private final Binding binding;
    private final Method bootstrapMethod;

    private SqlTypeBytecodeExpression(Type type, Binding binding, Method bootstrapMethod)
    {
        super(type(Type.class));

        this.type = requireNonNull(type, "type is null");
        this.binding = requireNonNull(binding, "binding is null");
        this.bootstrapMethod = requireNonNull(bootstrapMethod, "bootstrapMethod is null");
    }

    @Override
    public BytecodeNode getBytecode(MethodGenerationContext generationContext)
    {
        return InvokeInstruction.invokeDynamic(type.getTypeSignature().toString().replaceAll("\\W+", "_"), binding.getType(), bootstrapMethod, binding.getBindingId());
    }

    @Override
    public List<BytecodeNode> getChildNodes()
    {
        return ImmutableList.of();
    }

    @Override
    protected String formatOneLine()
    {
        return type.getTypeSignature().toString();
    }

    public BytecodeExpression getValue(BytecodeExpression block, BytecodeExpression position)
    {
        Class<?> fromJavaElementType = type.getJavaType();

        if (fromJavaElementType == boolean.class) {
            return invoke("getBoolean", boolean.class, block, position);
        }
        if (fromJavaElementType == long.class) {
            return invoke("getLong", long.class, block, position);
        }
        if (fromJavaElementType == double.class) {
            return invoke("getDouble", double.class, block, position);
        }
        if (fromJavaElementType == Slice.class) {
            return invoke("getSlice", Slice.class, block, position);
        }
        return invoke("getObject", Object.class, block, position).cast(fromJavaElementType);
    }

    public BytecodeExpression writeValue(BytecodeExpression blockBuilder, BytecodeExpression value)
    {
        Class<?> fromJavaElementType = type.getJavaType();

        if (fromJavaElementType == boolean.class) {
            return invoke("writeBoolean", void.class, blockBuilder, value);
        }
        if (fromJavaElementType == long.class) {
            return invoke("writeLong", void.class, blockBuilder, value);
        }
        if (fromJavaElementType == double.class) {
            return invoke("writeDouble", void.class, blockBuilder, value);
        }
        if (fromJavaElementType == Slice.class) {
            return invoke("writeSlice", void.class, blockBuilder, value);
        }
        return invoke("writeObject", void.class, blockBuilder, value.cast(Object.class));
    }
}
