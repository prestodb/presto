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
import com.facebook.presto.byteCode.MethodGenerationContext;
import com.facebook.presto.byteCode.expression.ByteCodeExpression;
import com.facebook.presto.byteCode.instruction.InvokeInstruction;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.lang.reflect.Method;
import java.util.List;

import static com.facebook.presto.byteCode.ParameterizedType.type;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.sql.gen.Bootstrap.BOOTSTRAP_METHOD;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

public class SqlTypeByteCodeExpression
        extends ByteCodeExpression
{
    public static SqlTypeByteCodeExpression constantType(CallSiteBinder callSiteBinder, Type type)
    {
        checkNotNull(callSiteBinder, "callSiteBinder is null");
        checkNotNull(type, "type is null");

        Binding binding = callSiteBinder.bind(type, Type.class);
        return new SqlTypeByteCodeExpression(type, binding, BOOTSTRAP_METHOD);
    }

    private final Type type;
    private final Binding binding;
    private final Method bootstrapMethod;

    private SqlTypeByteCodeExpression(Type type, Binding binding, Method bootstrapMethod)
    {
        super(type(Type.class));

        this.type = checkNotNull(type, "type is null");
        this.binding = checkNotNull(binding, "binding is null");
        this.bootstrapMethod = checkNotNull(bootstrapMethod, "bootstrapMethod is null");
    }

    @Override
    public ByteCodeNode getByteCode(MethodGenerationContext generationContext)
    {
        return InvokeInstruction.invokeDynamic(type.getTypeSignature().toString().replaceAll("\\W+", "_"), binding.getType(), bootstrapMethod, binding.getBindingId());
    }

    @Override
    public List<ByteCodeNode> getChildNodes()
    {
        return ImmutableList.of();
    }

    @Override
    protected String formatOneLine()
    {
        return type.getTypeSignature().toString();
    }

    public ByteCodeExpression getValue(ByteCodeExpression block, ByteCodeExpression position)
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
        throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("Unexpected type %s", fromJavaElementType.getName()));
    }

    public ByteCodeExpression writeValue(ByteCodeExpression blockBuilder, ByteCodeExpression value)
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
        throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("Unexpected type %s", fromJavaElementType.getName()));
    }
}
