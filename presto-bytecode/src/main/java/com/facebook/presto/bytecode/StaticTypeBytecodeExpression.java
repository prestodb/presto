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
package com.facebook.presto.bytecode;

import com.facebook.presto.bytecode.expression.BytecodeExpression;
import com.facebook.presto.bytecode.instruction.InvokeInstruction;
import io.airlift.slice.Slice;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;

import static com.facebook.presto.bytecode.ParameterizedType.type;
import static java.util.Objects.requireNonNull;

public class StaticTypeBytecodeExpression
        extends BytecodeExpression
{
    private final String typeName;
    private final Class<?> javaType;
    private final Binding binding;
    private final Method bootstrapMethod;

    public StaticTypeBytecodeExpression(Binding binding, Method bootstrapMethod, Class<?> sqlType, String typeName, Class<?> javaType)
    {
        super(type(sqlType));
        this.typeName = requireNonNull(typeName, "type is null");
        this.javaType = javaType;
        this.binding = requireNonNull(binding, "binding is null");
        this.bootstrapMethod = requireNonNull(bootstrapMethod, "bootstrapMethod is null");
    }

    @Override
    public BytecodeNode getBytecode(MethodGenerationContext generationContext)
    {
        return InvokeInstruction.invokeDynamic(typeName.replaceAll("\\W+", "_"), binding.getType(), bootstrapMethod, binding.getBindingId());
    }

    @Override
    public List<BytecodeNode> getChildNodes()
    {
        return Collections.emptyList();
    }

    @Override
    protected String formatOneLine()
    {
        return typeName;
    }

    public BytecodeExpression getValue(BytecodeExpression block, BytecodeExpression position)
    {
        if (javaType == boolean.class) {
            return invoke("getBoolean", boolean.class, block, position);
        }
        if (javaType == long.class) {
            return invoke("getLong", long.class, block, position);
        }
        if (javaType == double.class) {
            return invoke("getDouble", double.class, block, position);
        }
        if (javaType == Slice.class) {
            return invoke("getSlice", Slice.class, block, position);
        }
        return invoke("getObject", Object.class, block, position).cast(javaType);
    }

    public BytecodeExpression writeValue(BytecodeExpression blockBuilder, BytecodeExpression value)
    {
        if (javaType == boolean.class) {
            return invoke("writeBoolean", void.class, blockBuilder, value);
        }
        if (javaType == long.class) {
            return invoke("writeLong", void.class, blockBuilder, value);
        }
        if (javaType == double.class) {
            return invoke("writeDouble", void.class, blockBuilder, value);
        }
        if (javaType == Slice.class) {
            return invoke("writeSlice", void.class, blockBuilder, value);
        }
        return invoke("writeObject", void.class, blockBuilder, value.cast(Object.class));
    }
}
