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
import com.facebook.presto.byteCode.FieldDefinition;
import com.facebook.presto.byteCode.MethodGenerationContext;
import com.facebook.presto.byteCode.ParameterizedType;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.List;

import static com.facebook.presto.byteCode.Access.STATIC;
import static com.facebook.presto.byteCode.ParameterizedType.type;
import static com.facebook.presto.byteCode.instruction.FieldInstruction.getStaticInstruction;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

class GetFieldByteCodeExpression
        extends ByteCodeExpression
{
    private final ByteCodeExpression instance;
    private final ParameterizedType declaringClass;
    private final String name;

    public GetFieldByteCodeExpression(@Nullable ByteCodeExpression instance, Class<?> declaringClass, String name)
    {
        this(instance, getDeclaredField(declaringClass, name));
    }

    public GetFieldByteCodeExpression(@Nullable ByteCodeExpression instance, Field field)
    {
        this(instance, type(requireNonNull(field, "field is null").getDeclaringClass()), field.getName(), type(field.getType()));

        boolean isStatic = Modifier.isStatic(field.getModifiers());
        if (instance == null) {
            checkArgument(isStatic, "Field is not static: %s", field);
        }
        else {
            checkArgument(!isStatic, "Field is static: %s", field);
        }
    }

    public GetFieldByteCodeExpression(@Nullable ByteCodeExpression instance, FieldDefinition field)
    {
        this(instance, requireNonNull(field, "field is null").getDeclaringClass().getType(), field.getName(), field.getType());
        if (instance == null) {
            checkArgument(field.getAccess().contains(STATIC), "Field is not static: %s", field);
        }
        else {
            checkArgument(!field.getAccess().contains(STATIC), "Field is static: %s", field);
        }
    }

    public GetFieldByteCodeExpression(@Nullable ByteCodeExpression instance, ParameterizedType declaringClass, String name, ParameterizedType type)
    {
        super(type);
        checkArgument(instance == null || !instance.getType().isPrimitive(), "Type %s does not have fields", getType());
        this.instance = instance;
        this.declaringClass = requireNonNull(declaringClass, "declaringClass is null");
        this.name = requireNonNull(name, "name is null");
    }

    @Override
    public ByteCodeNode getByteCode(MethodGenerationContext generationContext)
    {
        if (instance == null) {
            return getStaticInstruction(declaringClass, name, getType());
        }

        return new ByteCodeBlock()
                .append(instance.getByteCode(generationContext))
                .getField(declaringClass, name, getType());
    }

    @Override
    protected String formatOneLine()
    {
        if (instance == null) {
            return declaringClass.getSimpleName() + "." + name;
        }
        return instance + "." + name;
    }

    @Override
    public List<ByteCodeNode> getChildNodes()
    {
        return (instance == null) ? ImmutableList.<ByteCodeNode>of() : ImmutableList.<ByteCodeNode>of(instance);
    }

    private static Field getDeclaredField(Class<?> declaringClass, String name)
    {
        requireNonNull(declaringClass, "declaringClass is null");
        requireNonNull(name, "name is null");

        try {
            return declaringClass.getField(name);
        }
        catch (NoSuchFieldException e) {
            throw new IllegalArgumentException(format("Class %s does not have a '%s' field", declaringClass.getName(), name));
        }
    }
}
