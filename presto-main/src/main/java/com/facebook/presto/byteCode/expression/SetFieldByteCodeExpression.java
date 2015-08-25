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
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

class SetFieldByteCodeExpression
        extends ByteCodeExpression
{
    private final ByteCodeExpression instance;
    private final ParameterizedType declaringClass;
    private final String name;
    private final ByteCodeExpression value;
    private final ParameterizedType fieldType;

    public SetFieldByteCodeExpression(@Nullable ByteCodeExpression instance, Class<?> declaringClass, String name, ByteCodeExpression value)
    {
        this(instance, getDeclaredField(declaringClass, name), value);
    }

    public SetFieldByteCodeExpression(@Nullable ByteCodeExpression instance, Field field, ByteCodeExpression value)
    {
        this(instance, type(checkNotNull(field, "field is null").getDeclaringClass()), field.getName(), value, type(field.getType()));

        boolean isStatic = Modifier.isStatic(field.getModifiers());
        if (instance == null) {
            checkArgument(isStatic, "Field is not static: %s", field);
        }
        else {
            checkArgument(!isStatic, "Field is static: %s", field);
        }
    }

    public SetFieldByteCodeExpression(@Nullable ByteCodeExpression instance, FieldDefinition field, ByteCodeExpression value)
    {
        this(instance, checkNotNull(field, "field is null").getDeclaringClass().getType(), field.getName(), value, field.getType());
        if (instance == null) {
            checkArgument(field.getAccess().contains(STATIC), "Field is not static: %s", field);
        }
        else {
            checkArgument(!field.getAccess().contains(STATIC), "Field is static: %s", field);
        }
    }

    public SetFieldByteCodeExpression(@Nullable ByteCodeExpression instance, ParameterizedType declaringClass, String name, ByteCodeExpression value)
    {
        this(instance, declaringClass, name, value, value.getType());
    }

    public SetFieldByteCodeExpression(@Nullable ByteCodeExpression instance,
            ParameterizedType declaringClass,
            String name,
            ByteCodeExpression value,
            ParameterizedType fieldType)
    {
        super(type(void.class));
        if (instance != null) {
            checkArgument(!instance.getType().isPrimitive(), "Type %s does not have fields", instance.getType());
        }
        this.instance = instance;
        this.declaringClass = checkNotNull(declaringClass, "declaringClass is null");
        this.name = checkNotNull(name, "name is null");
        this.fieldType = checkNotNull(fieldType, "fieldType is null");
        this.value = checkNotNull(value, "value is null");
    }

    @Override
    public ByteCodeNode getByteCode(MethodGenerationContext generationContext)
    {
        if (instance == null) {
            return new ByteCodeBlock()
                    .append(value.getByteCode(generationContext))
                    .putStaticField(declaringClass, name, fieldType);
        }

        return new ByteCodeBlock()
                .append(instance.getByteCode(generationContext))
                .append(value.getByteCode(generationContext))
                .putField(declaringClass, name, fieldType);
    }

    @Override
    protected String formatOneLine()
    {
        if (instance == null) {
            return declaringClass.getSimpleName() + "." + name + " = " + value;
        }
        else {
            return instance + "." + name + " = " + value;
        }
    }

    @Override
    public List<ByteCodeNode> getChildNodes()
    {
        ImmutableList.Builder<ByteCodeNode> children = ImmutableList.builder();
        if (instance != null) {
            children.add(instance);
        }
        children.add(value);
        return children.build();
    }

    private static Field getDeclaredField(Class<?> declaringClass, String name)
    {
        checkNotNull(declaringClass, "declaringClass is null");
        checkNotNull(name, "name is null");

        try {
            return declaringClass.getField(name);
        }
        catch (NoSuchFieldException e) {
            throw new IllegalArgumentException(format("Class %s does not have a '%s' field", declaringClass.getName(), name));
        }
    }
}
