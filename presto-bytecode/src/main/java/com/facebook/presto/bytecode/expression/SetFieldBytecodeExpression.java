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
package com.facebook.presto.bytecode.expression;

import com.facebook.presto.bytecode.BytecodeBlock;
import com.facebook.presto.bytecode.BytecodeNode;
import com.facebook.presto.bytecode.FieldDefinition;
import com.facebook.presto.bytecode.MethodGenerationContext;
import com.facebook.presto.bytecode.ParameterizedType;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.List;

import static com.facebook.presto.bytecode.Access.STATIC;
import static com.facebook.presto.bytecode.ParameterizedType.type;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

class SetFieldBytecodeExpression
        extends BytecodeExpression
{
    private final BytecodeExpression instance;
    private final ParameterizedType declaringClass;
    private final String name;
    private final BytecodeExpression value;
    private final ParameterizedType fieldType;

    public SetFieldBytecodeExpression(@Nullable BytecodeExpression instance, Class<?> declaringClass, String name, BytecodeExpression value)
    {
        this(instance, getDeclaredField(declaringClass, name), value);
    }

    public SetFieldBytecodeExpression(@Nullable BytecodeExpression instance, Field field, BytecodeExpression value)
    {
        this(instance, type(requireNonNull(field, "field is null").getDeclaringClass()), field.getName(), value, type(field.getType()));

        boolean isStatic = Modifier.isStatic(field.getModifiers());
        if (instance == null) {
            checkArgument(isStatic, "Field is not static: %s", field);
        }
        else {
            checkArgument(!isStatic, "Field is static: %s", field);
        }
    }

    public SetFieldBytecodeExpression(@Nullable BytecodeExpression instance, FieldDefinition field, BytecodeExpression value)
    {
        this(instance, requireNonNull(field, "field is null").getDeclaringClass().getType(), field.getName(), value, field.getType());
        if (instance == null) {
            checkArgument(field.getAccess().contains(STATIC), "Field is not static: %s", field);
        }
        else {
            checkArgument(!field.getAccess().contains(STATIC), "Field is static: %s", field);
        }
    }

    public SetFieldBytecodeExpression(@Nullable BytecodeExpression instance, ParameterizedType declaringClass, String name, BytecodeExpression value)
    {
        this(instance, declaringClass, name, value, value.getType());
    }

    public SetFieldBytecodeExpression(@Nullable BytecodeExpression instance,
            ParameterizedType declaringClass,
            String name,
            BytecodeExpression value,
            ParameterizedType fieldType)
    {
        super(type(void.class));
        if (instance != null) {
            checkArgument(!instance.getType().isPrimitive(), "Type %s does not have fields", instance.getType());
        }
        this.instance = instance;
        this.declaringClass = requireNonNull(declaringClass, "declaringClass is null");
        this.name = requireNonNull(name, "name is null");
        this.fieldType = requireNonNull(fieldType, "fieldType is null");
        this.value = requireNonNull(value, "value is null");
    }

    @Override
    public BytecodeNode getBytecode(MethodGenerationContext generationContext)
    {
        if (instance == null) {
            return new BytecodeBlock()
                    .append(value.getBytecode(generationContext))
                    .putStaticField(declaringClass, name, fieldType);
        }

        return new BytecodeBlock()
                .append(instance.getBytecode(generationContext))
                .append(value.getBytecode(generationContext))
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
    public List<BytecodeNode> getChildNodes()
    {
        ImmutableList.Builder<BytecodeNode> children = ImmutableList.builder();
        if (instance != null) {
            children.add(instance);
        }
        children.add(value);
        return children.build();
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
