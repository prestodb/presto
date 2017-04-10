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

import com.facebook.presto.bytecode.BytecodeNode;
import com.facebook.presto.bytecode.BytecodeVisitor;
import com.facebook.presto.bytecode.FieldDefinition;
import com.facebook.presto.bytecode.MethodDefinition;
import com.facebook.presto.bytecode.MethodGenerationContext;
import com.facebook.presto.bytecode.ParameterizedType;
import com.google.common.collect.ImmutableList;
import org.objectweb.asm.MethodVisitor;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;

import static com.facebook.presto.bytecode.ParameterizedType.type;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantInt;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.transform;
import static java.util.Objects.requireNonNull;

/**
 * A BytecodeExpression is chain of Java like expressions that results in at most
 * a single value being pushed on the stack.  The chain starts with a constant,
 * local variable, static field, static method or invoke dynamic followed
 * by zero or more invocations, field dereferences, array element fetches, or casts.
 * The expression can optionally be terminated by a set expression, and in this
 * case no value is pushed on the stack.
 *
 * A BytecodeExpression is a BytecodeNode so it works with tools like tree dump.
 *
 * This abstraction makes it easy to write generic byte code generators that can
 * work with data that may come from a parameter, field or the result of a method
 * invocation.
 */
public abstract class BytecodeExpression
        implements BytecodeNode
{
    private final ParameterizedType type;

    protected BytecodeExpression(ParameterizedType type)
    {
        this.type = requireNonNull(type, "type is null");
    }

    public final ParameterizedType getType()
    {
        return type;
    }

    public abstract BytecodeNode getBytecode(MethodGenerationContext generationContext);

    protected abstract String formatOneLine();

    @Override
    public final String toString()
    {
        return formatOneLine() + (type.getPrimitiveType() == void.class ? ";" : "");
    }

    public final BytecodeExpression getField(Class<?> declaringClass, String name)
    {
        return new GetFieldBytecodeExpression(this, declaringClass, name);
    }

    public final BytecodeExpression getField(String name, Class<?> type)
    {
        return new GetFieldBytecodeExpression(this, this.getType(), name, type(type));
    }

    public final BytecodeExpression getField(Field field)
    {
        return new GetFieldBytecodeExpression(this, field);
    }

    public final BytecodeExpression getField(FieldDefinition field)
    {
        return new GetFieldBytecodeExpression(this, field);
    }

    public final BytecodeExpression getField(ParameterizedType declaringClass, String name, ParameterizedType type)
    {
        return new GetFieldBytecodeExpression(this, declaringClass, name, type);
    }

    public final BytecodeExpression setField(String name, BytecodeExpression value)
    {
        return new SetFieldBytecodeExpression(this, this.getType(), name, value);
    }

    public final BytecodeExpression setField(Field field, BytecodeExpression value)
    {
        return new SetFieldBytecodeExpression(this, field, value);
    }

    public final BytecodeExpression setField(FieldDefinition field, BytecodeExpression value)
    {
        return new SetFieldBytecodeExpression(this, field, value);
    }

    public final BytecodeExpression cast(Class<?> type)
    {
        return new CastBytecodeExpression(this, type(type));
    }

    public final BytecodeExpression cast(ParameterizedType type)
    {
        return new CastBytecodeExpression(this, type);
    }

    public final BytecodeExpression invoke(Method method, BytecodeExpression... parameters)
    {
        return invoke(method, ImmutableList.copyOf(requireNonNull(parameters, "parameters is null")));
    }

    public final BytecodeExpression invoke(MethodDefinition method, Iterable<? extends BytecodeExpression> parameters)
    {
        List<BytecodeExpression> params = ImmutableList.copyOf(parameters);
        checkArgument(method.getParameters().size() == params.size(), "Expected %s params found %s", method.getParameters().size(), params.size());
        return invoke(method.getName(), method.getReturnType(), parameters);
    }

    public final BytecodeExpression invoke(Method method, Iterable<? extends BytecodeExpression> parameters)
    {
        return invoke(method.getName(), type(method.getReturnType()), parameters);
    }

    public final BytecodeExpression invoke(String methodName, Class<?> returnType, BytecodeExpression... parameters)
    {
        return invoke(methodName, type(returnType), ImmutableList.copyOf(requireNonNull(parameters, "parameters is null")));
    }

    public final BytecodeExpression invoke(String methodName, Class<?> returnType, Iterable<? extends BytecodeExpression> parameters)
    {
        return invoke(methodName, type(returnType), parameters);
    }

    public final BytecodeExpression invoke(String methodName, ParameterizedType returnType, Iterable<? extends BytecodeExpression> parameters)
    {
        requireNonNull(parameters, "parameters is null");

        return invoke(methodName,
                returnType,
                ImmutableList.copyOf(transform(parameters, BytecodeExpression::getType)),
                parameters);
    }

    public final BytecodeExpression invoke(String methodName, Class<?> returnType, Iterable<? extends Class<?>> parameterTypes, BytecodeExpression... parameters)
    {
        return invoke(methodName, type(returnType), transform(parameterTypes, ParameterizedType::type), ImmutableList.copyOf(requireNonNull(parameters, "parameters is null")));
    }

    public final BytecodeExpression invoke(String methodName, ParameterizedType returnType, Iterable<ParameterizedType> parameterTypes, BytecodeExpression... parameters)
    {
        return invoke(methodName, returnType, parameterTypes, ImmutableList.copyOf(requireNonNull(parameters, "parameters is null")));
    }

    public final BytecodeExpression invoke(
            String methodName,
            ParameterizedType returnType,
            Iterable<ParameterizedType> parameterTypes,
            Iterable<? extends BytecodeExpression> parameters)
    {
        return InvokeBytecodeExpression.createInvoke(
                this,
                methodName,
                returnType,
                parameterTypes,
                parameters);
    }

    public final BytecodeExpression getElement(int index)
    {
        return new GetElementBytecodeExpression(this, constantInt(index));
    }

    public final BytecodeExpression getElement(BytecodeExpression index)
    {
        return new GetElementBytecodeExpression(this, index);
    }

    public final BytecodeExpression setElement(int index, BytecodeExpression value)
    {
        return new SetArrayElementBytecodeExpression(this, constantInt(index), value);
    }

    public final BytecodeExpression setElement(BytecodeExpression index, BytecodeExpression value)
    {
        return new SetArrayElementBytecodeExpression(this, index, value);
    }

    public final BytecodeExpression length()
    {
        return new ArrayLengthBytecodeExpression(this);
    }

    public final BytecodeExpression ret()
    {
        return new ReturnBytecodeExpression(this);
    }

    public final BytecodeExpression pop()
    {
        if (this.getType().getPrimitiveType() == void.class) {
            return this;
        }
        return new PopBytecodeExpression(this);
    }

    @Override
    public final void accept(MethodVisitor visitor, MethodGenerationContext generationContext)
    {
        getBytecode(generationContext).accept(visitor, generationContext);
    }

    @Override
    public final <T> T accept(BytecodeNode parent, BytecodeVisitor<T> visitor)
    {
        return visitor.visitBytecodeExpression(parent, this);
    }

    public BytecodeExpression instanceOf(Class<?> type)
    {
        return InstanceOfBytecodeExpression.instanceOf(this, type);
    }
}
