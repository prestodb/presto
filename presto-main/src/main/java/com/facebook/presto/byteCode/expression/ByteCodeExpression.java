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

import com.facebook.presto.byteCode.ByteCodeNode;
import com.facebook.presto.byteCode.ByteCodeVisitor;
import com.facebook.presto.byteCode.FieldDefinition;
import com.facebook.presto.byteCode.MethodDefinition;
import com.facebook.presto.byteCode.MethodGenerationContext;
import com.facebook.presto.byteCode.ParameterizedType;
import com.google.common.collect.ImmutableList;
import org.objectweb.asm.MethodVisitor;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;

import static com.facebook.presto.byteCode.ParameterizedType.type;
import static com.facebook.presto.byteCode.expression.ByteCodeExpressions.constantInt;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.transform;
import static java.util.Objects.requireNonNull;

/**
 * A ByteCodeExpression is chain of Java like expressions that results in at most
 * a single value being pushed on the stack.  The chain starts with a constant,
 * local variable, static field, static method or invoke dynamic followed followed
 * by zero or more invocations, field dereferences, array element fetches, or casts.
 * The expression can optionally be terminated by a set expression, and in this
 * case no value is pushed on the stack.
 *
 * A ByteCodeExpression is a ByteCodeNode so it works with tools like tree dump.
 *
 * This abstraction makes it easy to write generic byte code generators that can
 * work with data that may come from a parameter, field or the result of a method
 * invocation.
 */
public abstract class ByteCodeExpression
        implements ByteCodeNode
{
    private final ParameterizedType type;

    protected ByteCodeExpression(ParameterizedType type)
    {
        this.type = requireNonNull(type, "type is null");
    }

    public final ParameterizedType getType()
    {
        return type;
    }

    public abstract ByteCodeNode getByteCode(MethodGenerationContext generationContext);

    protected abstract String formatOneLine();

    @Override
    public final String toString()
    {
        return formatOneLine() + (type.getPrimitiveType() == void.class ? ";" : "");
    }

    public final ByteCodeExpression getField(Class<?> declaringClass, String name)
    {
        return new GetFieldByteCodeExpression(this, declaringClass, name);
    }

    public final ByteCodeExpression getField(String name, Class<?> type)
    {
        return new GetFieldByteCodeExpression(this, this.getType(), name, type(type));
    }

    public final ByteCodeExpression getField(Field field)
    {
        return new GetFieldByteCodeExpression(this, field);
    }

    public final ByteCodeExpression getField(FieldDefinition field)
    {
        return new GetFieldByteCodeExpression(this, field);
    }

    public final ByteCodeExpression getField(ParameterizedType declaringClass, String name, ParameterizedType type)
    {
        return new GetFieldByteCodeExpression(this, declaringClass, name, type);
    }

    public final ByteCodeExpression setField(String name, ByteCodeExpression value)
    {
        return new SetFieldByteCodeExpression(this, this.getType(), name, value);
    }

    public final ByteCodeExpression setField(Field field, ByteCodeExpression value)
    {
        return new SetFieldByteCodeExpression(this, field, value);
    }

    public final ByteCodeExpression setField(FieldDefinition field, ByteCodeExpression value)
    {
        return new SetFieldByteCodeExpression(this, field, value);
    }

    public final ByteCodeExpression cast(Class<?> type)
    {
        return new CastByteCodeExpression(this, type(type));
    }

    public final ByteCodeExpression cast(ParameterizedType type)
    {
        return new CastByteCodeExpression(this, type);
    }

    public final ByteCodeExpression invoke(Method method, ByteCodeExpression... parameters)
    {
        return invoke(method, ImmutableList.copyOf(requireNonNull(parameters, "parameters is null")));
    }

    public final ByteCodeExpression invoke(MethodDefinition method, Iterable<? extends ByteCodeExpression> parameters)
    {
        List<ByteCodeExpression> params = ImmutableList.copyOf(parameters);
        checkArgument(method.getParameters().size() == params.size(), "Expected %s params found %s", method.getParameters().size(), params.size());
        return invoke(method.getName(), method.getReturnType(), parameters);
    }

    public final ByteCodeExpression invoke(Method method, Iterable<? extends ByteCodeExpression> parameters)
    {
        return invoke(method.getName(), type(method.getReturnType()), parameters);
    }

    public final ByteCodeExpression invoke(String methodName, Class<?> returnType, ByteCodeExpression... parameters)
    {
        return invoke(methodName, type(returnType), ImmutableList.copyOf(requireNonNull(parameters, "parameters is null")));
    }

    public final ByteCodeExpression invoke(String methodName, Class<?> returnType, Iterable<? extends ByteCodeExpression> parameters)
    {
        return invoke(methodName, type(returnType), parameters);
    }

    public final ByteCodeExpression invoke(String methodName, ParameterizedType returnType, Iterable<? extends ByteCodeExpression> parameters)
    {
        requireNonNull(parameters, "parameters is null");

        return invoke(methodName,
                returnType,
                ImmutableList.copyOf(transform(parameters, ByteCodeExpression::getType)),
                parameters);
    }

    public final ByteCodeExpression invoke(String methodName, Class<?> returnType, Iterable<? extends Class<?>> parameterTypes, ByteCodeExpression... parameters)
    {
        return invoke(methodName, type(returnType), transform(parameterTypes, ParameterizedType::type), ImmutableList.copyOf(requireNonNull(parameters, "parameters is null")));
    }

    public final ByteCodeExpression invoke(String methodName, ParameterizedType returnType, Iterable<ParameterizedType> parameterTypes, ByteCodeExpression... parameters)
    {
        return invoke(methodName, returnType, parameterTypes, ImmutableList.copyOf(requireNonNull(parameters, "parameters is null")));
    }

    public final ByteCodeExpression invoke(
            String methodName,
            ParameterizedType returnType,
            Iterable<ParameterizedType> parameterTypes,
            Iterable<? extends ByteCodeExpression> parameters)
    {
        return InvokeByteCodeExpression.createInvoke(
                this,
                methodName,
                returnType,
                parameterTypes,
                parameters);
    }

    public final ByteCodeExpression getElement(int index)
    {
        return new GetElementByteCodeExpression(this, constantInt(index));
    }

    public final ByteCodeExpression getElement(ByteCodeExpression index)
    {
        return new GetElementByteCodeExpression(this, index);
    }

    public final ByteCodeExpression setElement(int index, ByteCodeExpression value)
    {
        return new SetArrayElementByteCodeExpression(this, constantInt(index), value);
    }

    public final ByteCodeExpression setElement(ByteCodeExpression index, ByteCodeExpression value)
    {
        return new SetArrayElementByteCodeExpression(this, index, value);
    }

    public final ByteCodeExpression length()
    {
        return new ArrayLengthByteCodeExpression(this);
    }

    public final ByteCodeExpression ret()
    {
        return new ReturnByteCodeExpression(this);
    }

    public final ByteCodeExpression pop()
    {
        if (this.getType().getPrimitiveType() == void.class) {
            return this;
        }
        return new PopByteCodeExpression(this);
    }

    @Override
    public final void accept(MethodVisitor visitor, MethodGenerationContext generationContext)
    {
        getByteCode(generationContext).accept(visitor, generationContext);
    }

    @Override
    public final <T> T accept(ByteCodeNode parent, ByteCodeVisitor<T> visitor)
    {
        return visitor.visitByteCodeExpression(parent, this);
    }

    public ByteCodeExpression instanceOf(Class<?> type)
    {
        return InstanceOfByteCodeExpression.instanceOf(this, type);
    }
}
