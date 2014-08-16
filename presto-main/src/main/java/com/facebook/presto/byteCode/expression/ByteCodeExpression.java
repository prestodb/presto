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
import com.facebook.presto.byteCode.ParameterizedType;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import org.objectweb.asm.MethodVisitor;

import java.lang.invoke.MethodType;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;

import static com.facebook.presto.byteCode.ParameterizedType.toParameterizedType;
import static com.facebook.presto.byteCode.ParameterizedType.type;
import static com.facebook.presto.byteCode.instruction.Constant.loadBoolean;
import static com.facebook.presto.byteCode.instruction.Constant.loadClass;
import static com.facebook.presto.byteCode.instruction.Constant.loadDouble;
import static com.facebook.presto.byteCode.instruction.Constant.loadFloat;
import static com.facebook.presto.byteCode.instruction.Constant.loadInt;
import static com.facebook.presto.byteCode.instruction.Constant.loadLong;
import static com.facebook.presto.byteCode.instruction.Constant.loadNull;
import static com.facebook.presto.byteCode.instruction.Constant.loadString;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;

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
    public static ByteCodeExpression constantBoolean(boolean value)
    {
        return new ConstantByteCodeExpression(boolean.class, loadBoolean(value));
    }

    public static ByteCodeExpression constantClass(Class<?> value)
    {
        return new ConstantByteCodeExpression(Class.class, loadClass(value));
    }

    public static ByteCodeExpression constantClass(ParameterizedType value)
    {
        return new ConstantByteCodeExpression(Class.class, loadClass(value));
    }

    public static ByteCodeExpression constantDouble(double value)
    {
        return new ConstantByteCodeExpression(double.class, loadDouble(value));
    }

    public static ByteCodeExpression constantFloat(float value)
    {
        return new ConstantByteCodeExpression(float.class, loadFloat(value));
    }

    public static ByteCodeExpression constantInt(int value)
    {
        return new ConstantByteCodeExpression(int.class, loadInt(value));
    }

    public static ByteCodeExpression constantLong(long value)
    {
        return new ConstantByteCodeExpression(long.class, loadLong(value));
    }

    public static ByteCodeExpression constantNull(Class<?> type)
    {
        return new ConstantByteCodeExpression(type, loadNull());
    }

    public static ByteCodeExpression constantNull(ParameterizedType type)
    {
        return new ConstantByteCodeExpression(type, loadNull());
    }

    public static ByteCodeExpression constantString(String value)
    {
        return new ConstantByteCodeExpression(String.class, loadString(value));
    }

    public static ByteCodeExpression getStatic(Class<?> declaringClass, String name)
    {
        return new GetFieldByteCodeExpression(null, declaringClass, name);
    }

    public static ByteCodeExpression getStatic(Field staticField)
    {
        return new GetFieldByteCodeExpression(null, staticField);
    }

    public static ByteCodeExpression getStatic(FieldDefinition staticField)
    {
        return new GetFieldByteCodeExpression(null, staticField);
    }

    public static ByteCodeExpression getStatic(ParameterizedType declaringClass, String name, ParameterizedType type)
    {
        return new GetFieldByteCodeExpression(null, declaringClass, name, type);
    }

    public static ByteCodeExpression setStatic(Class<?> declaringClass, String name, ByteCodeExpression value)
    {
        return new SetFieldByteCodeExpression(null, declaringClass, name, value);
    }

    public static ByteCodeExpression setStatic(Field staticField, ByteCodeExpression value)
    {
        return new SetFieldByteCodeExpression(null, staticField, value);
    }

    public static ByteCodeExpression setStatic(FieldDefinition staticField, ByteCodeExpression value)
    {
        return new SetFieldByteCodeExpression(null, staticField, value);
    }

    public static ByteCodeExpression setStatic(ParameterizedType declaringClass, String name, ByteCodeExpression value)
    {
        return new SetFieldByteCodeExpression(null, declaringClass, name, value);
    }

    public static ByteCodeExpression invokeStatic(
            Class<?> methodTargetType,
            String methodName,
            Class<?> returnType,
            ByteCodeExpression... parameters)
    {
        return invokeStatic(
                type(checkNotNull(methodTargetType, "methodTargetType is null")),
                methodName,
                type(checkNotNull(returnType, "returnType is null")),
                ImmutableList.copyOf(checkNotNull(parameters, "parameters is null")));
    }

    public static ByteCodeExpression invokeStatic(
            Class<?> methodTargetType,
            String methodName,
            Class<?> returnType,
            Iterable<? extends ByteCodeExpression> parameters)
    {
        return invokeStatic(
                type(checkNotNull(methodTargetType, "methodTargetType is null")),
                methodName,
                type(checkNotNull(returnType, "returnType is null")),
                parameters);
    }

    public static ByteCodeExpression invokeStatic(
            ParameterizedType methodTargetType,
            String methodName,
            ParameterizedType returnType,
            Iterable<? extends ByteCodeExpression> parameters)
    {
        return new InvokeByteCodeExpression(
                null,
                methodTargetType,
                methodName,
                returnType,
                ImmutableList.copyOf(transform(checkNotNull(parameters, "parameters is null"), typeGetter())),
                parameters);
    }

    public static ByteCodeExpression invokeDynamic(
            String methodName,
            MethodType methodType,
            Method bootstrapMethod,
            Object... bootstrapArgs)
    {
        return invokeDynamic(methodName, methodType, bootstrapMethod, ImmutableList.copyOf(bootstrapArgs));
    }

    public static ByteCodeExpression invokeDynamic(
            String methodName,
            MethodType methodType,
            Method bootstrapMethod,
            List<Object> bootstrapArgs)
    {
        return new InvokeDynamicByteCodeExpression(methodName, methodType, bootstrapMethod, bootstrapArgs);
    }

    private final ParameterizedType type;

    protected ByteCodeExpression(ParameterizedType type)
    {
        this.type = checkNotNull(type, "type is null");
    }

    public final ParameterizedType getType()
    {
        return type;
    }

    public abstract ByteCodeNode getByteCode();

    public ByteCodeExpression getField(Class<?> declaringClass, String name)
    {
        return new GetFieldByteCodeExpression(this, declaringClass, name);
    }

    public ByteCodeExpression getField(Field field)
    {
        return new GetFieldByteCodeExpression(this, field);
    }

    public ByteCodeExpression getField(FieldDefinition field)
    {
        return new GetFieldByteCodeExpression(this, field);
    }

    public ByteCodeExpression getField(ParameterizedType declaringClass, String name, ParameterizedType type)
    {
        return new GetFieldByteCodeExpression(this, declaringClass, name, type);
    }

    public ByteCodeExpression setField(Class<?> declaringClass, String name, ByteCodeExpression value)
    {
        return new SetFieldByteCodeExpression(this, declaringClass, name, value);
    }

    public ByteCodeExpression setField(Field field, ByteCodeExpression value)
    {
        return new SetFieldByteCodeExpression(this, field, value);
    }

    public ByteCodeExpression setField(FieldDefinition field, ByteCodeExpression value)
    {
        return new SetFieldByteCodeExpression(this, field, value);
    }

    public ByteCodeExpression setField(ParameterizedType declaringClass, String name, ByteCodeExpression value)
    {
        return new SetFieldByteCodeExpression(this, declaringClass, name, value);
    }

    public final ByteCodeExpression cast(Class<?> type)
    {
        return new CastByteCodeExpression(this, type(type));
    }

    public final ByteCodeExpression cast(ParameterizedType type)
    {
        return new CastByteCodeExpression(this, type);
    }

    public final ByteCodeExpression invoke(String methodName, Class<?> returnType, ByteCodeExpression... parameters)
    {
        return invoke(methodName, type(returnType), ImmutableList.copyOf(checkNotNull(parameters, "parameters is null")));
    }

    public final ByteCodeExpression invoke(String methodName, Class<?> returnType, Iterable<? extends ByteCodeExpression> parameters)
    {
        return invoke(methodName, type(returnType), parameters);
    }

    public final ByteCodeExpression invoke(String methodName, ParameterizedType returnType, Iterable<? extends ByteCodeExpression> parameters)
    {
        return invoke(methodName,
                returnType,
                ImmutableList.copyOf(transform(checkNotNull(parameters, "parameters is null"), typeGetter())),
                parameters);
    }

    public final ByteCodeExpression invoke(String methodName, Class<?> returnType, Iterable<? extends Class<?>> parameterTypes, ByteCodeExpression... parameters)
    {
        return invoke(methodName, type(returnType), transform(parameterTypes, toParameterizedType()), ImmutableList.copyOf(checkNotNull(parameters, "parameters is null")));
    }

    public final ByteCodeExpression invoke(String methodName, ParameterizedType returnType, Iterable<ParameterizedType> parameterTypes, ByteCodeExpression... parameters)
    {
        return invoke(methodName, returnType, parameterTypes, ImmutableList.copyOf(checkNotNull(parameters, "parameters is null")));
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

    @Override
    public final void accept(MethodVisitor visitor)
    {
        getByteCode().accept(visitor);
    }

    @Override
    public final <T> T accept(ByteCodeNode parent, ByteCodeVisitor<T> visitor)
    {
        return visitor.visitByteCodeExpression(parent, this);
    }

    public static Function<ByteCodeExpression, ParameterizedType> typeGetter()
    {
        return new Function<ByteCodeExpression, ParameterizedType>()
        {
            @Override
            public ParameterizedType apply(ByteCodeExpression byteCodeExpression)
            {
                return byteCodeExpression.getType();
            }
        };
    }
}
