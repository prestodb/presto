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

import com.facebook.presto.byteCode.FieldDefinition;
import com.facebook.presto.byteCode.MethodDefinition;
import com.facebook.presto.byteCode.OpCode;
import com.facebook.presto.byteCode.ParameterizedType;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodType;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

import static com.facebook.presto.byteCode.ParameterizedType.type;
import static com.facebook.presto.byteCode.expression.ArithmeticByteCodeExpression.createArithmeticByteCodeExpression;
import static com.facebook.presto.byteCode.instruction.Constant.loadBoolean;
import static com.facebook.presto.byteCode.instruction.Constant.loadClass;
import static com.facebook.presto.byteCode.instruction.Constant.loadDouble;
import static com.facebook.presto.byteCode.instruction.Constant.loadFloat;
import static com.facebook.presto.byteCode.instruction.Constant.loadInt;
import static com.facebook.presto.byteCode.instruction.Constant.loadLong;
import static com.facebook.presto.byteCode.instruction.Constant.loadNull;
import static com.facebook.presto.byteCode.instruction.Constant.loadString;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.transform;
import static java.util.Objects.requireNonNull;

public final class ByteCodeExpressions
{
    private ByteCodeExpressions()
    {
    }

    //
    // Constants
    //

    public static ByteCodeExpression constantTrue()
    {
        return new ConstantByteCodeExpression(boolean.class, loadBoolean(true));
    }

    public static ByteCodeExpression constantFalse()
    {
        return new ConstantByteCodeExpression(boolean.class, loadBoolean(false));
    }

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

    public static ByteCodeExpression constantNumber(Number value)
    {
        if (value instanceof Byte) {
            return constantInt((value).intValue()).cast(byte.class);
        }
        if (value instanceof Short) {
            return constantInt((value).intValue()).cast(short.class);
        }
        if (value instanceof Integer) {
            return constantInt((Integer) value);
        }
        if (value instanceof Long) {
            return constantLong((Long) value);
        }
        if (value instanceof Float) {
            return constantFloat((Float) value);
        }
        if (value instanceof Double) {
            return constantDouble((Double) value);
        }
        throw new IllegalStateException("Unsupported number type " + value.getClass().getSimpleName());
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

    public static ByteCodeExpression defaultValue(Class<?> type)
    {
        requireNonNull(type, "type is null");
        if (type == boolean.class) {
            return constantInt(0).cast(boolean.class);
        }
        if (type == byte.class) {
            return constantInt(0).cast(byte.class);
        }
        if (type == int.class) {
            return constantInt(0);
        }
        if (type == short.class) {
            return constantInt(0).cast(short.class);
        }
        if (type == long.class) {
            return constantLong(0L);
        }
        if (type == float.class) {
            return constantFloat(0.0f);
        }
        if (type == double.class) {
            return constantDouble(0.0d);
        }
        checkArgument(!type.isPrimitive(), "Unsupported type %s", type);
        return constantNull(type);
    }

    //
    // Get static field
    //

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

    //
    // Set static field
    //

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

    //
    // New instance
    //

    public static ByteCodeExpression newInstance(Class<?> returnType, ByteCodeExpression... parameters)
    {
        return newInstance(type(returnType), ImmutableList.copyOf(requireNonNull(parameters, "parameters is null")));
    }

    public static ByteCodeExpression newInstance(Class<?> returnType, Iterable<? extends ByteCodeExpression> parameters)
    {
        return newInstance(type(returnType), parameters);
    }

    public static ByteCodeExpression newInstance(ParameterizedType returnType, ByteCodeExpression... parameters)
    {
        requireNonNull(parameters, "parameters is null");

        return newInstance(returnType, ImmutableList.copyOf(parameters));
    }

    public static ByteCodeExpression newInstance(ParameterizedType returnType, Iterable<? extends ByteCodeExpression> parameters)
    {
        requireNonNull(parameters, "parameters is null");

        return newInstance(
                returnType,
                ImmutableList.copyOf(transform(parameters, ByteCodeExpression::getType)),
                parameters);
    }

    public static ByteCodeExpression newInstance(Class<?> returnType, Iterable<? extends Class<?>> parameterTypes, ByteCodeExpression... parameters)
    {
        return newInstance(type(returnType), transform(parameterTypes, ParameterizedType::type), ImmutableList.copyOf(requireNonNull(parameters, "parameters is null")));
    }

    public static ByteCodeExpression newInstance(ParameterizedType returnType, Iterable<ParameterizedType> parameterTypes, ByteCodeExpression... parameters)
    {
        return newInstance(returnType, parameterTypes, ImmutableList.copyOf(requireNonNull(parameters, "parameters is null")));
    }

    public static ByteCodeExpression newInstance(
            ParameterizedType type,
            Iterable<ParameterizedType> parameterTypes,
            Iterable<? extends ByteCodeExpression> parameters)
    {
        return new NewInstanceByteCodeExpression(type, parameterTypes, parameters);
    }

    //
    // Array
    //
    public static ByteCodeExpression newArray(ParameterizedType type, int length)
    {
        return new NewArrayByteCodeExpression(type, length);
    }

    public static ByteCodeExpression newArray(ParameterizedType type, ByteCodeExpression length)
    {
        return new NewArrayByteCodeExpression(type, length);
    }

    public static ByteCodeExpression length(ByteCodeExpression instance)
    {
        return new ArrayLengthByteCodeExpression(instance);
    }

    public static ByteCodeExpression get(ByteCodeExpression instance, ByteCodeExpression index)
    {
        return new GetElementByteCodeExpression(instance, index);
    }

    public static ByteCodeExpression set(ByteCodeExpression instance, ByteCodeExpression index, ByteCodeExpression value)
    {
        return new SetArrayElementByteCodeExpression(instance, index, value);
    }

    //
    // Invoke static method
    //

    public static ByteCodeExpression invokeStatic(MethodDefinition method,  ByteCodeExpression... parameters)
    {
        return invokeStatic(method.getDeclaringClass().getType(), method.getName(), method.getReturnType(), ImmutableList.copyOf(parameters));
    }

    public static ByteCodeExpression invokeStatic(Method method,  ByteCodeExpression... parameters)
    {
        return invokeStatic(method, ImmutableList.copyOf(requireNonNull(parameters, "parameters is null")));
    }

    public static ByteCodeExpression invokeStatic(Method method,  Iterable<? extends ByteCodeExpression> parameters)
    {
        return invokeStatic(method.getDeclaringClass(), method.getName(), method.getReturnType(), parameters);
    }

    public static ByteCodeExpression invokeStatic(Class<?> methodTargetType, String methodName, Class<?> returnType, ByteCodeExpression... parameters)
    {
        return invokeStatic(methodTargetType, methodName, returnType, ImmutableList.copyOf(requireNonNull(parameters, "parameters is null")));
    }

    public static ByteCodeExpression invokeStatic(
            Class<?> methodTargetType,
            String methodName,
            Class<?> returnType,
            Iterable<? extends ByteCodeExpression> parameters)
    {
        return invokeStatic(type(methodTargetType), methodName, type(returnType), parameters);
    }

    public static ByteCodeExpression invokeStatic(
            ParameterizedType methodTargetType,
            String methodName,
            ParameterizedType returnType,
            Iterable<? extends ByteCodeExpression> parameters)
    {
        requireNonNull(methodTargetType, "methodTargetType is null");
        requireNonNull(returnType, "returnType is null");
        requireNonNull(parameters, "parameters is null");

        return invokeStatic(
                methodTargetType,
                methodName,
                returnType,
                ImmutableList.copyOf(transform(parameters, ByteCodeExpression::getType)),
                parameters);
    }
    public static ByteCodeExpression invokeStatic(
            Class<?> methodTargetType,
            String methodName,
            Class<?> returnType,
            Iterable<? extends Class<?>> parameterTypes,
            ByteCodeExpression... parameters)
    {
        requireNonNull(methodTargetType, "methodTargetType is null");
        requireNonNull(returnType, "returnType is null");
        requireNonNull(parameterTypes, "parameterTypes is null");
        requireNonNull(parameters, "parameters is null");

        return invokeStatic(
                type(methodTargetType),
                methodName,
                type(returnType),
                transform(parameterTypes, ParameterizedType::type),
                ImmutableList.copyOf(parameters));
    }

    public static ByteCodeExpression invokeStatic(
            ParameterizedType methodTargetType,
            String methodName,
            ParameterizedType returnType,
            Iterable<ParameterizedType> parameterTypes,
            ByteCodeExpression... parameters)
    {
        return invokeStatic(methodTargetType, methodName, returnType, parameterTypes, ImmutableList.copyOf(requireNonNull(parameters, "parameters is null")));
    }

    public static ByteCodeExpression invokeStatic(
            ParameterizedType methodTargetType,
            String methodName,
            ParameterizedType returnType,
            Iterable<ParameterizedType> parameterTypes,
            Iterable<? extends ByteCodeExpression> parameters)
    {
        return new InvokeByteCodeExpression(
                null,
                methodTargetType,
                methodName,
                returnType,
                parameterTypes,
                parameters);
    }

    //
    // Invoke dynamic
    //

    public static ByteCodeExpression invokeDynamic(
            Method bootstrapMethod,
            Iterable<? extends Object> bootstrapArgs,
            String methodName,
            Class<?> returnType,
            ByteCodeExpression... parameters)
    {
        return invokeDynamic(bootstrapMethod, bootstrapArgs, methodName, returnType, ImmutableList.copyOf(requireNonNull(parameters, "parameters is null")));
    }

    public static ByteCodeExpression invokeDynamic(
            Method bootstrapMethod,
            Iterable<? extends Object> bootstrapArgs,
            String methodName,
            Class<?> returnType,
            Iterable<? extends ByteCodeExpression> parameters)
    {
        requireNonNull(returnType, "returnType is null");
        requireNonNull(parameters, "parameters is null");

        return invokeDynamic(
                bootstrapMethod,
                bootstrapArgs,
                methodName,
                type(returnType),
                ImmutableList.copyOf(transform(parameters, ByteCodeExpression::getType)),
                parameters);
    }

    public static ByteCodeExpression invokeDynamic(
            Method bootstrapMethod,
            Iterable<? extends Object> bootstrapArgs,
            String methodName,
            ParameterizedType returnType,
            ByteCodeExpression... parameters)
    {
        return invokeDynamic(bootstrapMethod, bootstrapArgs, methodName, returnType, ImmutableList.copyOf(requireNonNull(parameters, "parameters is null")));
    }

    public static ByteCodeExpression invokeDynamic(
            Method bootstrapMethod,
            Iterable<? extends Object> bootstrapArgs,
            String methodName,
            ParameterizedType returnType,
            Iterable<? extends ByteCodeExpression> parameters)
    {
        requireNonNull(returnType, "returnType is null");
        requireNonNull(parameters, "parameters is null");

        return invokeDynamic(
                bootstrapMethod,
                bootstrapArgs,
                methodName,
                returnType,
                ImmutableList.copyOf(transform(parameters, ByteCodeExpression::getType)),
                parameters);
    }

    public static ByteCodeExpression invokeDynamic(
            Method bootstrapMethod,
            Iterable<? extends Object> bootstrapArgs,
            String methodName,
            MethodType methodType,
            ByteCodeExpression... parameters)
    {
        requireNonNull(methodType, "methodType is null");
        requireNonNull(parameters, "parameters is null");

        return invokeDynamic(bootstrapMethod, bootstrapArgs, methodName, methodType, ImmutableList.copyOf(parameters));
    }

    public static ByteCodeExpression invokeDynamic(
            Method bootstrapMethod,
            Iterable<? extends Object> bootstrapArgs,
            String methodName,
            MethodType methodType,
            Iterable<? extends ByteCodeExpression> parameters)
    {
        return invokeDynamic(
                bootstrapMethod,
                bootstrapArgs,
                methodName,
                type(methodType.returnType()),
                transform(methodType.parameterList(), ParameterizedType::type),
                ImmutableList.copyOf(requireNonNull(parameters, "parameters is null")));
    }

    public static ByteCodeExpression invokeDynamic(
            Method bootstrapMethod,
            Iterable<? extends Object> bootstrapArgs,
            String methodName,
            ParameterizedType returnType,
            Iterable<ParameterizedType> parameterTypes,
            Iterable<? extends ByteCodeExpression> parameters)
    {
        return new InvokeDynamicByteCodeExpression(
                bootstrapMethod,
                bootstrapArgs,
                methodName,
                returnType,
                parameters,
                parameterTypes);
    }

    //
    // Arithmetic operations
    //

    public static ByteCodeExpression add(ByteCodeExpression left, ByteCodeExpression right)
    {
        return createArithmeticByteCodeExpression(OpCode.IADD, left, right);
    }

    public static ByteCodeExpression subtract(ByteCodeExpression left, ByteCodeExpression right)
    {
        return createArithmeticByteCodeExpression(OpCode.ISUB, left, right);
    }

    public static ByteCodeExpression multiply(ByteCodeExpression left, ByteCodeExpression right)
    {
        return createArithmeticByteCodeExpression(OpCode.IMUL, left, right);
    }

    public static ByteCodeExpression divide(ByteCodeExpression left, ByteCodeExpression right)
    {
        return createArithmeticByteCodeExpression(OpCode.IDIV, left, right);
    }

    public static ByteCodeExpression remainder(ByteCodeExpression left, ByteCodeExpression right)
    {
        return createArithmeticByteCodeExpression(OpCode.IREM, left, right);
    }

    public static ByteCodeExpression bitwiseAnd(ByteCodeExpression left, ByteCodeExpression right)
    {
        return createArithmeticByteCodeExpression(OpCode.IAND, left, right);
    }

    public static ByteCodeExpression bitwiseOr(ByteCodeExpression left, ByteCodeExpression right)
    {
        return createArithmeticByteCodeExpression(OpCode.IOR, left, right);
    }

    public static ByteCodeExpression bitwiseXor(ByteCodeExpression left, ByteCodeExpression right)
    {
        return createArithmeticByteCodeExpression(OpCode.IXOR, left, right);
    }

    public static ByteCodeExpression shiftLeft(ByteCodeExpression left, ByteCodeExpression right)
    {
        return createArithmeticByteCodeExpression(OpCode.ISHL, left, right);
    }

    public static ByteCodeExpression shiftRight(ByteCodeExpression left, ByteCodeExpression right)
    {
        return createArithmeticByteCodeExpression(OpCode.ISHR, left, right);
    }

    public static ByteCodeExpression shiftRightUnsigned(ByteCodeExpression left, ByteCodeExpression right)
    {
        return createArithmeticByteCodeExpression(OpCode.IUSHR, left, right);
    }

    public static ByteCodeExpression negate(ByteCodeExpression value)
    {
        return new NegateByteCodeExpression(value);
    }

    //
    // Comparison operations
    //

    public static ByteCodeExpression lessThan(ByteCodeExpression left, ByteCodeExpression right)
    {
        return ComparisonByteCodeExpression.lessThan(left, right);
    }

    public static ByteCodeExpression greaterThan(ByteCodeExpression left, ByteCodeExpression right)
    {
        return ComparisonByteCodeExpression.greaterThan(left, right);
    }

    public static ByteCodeExpression lessThanOrEqual(ByteCodeExpression left, ByteCodeExpression right)
    {
        return ComparisonByteCodeExpression.lessThanOrEqual(left, right);
    }

    public static ByteCodeExpression greaterThanOrEqual(ByteCodeExpression left, ByteCodeExpression right)
    {
        return ComparisonByteCodeExpression.greaterThanOrEqual(left, right);
    }

    public static ByteCodeExpression equal(ByteCodeExpression left, ByteCodeExpression right)
    {
        return ComparisonByteCodeExpression.equal(left, right);
    }

    public static ByteCodeExpression notEqual(ByteCodeExpression left, ByteCodeExpression right)
    {
        return ComparisonByteCodeExpression.notEqual(left, right);
    }

    //
    // Logical binary operations
    //

    public static ByteCodeExpression and(ByteCodeExpression left, ByteCodeExpression right)
    {
        return new AndByteCodeExpression(left, right);
    }

    public static ByteCodeExpression or(ByteCodeExpression left, ByteCodeExpression right)
    {
        return new OrByteCodeExpression(left, right);
    }

    public static ByteCodeExpression not(ByteCodeExpression value)
    {
        return new NotByteCodeExpression(value);
    }

    //
    // Complex expressions
    //

    public static ByteCodeExpression inlineIf(ByteCodeExpression condition, ByteCodeExpression ifTrue, ByteCodeExpression ifFalse)
    {
        return new InlineIfByteCodeExpression(condition, ifTrue, ifFalse);
    }
}
