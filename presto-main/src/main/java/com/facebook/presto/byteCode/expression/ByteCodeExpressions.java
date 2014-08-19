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
import com.facebook.presto.byteCode.OpCode;
import com.facebook.presto.byteCode.ParameterizedType;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodType;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;

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
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;

public final class ByteCodeExpressions
{
    private ByteCodeExpressions()
    {
    }

    //
    // Constants
    //

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
    // Invoke static method
    //

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
                ImmutableList.copyOf(transform(checkNotNull(parameters, "parameters is null"), ByteCodeExpression.typeGetter())),
                parameters);
    }

    //
    // Invoke dynamic
    //

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
}
