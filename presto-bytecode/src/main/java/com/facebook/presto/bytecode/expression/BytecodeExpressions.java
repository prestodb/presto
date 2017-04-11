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

import com.facebook.presto.bytecode.FieldDefinition;
import com.facebook.presto.bytecode.MethodDefinition;
import com.facebook.presto.bytecode.OpCode;
import com.facebook.presto.bytecode.ParameterizedType;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodType;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

import static com.facebook.presto.bytecode.ParameterizedType.type;
import static com.facebook.presto.bytecode.expression.ArithmeticBytecodeExpression.createArithmeticBytecodeExpression;
import static com.facebook.presto.bytecode.instruction.Constant.loadBoolean;
import static com.facebook.presto.bytecode.instruction.Constant.loadClass;
import static com.facebook.presto.bytecode.instruction.Constant.loadDouble;
import static com.facebook.presto.bytecode.instruction.Constant.loadFloat;
import static com.facebook.presto.bytecode.instruction.Constant.loadInt;
import static com.facebook.presto.bytecode.instruction.Constant.loadLong;
import static com.facebook.presto.bytecode.instruction.Constant.loadNull;
import static com.facebook.presto.bytecode.instruction.Constant.loadString;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.transform;
import static java.util.Objects.requireNonNull;

public final class BytecodeExpressions
{
    private BytecodeExpressions()
    {
    }

    //
    // Constants
    //

    public static BytecodeExpression constantTrue()
    {
        return new ConstantBytecodeExpression(boolean.class, loadBoolean(true));
    }

    public static BytecodeExpression constantFalse()
    {
        return new ConstantBytecodeExpression(boolean.class, loadBoolean(false));
    }

    public static BytecodeExpression constantBoolean(boolean value)
    {
        return new ConstantBytecodeExpression(boolean.class, loadBoolean(value));
    }

    public static BytecodeExpression constantClass(Class<?> value)
    {
        return new ConstantBytecodeExpression(Class.class, loadClass(value));
    }

    public static BytecodeExpression constantClass(ParameterizedType value)
    {
        return new ConstantBytecodeExpression(Class.class, loadClass(value));
    }

    public static BytecodeExpression constantDouble(double value)
    {
        return new ConstantBytecodeExpression(double.class, loadDouble(value));
    }

    public static BytecodeExpression constantFloat(float value)
    {
        return new ConstantBytecodeExpression(float.class, loadFloat(value));
    }

    public static BytecodeExpression constantInt(int value)
    {
        return new ConstantBytecodeExpression(int.class, loadInt(value));
    }

    public static BytecodeExpression constantLong(long value)
    {
        return new ConstantBytecodeExpression(long.class, loadLong(value));
    }

    public static BytecodeExpression constantNumber(Number value)
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

    public static BytecodeExpression constantNull(Class<?> type)
    {
        return new ConstantBytecodeExpression(type, loadNull());
    }

    public static BytecodeExpression constantNull(ParameterizedType type)
    {
        return new ConstantBytecodeExpression(type, loadNull());
    }

    public static BytecodeExpression constantString(String value)
    {
        return new ConstantBytecodeExpression(String.class, loadString(value));
    }

    public static BytecodeExpression defaultValue(ParameterizedType type)
    {
        if (type.isPrimitive()) {
            return defaultValue(type.getPrimitiveType());
        }
        return constantNull(type);
    }

    public static BytecodeExpression defaultValue(Class<?> type)
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

    public static BytecodeExpression getStatic(Class<?> declaringClass, String name)
    {
        return new GetFieldBytecodeExpression(null, declaringClass, name);
    }

    public static BytecodeExpression getStatic(Field staticField)
    {
        return new GetFieldBytecodeExpression(null, staticField);
    }

    public static BytecodeExpression getStatic(FieldDefinition staticField)
    {
        return new GetFieldBytecodeExpression(null, staticField);
    }

    public static BytecodeExpression getStatic(ParameterizedType declaringClass, String name, ParameterizedType type)
    {
        return new GetFieldBytecodeExpression(null, declaringClass, name, type);
    }

    //
    // Set static field
    //

    public static BytecodeExpression setStatic(Class<?> declaringClass, String name, BytecodeExpression value)
    {
        return new SetFieldBytecodeExpression(null, declaringClass, name, value);
    }

    public static BytecodeExpression setStatic(Field staticField, BytecodeExpression value)
    {
        return new SetFieldBytecodeExpression(null, staticField, value);
    }

    public static BytecodeExpression setStatic(FieldDefinition staticField, BytecodeExpression value)
    {
        return new SetFieldBytecodeExpression(null, staticField, value);
    }

    public static BytecodeExpression setStatic(ParameterizedType declaringClass, String name, BytecodeExpression value)
    {
        return new SetFieldBytecodeExpression(null, declaringClass, name, value);
    }

    //
    // New instance
    //

    public static BytecodeExpression newInstance(Class<?> returnType, BytecodeExpression... parameters)
    {
        return newInstance(type(returnType), ImmutableList.copyOf(requireNonNull(parameters, "parameters is null")));
    }

    public static BytecodeExpression newInstance(Class<?> returnType, Iterable<? extends BytecodeExpression> parameters)
    {
        return newInstance(type(returnType), parameters);
    }

    public static BytecodeExpression newInstance(ParameterizedType returnType, BytecodeExpression... parameters)
    {
        requireNonNull(parameters, "parameters is null");

        return newInstance(returnType, ImmutableList.copyOf(parameters));
    }

    public static BytecodeExpression newInstance(ParameterizedType returnType, Iterable<? extends BytecodeExpression> parameters)
    {
        requireNonNull(parameters, "parameters is null");

        return newInstance(
                returnType,
                ImmutableList.copyOf(transform(parameters, BytecodeExpression::getType)),
                parameters);
    }

    public static BytecodeExpression newInstance(Class<?> returnType, Iterable<? extends Class<?>> parameterTypes, BytecodeExpression... parameters)
    {
        return newInstance(type(returnType), transform(parameterTypes, ParameterizedType::type), ImmutableList.copyOf(requireNonNull(parameters, "parameters is null")));
    }

    public static BytecodeExpression newInstance(ParameterizedType returnType, Iterable<ParameterizedType> parameterTypes, BytecodeExpression... parameters)
    {
        return newInstance(returnType, parameterTypes, ImmutableList.copyOf(requireNonNull(parameters, "parameters is null")));
    }

    public static BytecodeExpression newInstance(
            ParameterizedType type,
            Iterable<ParameterizedType> parameterTypes,
            Iterable<? extends BytecodeExpression> parameters)
    {
        return new NewInstanceBytecodeExpression(type, parameterTypes, parameters);
    }

    //
    // Array
    //
    public static BytecodeExpression newArray(ParameterizedType type, int length)
    {
        return new NewArrayBytecodeExpression(type, length);
    }

    public static BytecodeExpression newArray(ParameterizedType type, BytecodeExpression length)
    {
        return new NewArrayBytecodeExpression(type, length);
    }

    public static BytecodeExpression newArray(ParameterizedType type, Iterable<? extends BytecodeExpression> elements)
    {
        return new NewArrayBytecodeExpression(type, ImmutableList.copyOf(elements));
    }

    public static BytecodeExpression length(BytecodeExpression instance)
    {
        return new ArrayLengthBytecodeExpression(instance);
    }

    public static BytecodeExpression get(BytecodeExpression instance, BytecodeExpression index)
    {
        return new GetElementBytecodeExpression(instance, index);
    }

    public static BytecodeExpression set(BytecodeExpression instance, BytecodeExpression index, BytecodeExpression value)
    {
        return new SetArrayElementBytecodeExpression(instance, index, value);
    }

    //
    // Invoke static method
    //

    public static BytecodeExpression invokeStatic(MethodDefinition method,  BytecodeExpression... parameters)
    {
        return invokeStatic(method.getDeclaringClass().getType(), method.getName(), method.getReturnType(), ImmutableList.copyOf(parameters));
    }

    public static BytecodeExpression invokeStatic(Method method,  BytecodeExpression... parameters)
    {
        return invokeStatic(method, ImmutableList.copyOf(requireNonNull(parameters, "parameters is null")));
    }

    public static BytecodeExpression invokeStatic(Method method,  Iterable<? extends BytecodeExpression> parameters)
    {
        return invokeStatic(method.getDeclaringClass(), method.getName(), method.getReturnType(), parameters);
    }

    public static BytecodeExpression invokeStatic(Class<?> methodTargetType, String methodName, Class<?> returnType, BytecodeExpression... parameters)
    {
        return invokeStatic(methodTargetType, methodName, returnType, ImmutableList.copyOf(requireNonNull(parameters, "parameters is null")));
    }

    public static BytecodeExpression invokeStatic(
            Class<?> methodTargetType,
            String methodName,
            Class<?> returnType,
            Iterable<? extends BytecodeExpression> parameters)
    {
        return invokeStatic(type(methodTargetType), methodName, type(returnType), parameters);
    }

    public static BytecodeExpression invokeStatic(
            ParameterizedType methodTargetType,
            String methodName,
            ParameterizedType returnType,
            Iterable<? extends BytecodeExpression> parameters)
    {
        requireNonNull(methodTargetType, "methodTargetType is null");
        requireNonNull(returnType, "returnType is null");
        requireNonNull(parameters, "parameters is null");

        return invokeStatic(
                methodTargetType,
                methodName,
                returnType,
                ImmutableList.copyOf(transform(parameters, BytecodeExpression::getType)),
                parameters);
    }
    public static BytecodeExpression invokeStatic(
            Class<?> methodTargetType,
            String methodName,
            Class<?> returnType,
            Iterable<? extends Class<?>> parameterTypes,
            BytecodeExpression... parameters)
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

    public static BytecodeExpression invokeStatic(
            ParameterizedType methodTargetType,
            String methodName,
            ParameterizedType returnType,
            Iterable<ParameterizedType> parameterTypes,
            BytecodeExpression... parameters)
    {
        return invokeStatic(methodTargetType, methodName, returnType, parameterTypes, ImmutableList.copyOf(requireNonNull(parameters, "parameters is null")));
    }

    public static BytecodeExpression invokeStatic(
            ParameterizedType methodTargetType,
            String methodName,
            ParameterizedType returnType,
            Iterable<ParameterizedType> parameterTypes,
            Iterable<? extends BytecodeExpression> parameters)
    {
        return new InvokeBytecodeExpression(
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

    public static BytecodeExpression invokeDynamic(
            Method bootstrapMethod,
            Iterable<? extends Object> bootstrapArgs,
            String methodName,
            Class<?> returnType,
            BytecodeExpression... parameters)
    {
        return invokeDynamic(bootstrapMethod, bootstrapArgs, methodName, returnType, ImmutableList.copyOf(requireNonNull(parameters, "parameters is null")));
    }

    public static BytecodeExpression invokeDynamic(
            Method bootstrapMethod,
            Iterable<? extends Object> bootstrapArgs,
            String methodName,
            Class<?> returnType,
            Iterable<? extends BytecodeExpression> parameters)
    {
        requireNonNull(returnType, "returnType is null");
        requireNonNull(parameters, "parameters is null");

        return invokeDynamic(
                bootstrapMethod,
                bootstrapArgs,
                methodName,
                type(returnType),
                ImmutableList.copyOf(transform(parameters, BytecodeExpression::getType)),
                parameters);
    }

    public static BytecodeExpression invokeDynamic(
            Method bootstrapMethod,
            Iterable<? extends Object> bootstrapArgs,
            String methodName,
            ParameterizedType returnType,
            BytecodeExpression... parameters)
    {
        return invokeDynamic(bootstrapMethod, bootstrapArgs, methodName, returnType, ImmutableList.copyOf(requireNonNull(parameters, "parameters is null")));
    }

    public static BytecodeExpression invokeDynamic(
            Method bootstrapMethod,
            Iterable<? extends Object> bootstrapArgs,
            String methodName,
            ParameterizedType returnType,
            Iterable<? extends BytecodeExpression> parameters)
    {
        requireNonNull(returnType, "returnType is null");
        requireNonNull(parameters, "parameters is null");

        return invokeDynamic(
                bootstrapMethod,
                bootstrapArgs,
                methodName,
                returnType,
                ImmutableList.copyOf(transform(parameters, BytecodeExpression::getType)),
                parameters);
    }

    public static BytecodeExpression invokeDynamic(
            Method bootstrapMethod,
            Iterable<? extends Object> bootstrapArgs,
            String methodName,
            MethodType methodType,
            BytecodeExpression... parameters)
    {
        requireNonNull(methodType, "methodType is null");
        requireNonNull(parameters, "parameters is null");

        return invokeDynamic(bootstrapMethod, bootstrapArgs, methodName, methodType, ImmutableList.copyOf(parameters));
    }

    public static BytecodeExpression invokeDynamic(
            Method bootstrapMethod,
            Iterable<? extends Object> bootstrapArgs,
            String methodName,
            MethodType methodType,
            Iterable<? extends BytecodeExpression> parameters)
    {
        return invokeDynamic(
                bootstrapMethod,
                bootstrapArgs,
                methodName,
                type(methodType.returnType()),
                transform(methodType.parameterList(), ParameterizedType::type),
                ImmutableList.copyOf(requireNonNull(parameters, "parameters is null")));
    }

    public static BytecodeExpression invokeDynamic(
            Method bootstrapMethod,
            Iterable<? extends Object> bootstrapArgs,
            String methodName,
            ParameterizedType returnType,
            Iterable<ParameterizedType> parameterTypes,
            Iterable<? extends BytecodeExpression> parameters)
    {
        return new InvokeDynamicBytecodeExpression(
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

    public static BytecodeExpression add(BytecodeExpression left, BytecodeExpression right)
    {
        return createArithmeticBytecodeExpression(OpCode.IADD, left, right);
    }

    public static BytecodeExpression subtract(BytecodeExpression left, BytecodeExpression right)
    {
        return createArithmeticBytecodeExpression(OpCode.ISUB, left, right);
    }

    public static BytecodeExpression multiply(BytecodeExpression left, BytecodeExpression right)
    {
        return createArithmeticBytecodeExpression(OpCode.IMUL, left, right);
    }

    public static BytecodeExpression divide(BytecodeExpression left, BytecodeExpression right)
    {
        return createArithmeticBytecodeExpression(OpCode.IDIV, left, right);
    }

    public static BytecodeExpression remainder(BytecodeExpression left, BytecodeExpression right)
    {
        return createArithmeticBytecodeExpression(OpCode.IREM, left, right);
    }

    public static BytecodeExpression bitwiseAnd(BytecodeExpression left, BytecodeExpression right)
    {
        return createArithmeticBytecodeExpression(OpCode.IAND, left, right);
    }

    public static BytecodeExpression bitwiseOr(BytecodeExpression left, BytecodeExpression right)
    {
        return createArithmeticBytecodeExpression(OpCode.IOR, left, right);
    }

    public static BytecodeExpression bitwiseXor(BytecodeExpression left, BytecodeExpression right)
    {
        return createArithmeticBytecodeExpression(OpCode.IXOR, left, right);
    }

    public static BytecodeExpression shiftLeft(BytecodeExpression left, BytecodeExpression right)
    {
        return createArithmeticBytecodeExpression(OpCode.ISHL, left, right);
    }

    public static BytecodeExpression shiftRight(BytecodeExpression left, BytecodeExpression right)
    {
        return createArithmeticBytecodeExpression(OpCode.ISHR, left, right);
    }

    public static BytecodeExpression shiftRightUnsigned(BytecodeExpression left, BytecodeExpression right)
    {
        return createArithmeticBytecodeExpression(OpCode.IUSHR, left, right);
    }

    public static BytecodeExpression negate(BytecodeExpression value)
    {
        return new NegateBytecodeExpression(value);
    }

    //
    // Comparison operations
    //

    public static BytecodeExpression lessThan(BytecodeExpression left, BytecodeExpression right)
    {
        return ComparisonBytecodeExpression.lessThan(left, right);
    }

    public static BytecodeExpression greaterThan(BytecodeExpression left, BytecodeExpression right)
    {
        return ComparisonBytecodeExpression.greaterThan(left, right);
    }

    public static BytecodeExpression lessThanOrEqual(BytecodeExpression left, BytecodeExpression right)
    {
        return ComparisonBytecodeExpression.lessThanOrEqual(left, right);
    }

    public static BytecodeExpression greaterThanOrEqual(BytecodeExpression left, BytecodeExpression right)
    {
        return ComparisonBytecodeExpression.greaterThanOrEqual(left, right);
    }

    public static BytecodeExpression equal(BytecodeExpression left, BytecodeExpression right)
    {
        return ComparisonBytecodeExpression.equal(left, right);
    }

    public static BytecodeExpression notEqual(BytecodeExpression left, BytecodeExpression right)
    {
        return ComparisonBytecodeExpression.notEqual(left, right);
    }

    //
    // Logical binary operations
    //

    public static BytecodeExpression and(BytecodeExpression left, BytecodeExpression right)
    {
        return new AndBytecodeExpression(left, right);
    }

    public static BytecodeExpression or(BytecodeExpression left, BytecodeExpression right)
    {
        return new OrBytecodeExpression(left, right);
    }

    public static BytecodeExpression not(BytecodeExpression value)
    {
        return new NotBytecodeExpression(value);
    }

    //
    // Complex expressions
    //

    public static BytecodeExpression inlineIf(BytecodeExpression condition, BytecodeExpression ifTrue, BytecodeExpression ifFalse)
    {
        return new InlineIfBytecodeExpression(condition, ifTrue, ifFalse);
    }

    //
    // Print
    //
    public static BytecodeExpression print(BytecodeExpression variable)
    {
        BytecodeExpression out = getStatic(System.class, "out");
        return out.invoke("println", void.class, variable);
    }
}
