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
package com.facebook.presto.common.type;

import com.facebook.presto.common.ConnectorSession;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.function.BlockIndex;
import com.facebook.presto.common.function.BlockPosition;
import com.facebook.presto.common.function.InvocationConvention;
import com.facebook.presto.common.function.InvocationConvention.InvocationArgumentConvention;
import com.facebook.presto.common.function.InvocationConvention.InvocationReturnConvention;
import com.facebook.presto.common.function.IsNull;
import com.facebook.presto.common.function.OperatorMethodHandle;
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.function.ScalarOperator;
import com.facebook.presto.common.function.SqlNullable;

import java.lang.annotation.Annotation;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.invoke.MethodType;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.facebook.presto.common.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static com.facebook.presto.common.function.InvocationConvention.InvocationArgumentConvention.BOXED_NULLABLE;
import static com.facebook.presto.common.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static com.facebook.presto.common.function.InvocationConvention.InvocationArgumentConvention.NULL_FLAG;
import static com.facebook.presto.common.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static com.facebook.presto.common.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static com.facebook.presto.common.function.InvocationConvention.simpleConvention;
import static java.lang.String.format;
import static java.lang.invoke.MethodType.methodType;
import static java.util.Objects.requireNonNull;

public class TypeOperatorDeclaration
{
    public static final TypeOperatorDeclaration NO_TYPE_OPERATOR_DECLARATION = builder(boolean.class).build();

    private final Collection<OperatorMethodHandle> equalOperators;
    private final Collection<OperatorMethodHandle> hashCodeOperators;
    private final Collection<OperatorMethodHandle> xxHash64Operators;
    private final Collection<OperatorMethodHandle> distinctFromOperators;
    private final Collection<OperatorMethodHandle> indeterminateOperators;

    private TypeOperatorDeclaration(
            Collection<OperatorMethodHandle> equalOperators,
            Collection<OperatorMethodHandle> hashCodeOperators,
            Collection<OperatorMethodHandle> xxHash64Operators,
            Collection<OperatorMethodHandle> distinctFromOperators,
            Collection<OperatorMethodHandle> indeterminateOperators)
    {
        this.equalOperators = Collections.unmodifiableList(requireNonNull(equalOperators, "equalOperators is null").stream().collect(Collectors.toList()));
        this.hashCodeOperators = Collections.unmodifiableList(requireNonNull(hashCodeOperators, "hashCodeOperators is null").stream().collect(Collectors.toList()));
        this.xxHash64Operators = Collections.unmodifiableList(requireNonNull(xxHash64Operators, "xxHash64Operators is null").stream().collect(Collectors.toList()));
        this.distinctFromOperators = Collections.unmodifiableList(requireNonNull(distinctFromOperators, "distinctFromOperators is null").stream().collect(Collectors.toList()));
        this.indeterminateOperators = Collections.unmodifiableList(requireNonNull(indeterminateOperators, "indeterminateOperators is null").stream().collect(Collectors.toList()));
    }

    public boolean isComparable()
    {
        return !equalOperators.isEmpty();
    }

    public Collection<OperatorMethodHandle> getEqualOperators()
    {
        return equalOperators;
    }

    public Collection<OperatorMethodHandle> getHashCodeOperators()
    {
        return hashCodeOperators;
    }

    public Collection<OperatorMethodHandle> getXxHash64Operators()
    {
        return xxHash64Operators;
    }

    public Collection<OperatorMethodHandle> getDistinctFromOperators()
    {
        return distinctFromOperators;
    }

    public Collection<OperatorMethodHandle> getIndeterminateOperators()
    {
        return indeterminateOperators;
    }

    public static Builder builder(Class<?> typeJavaType)
    {
        return new Builder(typeJavaType);
    }

    public static TypeOperatorDeclaration extractOperatorDeclaration(Class<?> operatorsClass, Lookup lookup, Class<?> typeJavaType)
    {
        return new Builder(typeJavaType)
                .addOperators(operatorsClass, lookup)
                .build();
    }

    public static class Builder
    {
        private final Class<?> typeJavaType;

        private final Collection<OperatorMethodHandle> equalOperators = new ArrayList<>();
        private final Collection<OperatorMethodHandle> hashCodeOperators = new ArrayList<>();
        private final Collection<OperatorMethodHandle> xxHash64Operators = new ArrayList<>();
        private final Collection<OperatorMethodHandle> distinctFromOperators = new ArrayList<>();
        private final Collection<OperatorMethodHandle> indeterminateOperators = new ArrayList<>();

        public Builder(Class<?> typeJavaType)
        {
            this.typeJavaType = requireNonNull(typeJavaType, "typeJavaType is null");
            checkArgument(!typeJavaType.equals(void.class), "void type is not supported");
        }

        public Builder addEqualOperator(OperatorMethodHandle equalOperator)
        {
            verifyMethodHandleSignature(2, boolean.class, equalOperator);
            this.equalOperators.add(equalOperator);
            return this;
        }

        public Builder addEqualOperators(Collection<OperatorMethodHandle> equalOperators)
        {
            for (OperatorMethodHandle equalOperator : equalOperators) {
                verifyMethodHandleSignature(2, boolean.class, equalOperator);
            }
            this.equalOperators.addAll(equalOperators);
            return this;
        }

        public Builder addHashCodeOperator(OperatorMethodHandle hashCodeOperator)
        {
            verifyMethodHandleSignature(1, long.class, hashCodeOperator);
            this.hashCodeOperators.add(hashCodeOperator);
            return this;
        }

        public Builder addHashCodeOperators(Collection<OperatorMethodHandle> hashCodeOperators)
        {
            for (OperatorMethodHandle hashCodeOperator : hashCodeOperators) {
                verifyMethodHandleSignature(1, long.class, hashCodeOperator);
            }
            this.hashCodeOperators.addAll(hashCodeOperators);
            return this;
        }

        public Builder addXxHash64Operator(OperatorMethodHandle xxHash64Operator)
        {
            verifyMethodHandleSignature(1, long.class, xxHash64Operator);
            this.xxHash64Operators.add(xxHash64Operator);
            return this;
        }

        public Builder addXxHash64Operators(Collection<OperatorMethodHandle> xxHash64Operators)
        {
            for (OperatorMethodHandle xxHash64Operator : xxHash64Operators) {
                verifyMethodHandleSignature(1, long.class, xxHash64Operator);
            }
            this.xxHash64Operators.addAll(xxHash64Operators);
            return this;
        }

        public Builder addDistinctFromOperator(OperatorMethodHandle distinctFromOperator)
        {
            verifyMethodHandleSignature(2, boolean.class, distinctFromOperator);
            this.distinctFromOperators.add(distinctFromOperator);
            return this;
        }

        public Builder addDistinctFromOperators(Collection<OperatorMethodHandle> distinctFromOperators)
        {
            for (OperatorMethodHandle distinctFromOperator : distinctFromOperators) {
                verifyMethodHandleSignature(2, boolean.class, distinctFromOperator);
            }
            this.distinctFromOperators.addAll(distinctFromOperators);
            return this;
        }

        public Builder addIndeterminateOperator(OperatorMethodHandle indeterminateOperator)
        {
            verifyMethodHandleSignature(1, boolean.class, indeterminateOperator);
            this.indeterminateOperators.add(indeterminateOperator);
            return this;
        }

        public Builder addIndeterminateOperators(Collection<OperatorMethodHandle> indeterminateOperators)
        {
            for (OperatorMethodHandle indeterminateOperator : indeterminateOperators) {
                verifyMethodHandleSignature(1, boolean.class, indeterminateOperator);
            }
            this.indeterminateOperators.addAll(indeterminateOperators);
            return this;
        }

        public Builder addOperators(Class<?> operatorsClass, Lookup lookup)
        {
            boolean addedOperator = false;
            for (Method method : operatorsClass.getDeclaredMethods()) {
                ScalarOperator scalarOperator = method.getAnnotation(ScalarOperator.class);
                if (scalarOperator == null) {
                    continue;
                }
                OperatorType operatorType = scalarOperator.value();

                MethodHandle methodHandle;
                try {
                    methodHandle = lookup.unreflect(method);
                }
                catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }

                switch (operatorType) {
                    case EQUAL:
                        addEqualOperator(new OperatorMethodHandle(parseInvocationConvention(operatorType, typeJavaType, method, boolean.class), methodHandle));
                        break;
                    case HASH_CODE:
                        addHashCodeOperator(new OperatorMethodHandle(parseInvocationConvention(operatorType, typeJavaType, method, long.class), methodHandle));
                        break;
                    case XX_HASH_64:
                        addXxHash64Operator(new OperatorMethodHandle(parseInvocationConvention(operatorType, typeJavaType, method, long.class), methodHandle));
                        break;
                    case IS_DISTINCT_FROM:
                        addDistinctFromOperator(new OperatorMethodHandle(parseInvocationConvention(operatorType, typeJavaType, method, boolean.class), methodHandle));
                        break;
                    case INDETERMINATE:
                        addIndeterminateOperator(new OperatorMethodHandle(parseInvocationConvention(operatorType, typeJavaType, method, boolean.class), methodHandle));
                        break;
                    default:
                        throw new IllegalArgumentException(operatorType + " operator is not supported: " + method);
                }
                addedOperator = true;
            }
            if (!addedOperator) {
                throw new IllegalArgumentException(operatorsClass + " does not contain any operators");
            }
            return this;
        }

        private void verifyMethodHandleSignature(int expectedArgumentCount, Class<?> returnJavaType, OperatorMethodHandle operatorMethodHandle)
        {
            MethodType methodType = operatorMethodHandle.getMethodHandle().type();
            InvocationConvention convention = operatorMethodHandle.getCallingConvention();

            checkArgument(convention.getArgumentConventions().size() == expectedArgumentCount,
                    "Expected %s arguments, but got %s", expectedArgumentCount, convention.getArgumentConventions().size());

            checkArgument(methodType.parameterList().stream().noneMatch(ConnectorSession.class::equals),
                    "Session is not supported in type operators");

            int expectedParameterCount = convention.getArgumentConventions().stream()
                    .mapToInt(InvocationConvention.InvocationArgumentConvention::getParameterCount)
                    .sum();
            checkArgument(expectedParameterCount == methodType.parameterCount(),
                    "Expected %s method parameters, but got %s", expectedParameterCount, methodType.parameterCount());

            int parameterIndex = 0;
            for (InvocationArgumentConvention argumentConvention : convention.getArgumentConventions()) {
                Class<?> parameterType = methodType.parameterType(parameterIndex);
                checkArgument(!parameterType.equals(ConnectorSession.class), "Session is not supported in type operators");
                switch (argumentConvention) {
                    case NEVER_NULL:
                        checkArgument(parameterType.isAssignableFrom(typeJavaType),
                                "Expected argument type to be %s, but is %s", typeJavaType, parameterType);
                        break;
                    case NULL_FLAG:
                        checkArgument(parameterType.isAssignableFrom(typeJavaType),
                                "Expected argument type to be %s, but is %s", typeJavaType, parameterType);
                        checkArgument(methodType.parameterType(parameterIndex + 1).equals(boolean.class),
                                "Expected null flag parameter to be followed by a boolean parameter");
                        break;
                    case BOXED_NULLABLE:
                        checkArgument(parameterType.isAssignableFrom(wrap(typeJavaType)),
                                "Expected argument type to be %s, but is %s", wrap(typeJavaType), parameterType);
                        break;
                    case BLOCK_POSITION:
                        checkArgument(parameterType.equals(Block.class) && methodType.parameterType(parameterIndex + 1).equals(int.class),
                                "Expected BLOCK_POSITION argument have parameters Block and int");
                        break;
                    case FUNCTION:
                        throw new IllegalArgumentException("Function argument convention is not supported in type operators");
                    default:
                        throw new UnsupportedOperationException("Unknown argument convention: " + argumentConvention);
                }
                parameterIndex += argumentConvention.getParameterCount();
            }

            InvocationReturnConvention returnConvention = convention.getReturnConvention();
            switch (returnConvention) {
                case FAIL_ON_NULL:
                    checkArgument(methodType.returnType().equals(returnJavaType),
                            "Expected return type to be %s, but is %s", returnJavaType, methodType.returnType());
                    break;
                case NULLABLE_RETURN:
                    checkArgument(methodType.returnType().equals(wrap(returnJavaType)),
                            "Expected return type to be %s, but is %s", returnJavaType, wrap(methodType.returnType()));
                    break;
                default:
                    throw new UnsupportedOperationException("Unknown return convention: " + returnConvention);
            }
        }

        private static InvocationConvention parseInvocationConvention(OperatorType operatorType, Class<?> typeJavaType, Method method, Class<?> expectedReturnType)
        {
            checkArgument(expectedReturnType.isPrimitive(), "Expected return type must be a primitive: " + expectedReturnType);

            InvocationConvention.InvocationReturnConvention returnConvention = getReturnConvention(expectedReturnType, operatorType, method);

            List<Class<?>> parameterTypes = Collections.unmodifiableList(Arrays.asList(method.getParameterTypes()));
            List<Annotation[]> parameterAnnotations = Collections.unmodifiableList(Arrays.asList(method.getParameterAnnotations()));

            InvocationArgumentConvention leftArgumentConvention = extractNextArgumentConvention(typeJavaType, parameterTypes, parameterAnnotations, operatorType, method);
            if (leftArgumentConvention.getParameterCount() == parameterTypes.size()) {
                return simpleConvention(returnConvention, leftArgumentConvention);
            }

            InvocationArgumentConvention rightArgumentConvention = extractNextArgumentConvention(
                    typeJavaType,
                    parameterTypes.subList(leftArgumentConvention.getParameterCount(), parameterTypes.size()),
                    parameterAnnotations.subList(leftArgumentConvention.getParameterCount(), parameterTypes.size()),
                    operatorType,
                    method);

            checkArgument(leftArgumentConvention.getParameterCount() + rightArgumentConvention.getParameterCount() == parameterTypes.size(),
                    "Unexpected parameters for %s operator: %s", operatorType, method);

            return simpleConvention(returnConvention, leftArgumentConvention, rightArgumentConvention);
        }

        private static boolean isAnnotationPresent(Annotation[] annotations, Class<? extends Annotation> annotationType)
        {
            return Arrays.stream(annotations).anyMatch(annotationType::isInstance);
        }

        private static InvocationConvention.InvocationReturnConvention getReturnConvention(Class<?> expectedReturnType, OperatorType operatorType, Method method)
        {
            InvocationConvention.InvocationReturnConvention returnConvention;
            if (!method.isAnnotationPresent(SqlNullable.class) && method.getReturnType().equals(expectedReturnType)) {
                returnConvention = FAIL_ON_NULL;
            }
            else if (method.isAnnotationPresent(SqlNullable.class) && method.getReturnType().equals(wrap(expectedReturnType))) {
                returnConvention = NULLABLE_RETURN;
            }
            else {
                throw new IllegalArgumentException(format("Expected %s operator to return %s: %s", operatorType, expectedReturnType, method));
            }
            return returnConvention;
        }

        private static InvocationConvention.InvocationArgumentConvention extractNextArgumentConvention(
                Class<?> typeJavaType,
                List<Class<?>> parameterTypes,
                List<Annotation[]> parameterAnnotations,
                OperatorType operatorType,
                Method method)
        {
            if (isAnnotationPresent(parameterAnnotations.get(0), SqlNullable.class)) {
                if (parameterTypes.get(0).equals(wrap(typeJavaType))) {
                    return BOXED_NULLABLE;
                }
            }
            else if (isAnnotationPresent(parameterAnnotations.get(0), BlockPosition.class)) {
                if (parameterTypes.size() > 1 &&
                        isAnnotationPresent(parameterAnnotations.get(1), BlockIndex.class) &&
                        parameterTypes.get(0).equals(Block.class) &&
                        parameterTypes.get(1).equals(int.class)) {
                    return BLOCK_POSITION;
                }
            }
            else if (parameterTypes.size() > 1 && isAnnotationPresent(parameterAnnotations.get(1), IsNull.class)) {
                if (parameterTypes.size() > 1 &&
                        parameterTypes.get(0).equals(typeJavaType) &&
                        parameterTypes.get(1).equals(boolean.class)) {
                    return NULL_FLAG;
                }
            }
            else {
                if (parameterTypes.get(0).equals(typeJavaType)) {
                    return NEVER_NULL;
                }
            }
            throw new IllegalArgumentException(format("Unexpected parameters for %s operator: %s", operatorType, method));
        }

        private static void checkArgument(boolean test, String message, Object... arguments)
        {
            if (!test) {
                throw new IllegalArgumentException(format(message, arguments));
            }
        }

        private static Class<?> wrap(Class<?> type)
        {
            return methodType(type).wrap().returnType();
        }

        public TypeOperatorDeclaration build()
        {
            if (equalOperators.isEmpty()) {
                if (!hashCodeOperators.isEmpty()) {
                    throw new IllegalStateException("Hash code operators can not be supplied when equal operators are not supplied");
                }
                if (!xxHash64Operators.isEmpty()) {
                    throw new IllegalStateException("xxHash64 operators can not be supplied when equal operators are not supplied");
                }
            }
            else {
                if (xxHash64Operators.isEmpty()) {
                    throw new IllegalStateException("xxHash64 operators must be supplied when equal operators are supplied");
                }
            }

            return new TypeOperatorDeclaration(
                    equalOperators,
                    hashCodeOperators,
                    xxHash64Operators,
                    distinctFromOperators,
                    indeterminateOperators);
        }
    }
}
