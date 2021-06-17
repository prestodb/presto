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

import com.facebook.presto.common.PrestoException;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.function.InvocationConvention;
import com.facebook.presto.common.function.InvocationConvention.InvocationArgumentConvention;
import com.facebook.presto.common.function.OperatorMethodHandle;
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.function.ScalarFunctionAdapter;
import com.facebook.presto.common.function.ScalarFunctionAdapter.NullAdaptationPolicy;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.invoke.MethodType;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.facebook.presto.common.StandardErrorCode.FUNCTION_NOT_FOUND;
import static com.facebook.presto.common.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static com.facebook.presto.common.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static com.facebook.presto.common.function.InvocationConvention.InvocationArgumentConvention.NULL_FLAG;
import static com.facebook.presto.common.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static com.facebook.presto.common.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static java.lang.invoke.MethodHandles.dropArguments;
import static java.lang.invoke.MethodHandles.filterReturnValue;
import static java.lang.invoke.MethodHandles.guardWithTest;
import static java.lang.invoke.MethodHandles.lookup;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public class TypeOperators
{
    private final ScalarFunctionAdapter functionAdapter = new ScalarFunctionAdapter(NullAdaptationPolicy.UNSUPPORTED);
    private final ConcurrentMap<OperatorConvention, OperatorAdaptor> cache = new ConcurrentHashMap<>();

    public MethodHandle getEqualOperator(Type type, InvocationConvention callingConvention)
    {
        if (!type.isComparable()) {
            throw new UnsupportedOperationException(type + " is not comparable");
        }
        return getOperatorAdaptor(type, callingConvention, OperatorType.EQUAL).get();
    }

    public MethodHandle getHashCodeOperator(Type type, InvocationConvention callingConvention)
    {
        if (!type.isComparable()) {
            throw new UnsupportedOperationException(type + " is not comparable");
        }
        return getOperatorAdaptor(type, callingConvention, OperatorType.HASH_CODE).get();
    }

    public MethodHandle getXxHash64Operator(Type type, InvocationConvention callingConvention)
    {
        if (!type.isComparable()) {
            throw new UnsupportedOperationException(type + " is not comparable");
        }
        return getOperatorAdaptor(type, callingConvention, OperatorType.XX_HASH_64).get();
    }

    public MethodHandle getDistinctFromOperator(Type type, InvocationConvention callingConvention)
    {
        if (!type.isComparable()) {
            throw new UnsupportedOperationException(type + " is not comparable");
        }
        return getOperatorAdaptor(type, callingConvention, OperatorType.IS_DISTINCT_FROM).get();
    }

    public MethodHandle getIndeterminateOperator(Type type, InvocationConvention callingConvention)
    {
        if (!type.isComparable()) {
            throw new UnsupportedOperationException(type + " is not comparable");
        }
        return getOperatorAdaptor(type, callingConvention, OperatorType.INDETERMINATE).get();
    }

    private OperatorAdaptor getOperatorAdaptor(Type type, InvocationConvention callingConvention, OperatorType operatorType)
    {
        return cache.computeIfAbsent(
                new OperatorConvention(type, operatorType, callingConvention),
                operatorConvention -> new OperatorAdaptor(functionAdapter, operatorConvention));
    }

    private class OperatorAdaptor
    {
        private final ScalarFunctionAdapter functionAdapter;
        private final OperatorConvention operatorConvention;
        private MethodHandle adapted;

        public OperatorAdaptor(ScalarFunctionAdapter functionAdapter, OperatorConvention operatorConvention)
        {
            this.functionAdapter = functionAdapter;
            this.operatorConvention = operatorConvention;
        }

        public synchronized MethodHandle get()
        {
            if (adapted == null) {
                adapted = adaptOperator(operatorConvention);
            }
            return adapted;
        }

        private MethodHandle adaptOperator(OperatorConvention operatorConvention)
        {
            OperatorMethodHandle operatorMethodHandle = selectOperatorMethodHandleToAdapt(operatorConvention);
            MethodHandle methodHandle = adaptOperator(operatorConvention, operatorMethodHandle);
            return methodHandle;
        }

        private MethodHandle adaptOperator(OperatorConvention operatorConvention, OperatorMethodHandle operatorMethodHandle)
        {
            return functionAdapter.adapt(
                    operatorMethodHandle.getMethodHandle(),
                    getOperatorArgumentTypes(operatorConvention),
                    operatorMethodHandle.getCallingConvention(),
                    operatorConvention.getCallingConvention());
        }

        private OperatorMethodHandle selectOperatorMethodHandleToAdapt(OperatorConvention operatorConvention)
        {
            List<OperatorMethodHandle> operatorMethodHandles = Collections.unmodifiableList(getOperatorMethodHandles(operatorConvention).stream()
                    .sorted(Comparator.comparing(TypeOperators::getScore).reversed())
                    .collect(toList()));

            for (OperatorMethodHandle operatorMethodHandle : operatorMethodHandles) {
                if (functionAdapter.canAdapt(operatorMethodHandle.getCallingConvention(), operatorConvention.getCallingConvention())) {
                    return operatorMethodHandle;
                }
            }

            throw new PrestoException(FUNCTION_NOT_FOUND, String.format(
                    "%s %s operator can not be adapted to convention (%s). Available implementations: %s",
                    operatorConvention.getType(),
                    operatorConvention.getOperatorType(),
                    operatorConvention.getCallingConvention(),
                    operatorMethodHandles.stream()
                            .map(OperatorMethodHandle::getCallingConvention)
                            .map(Object::toString)
                            .collect(joining(", ", "[", "]"))));
        }

        private Collection<OperatorMethodHandle> getOperatorMethodHandles(OperatorConvention operatorConvention)
        {
            TypeOperatorDeclaration typeOperatorDeclaration = operatorConvention.getType().getTypeOperatorDeclaration(TypeOperators.this);
            requireNonNull(typeOperatorDeclaration, "typeOperators is null for " + operatorConvention.getType());
            switch (operatorConvention.getOperatorType()) {
                case EQUAL:
                    return typeOperatorDeclaration.getEqualOperators();
                case HASH_CODE:
                    Collection<OperatorMethodHandle> hashCodeOperators = typeOperatorDeclaration.getHashCodeOperators();
                    if (hashCodeOperators.isEmpty()) {
                        return typeOperatorDeclaration.getXxHash64Operators();
                    }
                    return hashCodeOperators;
                case XX_HASH_64:
                    return typeOperatorDeclaration.getXxHash64Operators();
                case IS_DISTINCT_FROM:
                    Collection<OperatorMethodHandle> distinctFromOperators = typeOperatorDeclaration.getDistinctFromOperators();
                    if (distinctFromOperators.isEmpty()) {
                        return Collections.unmodifiableList(Arrays.asList(generateDistinctFromOperator(operatorConvention)));
                    }
                    return distinctFromOperators;
                case INDETERMINATE:
                    Collection<OperatorMethodHandle> indeterminateOperators = typeOperatorDeclaration.getIndeterminateOperators();
                    if (indeterminateOperators.isEmpty()) {
                        return Collections.unmodifiableList(Arrays.asList(defaultIndeterminateOperator(operatorConvention.getType().getJavaType())));
                    }
                    return indeterminateOperators;
            }
            throw new IllegalArgumentException("Unsupported operator type: " + operatorConvention.getOperatorType());
        }

        private OperatorMethodHandle generateDistinctFromOperator(OperatorConvention operatorConvention)
        {
            if (operatorConvention.getCallingConvention().getArgumentConventions().equals(Collections.unmodifiableList(Arrays.asList(BLOCK_POSITION, BLOCK_POSITION)))) {
                OperatorConvention equalOperator = new OperatorConvention(operatorConvention.getType(), OperatorType.EQUAL, InvocationConvention.simpleConvention(NULLABLE_RETURN, BLOCK_POSITION, BLOCK_POSITION));
                MethodHandle equalMethodHandle = adaptOperator(equalOperator);
                return adaptBlockPositionEqualToDistinctFrom(equalMethodHandle);
            }

            OperatorConvention equalOperator = new OperatorConvention(operatorConvention.getType(), OperatorType.EQUAL, InvocationConvention.simpleConvention(NULLABLE_RETURN, NEVER_NULL, NEVER_NULL));
            MethodHandle equalMethodHandle = adaptOperator(equalOperator);
            return adaptNeverNullEqualToDistinctFrom(equalMethodHandle);
        }

        private List<Type> getOperatorArgumentTypes(OperatorConvention operatorConvention)
        {
            switch (operatorConvention.getOperatorType()) {
                case EQUAL:
                case IS_DISTINCT_FROM:
                    return Collections.unmodifiableList(Arrays.asList(operatorConvention.getType(), operatorConvention.getType()));
                case HASH_CODE:
                case XX_HASH_64:
                case INDETERMINATE:
                    return Collections.unmodifiableList(Arrays.asList(operatorConvention.getType()));
            }
            throw new IllegalArgumentException("Unsupported operator type: " + operatorConvention.getOperatorType());
        }
    }

    private static int getScore(OperatorMethodHandle operatorMethodHandle)
    {
        int score = 0;
        for (InvocationArgumentConvention argument : operatorMethodHandle.getCallingConvention().getArgumentConventions()) {
            if (argument == NULL_FLAG) {
                score += 1000;
            }
            else if (argument == BLOCK_POSITION) {
                score += 1;
            }
        }
        return score;
    }

    private static final class OperatorConvention
    {
        private final Type type;
        private final OperatorType operatorType;
        private final InvocationConvention callingConvention;

        public OperatorConvention(Type type, OperatorType operatorType, InvocationConvention callingConvention)
        {
            this.type = requireNonNull(type, "type is null");
            this.operatorType = requireNonNull(operatorType, "operatorType is null");
            this.callingConvention = requireNonNull(callingConvention, "callingConvention is null");
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            OperatorConvention operatorConvention = (OperatorConvention) o;
            return type.equals(operatorConvention.type) &&
                    operatorType == operatorConvention.operatorType &&
                    callingConvention.equals(operatorConvention.callingConvention);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(type, operatorType, callingConvention);
        }

        @Override
        public String toString()
        {
            return new StringJoiner(", ", OperatorConvention.class.getSimpleName() + "[", "]")
                    .add("type=" + type)
                    .add("operatorType=" + operatorType)
                    .add("callingConvention=" + callingConvention)
                    .toString();
        }

        public Type getType()
        {
            return type;
        }

        public OperatorType getOperatorType()
        {
            return operatorType;
        }

        public InvocationConvention getCallingConvention()
        {
            return callingConvention;
        }
    }

    private static final MethodHandle BLOCK_POSITION_DISTINCT_FROM;
    private static final MethodHandle LOGICAL_OR;
    private static final MethodHandle LOGICAL_XOR;
    private static final MethodHandle NOT_EQUAL;

    static {
        try {
            Lookup lookup = lookup();
            BLOCK_POSITION_DISTINCT_FROM = lookup.findStatic(
                    TypeOperators.class,
                    "genericBlockPositionDistinctFrom",
                    MethodType.methodType(boolean.class, MethodHandle.class, Block.class, int.class, Block.class, int.class));
            LOGICAL_OR = lookup.findStatic(Boolean.class, "logicalOr", MethodType.methodType(boolean.class, boolean.class, boolean.class));
            LOGICAL_XOR = lookup.findStatic(Boolean.class, "logicalXor", MethodType.methodType(boolean.class, boolean.class, boolean.class));
            NOT_EQUAL = lookup.findStatic(TypeOperators.class, "notEqual", MethodType.methodType(boolean.class, Boolean.class));
        }
        catch (NoSuchMethodException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private static OperatorMethodHandle adaptBlockPositionEqualToDistinctFrom(MethodHandle blockPositionEqual)
    {
        return new OperatorMethodHandle(
                InvocationConvention.simpleConvention(FAIL_ON_NULL, BLOCK_POSITION, BLOCK_POSITION),
                BLOCK_POSITION_DISTINCT_FROM.bindTo(blockPositionEqual));
    }

    private static boolean genericBlockPositionDistinctFrom(MethodHandle equalOperator, Block left, int leftPosition, Block right, int rightPosition)
            throws Throwable
    {
        boolean leftIsNull = left.isNull(leftPosition);
        boolean rightIsNull = right.isNull(rightPosition);
        if (leftIsNull || rightIsNull) {
            return leftIsNull != rightIsNull;
        }
        return notEqual((Boolean) equalOperator.invokeExact(left, leftPosition, right, rightPosition));
    }

    private static OperatorMethodHandle adaptNeverNullEqualToDistinctFrom(MethodHandle neverNullEqual)
    {
        // boolean distinctFrom(T left, boolean leftIsNull, T right, boolean rightIsNull)
        // {
        //     if (leftIsNull || rightIsNull) {
        //         return leftIsNull ^ rightIsNull;
        //     }
        //     return notEqual(equalOperator.invokeExact(left, leftIsNull, right, rightIsNull));
        // }
        MethodHandle eitherArgIsNull = LOGICAL_OR;
        eitherArgIsNull = dropArguments(eitherArgIsNull, 0, neverNullEqual.type().parameterType(0));
        eitherArgIsNull = dropArguments(eitherArgIsNull, 2, neverNullEqual.type().parameterType(1));

        MethodHandle distinctNullValues = LOGICAL_XOR;
        distinctNullValues = dropArguments(distinctNullValues, 0, neverNullEqual.type().parameterType(0));
        distinctNullValues = dropArguments(distinctNullValues, 2, neverNullEqual.type().parameterType(1));

        MethodHandle notEqual = filterReturnValue(neverNullEqual, NOT_EQUAL);
        notEqual = dropArguments(notEqual, 1, boolean.class);
        notEqual = dropArguments(notEqual, 3, boolean.class);

        return new OperatorMethodHandle(
                InvocationConvention.simpleConvention(FAIL_ON_NULL, NULL_FLAG, NULL_FLAG),
                guardWithTest(eitherArgIsNull, distinctNullValues, notEqual));
    }

    private static OperatorMethodHandle defaultIndeterminateOperator(Class<?> javaType)
    {
        // boolean distinctFrom(T value, boolean valueIsNull)
        // {
        //     return valueIsNull;
        // }
        MethodHandle methodHandle = MethodHandles.identity(boolean.class);
        methodHandle = dropArguments(methodHandle, 0, javaType);
        return new OperatorMethodHandle(InvocationConvention.simpleConvention(FAIL_ON_NULL, NULL_FLAG), methodHandle);
    }

    private static boolean notEqual(Boolean equal)
    {
        return !requireNonNull(equal, "equal returned null");
    }
}
