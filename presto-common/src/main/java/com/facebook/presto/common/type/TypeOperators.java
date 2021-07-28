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
import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.common.function.InvocationConvention;
import com.facebook.presto.common.function.InvocationConvention.InvocationArgumentConvention;
import com.facebook.presto.common.function.InvocationConvention.InvocationReturnConvention;
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
import java.util.Optional;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import static com.facebook.presto.common.StandardErrorCode.FUNCTION_NOT_FOUND;
import static com.facebook.presto.common.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static com.facebook.presto.common.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static com.facebook.presto.common.function.InvocationConvention.InvocationArgumentConvention.NULL_FLAG;
import static com.facebook.presto.common.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static com.facebook.presto.common.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static com.facebook.presto.common.function.InvocationConvention.simpleConvention;
import static com.facebook.presto.common.function.OperatorType.COMPARISON;
import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.common.function.OperatorType.LESS_THAN;
import static com.facebook.presto.common.function.OperatorType.LESS_THAN_OR_EQUAL;
import static java.lang.invoke.MethodHandles.collectArguments;
import static java.lang.invoke.MethodHandles.dropArguments;
import static java.lang.invoke.MethodHandles.filterReturnValue;
import static java.lang.invoke.MethodHandles.guardWithTest;
import static java.lang.invoke.MethodHandles.lookup;
import static java.lang.invoke.MethodHandles.permuteArguments;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public class TypeOperators
{
    private final ScalarFunctionAdapter functionAdapter = new ScalarFunctionAdapter(NullAdaptationPolicy.UNSUPPORTED);
    private final BiFunction<Object, Supplier<Object>, Object> cache;

    public TypeOperators()
    {
        ConcurrentHashMap<Object, Object> cache = new ConcurrentHashMap<>();
        this.cache = (operatorConvention, supplier) -> cache.computeIfAbsent(operatorConvention, key -> supplier.get());
    }

    public TypeOperators(BiFunction<Object, Supplier<Object>, Object> cache)
    {
        this.cache = cache;
    }

    public MethodHandle getEqualOperator(Type type, InvocationConvention callingConvention)
    {
        if (!type.isComparable()) {
            throw new UnsupportedOperationException(type + " is not comparable");
        }
        return getOperatorAdaptor(type, callingConvention, EQUAL).get();
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

    public MethodHandle getComparisonOperator(Type type, InvocationConvention callingConvention)
    {
        if (!type.isOrderable()) {
            throw new UnsupportedOperationException(type + " is not orderable");
        }
        return getOperatorAdaptor(type, callingConvention, COMPARISON).get();
    }

    public MethodHandle getOrderingOperator(Type type, SortOrder sortOrder, InvocationConvention callingConvention)
    {
        if (!type.isOrderable()) {
            throw new UnsupportedOperationException(type + " is not orderable");
        }
        return getOperatorAdaptor(type, Optional.of(sortOrder), callingConvention, COMPARISON).get();
    }

    public MethodHandle getLessThanOperator(Type type, InvocationConvention callingConvention)
    {
        if (!type.isOrderable()) {
            throw new UnsupportedOperationException(type + " is not orderable");
        }
        return getOperatorAdaptor(type, callingConvention, LESS_THAN).get();
    }

    public MethodHandle getLessThanOrEqualOperator(Type type, InvocationConvention callingConvention)
    {
        if (!type.isOrderable()) {
            throw new UnsupportedOperationException(type + " is not orderable");
        }
        return getOperatorAdaptor(type, callingConvention, LESS_THAN_OR_EQUAL).get();
    }

    private OperatorAdaptor getOperatorAdaptor(Type type, InvocationConvention callingConvention, OperatorType operatorType)
    {
        return getOperatorAdaptor(type, Optional.empty(), callingConvention, operatorType);
    }

    private OperatorAdaptor getOperatorAdaptor(Type type, Optional<SortOrder> sortOrder, InvocationConvention callingConvention, OperatorType operatorType)
    {
        OperatorConvention operatorConvention = new OperatorConvention(type, operatorType, sortOrder, callingConvention);
        return (OperatorAdaptor) cache.apply(operatorConvention, () -> new OperatorAdaptor(functionAdapter, operatorConvention));
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
                case COMPARISON:
                    if (operatorConvention.getSortOrder().isPresent()) {
                        return Collections.unmodifiableList(Arrays.asList(generateOrderingOperator(operatorConvention)));
                    }
                    return typeOperatorDeclaration.getComparisonOperators();
                case LESS_THAN:
                    Collection<OperatorMethodHandle> lessThanOperators = typeOperatorDeclaration.getLessThanOperators();
                    if (lessThanOperators.isEmpty()) {
                        return Collections.unmodifiableList(Arrays.asList(generateLessThanOperator(operatorConvention, false)));
                    }
                    return lessThanOperators;
                case LESS_THAN_OR_EQUAL:
                    Collection<OperatorMethodHandle> lessThanOrEqualOperators = typeOperatorDeclaration.getLessThanOrEqualOperators();
                    if (lessThanOrEqualOperators.isEmpty()) {
                        return Collections.unmodifiableList(Arrays.asList(generateLessThanOperator(operatorConvention, true)));
                    }
                    return lessThanOrEqualOperators;
            }
            throw new IllegalArgumentException("Unsupported operator type: " + operatorConvention.getOperatorType());
        }

        private OperatorMethodHandle generateDistinctFromOperator(OperatorConvention operatorConvention)
        {
            if (operatorConvention.getCallingConvention().getArgumentConventions().equals(Collections.unmodifiableList(Arrays.asList(BLOCK_POSITION, BLOCK_POSITION)))) {
                OperatorConvention equalOperator = new OperatorConvention(operatorConvention.getType(), EQUAL, Optional.empty(), simpleConvention(NULLABLE_RETURN, BLOCK_POSITION, BLOCK_POSITION));
                MethodHandle equalMethodHandle = adaptOperator(equalOperator);
                return adaptBlockPositionEqualToDistinctFrom(equalMethodHandle);
            }

            OperatorConvention equalOperator = new OperatorConvention(operatorConvention.getType(), EQUAL, Optional.empty(), simpleConvention(NULLABLE_RETURN, NEVER_NULL, NEVER_NULL));
            MethodHandle equalMethodHandle = adaptOperator(equalOperator);
            return adaptNeverNullEqualToDistinctFrom(equalMethodHandle);
        }

        private OperatorMethodHandle generateLessThanOperator(OperatorConvention operatorConvention, boolean orEqual)
        {
            InvocationConvention comparisonCallingConvention;
            if (operatorConvention.getCallingConvention().getArgumentConventions().equals(Collections.unmodifiableList(Arrays.asList(BLOCK_POSITION, BLOCK_POSITION)))) {
                comparisonCallingConvention = simpleConvention(FAIL_ON_NULL, BLOCK_POSITION, BLOCK_POSITION);
            }
            else {
                comparisonCallingConvention = simpleConvention(FAIL_ON_NULL, NEVER_NULL, NEVER_NULL);
            }

            OperatorConvention comparisonOperator = new OperatorConvention(operatorConvention.getType(), COMPARISON, Optional.empty(), comparisonCallingConvention);
            MethodHandle comparisonMethod = adaptOperator(comparisonOperator);
            if (orEqual) {
                return adaptComparisonToLessThanOrEqual(new OperatorMethodHandle(comparisonCallingConvention, comparisonMethod));
            }
            return adaptComparisonToLessThan(new OperatorMethodHandle(comparisonCallingConvention, comparisonMethod));
        }

        private OperatorMethodHandle generateOrderingOperator(OperatorConvention operatorConvention)
        {
            SortOrder sortOrder = operatorConvention.getSortOrder().orElseThrow(() -> new IllegalArgumentException("Operator convention does not contain a sort order"));
            if (operatorConvention.getCallingConvention().getArgumentConventions().equals(Collections.unmodifiableList(Arrays.asList(BLOCK_POSITION, BLOCK_POSITION)))) {
                OperatorConvention comparisonOperator = new OperatorConvention(operatorConvention.getType(), COMPARISON, Optional.empty(), simpleConvention(FAIL_ON_NULL, BLOCK_POSITION, BLOCK_POSITION));
                MethodHandle comparisonInvoker = adaptOperator(comparisonOperator);
                return adaptBlockPositionComparisonToOrdering(sortOrder, comparisonInvoker);
            }

            OperatorConvention comparisonOperator = new OperatorConvention(operatorConvention.getType(), COMPARISON, Optional.empty(), simpleConvention(FAIL_ON_NULL, NULL_FLAG, NULL_FLAG));
            MethodHandle comparisonInvoker = adaptOperator(comparisonOperator);
            return adaptNeverNullComparisonToOrdering(sortOrder, comparisonInvoker);
        }

        private List<Type> getOperatorArgumentTypes(OperatorConvention operatorConvention)
        {
            switch (operatorConvention.getOperatorType()) {
                case EQUAL:
                case IS_DISTINCT_FROM:
                case COMPARISON:
                case LESS_THAN:
                case LESS_THAN_OR_EQUAL:
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
        private final Optional<SortOrder> sortOrder;
        private final InvocationConvention callingConvention;

        public OperatorConvention(Type type, OperatorType operatorType, Optional<SortOrder> sortOrder, InvocationConvention callingConvention)
        {
            this.type = requireNonNull(type, "type is null");
            this.operatorType = requireNonNull(operatorType, "operatorType is null");
            this.sortOrder = requireNonNull(sortOrder, "sortOrder is null");
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
                    sortOrder.equals(operatorConvention.sortOrder) &&
                    callingConvention.equals(operatorConvention.callingConvention);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(type, operatorType, sortOrder, callingConvention);
        }

        @Override
        public String toString()
        {
            return new StringJoiner(", ", OperatorConvention.class.getSimpleName() + "[", "]")
                    .add("type=" + type)
                    .add("operatorType=" + sortOrder.map(order -> "ORDER_" + order).orElseGet(operatorType::toString))
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

        public Optional<SortOrder> getSortOrder()
        {
            return sortOrder;
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
    private static final MethodHandle IS_COMPARISON_LESS_THAN;
    private static final MethodHandle IS_COMPARISON_LESS_THAN_OR_EQUAL;
    private static final MethodHandle ORDER_NULLS;
    private static final MethodHandle ORDER_COMPARISON_RESULT;
    private static final MethodHandle BLOCK_IS_NULL;

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
            IS_COMPARISON_LESS_THAN = lookup.findStatic(TypeOperators.class, "isComparisonLessThan", MethodType.methodType(boolean.class, long.class));
            IS_COMPARISON_LESS_THAN_OR_EQUAL = lookup.findStatic(TypeOperators.class, "isComparisonLessThanOrEqual", MethodType.methodType(boolean.class, long.class));
            ORDER_NULLS = lookup.findStatic(TypeOperators.class, "orderNulls", MethodType.methodType(int.class, SortOrder.class, boolean.class, boolean.class));
            ORDER_COMPARISON_RESULT = lookup.findStatic(TypeOperators.class, "orderComparisonResult", MethodType.methodType(int.class, SortOrder.class, long.class));
            BLOCK_IS_NULL = lookup.findVirtual(Block.class, "isNull", MethodType.methodType(boolean.class, int.class));
        }
        catch (NoSuchMethodException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    //
    // Adapt equal to is distinct from
    //

    private static OperatorMethodHandle adaptBlockPositionEqualToDistinctFrom(MethodHandle blockPositionEqual)
    {
        return new OperatorMethodHandle(
                simpleConvention(FAIL_ON_NULL, BLOCK_POSITION, BLOCK_POSITION),
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
                simpleConvention(FAIL_ON_NULL, NULL_FLAG, NULL_FLAG),
                guardWithTest(eitherArgIsNull, distinctNullValues, notEqual));
    }

    //
    // Generate default indeterminate
    //

    private static OperatorMethodHandle defaultIndeterminateOperator(Class<?> javaType)
    {
        // boolean distinctFrom(T value, boolean valueIsNull)
        // {
        //     return valueIsNull;
        // }
        MethodHandle methodHandle = MethodHandles.identity(boolean.class);
        methodHandle = dropArguments(methodHandle, 0, javaType);
        return new OperatorMethodHandle(simpleConvention(FAIL_ON_NULL, NULL_FLAG), methodHandle);
    }

    private static boolean notEqual(Boolean equal)
    {
        return !requireNonNull(equal, "equal returned null");
    }

    //
    // Adapt comparison to ordering
    //

    private static OperatorMethodHandle adaptNeverNullComparisonToOrdering(SortOrder sortOrder, MethodHandle neverNullComparison)
    {
        MethodType finalSignature = MethodType.methodType(
                int.class,
                boolean.class,
                neverNullComparison.type().parameterType(0),
                boolean.class,
                neverNullComparison.type().parameterType(1));

        // (leftIsNull, rightIsNull, leftValue, rightValue)::int
        MethodHandle order = adaptComparisonToOrdering(sortOrder, neverNullComparison);
        // (leftIsNull, leftValue, rightIsNull, rightValue)::int
        order = permuteArguments(order, finalSignature, 0, 2, 1, 3);
        return new OperatorMethodHandle(simpleConvention(FAIL_ON_NULL, NULL_FLAG, NULL_FLAG), order);
    }

    private static OperatorMethodHandle adaptBlockPositionComparisonToOrdering(SortOrder sortOrder, MethodHandle blockPositionComparison)
    {
        MethodType finalSignature = MethodType.methodType(
                int.class,
                Block.class,
                int.class,
                Block.class,
                int.class);

        // (leftIsNull, rightIsNull, leftBlock, leftPosition, rightBlock, rightPosition)::int
        MethodHandle order = adaptComparisonToOrdering(sortOrder, blockPositionComparison);
        // (leftBlock, leftPosition, rightBlock, rightPosition, leftBlock, leftPosition, rightBlock, rightPosition)::int
        order = collectArguments(order, 1, BLOCK_IS_NULL);
        order = collectArguments(order, 0, BLOCK_IS_NULL);
        // (leftBlock, leftPosition, rightBlock, rightPosition)::int
        order = permuteArguments(order, finalSignature, 0, 1, 2, 3, 0, 1, 2, 3);

        return new OperatorMethodHandle(simpleConvention(FAIL_ON_NULL, BLOCK_POSITION, BLOCK_POSITION), order);
    }

    // input: (args)::int
    // output: (leftIsNull, rightIsNull, comparison_args)::int
    private static MethodHandle adaptComparisonToOrdering(SortOrder sortOrder, MethodHandle comparison)
    {
        // Guard: if (leftIsNull | rightIsNull)
        // (leftIsNull, rightIsNull)::boolean
        MethodHandle eitherIsNull = LOGICAL_OR;
        // (leftIsNull, rightIsNull, comparison_args)::boolean
        eitherIsNull = dropArguments(eitherIsNull, 2, comparison.type().parameterList());

        // True: return orderNulls(leftIsNull, rightIsNull)
        // (leftIsNull, rightIsNull)::int
        MethodHandle orderNulls = ORDER_NULLS.bindTo(sortOrder);
        // (leftIsNull, rightIsNull, comparison_args)::int
        orderNulls = dropArguments(orderNulls, 2, comparison.type().parameterList());

        // False; return orderComparisonResult(comparison(leftValue, rightValue))
        // (leftValue, rightValue)::int
        MethodHandle orderComparision = filterReturnValue(comparison, ORDER_COMPARISON_RESULT.bindTo(sortOrder));
        // (leftIsNull, rightIsNull, comparison_args)::int
        orderComparision = dropArguments(orderComparision, 0, boolean.class, boolean.class);

        // (leftIsNull, rightIsNull, comparison_args)::int
        return guardWithTest(eitherIsNull, orderNulls, orderComparision);
    }

    private static int orderNulls(SortOrder sortOrder, boolean leftIsNull, boolean rightIsNull)
    {
        if (leftIsNull && rightIsNull) {
            return 0;
        }
        if (leftIsNull) {
            return sortOrder.isNullsFirst() ? -1 : 1;
        }
        if (rightIsNull) {
            return sortOrder.isNullsFirst() ? 1 : -1;
        }
        throw new IllegalArgumentException("Neither left or right is null");
    }

    private static int orderComparisonResult(SortOrder sortOrder, long result)
    {
        return (int) (sortOrder.isAscending() ? result : -result);
    }

    //
    // Adapt comparison to less than
    //

    private static OperatorMethodHandle adaptComparisonToLessThan(OperatorMethodHandle invoker)
    {
        InvocationReturnConvention returnConvention = invoker.getCallingConvention().getReturnConvention();
        if (returnConvention != FAIL_ON_NULL) {
            throw new IllegalArgumentException("Return convention must be " + FAIL_ON_NULL + ", but is " + returnConvention);
        }
        return new OperatorMethodHandle(invoker.getCallingConvention(), filterReturnValue(invoker.getMethodHandle(), IS_COMPARISON_LESS_THAN));
    }

    private static boolean isComparisonLessThan(long comparisonResult)
    {
        return comparisonResult < 0;
    }

    private static OperatorMethodHandle adaptComparisonToLessThanOrEqual(OperatorMethodHandle invoker)
    {
        InvocationReturnConvention returnConvention = invoker.getCallingConvention().getReturnConvention();
        if (returnConvention != FAIL_ON_NULL) {
            throw new IllegalArgumentException("Return convention must be " + FAIL_ON_NULL + ", but is " + returnConvention);
        }
        return new OperatorMethodHandle(invoker.getCallingConvention(), filterReturnValue(invoker.getMethodHandle(), IS_COMPARISON_LESS_THAN_OR_EQUAL));
    }

    private static boolean isComparisonLessThanOrEqual(long comparisonResult)
    {
        return comparisonResult <= 0;
    }
}
