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
package com.facebook.presto.type;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.function.InvocationConvention;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeOperators;

import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;

import java.lang.invoke.MethodHandle;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

import static com.facebook.presto.common.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static com.facebook.presto.common.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static com.facebook.presto.common.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static com.facebook.presto.common.function.InvocationConvention.simpleConvention;
import static com.facebook.presto.type.SingleAccessMethodCompiler.compileSingleAccessMethod;
import static com.facebook.presto.type.TypeUtils.NULL_HASH_CODE;
import static java.util.Objects.requireNonNull;

public final class BlockTypeOperators
{
    private static final InvocationConvention BLOCK_EQUAL_CONVENTION = simpleConvention(NULLABLE_RETURN, BLOCK_POSITION, BLOCK_POSITION);
    private static final InvocationConvention HASH_CODE_CONVENTION = simpleConvention(FAIL_ON_NULL, BLOCK_POSITION);
    private static final InvocationConvention XX_HASH_64_CONVENTION = simpleConvention(FAIL_ON_NULL, BLOCK_POSITION);
    private static final InvocationConvention IS_DISTINCT_FROM_CONVENTION = simpleConvention(FAIL_ON_NULL, BLOCK_POSITION, BLOCK_POSITION);

    private final ConcurrentMap<GeneratedBlockOperatorKey<?>, GeneratedBlockOperator<?>> generatedBlockOperatorCache = new ConcurrentHashMap<>();
    private final TypeOperators typeOperators;

    @Inject
    public BlockTypeOperators(TypeOperators typeOperators)
    {
        this.typeOperators = requireNonNull(typeOperators, "typeOperators is null");
    }

    public BlockPositionEqual getEqualOperator(Type type)
    {
        return getBlockOperator(type, BlockPositionEqual.class, () -> typeOperators.getEqualOperator(type, BLOCK_EQUAL_CONVENTION));
    }

    public interface BlockPositionEqual
    {
        Boolean equal(Block left, int leftPosition, Block right, int rightPosition);

        default boolean equalNullSafe(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
        {
            boolean leftIsNull = leftBlock.isNull(leftPosition);
            boolean rightIsNull = rightBlock.isNull(rightPosition);
            if (leftIsNull || rightIsNull) {
                return leftIsNull && rightIsNull;
            }
            return equal(leftBlock, leftPosition, rightBlock, rightPosition);
        }
    }

    public BlockPositionHashCode getHashCodeOperator(Type type)
    {
        return getBlockOperator(type, BlockPositionHashCode.class, () -> typeOperators.getHashCodeOperator(type, HASH_CODE_CONVENTION));
    }

    public interface BlockPositionHashCode
    {
        long hashCode(Block block, int position);

        default long hashCodeNullSafe(Block block, int position)
        {
            if (block.isNull(position)) {
                return NULL_HASH_CODE;
            }
            return hashCode(block, position);
        }
    }

    public BlockPositionXxHash64 getXxHash64Operator(Type type)
    {
        return getBlockOperator(type, BlockPositionXxHash64.class, () -> typeOperators.getXxHash64Operator(type, XX_HASH_64_CONVENTION));
    }

    public interface BlockPositionXxHash64
    {
        long xxHash64(Block block, int position);
    }

    public BlockPositionIsDistinctFrom getDistinctFromOperator(Type type)
    {
        return getBlockOperator(type, BlockPositionIsDistinctFrom.class, () -> typeOperators.getDistinctFromOperator(type, IS_DISTINCT_FROM_CONVENTION));
    }

    public interface BlockPositionIsDistinctFrom
    {
        boolean isDistinctFrom(Block left, int leftPosition, Block right, int rightPosition);
    }

    private <T> T getBlockOperator(Type type, Class<T> operatorInterface, Supplier<MethodHandle> methodHandleSupplier)
    {
        @SuppressWarnings("unchecked")
        GeneratedBlockOperator<T> generatedBlockOperator = (GeneratedBlockOperator<T>) generatedBlockOperatorCache.computeIfAbsent(
                new GeneratedBlockOperatorKey<>(type, operatorInterface),
                key -> new GeneratedBlockOperator<>(key.getType(), key.getOperatorInterface(), methodHandleSupplier.get()));
        return generatedBlockOperator.get();
    }

    private static class GeneratedBlockOperatorKey<T>
    {
        private final Type type;
        private final Class<T> operatorInterface;

        public GeneratedBlockOperatorKey(Type type, Class<T> operatorInterface)
        {
            this.type = requireNonNull(type, "type is null");
            this.operatorInterface = requireNonNull(operatorInterface, "operatorInterface is null");
        }

        public Type getType()
        {
            return type;
        }

        public Class<T> getOperatorInterface()
        {
            return operatorInterface;
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
            GeneratedBlockOperatorKey<?> that = (GeneratedBlockOperatorKey<?>) o;
            return type.equals(that.type) &&
                    operatorInterface.equals(that.operatorInterface);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(type, operatorInterface);
        }
    }

    private static class GeneratedBlockOperator<T>
    {
        private final Type type;
        private final Class<T> operatorInterface;
        private final MethodHandle methodHandle;
        @GuardedBy("this")
        private T operator;

        public GeneratedBlockOperator(Type type, Class<T> operatorInterface, MethodHandle methodHandle)
        {
            this.type = requireNonNull(type, "type is null");
            this.operatorInterface = requireNonNull(operatorInterface, "operatorInterface is null");
            this.methodHandle = requireNonNull(methodHandle, "methodHandle is null");
        }

        public synchronized T get()
        {
            if (operator != null) {
                return operator;
            }
            String suggestedClassName = operatorInterface.getSimpleName() + "_" + type.getDisplayName();
            operator = compileSingleAccessMethod(suggestedClassName, operatorInterface, methodHandle);
            return operator;
        }
    }
}
