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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.spi.function.TypeParameterSpecialization;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.gen.lambda.LambdaFunctionInterface;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.airlift.slice.Slice;

import java.util.List;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.util.Failures.checkCondition;

@ScalarFunction("array_sort")
@Description("Sorts the given array with a lambda comparator.")
public final class ArraySortComparatorFunction
{
    private final PageBuilder pageBuilder;
    private static final int INITIAL_LENGTH = 128;
    private static final String COMPARATOR_RETURN_ERROR = "Lambda comparator must return either -1, 0, or 1";
    private List<Integer> positions = Ints.asList(new int[INITIAL_LENGTH]);

    @TypeParameter("T")
    public ArraySortComparatorFunction(@TypeParameter("T") Type elementType)
    {
        pageBuilder = new PageBuilder(ImmutableList.of(elementType));
    }

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = long.class)
    @SqlType("array(T)")
    public Block sortLong(
            @TypeParameter("T") Type type,
            @SqlType("array(T)") Block block,
            @SqlType("function(T, T, int)") ComparatorLongLambda function)
    {
        int arrayLength = block.getPositionCount();
        initPositionsList(arrayLength);

        positions.subList(0, arrayLength).sort((p1, p2) -> {
            Long lambdaResult = function.apply(
                    block.isNull(p1) ? null : type.getLong(block, p1),
                    block.isNull(p2) ? null : type.getLong(block, p2));
            checkCondition(
                    lambdaResult != null && (lambdaResult == -1 || lambdaResult == 0 || lambdaResult == 1),
                    INVALID_FUNCTION_ARGUMENT,
                    COMPARATOR_RETURN_ERROR);
            return lambdaResult.intValue();
        });

        return computeResultBlock(type, block, arrayLength);
    }

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = double.class)
    @SqlType("array(T)")
    public Block sortDouble(
            @TypeParameter("T") Type type,
            @SqlType("array(T)") Block block,
            @SqlType("function(T, T, int)") ComparatorDoubleLambda function)
    {
        int arrayLength = block.getPositionCount();
        initPositionsList(arrayLength);

        positions.subList(0, arrayLength).sort((p1, p2) -> {
            Long lambdaResult = function.apply(
                    block.isNull(p1) ? null : type.getDouble(block, p1),
                    block.isNull(p2) ? null : type.getDouble(block, p2));
            checkCondition(
                    lambdaResult != null && (lambdaResult == -1 || lambdaResult == 0 || lambdaResult == 1),
                    INVALID_FUNCTION_ARGUMENT,
                    COMPARATOR_RETURN_ERROR);
            return lambdaResult.intValue();
        });

        return computeResultBlock(type, block, arrayLength);
    }

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = boolean.class)
    @SqlType("array(T)")
    public Block sortBoolean(
            @TypeParameter("T") Type type,
            @SqlType("array(T)") Block block,
            @SqlType("function(T, T, int)") ComparatorBooleanLambda function)
    {
        int arrayLength = block.getPositionCount();
        initPositionsList(arrayLength);

        positions.subList(0, arrayLength).sort((p1, p2) -> {
            Long lambdaResult = function.apply(
                    block.isNull(p1) ? null : type.getBoolean(block, p1),
                    block.isNull(p2) ? null : type.getBoolean(block, p2));
            checkCondition(
                    lambdaResult != null && (lambdaResult == -1 || lambdaResult == 0 || lambdaResult == 1),
                    INVALID_FUNCTION_ARGUMENT,
                    COMPARATOR_RETURN_ERROR);
            return lambdaResult.intValue();
        });

        return computeResultBlock(type, block, arrayLength);
    }

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = Slice.class)
    @SqlType("array(T)")
    public Block sortSlice(
            @TypeParameter("T") Type type,
            @SqlType("array(T)") Block block,
            @SqlType("function(T, T, int)") ComparatorSliceLambda function)
    {
        int arrayLength = block.getPositionCount();
        initPositionsList(arrayLength);

        positions.subList(0, arrayLength).sort((p1, p2) -> {
            Long lambdaResult = function.apply(
                    block.isNull(p1) ? null : type.getSlice(block, p1),
                    block.isNull(p2) ? null : type.getSlice(block, p2));
            checkCondition(
                    lambdaResult != null && (lambdaResult == -1 || lambdaResult == 0 || lambdaResult == 1),
                    INVALID_FUNCTION_ARGUMENT,
                    COMPARATOR_RETURN_ERROR);
            return lambdaResult.intValue();
        });

        return computeResultBlock(type, block, arrayLength);
    }

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = Block.class)
    @SqlType("array(T)")
    public Block sortObject(
            @TypeParameter("T") Type type,
            @SqlType("array(T)") Block block,
            @SqlType("function(T, T, int)") ComparatorBlockLambda function)
    {
        int arrayLength = block.getPositionCount();
        initPositionsList(arrayLength);

        positions.subList(0, arrayLength).sort((p1, p2) -> {
            Long lambdaResult = function.apply(
                    block.isNull(p1) ? null : (Block) type.getObject(block, p1),
                    block.isNull(p2) ? null : (Block) type.getObject(block, p2));
            checkCondition(
                    lambdaResult != null && (lambdaResult == -1 || lambdaResult == 0 || lambdaResult == 1),
                    INVALID_FUNCTION_ARGUMENT,
                    COMPARATOR_RETURN_ERROR);
            return lambdaResult.intValue();
        });

        return computeResultBlock(type, block, arrayLength);
    }

    private void initPositionsList(int arrayLength)
    {
        if (positions.size() < arrayLength) {
            positions = Ints.asList(new int[arrayLength]);
        }
        for (int i = 0; i < arrayLength; i++) {
            positions.set(i, i);
        }
    }

    private Block computeResultBlock(Type type, Block block, int arrayLength)
    {
        if (pageBuilder.isFull()) {
            pageBuilder.reset();
        }

        BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(0);

        for (int i = 0; i < arrayLength; ++i) {
            type.appendTo(block, positions.get(i), blockBuilder);
        }
        pageBuilder.declarePositions(arrayLength);

        return blockBuilder.getRegion(blockBuilder.getPositionCount() - arrayLength, arrayLength);
    }

    @FunctionalInterface
    public interface ComparatorLongLambda
            extends LambdaFunctionInterface
    {
        Long apply(Long x, Long y);
    }

    @FunctionalInterface
    public interface ComparatorDoubleLambda
            extends LambdaFunctionInterface
    {
        Long apply(Double x, Double y);
    }

    @FunctionalInterface
    public interface ComparatorBooleanLambda
            extends LambdaFunctionInterface
    {
        Long apply(Boolean x, Boolean y);
    }

    @FunctionalInterface
    public interface ComparatorSliceLambda
            extends LambdaFunctionInterface
    {
        Long apply(Slice x, Slice y);
    }

    @FunctionalInterface
    public interface ComparatorBlockLambda
            extends LambdaFunctionInterface
    {
        Long apply(Block x, Block y);
    }
}
