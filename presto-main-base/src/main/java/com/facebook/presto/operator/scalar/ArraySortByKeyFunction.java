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

import com.facebook.presto.common.NotSupportedException;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.sql.gen.lambda.LambdaFunctionInterface;
import io.airlift.slice.Slice;
import it.unimi.dsi.fastutil.ints.IntComparator;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static it.unimi.dsi.fastutil.ints.IntArrays.quickSort;

@ScalarFunction("array_sort")
@Description("Sorts the given array using a lambda function to extract sorting keys")
public final class ArraySortByKeyFunction
{
    private ArraySortByKeyFunction() {}

    @TypeParameter("T")
    @TypeParameter("K")
    @SqlType("array(T)")
    public static Block sortByKey(
            @TypeParameter("T") Type elementType,
            @TypeParameter("K") Type keyType,
            SqlFunctionProperties properties,
            @SqlType("array(T)") Block array,
            @SqlType("function(T, K)") KeyExtractorFunction function)
    {
        int arrayLength = array.getPositionCount();

        if (arrayLength < 2) {
            return array;
        }

        // Create array of indices and extracted keys
        int[] indices = new int[arrayLength];
        BlockBuilder keyBlockBuilder = keyType.createBlockBuilder(null, arrayLength);

        // Extract keys for all elements
        for (int i = 0; i < arrayLength; i++) {
            indices[i] = i;

            if (array.isNull(i)) {
                keyBlockBuilder.appendNull();
            }
            else {
                try {
                    // Get element value based on its Java type
                    Object element = getElementValue(elementType, array, i);
                    Object key = function.apply(element);

                    if (key == null) {
                        keyBlockBuilder.appendNull();
                    }
                    else {
                        writeToBlockBuilder(keyType, keyBlockBuilder, key);
                    }
                }
                catch (Throwable throwable) {
                    throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Key function failed with exception", throwable);
                }
            }
        }

        Block keysBlock = keyBlockBuilder.build();

        // Sort indices based on extracted keys using Type's compareTo
        try {
            if (array.mayHaveNull() || keysBlock.mayHaveNull()) {
                quickSort(indices, new NullableComparator(array, keysBlock, keyType));
            }
            else {
                quickSort(indices, new NonNullableComparator(keysBlock, keyType));
            }
        }
        catch (NotSupportedException | UnsupportedOperationException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Key type does not support comparison", e);
        }
        catch (PrestoException e) {
            if (e.getErrorCode() == NOT_SUPPORTED.toErrorCode()) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Key type does not support comparison", e);
            }
            throw e;
        }

        // Build result array using sorted indices
        BlockBuilder resultBuilder = elementType.createBlockBuilder(null, arrayLength);
        for (Integer index : indices) {
            elementType.appendTo(array, index, resultBuilder);
        }

        return resultBuilder.build();
    }

    private static Object getElementValue(Type elementType, Block array, int position)
    {
        Class<?> javaType = elementType.getJavaType();

        if (javaType == long.class) {
            return elementType.getLong(array, position);
        }
        else if (javaType == double.class) {
            return elementType.getDouble(array, position);
        }
        else if (javaType == boolean.class) {
            return elementType.getBoolean(array, position);
        }
        else if (javaType == Slice.class) {
            return elementType.getSlice(array, position);
        }
        else {
            return elementType.getObject(array, position);
        }
    }

    private static void writeToBlockBuilder(Type type, BlockBuilder blockBuilder, Object value)
    {
        Class<?> javaType = type.getJavaType();

        if (javaType == long.class) {
            type.writeLong(blockBuilder, ((Number) value).longValue());
        }
        else if (javaType == double.class) {
            type.writeDouble(blockBuilder, ((Number) value).doubleValue());
        }
        else if (javaType == boolean.class) {
            type.writeBoolean(blockBuilder, (Boolean) value);
        }
        else if (javaType == Slice.class) {
            type.writeSlice(blockBuilder, (Slice) value);
        }
        else {
            type.writeObject(blockBuilder, value);
        }
    }

    private static class NullableComparator
            implements IntComparator
    {
        private final Block array;
        private final Block keysBlock;
        private final Type keyType;

        public NullableComparator(Block array, Block keysBlock, Type keyType)
        {
            this.array = array;
            this.keysBlock = keysBlock;
            this.keyType = keyType;
        }

        @Override
        public int compare(int leftIndex, int rightIndex)
        {
            // Handle nulls in the original array
            boolean isNull1 = array.isNull(leftIndex);
            boolean isNull2 = array.isNull(rightIndex);

            if (isNull1) {
                return isNull2 ? 0 : 1;
            }
            if (isNull2) {
                return -1;
            }

            // Handle nulls in the keys
            boolean keyIsNull1 = keysBlock.isNull(leftIndex);
            boolean keyIsNull2 = keysBlock.isNull(rightIndex);

            if (keyIsNull1) {
                return keyIsNull2 ? 0 : 1;
            }
            if (keyIsNull2) {
                return -1;
            }

            // Use Type's compareTo for actual comparison
            return keyType.compareTo(keysBlock, leftIndex, keysBlock, rightIndex);
        }
    }

    private static class NonNullableComparator
            implements IntComparator
    {
        private final Block keysBlock;
        private final Type keyType;

        public NonNullableComparator(Block keysBlock, Type keyType)
        {
            this.keysBlock = keysBlock;
            this.keyType = keyType;
        }

        @Override
        public int compare(int leftIndex, int rightIndex)
        {
            // Use Type's compareTo for actual comparison
            return keyType.compareTo(keysBlock, leftIndex, keysBlock, rightIndex);
        }
    }

    @FunctionalInterface
    public interface KeyExtractorFunction
            extends LambdaFunctionInterface
    {
        Object apply(Object value);
    }
}
