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
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.OperatorDependency;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.google.common.primitives.Ints;

import java.lang.invoke.MethodHandle;
import java.util.AbstractList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static com.facebook.presto.common.function.OperatorType.LESS_THAN;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.Float.floatToIntBits;
import static java.lang.Float.intBitsToFloat;

@ScalarFunction("array_sort")
@Description("Sorts the given array in ascending order according to the natural ordering of its elements.")
public final class ArraySortFunction
{
    private ArraySortFunction() {}

    @TypeParameter("E")
    @SqlType("array(E)")
    public static Block sort(
            @OperatorDependency(operator = LESS_THAN, argumentTypes = {"E", "E"}) MethodHandle lessThanFunction,
            @TypeParameter("E") Type type,
            @SqlType("array(E)") Block block)
    {
        int arrayLength = block.getPositionCount();

        if (arrayLength < 2) {
            return block;
        }

        ListOfPositions listOfPositions = new ListOfPositions(block.getPositionCount());
        if (block.mayHaveNull()) {
            listOfPositions.sort(new Comparator<Integer>()
            {
                @Override
                public int compare(Integer p1, Integer p2)
                {
                    if (block.isNull(p1)) {
                        return block.isNull(p2) ? 0 : 1;
                    }
                    else if (block.isNull(p2)) {
                        return -1;
                    }

                    try {
                        //TODO: This could be quite slow, it should use parametric equals
                        return type.compareTo(block, p1, block, p2);
                    }
                    catch (PrestoException | NotSupportedException e) {
                        if (e instanceof NotSupportedException || ((PrestoException) e).getErrorCode() == NOT_SUPPORTED.toErrorCode()) {
                            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Array contains elements not supported for comparison", e);
                        }
                        throw e;
                    }
                }
            });
        }
        else {
            listOfPositions.sort(new Comparator<Integer>()
            {
                @Override
                public int compare(Integer p1, Integer p2)
                {
                    try {
                        //TODO: This could be quite slow, it should use parametric equals
                        return type.compareTo(block, p1, block, p2);
                    }
                    catch (PrestoException | NotSupportedException e) {
                        if (e instanceof NotSupportedException || ((PrestoException) e).getErrorCode() == NOT_SUPPORTED.toErrorCode()) {
                            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Array contains elements not supported for comparison", e);
                        }
                        throw e;
                    }
                }
            });
        }

        List<Integer> sortedListOfPositions = listOfPositions.getSortedListOfPositions();
        if (sortedListOfPositions == listOfPositions) {
            // Original array is already sorted.
            return block;
        }

        BlockBuilder blockBuilder = type.createBlockBuilder(null, arrayLength);

        for (int i = 0; i < arrayLength; i++) {
            type.appendTo(block, sortedListOfPositions.get(i), blockBuilder);
        }

        return blockBuilder.build();
    }

    @SqlType("array(bigint)")
    public static Block bigintSort(@SqlType("array(bigint)") Block array)
    {
        final int arrayLength = array.getPositionCount();
        if (arrayLength < 2) {
            return array;
        }

        long[] values = new long[array.getPositionCount()];
        int nulls = 0;

        if (array.mayHaveNull()) {
            int j = 0;
            for (int i = 0; i < array.getPositionCount(); i++) {
                if (array.isNull(i)) {
                    nulls++;
                }
                else {
                    values[j++] = BIGINT.getLong(array, i);
                }
            }
        }
        else {
            for (int i = 0; i < array.getPositionCount(); i++) {
                values[i] = BIGINT.getLong(array, i);
            }
        }

        Arrays.sort(values, 0, values.length - nulls);

        BlockBuilder blockBuilder = BIGINT.createBlockBuilder(null, arrayLength);
        for (int i = 0; i < values.length - nulls; i++) {
            BIGINT.writeLong(blockBuilder, values[i]);
        }
        // Nulls last
        for (int i = 0; i < nulls; i++) {
            blockBuilder.appendNull();
        }

        return blockBuilder.build();
    }

    @SqlType("array(double)")
    public static Block doubleSort(@SqlType("array(double)") Block array)
    {
        final int arrayLength = array.getPositionCount();
        if (arrayLength < 2) {
            return array;
        }

        double[] values = new double[array.getPositionCount()];
        int nulls = 0;

        if (array.mayHaveNull()) {
            int j = 0;
            for (int i = 0; i < array.getPositionCount(); i++) {
                if (array.isNull(i)) {
                    nulls++;
                }
                else {
                    values[j++] = DOUBLE.getDouble(array, i);
                }
            }
        }
        else {
            for (int i = 0; i < array.getPositionCount(); i++) {
                values[i] = DOUBLE.getDouble(array, i);
            }
        }

        Arrays.sort(values, 0, values.length - nulls);

        BlockBuilder blockBuilder = DOUBLE.createBlockBuilder(null, arrayLength);
        for (int i = 0; i < values.length - nulls; i++) {
            DOUBLE.writeDouble(blockBuilder, values[i]);
        }

        // Nulls last
        for (int i = 0; i < nulls; i++) {
            blockBuilder.appendNull();
        }

        return blockBuilder.build();
    }

    @SqlType("array(real)")
    public static Block floatSort(@SqlType("array(real)") Block array)
    {
        final int arrayLength = array.getPositionCount();
        if (arrayLength < 2) {
            return array;
        }

        float[] values = new float[array.getPositionCount()];
        int nulls = 0;

        if (array.mayHaveNull()) {
            int j = 0;
            for (int i = 0; i < array.getPositionCount(); i++) {
                if (array.isNull(i)) {
                    nulls++;
                }
                else {
                    values[j++] = intBitsToFloat(array.getInt(i));
                }
            }
        }
        else {
            for (int i = 0; i < array.getPositionCount(); i++) {
                values[i] = intBitsToFloat(array.getInt(i));
            }
        }

        Arrays.sort(values, 0, values.length - nulls);

        BlockBuilder blockBuilder = REAL.createBlockBuilder(null, arrayLength);
        for (int i = 0; i < values.length - nulls; i++) {
            REAL.writeLong(blockBuilder, floatToIntBits(values[i]));
        }

        // Nulls last
        for (int i = 0; i < nulls; i++) {
            blockBuilder.appendNull();
        }

        return blockBuilder.build();
    }

    private static class ListOfPositions
            extends AbstractList<Integer>
    {
        private final int size;
        private List<Integer> sortedListOfPositions;

        ListOfPositions(int size)
        {
            this.size = size;
        }

        @Override
        public final int size()
        {
            return size;
        }

        @Override
        public final Integer get(int i)
        {
            return i;
        }

        @Override
        public final Integer set(int index, Integer position)
        {
            if (index != position) {
                // The element at position is out of order.
                if (sortedListOfPositions == null) {
                    // So we need to store the entire position array in a new list.
                    sortedListOfPositions = Ints.asList(new int[size()]);
                    for (int i = 0; i < size(); i++) {
                        sortedListOfPositions.set(i, i);
                    }
                }

                // Set the new position to be used for this index.
                sortedListOfPositions.set(index, position);
            }

            return position;
        }

        List<Integer> getSortedListOfPositions()
        {
            return sortedListOfPositions == null ? this : sortedListOfPositions;
        }
    }
}
