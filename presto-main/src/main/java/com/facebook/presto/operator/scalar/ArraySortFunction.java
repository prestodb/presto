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

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.OperatorDependency;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.spi.type.Type;
import com.google.common.primitives.Ints;

import java.lang.invoke.MethodHandle;
import java.util.AbstractList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.function.OperatorType.LESS_THAN;

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
            Collections.sort(listOfPositions, new Comparator<Integer>()
            {
                @Override
                public int compare(Integer p1, Integer p2)
                {
                    if (block.isNull(p1)) {
                        return block.isNull(p2) ? 0 : 1;
                    } else if (block.isNull(p2)) {
                        return -1;
                    }

                    try {
                        //TODO: This could be quite slow, it should use parametric equals
                        return type.compareTo(block, p1, block, p2);
                    }
                    catch (PrestoException e) {
                        if (e.getErrorCode() == NOT_SUPPORTED.toErrorCode()) {
                            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Array contains elements not supported for comparison", e);
                        }
                        throw e;
                    }
                }
            });
        }
        else {
            Collections.sort(listOfPositions, new Comparator<Integer>()
            {
                @Override
                public int compare(Integer p1, Integer p2)
                {
                    try {
                        //TODO: This could be quite slow, it should use parametric equals
                        return type.compareTo(block, p1, block, p2);
                    }
                    catch (PrestoException e) {
                        if (e.getErrorCode() == NOT_SUPPORTED.toErrorCode()) {
                            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Array contains elements not supported for comparison", e);
                        }
                        throw e;
                    }
                }
            });
        }

        List<Integer> sortedPositions = listOfPositions.getSortedPositions();
        if (sortedPositions == null) {
            // Original array is already sorted.
            return block;
        }

        BlockBuilder blockBuilder = type.createBlockBuilder(null, arrayLength);

        for (int i = 0; i < arrayLength; i++) {
            type.appendTo(block, sortedPositions.get(i), blockBuilder);
        }

        return blockBuilder.build();
    }

    private static class ListOfPositions
            extends AbstractList<Integer>
    {
        private final int size;
        private List<Integer> newList;

        ListOfPositions(int size)
        {
            this.size = size;
        }

        @Override
        public int size()
        {
            return size;
        }

        @Override
        public Integer get(int i)
        {
            return i;
        }

        @Override
        public Integer set(int index, Integer position)
        {
            if (index != position) {
                // The element at position is out of order.
                if (newList == null) {
                    // So we need to store the entire position array in a new list.
                    newList = Ints.asList(new int[size()]);
                    for (int i = 0; i < size(); i++) {
                        newList.set(i, i);
                    }
                }

                // Set the new position to be used for this index.
                newList.set(index, position);
            }

            return position;
        }

        List<Integer> getSortedPositions()
        {
            return newList;
        }
    }
}
