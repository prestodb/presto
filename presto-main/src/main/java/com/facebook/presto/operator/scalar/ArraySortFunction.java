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
import java.util.Comparator;
import java.util.List;

import static com.facebook.presto.common.function.OperatorType.LESS_THAN;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;

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
