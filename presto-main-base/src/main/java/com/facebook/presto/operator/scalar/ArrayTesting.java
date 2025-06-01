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

import static com.facebook.presto.common.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;

@ScalarFunction("array_testing")
@Description("Remove specified values from the given array")
public final class ArrayTesting
{
    private ArrayTesting() {}

    @TypeParameter("E")
    @SqlType("array(E)")
    public static Block sort(
            @OperatorDependency(operator = GREATER_THAN, argumentTypes = {"E", "E"}) MethodHandle greaterThanFunction,
            @TypeParameter("E") Type type,
            @SqlType("array(E)") Block block,
            @SqlType("E") long value)
    {
        int arrayLength = block.getPositionCount();

        if (value < 0) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Parameter n: " + value + " to ARRAY_TOP_N is negative");
        }

        if (arrayLength < 2) {
            return block;
        }

        ListOfPositions listOfPositions = new ListOfPositions(arrayLength);
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
                        return -type.compareTo(block, p1, block, p2);
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
                        return -type.compareTo(block, p1, block, p2);
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

        int size = Math.min(arrayLength, (int) value);

        BlockBuilder blockBuilder = type.createBlockBuilder(null, size);

        for (int i = 0; i < size; i++) {
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

