package com.facebook.presto.operator.scalar.sql;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.operator.aggregation.TypedSet;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import static com.facebook.presto.common.type.BigintType.BIGINT;

@ScalarFunction("array_has_dupes")
public class ArrayHasDupesFunction {
    private ArrayHasDupesFunction() {
    }

    @TypeParameter("E")
    @SqlType("array(E)")
    public static Boolean arrayHasDupes(@TypeParameter("E") Type elementType, @SqlType("array(E)") Block array) {
        int arrayLength = array.getPositionCount();
        if (arrayLength < 2) {
            return false;
        }

        TypedSet typedSet = new TypedSet(elementType, arrayLength, "array_has_dupes");
        if (array.mayHaveNull()) {
            int firstPosition = 0;
            // Keep adding the element to the set as long as there are no dupes
            while (firstPosition < arrayLength && typedSet.add(array, firstPosition)) {
                firstPosition++;
            }

            if (firstPosition == arrayLength) {
                // All elements are distinct
                return false;
            }

            return true;
        } else {

            int firstPosition = 0;
            while (firstPosition < arrayLength && typedSet.addNonNull(array, firstPosition)) {
                firstPosition++;
            }

            if (firstPosition == arrayLength) {
                // All elements are distinct, so just return the original.
                return false;
            }
            return true;
        }

    }


    @TypeParameter("bigint")
    @SqlType("array(bigint)")
    public static Boolean arrayHasDupes(@SqlType("array(bigint)") Block array) {
        Boolean isDupe = false;
        final int arrayLength = array.getPositionCount();
        if (arrayLength < 2) {
            return false;
        }

        LongSet set = new LongOpenHashSet(arrayLength);
        if (array.mayHaveNull()) {
            int position = 0;
            boolean containsNull = false;

            // Keep adding the element to the set as long as there are no dupes.
            while (position < arrayLength) {
                if (array.isNull(position)) {
                    if (!containsNull) {
                        containsNull = true;
                    } else {
                        // Second null.
                        break;
                    }
                } else if (!set.add(BIGINT.getLong(array, position))) {
                    // Dupe found.
                    return true;
                }
                position++;
            }

            if (position == arrayLength) {
                // All elements are distinct, so just return the original.
                isDupe = false;
            }

        } else {
            int position = 0;
            // Keep adding the element to the set as long as there are no dupes.
            while (position < arrayLength && set.add(BIGINT.getLong(array, position))) {
                position++;
            }
            if (position == arrayLength) {
                // All elements are distinct, so just return the original.
                return false;
            }
            isDupe = true;
        }

        return isDupe;
    }
}

