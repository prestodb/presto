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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.spi.function.TypeParameterSpecialization;
import io.airlift.slice.Slice;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static java.lang.Boolean.TRUE;
import static java.lang.Math.toIntExact;

@Description("Return the index of the first element which matches the given predicate, null if no match")
@ScalarFunction(value = "find_first_index", deterministic = true)
public class ArrayFindFirstIndexWithOffsetFunction
{
    protected ArrayFindFirstIndexWithOffsetFunction() {}

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = Block.class)
    @SqlType(StandardTypes.BIGINT)
    @SqlNullable
    public static Long findBlockWithOffset(
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block arrayBlock,
            @SqlType(StandardTypes.BIGINT) long offset,
            @SqlType("function(T, boolean)") BlockToBooleanFunction function)
    {
        return findBlockUtil(elementType, arrayBlock, offset, function);
    }

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = Slice.class)
    @SqlType(StandardTypes.BIGINT)
    @SqlNullable
    public static Long findSliceWithOffset(
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block arrayBlock,
            @SqlType(StandardTypes.BIGINT) long offset,
            @SqlType("function(T, boolean)") SliceToBooleanFunction function)
    {
        return findSliceUtil(elementType, arrayBlock, offset, function);
    }

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = long.class)
    @SqlType(StandardTypes.BIGINT)
    @SqlNullable
    public static Long findLongWithOffset(
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block arrayBlock,
            @SqlType(StandardTypes.BIGINT) long offset,
            @SqlType("function(T, boolean)") LongToBooleanFunction function)
    {
        return findLongUtil(elementType, arrayBlock, offset, function);
    }

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = double.class)
    @SqlType(StandardTypes.BIGINT)
    @SqlNullable
    public static Long findDoubleWithOffset(
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block arrayBlock,
            @SqlType(StandardTypes.BIGINT) long offset,
            @SqlType("function(T, boolean)") DoubleToBooleanFunction function)
    {
        return findDoubleUtil(elementType, arrayBlock, offset, function);
    }

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = boolean.class)
    @SqlType(StandardTypes.BIGINT)
    @SqlNullable
    public static Long findBooleanWithOffset(
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block arrayBlock,
            @SqlType(StandardTypes.BIGINT) long offset,
            @SqlType("function(T, boolean)") BooleanToBooleanFunction function)
    {
        return findBooleanUtil(elementType, arrayBlock, offset, function);
    }

    public static Long findBlockUtil(
            Type elementType,
            Block arrayBlock,
            long offset,
            BlockToBooleanFunction function)
    {
        int startPosition = checkedIndexToBlockPosition(arrayBlock, offset);
        if (startPosition < 0) {
            return null;
        }
        int increment = offset > 0 ? 1 : -1;
        for (int i = startPosition; i < arrayBlock.getPositionCount() && i >= 0; i += increment) {
            Block element = null;
            if (!arrayBlock.isNull(i)) {
                element = (Block) elementType.getObject(arrayBlock, i);
            }
            Boolean match = function.apply(element);
            if (TRUE.equals(match)) {
                return Long.valueOf(i + 1);
            }
        }
        return null;
    }

    public static Long findSliceUtil(
            Type elementType,
            Block arrayBlock,
            long offset,
            SliceToBooleanFunction function)
    {
        int startPosition = checkedIndexToBlockPosition(arrayBlock, offset);
        if (startPosition < 0) {
            return null;
        }
        int increment = offset > 0 ? 1 : -1;
        for (int i = startPosition; i < arrayBlock.getPositionCount() && i >= 0; i += increment) {
            Slice element = null;
            if (!arrayBlock.isNull(i)) {
                element = elementType.getSlice(arrayBlock, i);
            }
            Boolean match = function.apply(element);
            if (TRUE.equals(match)) {
                return Long.valueOf(i + 1);
            }
        }
        return null;
    }

    public static Long findLongUtil(
            Type elementType,
            Block arrayBlock,
            long offset,
            LongToBooleanFunction function)
    {
        int startPosition = checkedIndexToBlockPosition(arrayBlock, offset);
        if (startPosition < 0) {
            return null;
        }
        int increment = offset > 0 ? 1 : -1;
        for (int i = startPosition; i < arrayBlock.getPositionCount() && i >= 0; i += increment) {
            Long element = null;
            if (!arrayBlock.isNull(i)) {
                element = elementType.getLong(arrayBlock, i);
            }
            Boolean match = function.apply(element);
            if (TRUE.equals(match)) {
                return Long.valueOf(i + 1);
            }
        }
        return null;
    }

    public static Long findDoubleUtil(
            Type elementType,
            Block arrayBlock,
            long offset,
            DoubleToBooleanFunction function)
    {
        int startPosition = checkedIndexToBlockPosition(arrayBlock, offset);
        if (startPosition < 0) {
            return null;
        }
        int increment = offset > 0 ? 1 : -1;
        for (int i = startPosition; i < arrayBlock.getPositionCount() && i >= 0; i += increment) {
            Double element = null;
            if (!arrayBlock.isNull(i)) {
                element = elementType.getDouble(arrayBlock, i);
            }
            Boolean match = function.apply(element);
            if (TRUE.equals(match)) {
                return Long.valueOf(i + 1);
            }
        }
        return null;
    }

    public static Long findBooleanUtil(
            Type elementType,
            Block arrayBlock,
            long offset,
            BooleanToBooleanFunction function)
    {
        int startPosition = checkedIndexToBlockPosition(arrayBlock, offset);
        if (startPosition < 0) {
            return null;
        }
        int increment = offset > 0 ? 1 : -1;
        for (int i = startPosition; i < arrayBlock.getPositionCount() && i >= 0; i += increment) {
            Boolean element = null;
            if (!arrayBlock.isNull(i)) {
                element = elementType.getBoolean(arrayBlock, i);
            }
            Boolean match = function.apply(element);
            if (TRUE.equals(match)) {
                return Long.valueOf(i + 1);
            }
        }
        return null;
    }

    /**
     * @return PrestoException if the index is 0, -1 if the index is out of range (to tell the calling function to return null), and the element position otherwise.
     */
    private static int checkedIndexToBlockPosition(Block block, long index)
    {
        int arrayLength = block.getPositionCount();
        if (index == 0) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "SQL array indices start at 1");
        }
        if (Math.abs(index) > arrayLength) {
            return -1; // -1 indicates that the element is out of range and "ELEMENT_AT" should return null
        }
        index = index > 0 ? index - 1 : arrayLength + index;
        return toIntExact(index);
    }
}
