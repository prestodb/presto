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
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.spi.type.Type;
import com.google.common.primitives.Ints;
import io.airlift.slice.Slice;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

@ScalarFunction("element_at")
@Description("Get element of array at given index")
public final class ArrayElementAtFunction
{
    private ArrayElementAtFunction() {}

    @TypeParameter("E")
    @SqlNullable
    @SqlType("E")
    public static Void voidElementAt(@SqlType("array(E)") Block array, @SqlType("bigint") long index)
    {
        checkedIndexToBlockPosition(array, index);
        return null;
    }

    @TypeParameter("E")
    @SqlNullable
    @SqlType("E")
    public static Long longElementAt(@TypeParameter("E") Type elementType, @SqlType("array(E)") Block array, @SqlType("bigint") long index)
    {
        int position = checkedIndexToBlockPosition(array, index);
        if (position == -1) {
            return null;
        }
        if (array.isNull(position)) {
            return null;
        }

        return elementType.getLong(array, position);
    }

    @TypeParameter("E")
    @SqlNullable
    @SqlType("E")
    public static Boolean booleanElementAt(@TypeParameter("E") Type elementType, @SqlType("array(E)") Block array, @SqlType("bigint") long index)
    {
        int position = checkedIndexToBlockPosition(array, index);
        if (position == -1) {
            return null;
        }
        if (array.isNull(position)) {
            return null;
        }

        return elementType.getBoolean(array, position);
    }

    @TypeParameter("E")
    @SqlNullable
    @SqlType("E")
    public static Double doubleElementAt(@TypeParameter("E") Type elementType, @SqlType("array(E)") Block array, @SqlType("bigint") long index)
    {
        int position = checkedIndexToBlockPosition(array, index);
        if (position == -1) {
            return null;
        }
        if (array.isNull(position)) {
            return null;
        }

        return elementType.getDouble(array, position);
    }

    @TypeParameter("E")
    @SqlNullable
    @SqlType("E")
    public static Slice sliceElementAt(@TypeParameter("E") Type elementType, @SqlType("array(E)") Block array, @SqlType("bigint") long index)
    {
        int position = checkedIndexToBlockPosition(array, index);
        if (position == -1) {
            return null;
        }
        if (array.isNull(position)) {
            return null;
        }

        return elementType.getSlice(array, position);
    }

    @TypeParameter("E")
    @SqlNullable
    @SqlType("E")
    public static Block blockElementAt(@TypeParameter("E") Type elementType, @SqlType("array(E)") Block array, @SqlType("bigint") long index)
    {
        int position = checkedIndexToBlockPosition(array, index);
        if (position == -1) {
            return null;
        }
        if (array.isNull(position)) {
            return null;
        }

        return (Block) elementType.getObject(array, position);
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
        if (index > 0) {
            return Ints.checkedCast(index - 1);
        }
        else {
            return Ints.checkedCast(arrayLength + index);
        }
    }
}
