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
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import io.airlift.slice.Slice;

@Description("Return the first element which matches the given predicate, null if no match")
@ScalarFunction(value = "find_first", deterministic = true)
public final class ArrayFindFirstFirstFunction
        extends ArrayFindFirstWithOffsetFunction
{
    private ArrayFindFirstFirstFunction() {}

    @TypeParameter("T")
    @SqlType("T")
    @SqlNullable
    public static Block findBlock(
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block arrayBlock,
            @SqlType("function(T, boolean)") BlockToBooleanFunction function)
    {
        return findBlockUtil(elementType, arrayBlock, 1, function);
    }

    @TypeParameter("T")
    @SqlType("T")
    @SqlNullable
    public static Slice findSlice(
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block arrayBlock,
            @SqlType("function(T, boolean)") SliceToBooleanFunction function)
    {
        return findSliceUtil(elementType, arrayBlock, 1, function);
    }

    @TypeParameter("T")
    @SqlType("T")
    @SqlNullable
    public static Long findLong(
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block arrayBlock,
            @SqlType("function(T, boolean)") LongToBooleanFunction function)
    {
        return findLongUtil(elementType, arrayBlock, 1, function);
    }

    @TypeParameter("T")
    @SqlType("T")
    @SqlNullable
    public static Double findDouble(
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block arrayBlock,
            @SqlType("function(T, boolean)") DoubleToBooleanFunction function)
    {
        return findDoubleUtil(elementType, arrayBlock, 1, function);
    }

    @TypeParameter("T")
    @SqlType("T")
    @SqlNullable
    public static Boolean findBoolean(
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block arrayBlock,
            @SqlType("function(T, boolean)") BooleanToBooleanFunction function)
    {
        return findBooleanUtil(elementType, arrayBlock, 1, function);
    }
}
