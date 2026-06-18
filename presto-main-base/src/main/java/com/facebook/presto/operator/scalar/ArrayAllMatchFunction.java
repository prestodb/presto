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
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.spi.function.TypeParameterSpecialization;
import io.airlift.slice.Slice;

import static java.lang.Boolean.FALSE;

@Description("Returns true if all elements of the array match the given predicate")
@ScalarFunction(value = "all_match")
public final class ArrayAllMatchFunction
{
    private ArrayAllMatchFunction() {}

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = Block.class)
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean allMatchBlock(
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block arrayBlock,
            @SqlType("function(T, boolean)") BlockToBooleanFunction function)
    {
        boolean hasNullResult = false;
        for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
            Block element = null;
            if (!arrayBlock.isNull(i)) {
                element = (Block) elementType.getObject(arrayBlock, i);
            }
            Boolean match = function.apply(element);
            if (FALSE.equals(match)) {
                return false;
            }
            if (match == null) {
                hasNullResult = true;
            }
        }
        if (hasNullResult) {
            return null;
        }
        return true;
    }

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = Slice.class)
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean allMatchSlice(
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block arrayBlock,
            @SqlType("function(T, boolean)") SliceToBooleanFunction function)
    {
        boolean hasNullResult = false;
        int positionCount = arrayBlock.getPositionCount();
        for (int i = 0; i < positionCount; i++) {
            Slice element = null;
            if (!arrayBlock.isNull(i)) {
                element = elementType.getSlice(arrayBlock, i);
            }
            Boolean match = function.apply(element);
            if (FALSE.equals(match)) {
                return false;
            }
            if (match == null) {
                hasNullResult = true;
            }
        }
        if (hasNullResult) {
            return null;
        }
        return true;
    }

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = long.class)
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean allMatchLong(
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block arrayBlock,
            @SqlType("function(T, boolean)") LongToBooleanFunction function)
    {
        boolean hasNullResult = false;
        int positionCount = arrayBlock.getPositionCount();
        for (int i = 0; i < positionCount; i++) {
            Long element = null;
            if (!arrayBlock.isNull(i)) {
                element = elementType.getLong(arrayBlock, i);
            }
            Boolean match = function.apply(element);
            if (FALSE.equals(match)) {
                return false;
            }
            if (match == null) {
                hasNullResult = true;
            }
        }
        if (hasNullResult) {
            return null;
        }
        return true;
    }

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = double.class)
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean allMatchDouble(
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block arrayBlock,
            @SqlType("function(T, boolean)") DoubleToBooleanFunction function)
    {
        boolean hasNullResult = false;
        int positionCount = arrayBlock.getPositionCount();
        for (int i = 0; i < positionCount; i++) {
            Double element = null;
            if (!arrayBlock.isNull(i)) {
                element = elementType.getDouble(arrayBlock, i);
            }
            Boolean match = function.apply(element);
            if (FALSE.equals(match)) {
                return false;
            }
            if (match == null) {
                hasNullResult = true;
            }
        }
        if (hasNullResult) {
            return null;
        }
        return true;
    }

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = boolean.class)
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean allMatchBoolean(
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block arrayBlock,
            @SqlType("function(T, boolean)") BooleanToBooleanFunction function)
    {
        boolean hasNullResult = false;
        int positionCount = arrayBlock.getPositionCount();
        for (int i = 0; i < positionCount; i++) {
            Boolean element = null;
            if (!arrayBlock.isNull(i)) {
                element = elementType.getBoolean(arrayBlock, i);
            }
            Boolean match = function.apply(element);
            if (FALSE.equals(match)) {
                return false;
            }
            if (match == null) {
                hasNullResult = true;
            }
        }
        if (hasNullResult) {
            return null;
        }
        return true;
    }
}
