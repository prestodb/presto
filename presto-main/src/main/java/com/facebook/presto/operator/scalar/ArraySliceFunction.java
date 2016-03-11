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

import com.facebook.presto.operator.Description;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.SqlType;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.util.Failures.checkCondition;

@ScalarFunction("slice")
@Description("Subsets an array given an offset (1-indexed) and length")
public final class ArraySliceFunction
{
    private ArraySliceFunction() {}

    @TypeParameter("E")
    @SqlType("array(E)")
    public static Block slice(
            @TypeParameter("E") Type type,
            @SqlType("array(E)") Block array,
            @SqlType(StandardTypes.BIGINT) long fromIndex,
            @SqlType(StandardTypes.BIGINT) long length)
    {
        checkCondition(length >= 0, INVALID_FUNCTION_ARGUMENT, "length must be greater than or equal to 0");
        checkCondition(fromIndex != 0, INVALID_FUNCTION_ARGUMENT, "SQL array indices start at 1");

        int size = array.getPositionCount();
        if (size == 0) {
            return array;
        }

        if (fromIndex < 0) {
            fromIndex = size + fromIndex + 1;
        }

        long toIndex = Math.min(fromIndex + length, size + 1);

        if (fromIndex >= toIndex || fromIndex < 0 || toIndex < 0) {
            return type.createBlockBuilder(new BlockBuilderStatus(), 0).build();
        }

        return array.getRegion((int) (fromIndex - 1), (int) (toIndex - fromIndex));
    }
}
