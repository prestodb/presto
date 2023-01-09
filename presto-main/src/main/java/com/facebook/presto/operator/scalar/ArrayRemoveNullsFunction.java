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
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;

@ScalarFunction("remove_nulls")
@Description("Removes null values from an array")
public final class ArrayRemoveNullsFunction
{
    @TypeParameter("E")
    public ArrayRemoveNullsFunction(@TypeParameter("E") Type elementType) {}

    @TypeParameter("E")
    @SqlType("array(E)")
    public Block remove(
            @TypeParameter("E") Type type,
            @SqlType("array(E)") Block array)
    {
        if (!array.mayHaveNull()) {
            return array;
        }

        int found = -1;
        for (int i = 0; i < array.getPositionCount(); i++) {
            if (array.isNull(i)) {
                found = i;
                break;
            }
        }

        if (found == -1) {
            // all elements are non-null
            return array;
        }

        BlockBuilder blockBuilder = type.createBlockBuilder(null, array.getPositionCount() - 1);

        // copy all elements up to found-1
        for (int i = 0; i < found; i++) {
            type.appendTo(array, i, blockBuilder);
        }

        // and then copy non-null elements from found+1 till the end of the array
        for (int i = found + 1; i < array.getPositionCount(); i++) {
            if (!array.isNull(i)) {
                type.appendTo(array, i, blockBuilder);
            }
        }

        return blockBuilder.build();
    }
}
