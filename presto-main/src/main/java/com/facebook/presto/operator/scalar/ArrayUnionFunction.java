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
import com.facebook.presto.operator.aggregation.OptimizedTypedSet;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;

@ScalarFunction("array_union")
@Description("Union elements of the two given arrays")
public final class ArrayUnionFunction
{
    private ArrayUnionFunction() {}

    @TypeParameter("E")
    @SqlType("array(E)")
    public static Block union(
            @TypeParameter("E") Type type,
            @SqlType("array(E)") Block leftArray,
            @SqlType("array(E)") Block rightArray)
    {
        int leftArrayCount = leftArray.getPositionCount();
        int rightArrayCount = rightArray.getPositionCount();
        OptimizedTypedSet typedSet = new OptimizedTypedSet(type, leftArrayCount + rightArrayCount);

        typedSet.union(leftArray);
        typedSet.union(rightArray);

        return typedSet.getBlock();
    }

    // TODO: Specialize for bigint using LongOpenHashSet
}
