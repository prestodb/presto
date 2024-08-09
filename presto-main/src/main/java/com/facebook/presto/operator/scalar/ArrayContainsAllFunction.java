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
import com.facebook.presto.operator.aggregation.TypedSet;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;

@ScalarFunction("contains_all")
@Description("Returns true if all elements of the second array are present in the first array")
public final class ArrayContainsAllFunction
{
    private ArrayContainsAllFunction() {}

    @TypeParameter("T")
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean containsAll(
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block firstArray,
            @SqlType("array(T)") Block secondArray)
    {
        TypedSet firstSet = new TypedSet(elementType, firstArray.getPositionCount(), "containsAll");
        for (int i = 0; i < firstArray.getPositionCount(); i++) {
            firstSet.add(firstArray, i);
        }

        for (int i = 0; i < secondArray.getPositionCount(); i++) {
            if (!firstSet.contains(secondArray, i)) {
                return false;
            }
        }
        return true;
    }
}
