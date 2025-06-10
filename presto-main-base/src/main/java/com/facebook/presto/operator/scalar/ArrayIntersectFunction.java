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
import com.facebook.presto.spi.function.OperatorDependency;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.common.function.OperatorType.IS_DISTINCT_FROM;

public final class ArrayIntersectFunction
{
    private ArrayIntersectFunction() {}

    @ScalarFunction("array_intersect")
    @Description("Intersects elements of the two given arrays")
    @TypeParameter("E")
    @SqlType("array(E)")
    public static Block intersect(
            @TypeParameter("E") Type type,
            @OperatorDependency(operator = IS_DISTINCT_FROM, argumentTypes = {"E", "E"}) MethodHandle elementIsDistinctFrom,
            @SqlType("array(E)") Block leftArray,
            @SqlType("array(E)") Block rightArray)
    {
        if (leftArray.getPositionCount() < rightArray.getPositionCount()) {
            Block tempArray = leftArray;
            leftArray = rightArray;
            rightArray = tempArray;
        }

        int rightPositionCount = rightArray.getPositionCount();

        if (rightPositionCount == 0) {
            return rightArray;
        }

        OptimizedTypedSet typedSet = new OptimizedTypedSet(type, elementIsDistinctFrom, rightPositionCount);
        typedSet.union(rightArray);
        typedSet.intersect(leftArray);

        return typedSet.getBlock();
    }
}
