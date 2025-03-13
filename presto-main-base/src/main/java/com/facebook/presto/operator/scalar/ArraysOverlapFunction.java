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

@ScalarFunction("arrays_overlap")
@Description("Returns true if arrays have common elements")
public final class ArraysOverlapFunction
{
    private static final int NESTED_LOOP_THRESHOLD = 200;

    private ArraysOverlapFunction() {}

    @TypeParameter("T")
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean arraysOverlap(
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block leftArray,
            @SqlType("array(T)") Block rightArray)
    {
        if (leftArray.getPositionCount() == 0 || rightArray.getPositionCount() == 0) {
            return false;
        }

        if (Math.max(rightArray.getPositionCount(), leftArray.getPositionCount()) >= NESTED_LOOP_THRESHOLD) {
            return arraysOverlapSetBased(elementType, leftArray, rightArray);
        }

        boolean hasNull = false;
        for (int i = 0; i < leftArray.getPositionCount(); i++) {
            if (leftArray.isNull(i)) {
                hasNull = true;
                continue;
            }

            for (int j = 0; j < rightArray.getPositionCount(); j++) {
                if (rightArray.isNull(j)) {
                    hasNull = true;
                    continue;
                }
                if (elementType.equalTo(leftArray, i, rightArray, j)) {
                    return true;
                }
            }
        }
        if (hasNull) {
            return null;
        }
        return false;
    }

    private static Boolean arraysOverlapSetBased(Type type,
            Block leftArray,
            Block rightArray)
    {
        int leftPositionCount = leftArray.getPositionCount();
        int rightPositionCount = rightArray.getPositionCount();

        if (leftPositionCount == 0 || rightPositionCount == 0) {
            return false;
        }

        Block lookArray = leftArray;
        Block itrArray = rightArray;
        if (leftPositionCount > rightPositionCount) {
            lookArray = rightArray;
            itrArray = leftArray;
        }

        boolean itrArrHasNull = false;
        TypedSet typedSet = new TypedSet(type, lookArray.getPositionCount(), "arraysOverlap");
        for (int i = 0; i < lookArray.getPositionCount(); i++) {
            typedSet.add(lookArray, i);
        }

        for (int i = 0; i < itrArray.getPositionCount(); i++) {
            if (itrArray.isNull(i)) {
                itrArrHasNull = true;
                continue;
            }
            if (typedSet.contains(itrArray, i)) {
                return true;
            }
        }
        if (itrArrHasNull || typedSet.isContainNullElements()) {
            return null;
        }
        return false;
    }
}
