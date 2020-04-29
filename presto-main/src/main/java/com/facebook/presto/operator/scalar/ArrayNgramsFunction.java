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

import com.facebook.presto.common.block.ArrayBlock;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;

import java.util.Optional;

import static com.facebook.presto.common.type.StandardTypes.INTEGER;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.util.Failures.checkCondition;
import static java.lang.Math.min;
import static java.lang.StrictMath.toIntExact;

@ScalarFunction("ngrams")
@Description("Return N-grams for the input")
public final class ArrayNgramsFunction
{
    private ArrayNgramsFunction() {}

    @TypeParameter("T")
    @SqlType("array(array(T))")
    public static Block ngrams(@SqlType("array(T)") Block array, @SqlType(INTEGER) long n)
    {
        checkCondition(n > 0, INVALID_FUNCTION_ARGUMENT, "N must be positive");

        // n should not be larger than the array length
        int elementsPerRecord = toIntExact(min(array.getPositionCount(), n));
        int totalRecords = array.getPositionCount() - elementsPerRecord + 1;
        int[] ids = new int[totalRecords * elementsPerRecord];
        int[] offset = new int[totalRecords + 1];
        for (int recordIndex = 0; recordIndex < totalRecords; recordIndex++) {
            for (int elementIndex = 0; elementIndex < elementsPerRecord; elementIndex++) {
                ids[recordIndex * elementsPerRecord + elementIndex] = recordIndex + elementIndex;
            }
            offset[recordIndex + 1] = (recordIndex + 1) * elementsPerRecord;
        }

        return ArrayBlock.fromElementBlock(totalRecords, Optional.empty(), offset, array.getPositions(ids, 0, totalRecords * elementsPerRecord));
    }
}
