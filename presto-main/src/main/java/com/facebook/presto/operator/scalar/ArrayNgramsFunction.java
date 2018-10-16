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

import com.facebook.presto.spi.block.ArrayBlock;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.spi.type.StandardTypes;

import java.util.Optional;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.util.Failures.checkCondition;
import static java.lang.Math.min;
import static java.lang.StrictMath.toIntExact;

@ScalarFunction("ngrams")
@Description("Return N-grams for the input")
public final class ArrayNgramsFunction
{
    private ArrayNgramsFunction(){}

    @TypeParameter("T")
    @SqlType("array(array(T))")
    public static Block ngrams(@SqlType("array(T)") Block array, @SqlType(StandardTypes.BIGINT) long n)
    {
        checkCondition(n > 0, INVALID_FUNCTION_ARGUMENT, "N must be positive");
        checkCondition(n <= Integer.MAX_VALUE, INVALID_FUNCTION_ARGUMENT, "N is too large");

        // n should not be larger than the array length
        int numOfRecords = toIntExact(min(array.getPositionCount(), n));
        int totalRecords = array.getPositionCount() - numOfRecords + 1;
        int[] ids = new int[totalRecords * numOfRecords];
        int[] offset = new int[totalRecords + 1];
        for (int i = 0; i < totalRecords; i++) {
            for (int j = 0; j < numOfRecords; j++) {
                ids[i * numOfRecords + j] = i + j;
            }
            offset[i + 1] = (i + 1) * numOfRecords;
        }

        return ArrayBlock.fromElementBlock(totalRecords, Optional.empty(), offset, array.getPositions(ids, 0, totalRecords * numOfRecords));
    }
}
