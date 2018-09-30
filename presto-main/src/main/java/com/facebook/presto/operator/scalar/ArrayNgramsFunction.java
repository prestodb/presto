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

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.util.Failures.checkCondition;

@ScalarFunction("ngrams")
@Description("Return N-grams for the input")
public final class ArrayNgramsFunction
{
    private ArrayNgramsFunction() {}

    @TypeParameter("T")
    @SqlType("array(array(T))")
    public static Block ngrams(@TypeParameter("T") Type type, @SqlType("array(E)") Block array, @SqlType(StandardTypes.BIGINT) long n)
    {
        checkCondition(n > 0, INVALID_FUNCTION_ARGUMENT, "N must be positive");
        checkCondition(n <= Integer.MAX_VALUE, INVALID_FUNCTION_ARGUMENT, "N is too large");

        // n should not be larger than the array length
        n = n > array.getPositionCount() ? array.getPositionCount() : n;
        ArrayType arrayType = new ArrayType(type);
        BlockBuilder parts = arrayType.createBlockBuilder(null, array.getPositionCount() - (int) n + 1);

        for (int i = 0; i <= array.getPositionCount() - n; i++) {
            BlockBuilder blockBuilder = type.createBlockBuilder(null, (int) n);
            for (int j = i; j < i + n; j++) {
                type.appendTo(array, j, blockBuilder);
            }
            arrayType.writeObject(parts, blockBuilder.build());
        }
        return parts.build();
    }
}
