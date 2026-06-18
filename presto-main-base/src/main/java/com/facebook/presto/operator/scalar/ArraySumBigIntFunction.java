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
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.OperatorDependency;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.common.function.OperatorType.ADD;
import static com.facebook.presto.util.Failures.internalError;

@Description("Returns the sum of all array elements, or 0 if the array is empty. Ignores null elements.")
@ScalarFunction(value = "array_sum")
public final class ArraySumBigIntFunction
{
    private ArraySumBigIntFunction() {}

    @SqlType("bigint")
    public static long arraySumBigInt(
            @OperatorDependency(operator = ADD, argumentTypes = {"bigint", "bigint"}) MethodHandle addFunction,
            @TypeParameter("bigint") Type elementType,
            @SqlType("array(bigint)") Block arrayBlock)
    {
        int positionCount = arrayBlock.getPositionCount();
        if (positionCount == 0) {
            return 0;
        }

        long sum = 0;
        for (int i = 0; i < positionCount; i++) {
            if (!arrayBlock.isNull(i)) {
                try {
                    sum = (long) addFunction.invoke(sum, arrayBlock.getLong(i));
                }
                catch (Throwable throwable) {
                    throw internalError(throwable);
                }
            }
        }
        return sum;
    }
}
