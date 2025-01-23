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
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.OperatorDependency;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.common.function.OperatorType.ADD;
import static com.facebook.presto.common.type.TypeUtils.readNativeValue;
import static com.facebook.presto.common.type.TypeUtils.writeNativeValue;
import static com.facebook.presto.util.Failures.internalError;

@Description("Get the cumulative sum array for the input array")
@ScalarFunction(value = "array_cum_sum", deterministic = true)
public final class ArrayCumSum
{
    private ArrayCumSum() {}

    @TypeParameter("T")
    @SqlType("array(T)")
    public static Block sum(
            @OperatorDependency(operator = ADD, argumentTypes = {"T", "T"}) MethodHandle addFunction,
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block arrayBlock)
    {
        int positionCount = arrayBlock.getPositionCount();
        BlockBuilder resultBuilder = elementType.createBlockBuilder(null, positionCount);
        if (positionCount == 0) {
            return resultBuilder.build();
        }

        if (arrayBlock.isNull(0)) {
            return RunLengthEncodedBlock.create(elementType, null, positionCount);
        }

        elementType.appendTo(arrayBlock, 0, resultBuilder);
        if (arrayBlock.mayHaveNull()) {
            int pos = 1;
            for (; pos < positionCount; ++pos) {
                if (arrayBlock.isNull(pos)) {
                    break;
                }
                writeSum(elementType, resultBuilder, addFunction, pos, arrayBlock);
            }
            for (; pos < positionCount; ++pos) {
                resultBuilder.appendNull();
            }
        }
        else {
            for (int pos = 1; pos < positionCount; ++pos) {
                writeSum(elementType, resultBuilder, addFunction, pos, arrayBlock);
            }
        }
        return resultBuilder.build();
    }

    private static void writeSum(Type elementType, BlockBuilder resultBuilder, MethodHandle addFunction, int pos, Block arrayBlock)
    {
        try {
            writeNativeValue(elementType, resultBuilder, addFunction.invoke(readNativeValue(elementType, resultBuilder, pos - 1), readNativeValue(elementType, arrayBlock, pos)));
        }
        catch (Throwable throwable) {
            throw internalError(throwable);
        }
    }
}
