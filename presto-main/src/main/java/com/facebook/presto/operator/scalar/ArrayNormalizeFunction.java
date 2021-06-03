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
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.spi.function.TypeParameterSpecialization;

import static com.facebook.presto.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_MISSING;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.util.Failures.checkCondition;
import static java.lang.String.format;

@ScalarFunction("array_normalize")
@Description("Normalizes an array by dividing each element by the p-norm of the array.")
public final class ArrayNormalizeFunction
{
    private ArrayNormalizeFunction() {}

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = double.class)
    @SqlType("array(T)")
    @SqlNullable
    public static Block normalizeDoubleArray(
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block block,
            @SqlType("T") double p)
    {
        return normalizeArray(elementType, block, p);
    }

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = long.class)
    @SqlType("array(T)")
    @SqlNullable
    public static Block normalizeRealArray(
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block block,
            @SqlType("T") long p)
    {
        return normalizeArray(elementType, block, Float.intBitsToFloat((int) p));
    }

    private static Block normalizeArray(Type elementType, Block block, double p)
    {
        if (!(elementType instanceof RealType) && !(elementType instanceof DoubleType)) {
            throw new PrestoException(
                    FUNCTION_IMPLEMENTATION_MISSING,
                    format("Unsupported array element type for array_normalize function: %s", elementType.getDisplayName()));
        }
        checkCondition(p >= 0, INVALID_FUNCTION_ARGUMENT, "array_normalize only supports non-negative p: %s", p);

        if (p == 0) {
            return block;
        }

        int elementCount = block.getPositionCount();
        double pNorm = 0;
        for (int i = 0; i < elementCount; i++) {
            if (block.isNull(i)) {
                return null;
            }
            pNorm += Math.pow(Math.abs(((Number) elementType.getObject(block, i)).doubleValue()), p);
        }
        if (pNorm == 0) {
            return block;
        }
        pNorm = Math.pow(pNorm, 1.0 / p);
        BlockBuilder blockBuilder = elementType.createBlockBuilder(null, elementCount);
        for (int i = 0; i < elementCount; i++) {
            elementType.writeObject(blockBuilder, ((Number) elementType.getObject(block, i)).doubleValue() / pNorm);
        }
        return blockBuilder.build();
    }
}
