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
    private static final ValueAccessor DOUBLE_VALUE_ACCESSOR = new DoubleValueAccessor();
    private static final ValueAccessor REAL_VALUE_ACCESSOR = new RealValueAccessor();

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
        return normalizeArray(elementType, block, p, DOUBLE_VALUE_ACCESSOR);
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
        return normalizeArray(elementType, block, Float.intBitsToFloat((int) p), REAL_VALUE_ACCESSOR);
    }

    private static Block normalizeArray(Type elementType, Block block, double p, ValueAccessor valueAccessor)
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
            pNorm += Math.pow(Math.abs(valueAccessor.getValue(elementType, block, i)), p);
        }
        if (pNorm == 0) {
            return block;
        }
        pNorm = Math.pow(pNorm, 1.0 / p);
        BlockBuilder blockBuilder = elementType.createBlockBuilder(null, elementCount);
        for (int i = 0; i < elementCount; i++) {
            valueAccessor.writeValue(elementType, blockBuilder, valueAccessor.getValue(elementType, block, i) / pNorm);
        }
        return blockBuilder.build();
    }

    private interface ValueAccessor
    {
        double getValue(Type elementType, Block block, int position);

        void writeValue(Type elementType, BlockBuilder blockBuilder, double value);
    }

    private static class DoubleValueAccessor
            implements ValueAccessor
    {
        @Override
        public double getValue(Type elementType, Block block, int position)
        {
            return elementType.getDouble(block, position);
        }

        @Override
        public void writeValue(Type elementType, BlockBuilder blockBuilder, double value)
        {
            elementType.writeDouble(blockBuilder, value);
        }
    }

    private static class RealValueAccessor
            implements ValueAccessor
    {
        @Override
        public double getValue(Type elementType, Block block, int position)
        {
            return Float.intBitsToFloat((int) elementType.getLong(block, position));
        }

        @Override
        public void writeValue(Type elementType, BlockBuilder blockBuilder, double value)
        {
            elementType.writeLong(blockBuilder, Float.floatToIntBits((float) value));
        }
    }
}
