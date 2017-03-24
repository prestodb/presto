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
package com.facebook.presto.type;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.SqlDecimal;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

import static com.facebook.presto.spi.type.Decimals.encodeScaledValue;
import static com.facebook.presto.spi.type.Decimals.encodeUnscaledValue;
import static com.facebook.presto.spi.type.Decimals.writeBigDecimal;
import static com.facebook.presto.spi.type.UnscaledDecimal128Arithmetic.unscaledDecimalToBigInteger;
import static org.testng.Assert.assertEquals;

public class TestLongDecimalType
        extends AbstractTestType
{
    private static final DecimalType LONG_DECIMAL_TYPE = DecimalType.createDecimalType(30, 10);

    public TestLongDecimalType()
    {
        super(LONG_DECIMAL_TYPE, SqlDecimal.class, createTestBlock());
    }

    @Test
    public void testUnscaledValueToSlice()
    {
        assertEquals(encodeUnscaledValue(0L), encodeUnscaledValue(BigInteger.valueOf(0L)));
        assertEquals(encodeUnscaledValue(1L), encodeUnscaledValue(BigInteger.valueOf(1L)));
        assertEquals(encodeUnscaledValue(-1L), encodeUnscaledValue(BigInteger.valueOf(-1L)));
        assertEquals(encodeUnscaledValue(Long.MAX_VALUE), encodeUnscaledValue(BigInteger.valueOf(Long.MAX_VALUE)));
        assertEquals(encodeUnscaledValue(Long.MIN_VALUE), encodeUnscaledValue(BigInteger.valueOf(Long.MIN_VALUE)));
    }

    public static Block createTestBlock()
    {
        BlockBuilder blockBuilder = LONG_DECIMAL_TYPE.createBlockBuilder(new BlockBuilderStatus(), 15);
        writeBigDecimal(LONG_DECIMAL_TYPE, blockBuilder, new BigDecimal("-12345678901234567890.1234567890"));
        writeBigDecimal(LONG_DECIMAL_TYPE, blockBuilder, new BigDecimal("-12345678901234567890.1234567890"));
        writeBigDecimal(LONG_DECIMAL_TYPE, blockBuilder, new BigDecimal("-12345678901234567890.1234567890"));
        writeBigDecimal(LONG_DECIMAL_TYPE, blockBuilder, new BigDecimal("22345678901234567890.1234567890"));
        writeBigDecimal(LONG_DECIMAL_TYPE, blockBuilder, new BigDecimal("22345678901234567890.1234567890"));
        writeBigDecimal(LONG_DECIMAL_TYPE, blockBuilder, new BigDecimal("22345678901234567890.1234567890"));
        writeBigDecimal(LONG_DECIMAL_TYPE, blockBuilder, new BigDecimal("22345678901234567890.1234567890"));
        writeBigDecimal(LONG_DECIMAL_TYPE, blockBuilder, new BigDecimal("22345678901234567890.1234567890"));
        writeBigDecimal(LONG_DECIMAL_TYPE, blockBuilder, new BigDecimal("32345678901234567890.1234567890"));
        writeBigDecimal(LONG_DECIMAL_TYPE, blockBuilder, new BigDecimal("32345678901234567890.1234567890"));
        writeBigDecimal(LONG_DECIMAL_TYPE, blockBuilder, new BigDecimal("42345678901234567890.1234567890"));
        return blockBuilder.build();
    }

    @Override
    protected Object getNonNullValue()
    {
        return Slices.wrappedBuffer(new byte[16]);
    }

    @Override
    protected Object getGreaterValue(Object value)
    {
        Slice slice = (Slice) value;
        BigDecimal decimal = toBigDecimal(slice, 10);
        BigDecimal greaterDecimal = decimal.add(BigDecimal.ONE);
        return encodeScaledValue(greaterDecimal);
    }

    private static BigDecimal toBigDecimal(Slice valueSlice, int scale)
    {
        return new BigDecimal(unscaledDecimalToBigInteger(valueSlice), scale);
    }
}
