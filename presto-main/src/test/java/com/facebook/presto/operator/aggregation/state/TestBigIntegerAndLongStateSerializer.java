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
package com.facebook.presto.operator.aggregation.state;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.VariableWidthBlockBuilder;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static org.testng.Assert.assertEquals;

public class TestBigIntegerAndLongStateSerializer
{
    @Test
    public void testRoundTrip()
            throws Exception
    {
        testRoundTrip(ImmutableList.of(
                bigIntegerAndLong(BigInteger.ONE, 1L),
                bigIntegerAndLong(BigInteger.TEN, 10L),
                bigIntegerAndLong(BigInteger.ZERO, 0L),
                bigIntegerAndLong(BigInteger.valueOf(Long.MAX_VALUE), Integer.MAX_VALUE),
                bigIntegerAndLong(BigInteger.valueOf(Long.MIN_VALUE), Integer.MIN_VALUE),
                bigIntegerAndLong(BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.valueOf(Long.MAX_VALUE)), Long.MAX_VALUE),
                bigIntegerAndLong(BigInteger.valueOf(Long.MIN_VALUE).multiply(BigInteger.valueOf(Long.MAX_VALUE)), Long.MIN_VALUE),
                bigIntegerAndLong(BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.valueOf(Long.MAX_VALUE)).multiply(BigInteger.valueOf(Long.MAX_VALUE)), Long.MAX_VALUE),
                bigIntegerAndLong(BigInteger.valueOf(Long.MIN_VALUE).multiply(BigInteger.valueOf(Long.MAX_VALUE)).multiply(BigInteger.valueOf(Long.MAX_VALUE)), Long.MIN_VALUE)
        ));
    }

    public static void testRoundTrip(List<BigIntegerAndLong> expected)
            throws Exception
    {
        BigIntegerAndLongStateSerializer serializer = new BigIntegerAndLongStateSerializer();
        BigIntegerAndLongState state = new BigIntegerAndLongStateFactory.SingleBigIntegerAndLongState();
        BlockBuilder blockBuilder = new VariableWidthBlockBuilder(new BlockBuilderStatus(), expected.size(), 3);
        for (BigIntegerAndLong bigIntegerAndLong : expected) {
            state.setBigInteger(bigIntegerAndLong.getBigInteger());
            state.setLong(bigIntegerAndLong.getLong());
            serializer.serialize(state, blockBuilder);
        }
        Block block = blockBuilder.build();
        List<BigIntegerAndLong> actual = new ArrayList<>();
        for (int i = 0; i < expected.size(); i++) {
            serializer.deserialize(block, i, state);
            actual.add(bigIntegerAndLong(state.getBigInteger(), state.getLong()));
        }
        assertEquals(actual, expected);
    }

    private static BigIntegerAndLong bigIntegerAndLong(BigInteger bigIntegerValue, long longValue)
    {
        return new BigIntegerAndLong(bigIntegerValue, longValue);
    }

    private static class BigIntegerAndLong
    {
        private final BigInteger bigIntegerValue;
        private final long longValue;

        private BigIntegerAndLong(BigInteger bigIntegerValue, long longValue)
        {
            this.bigIntegerValue = bigIntegerValue;
            this.longValue = longValue;
        }

        public BigInteger getBigInteger()
        {
            return bigIntegerValue;
        }

        public long getLong()
        {
            return longValue;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            BigIntegerAndLong that = (BigIntegerAndLong) o;

            return Objects.equals(this.bigIntegerValue, that.bigIntegerValue) &&
                    Objects.equals(this.longValue, that.longValue);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(bigIntegerValue, longValue);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("bigIntegerValue", bigIntegerValue)
                    .add("longValue", longValue)
                    .toString();
        }
    }
}
