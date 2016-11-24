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

import static org.testng.Assert.assertEquals;

public class TestBigIntegerStateSerializer
{
    @Test
    public void testRoundTrip()
            throws Exception
    {
        testRoundTrip(ImmutableList.of(
                BigInteger.ONE,
                BigInteger.TEN,
                BigInteger.ZERO,
                BigInteger.valueOf(Long.MAX_VALUE),
                BigInteger.valueOf(Long.MIN_VALUE),
                BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.valueOf(Long.MAX_VALUE)),
                BigInteger.valueOf(Long.MIN_VALUE).multiply(BigInteger.valueOf(Long.MAX_VALUE)),
                BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.valueOf(Long.MAX_VALUE)).multiply(BigInteger.valueOf(Long.MAX_VALUE)),
                BigInteger.valueOf(Long.MIN_VALUE).multiply(BigInteger.valueOf(Long.MAX_VALUE)).multiply(BigInteger.valueOf(Long.MAX_VALUE))
        ));
    }

    private static void testRoundTrip(List<BigInteger> expected)
    {
        BigIntegerStateSerializer serializer = new BigIntegerStateSerializer();
        BlockBuilder blockBuilder = new VariableWidthBlockBuilder(new BlockBuilderStatus(), expected.size(), 2);
        BigIntegerState state = new BigIntegerStateFactory.SingleBigIntegerState();
        for (BigInteger bigInteger : expected) {
            state.setBigInteger(bigInteger);
            serializer.serialize(state, blockBuilder);
        }
        Block block = blockBuilder.build();
        List<BigInteger> actual = new ArrayList<>(expected.size());
        for (int i = 0; i < expected.size(); i++) {
            serializer.deserialize(block, i, state);
            actual.add(state.getBigInteger());
        }
        assertEquals(actual, expected);
    }
}
