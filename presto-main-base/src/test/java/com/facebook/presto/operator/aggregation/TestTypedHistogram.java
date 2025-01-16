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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.operator.aggregation.histogram.SingleTypedHistogram;
import com.facebook.presto.operator.aggregation.histogram.TypedHistogram;
import org.testng.annotations.Test;

import java.util.function.IntUnaryOperator;
import java.util.stream.IntStream;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.util.StructuralTestUtil.mapType;
import static org.testng.Assert.assertEquals;

public class TestTypedHistogram
{
    @Test
    public void testMassive()
    {
        BlockBuilder inputBlockBuilder = BIGINT.createBlockBuilder(null, 5000);

        TypedHistogram typedHistogram = new SingleTypedHistogram(BIGINT, 1000);
        IntStream.range(1, 2000)
                .flatMap(i -> IntStream.iterate(i, IntUnaryOperator.identity()).limit(i))
                .forEach(j -> BIGINT.writeLong(inputBlockBuilder, j));

        Block inputBlock = inputBlockBuilder.build();
        for (int i = 0; i < inputBlock.getPositionCount(); i++) {
            typedHistogram.add(i, inputBlock, 1);
        }

        MapType mapType = mapType(BIGINT, BIGINT);
        BlockBuilder out = mapType.createBlockBuilder(null, 1);
        typedHistogram.serialize(out);
        Block outputBlock = mapType.getObject(out, 0);
        for (int i = 0; i < outputBlock.getPositionCount(); i += 2) {
            assertEquals(BIGINT.getLong(outputBlock, i + 1), BIGINT.getLong(outputBlock, i));
        }
    }
}
