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
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.SqlVarbinary;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeParameter;
import com.facebook.presto.tdigest.TDigest;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.function.BiFunction;

import static com.facebook.presto.common.type.TDigestParametricType.TDIGEST;
import static com.facebook.presto.tdigest.TDigest.createTDigest;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.lang.Math.abs;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;

public class TestMergeTDigestFunction
        extends TestMergeStatisticalDigestFunction
{
    private static final double[] quantiles = {0.01, 0.05, 0.1, 0.25, 0.50, 0.75, 0.9, 0.95, 0.99};

    public static final BiFunction<Object, Object, Boolean> TDIGEST_EQUALITY = (actualBinary, expectedBinary) -> {
        if (actualBinary == null && expectedBinary == null) {
            return true;
        }
        requireNonNull(actualBinary, "actual value was null");
        requireNonNull(expectedBinary, "expected value was null");

        TDigest actual = createTDigest(wrappedBuffer(((SqlVarbinary) actualBinary).getBytes()));
        TDigest expected = createTDigest(wrappedBuffer(((SqlVarbinary) expectedBinary).getBytes()));

        for (double quantile : quantiles) {
            if (abs(actual.getQuantile(quantile) - expected.getQuantile(quantile)) > max(0.5, abs(actual.getQuantile(quantile) * 0.01))) {
                return false;
            }
        }

        assertEquals(actual.getSum(), expected.getSum(), 0.0001);

        return actual.getSize() == expected.getSize() &&
                actual.getMin() == expected.getMin() &&
                actual.getMax() == expected.getMax() &&
                actual.getCompressionFactor() == expected.getCompressionFactor();
    };

    @Override
    protected BiFunction<Object, Object, Boolean> getEquality()
    {
        return TDIGEST_EQUALITY;
    }

    @Override
    public Block[] getSequenceBlocks(int start, int length)
    {
        Type type = TDIGEST.createType(ImmutableList.of(TypeParameter.of(DoubleType.DOUBLE)));
        BlockBuilder blockBuilder = type.createBlockBuilder(null, length);
        for (int i = start; i < start + length; i++) {
            TDigest tdigest = createTDigest(100);
            tdigest.add(i);
            type.writeSlice(blockBuilder, tdigest.serialize());
        }
        return new Block[] {blockBuilder.build()};
    }

    @Override
    protected List<String> getFunctionParameterTypes()
    {
        return ImmutableList.of("tdigest(double)");
    }

    @Override
    public Object getExpectedValue(int start, int length)
    {
        if (length == 0) {
            return null;
        }

        TDigest tdigest = createTDigest(100);
        for (int i = start; i < start + length; i++) {
            tdigest.add(i);
        }
        return new SqlVarbinary(tdigest.serialize().getBytes());
    }
}
