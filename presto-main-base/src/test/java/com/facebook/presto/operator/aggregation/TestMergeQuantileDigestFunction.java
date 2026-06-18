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

import com.facebook.airlift.stats.QuantileDigest;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.SqlVarbinary;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeParameter;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.function.BiFunction;

import static com.facebook.presto.common.type.QuantileDigestParametricType.QDIGEST;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.util.Objects.requireNonNull;

public class TestMergeQuantileDigestFunction
        extends TestMergeStatisticalDigestFunction
{
    public static final BiFunction<Object, Object, Boolean> QDIGEST_EQUALITY = (actualBinary, expectedBinary) -> {
        if (actualBinary == null && expectedBinary == null) {
            return true;
        }
        requireNonNull(actualBinary, "actual value was null");
        requireNonNull(expectedBinary, "expected value was null");

        QuantileDigest actual = new QuantileDigest(wrappedBuffer(((SqlVarbinary) actualBinary).getBytes()));
        QuantileDigest expected = new QuantileDigest(wrappedBuffer(((SqlVarbinary) expectedBinary).getBytes()));
        return actual.getCount() == expected.getCount() &&
                actual.getMin() == expected.getMin() &&
                actual.getMax() == expected.getMax() &&
                actual.getAlpha() == expected.getAlpha() &&
                actual.getMaxError() == expected.getMaxError();
    };

    @Override
    protected BiFunction<Object, Object, Boolean> getEquality()
    {
        return QDIGEST_EQUALITY;
    }

    @Override
    public Block[] getSequenceBlocks(int start, int length)
    {
        Type type = QDIGEST.createType(ImmutableList.of(TypeParameter.of(DoubleType.DOUBLE)));
        BlockBuilder blockBuilder = type.createBlockBuilder(null, length);
        for (int i = start; i < start + length; i++) {
            QuantileDigest qdigest = new QuantileDigest(0.0);
            qdigest.add(i);
            type.writeSlice(blockBuilder, qdigest.serialize());
        }
        return new Block[] {blockBuilder.build()};
    }

    @Override
    protected List<String> getFunctionParameterTypes()
    {
        return ImmutableList.of("qdigest(double)");
    }

    @Override
    public Object getExpectedValue(int start, int length)
    {
        if (length == 0) {
            return null;
        }

        QuantileDigest qdigest = new QuantileDigest(0.00);
        for (int i = start; i < start + length; i++) {
            qdigest.add(i);
        }
        return new SqlVarbinary(qdigest.serialize().getBytes());
    }
}
